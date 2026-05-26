"""Negative tests for illegal_transition_flag in gold_lifecycle_updater (B-040).

These tests exercise _apply_event() directly — no DB inserts, no transactions.
They prove the detector fires for genuine business-domain violations and does NOT
fire for clean sequences or cross-batch processing artifacts.

Run with:
    python -m pytest tests/test_gold_lifecycle_flag.py -v
"""
from datetime import datetime, timezone

import pytest

from scripts.gold_lifecycle_updater import _apply_event, _empty_row, _LIFECYCLE_SORT_KEY


# ── Helpers ────────────────────────────────────────────────────────────────────

_BID = "00000000-0000-0000-0000-000000000001"

T0 = datetime(2025, 1,  1, 10, 0, tzinfo=timezone.utc)  # booking
T1 = datetime(2025, 1,  5, 12, 0, tzinfo=timezone.utc)  # checkin
T2 = datetime(2025, 1, 10, 12, 0, tzinfo=timezone.utc)  # checkout (valid)
T_EARLY = datetime(2025, 1,  3,  0, 0, tzinfo=timezone.utc)  # before T1 (inversion)
T_CANCEL = datetime(2025, 1, 12,  0, 0, tzinfo=timezone.utc)


def _ev(event_type, ts, **extra):
    base = {
        "event_type":          event_type,
        "event_ts":            ts,
        "booking_id":          _BID,
        "customer_id":         None,
        "hotel_id":            None,
        "room_type_id":        None,
        "city":                None,
        "event_date":          None,
        "nights":              None,
        "num_guests":          None,
        "nightly_rate_inr":    None,
        "revenue_inr":         None,
        "booking_source":      None,
        "payment_mode":        None,
        "cancellation_reason": None,
        "source":              "test",
    }
    base.update(extra)
    return base


def _run(events):
    """Apply a sequence of events to a fresh lifecycle row, sorted by kind."""
    row = _empty_row(_BID)
    events.sort(key=lambda e: _LIFECYCLE_SORT_KEY.get(e["event_type"], 9))
    for ev in events:
        _apply_event(row, ev)
    return row


# ── Positive (flag MUST fire) ──────────────────────────────────────────────────


def test_checkout_timestamp_before_checkin_flags():
    """CHECKOUT whose event_ts predates CHECKIN is a timestamp inversion — must flag."""
    row = _run([
        _ev("BOOKING",  T0),
        _ev("CHECKIN",  T1),
        _ev("CHECKOUT", T_EARLY),   # T_EARLY < T1 — physically impossible
    ])
    assert row["illegal_transition_flag"] is True, (
        "CHECKOUT(ts=T_EARLY) before CHECKIN(ts=T1) must set illegal_transition_flag"
    )
    # Status still advances forward — we flag but never regress
    assert row["current_status"] == "COMPLETED"


def test_checkout_without_checkin_flags():
    """CHECKOUT with no preceding CHECKIN is an illegal sequence — must flag.

    Sequence: BOOKING → CHECKOUT (CHECKIN never emitted).
    _seeded=True after BOOKING, so this is a genuine skip, not a cross-batch artifact.
    """
    row = _run([
        _ev("BOOKING",  T0),
        _ev("CHECKOUT", T2),   # no CHECKIN event
    ])
    assert row["illegal_transition_flag"] is True, (
        "CHECKOUT with no CHECKIN must set illegal_transition_flag when _seeded=True"
    )


def test_cancellation_after_checkout_flags():
    """CANCELLATION after a completed CHECKOUT is mutually exclusive — must flag."""
    row = _run([
        _ev("BOOKING",      T0),
        _ev("CHECKIN",      T1),
        _ev("CHECKOUT",     T2),
        _ev("CANCELLATION", T_CANCEL),
    ])
    assert row["illegal_transition_flag"] is True, (
        "CANCELLATION after CHECKOUT must set illegal_transition_flag"
    )


# ── Negative (flag must NOT fire) ─────────────────────────────────────────────


def test_clean_booking_checkin_checkout_no_flag():
    """BOOKING → CHECKIN → CHECKOUT in correct timestamp order must NOT flag."""
    row = _run([
        _ev("BOOKING",  T0),
        _ev("CHECKIN",  T1),
        _ev("CHECKOUT", T2),
    ])
    assert row["illegal_transition_flag"] is False, (
        "Clean BOOKING→CHECKIN→CHECKOUT must not flag"
    )
    assert row["current_status"] == "COMPLETED"
    assert row["outcome"]        == "completed"


def test_clean_booking_cancellation_no_flag():
    """BOOKING → CANCELLATION (no CHECKIN) is a valid pre-arrival cancel — must NOT flag."""
    row = _run([
        _ev("BOOKING",      T0),
        _ev("CANCELLATION", T1),
    ])
    assert row["illegal_transition_flag"] is False, (
        "Pre-arrival cancellation must not flag"
    )
    assert row["current_status"] == "CANCELLED"


def test_cross_batch_artifact_checkout_before_booking_no_flag():
    """Cross-batch artifact: CHECKOUT arrives when _seeded=False (BOOKING not yet seen).

    Simulates the 7 history rows where the generator's bulk INSERT assigned a
    lower ingest_seq to CHECKOUT than to BOOKING.  When the updater processes
    the batch containing CHECKOUT, _seeded=False.  The flag must NOT fire —
    this is a processing-order artifact, not a business-domain violation.
    """
    row = _empty_row(_BID)
    # _seeded stays False — simulating a fresh row with no prior BOOKING seen
    assert row["_seeded"] is False
    _apply_event(row, _ev("CHECKOUT", T2))   # BOOKING hasn't arrived yet
    assert row["illegal_transition_flag"] is False, (
        "CHECKOUT before BOOKING in ingest order must not flag (_seeded=False guard)"
    )
