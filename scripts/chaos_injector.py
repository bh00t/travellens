"""
Chaos injection — shared across producers (B-047 extraction).

Extracted verbatim from scripts/kafka_event_producer.py so the forward
generator (B-047) and the prior calendar-replay producer can share one
implementation.  Behaviour and reason vocabulary are unchanged — the
consumer's quarantine reason strings still appear in S3 keys and team
triage workflows, so any change here is a wire-contract change.

Three generators:
  - five MALFORMED corruptors (one required-field-missing, one bad type,
    one bad city, one unparseable ts, one unparseable JSON bytes);
  - one LATE shifter that backdates event_ts past the consumer watermark
    grace;
  - one dispatcher (maybe_corrupt_or_delay) that decides what to do based
    on CLI/env percentages.

The dispatcher returns (payload, mode):
  - payload is either a dict (normal / late / most malformed) or bytes
    (unparseable_json — the consumer's Gate 1 catches this);
  - mode is one of "normal", "late", or "malformed:<reason>".  Callers
    bump a counter on mode and pass payload to producer.send().

Randomness is the global random module; callers seed it (typically from
--chaos-seed / CHAOS_SEED) for reproducible chaos.
"""

import random
from datetime import datetime, timedelta, timezone


# ── Malformed generators ──────────────────────────────────────────────────────

def _corrupt_missing_field(event):
    """Remove one required field at random (Gate 2: missing_field)."""
    field = random.choice(["event_type", "event_ts", "city", "hotel_id"])
    bad = dict(event)
    bad.pop(field, None)
    return bad, "missing_field"


def _corrupt_unknown_event_type(event):
    """Replace event_type with a string outside VALID_EVENT_TYPES (Gate 2)."""
    bad = dict(event)
    bad["event_type"] = random.choice(["book", "BOOK", "RESERVED", "checkout"])
    return bad, "unknown_event_type"


def _corrupt_unknown_city(event):
    """Replace city with a typo / wrong-language / fictional name (Gate 2)."""
    bad = dict(event)
    # Last entry is the Hindi for "Goa" — written as a Python string so the
    # parse check (cp1252 on Windows) doesn't choke.
    bad["city"] = random.choice(["Mumbay", "DELHI_TYPO", "Atlantis", "गोवा"])
    return bad, "unknown_city"


def _corrupt_unparseable_ts(event):
    """Make event_ts a string that isn't ISO 8601 (Gate 2)."""
    bad = dict(event)
    bad["event_ts"] = random.choice(["yesterday", "2026/05/19", "1747590000"])
    return bad, "unparseable_event_ts"


def _corrupt_unparseable_json(event):
    """Return raw bytes that don't deserialize to JSON (Gate 1)."""
    return b'{"event_type": "BOOKING", "city": ', "unparseable_json"


_MALFORMED_GENERATORS = [
    _corrupt_missing_field,
    _corrupt_unknown_event_type,
    _corrupt_unknown_city,
    _corrupt_unparseable_ts,
    _corrupt_unparseable_json,
]


# ── Late event shifter ────────────────────────────────────────────────────────

def _make_late(event, min_minutes_late=70, max_minutes_late=180):
    """
    Shift event_ts back into the past far enough that the consumer's
    late-event guard (Gate 4) fires.  Defaults target a 60-minute window
    + 5-minute grace.
    """
    bad = dict(event)
    try:
        ts = datetime.fromisoformat(bad["event_ts"].replace("Z", "+00:00"))
    except Exception:
        ts = datetime.now(timezone.utc)
    minutes_back = random.uniform(min_minutes_late, max_minutes_late)
    bad["event_ts"] = (ts - timedelta(minutes=minutes_back)).isoformat()
    return bad


# ── Dispatcher ────────────────────────────────────────────────────────────────

def maybe_corrupt_or_delay(event, malformed_pct, late_pct):
    """
    Decide what to do with this event.  Returns (payload, mode).

      r < malformed_pct                      → run one of the 5 malformed
                                                generators
      malformed_pct <= r < malformed+late    → backdate event_ts past the
                                                watermark
      otherwise                              → pass-through ("normal")

    Caller bumps a counter on `mode` and sends `payload` to Kafka.
    """
    r = random.random() * 100.0
    if r < malformed_pct:
        gen = random.choice(_MALFORMED_GENERATORS)
        payload, reason = gen(event)
        return payload, f"malformed:{reason}"
    if r < malformed_pct + late_pct:
        return _make_late(event), "late"
    return event, "normal"
