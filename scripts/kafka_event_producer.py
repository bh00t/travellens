"""
Event simulator — publishes booking events to Kafka at a configurable rate.

This producer is the upstream half of the streaming pipeline. It reads a
seed file of hotel records (created in Phase 1), generates synthetic
event-stream data (bookings, cancellations, check-ins, price changes) and
publishes them to a Kafka topic. The downstream consumer
(scripts/stream_consumer.py) reads from this topic, aggregates by city
into time windows, and dual-sinks to Postgres + S3.

────────────────────────────────────────────────────────────────────────
CHAOS INJECTION (new, env-driven)
────────────────────────────────────────────────────────────────────────
To exercise the consumer's quarantine and late-event paths, this producer
can deliberately corrupt or delay a configurable percentage of events.

Three env vars control it (all default to OFF — chaos is opt-in):

  CHAOS_MALFORMED_PCT=2  → 2% of events are corrupted in some way that
                           should trigger the consumer's validator and
                           land in s3://.../malformed_events/

  CHAOS_LATE_PCT=1       → 1% of events have their event_ts shifted far
                           enough into the past that they fall outside
                           the consumer's watermark + grace, and should
                           land in s3://.../late_events/

  CHAOS_SEED=42          → optional. Fixes random.seed for reproducible
                           chaos across runs. Without it, every run
                           produces a different distribution.

These two percentages are independent and ADDITIVE — if you set both to
5, 5% of events become malformed AND 5% become late, for 10% total chaos.
The remaining 90% are emitted normally.

Why chaos as a producer feature? Because the consumer's failure-handling
code is silent on the happy path. The only way to know whether the late
guard, validator, quarantine sinks, and counters actually work is to
deliberately produce broken data and watch the consumer classify it.

────────────────────────────────────────────────────────────────────────
Usage
────────────────────────────────────────────────────────────────────────
  python scripts/kafka_event_producer.py [--rate N] [--duration S]
    --rate      events per second (default 50)
    --duration  run for N seconds then stop; 0 = run forever (default 0)

  # Normal run, no chaos:
  python scripts/kafka_event_producer.py --rate 50 --duration 300

  # 5% malformed, 2% late, reproducible:
  set CHAOS_MALFORMED_PCT=5
  set CHAOS_LATE_PCT=2
  set CHAOS_SEED=42
  python scripts/kafka_event_producer.py --rate 50 --duration 300
"""

import argparse
import json
import os
import random
import signal
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer

sys.stdout.reconfigure(encoding="utf-8")

load_dotenv()

# ── Paths & seed data ─────────────────────────────────────────────────────────

DATA_DIR  = Path(os.getenv("DATA_DIR", "./data"))
SEED_FILE = DATA_DIR / "booking_events_seed.json"

# ── Domain constants (unchanged from your original) ───────────────────────────
# Seasonality groups — used to scale booking volume by city + month so the
# synthetic stream looks realistic instead of uniform random noise.

GOA_CITIES      = {"Goa", "Anjuna", "Calangute", "Panjim"}
HILL_CITIES     = {"Manali", "Shimla", "Darjeeling", "Dalhousie",
                   "Kasauli", "Mussoorie", "Nainital", "Dharamshala"}
HERITAGE_CITIES = {"Jaipur", "Udaipur", "Agra", "Jaisalmer",
                   "Jodhpur", "Bikaner", "Pushkar", "Ajmer"}

# Event type mix — BOOKING dominates, with smaller tails for the others.
# The weights here are also the *expected* distribution the consumer will
# see (before chaos is applied).
EVENT_TYPES   = ["BOOKING", "CHECKIN", "CANCELLATION", "PRICE_CHANGE"]
EVENT_WEIGHTS = [0.65, 0.20, 0.10, 0.05]

BOOKING_SOURCES = ["MakeMyTrip", "Goibibo", "Booking.com", "Agoda",
                   "Direct", "OYO", "Airbnb"]

# ══════════════════════════════════════════════════════════════════════════════
# CHAOS INJECTION CONFIG
# ══════════════════════════════════════════════════════════════════════════════
# Read once at startup. Both default to 0.0 (no chaos) so this code is
# completely inert on a normal run — important, because we do NOT want
# accidental corruption in dev runs where someone forgot the env vars.

CHAOS_MALFORMED_PCT = float(os.getenv("CHAOS_MALFORMED_PCT", "0"))
CHAOS_LATE_PCT      = float(os.getenv("CHAOS_LATE_PCT",      "0"))
CHAOS_SEED          = os.getenv("CHAOS_SEED")

if CHAOS_SEED:
    # Reproducible chaos — same seed + same producer rate = same broken events.
    # Useful when debugging consumer behavior on a specific corruption pattern.
    random.seed(int(CHAOS_SEED))

# Sanity check — chaos rates above 100% are nonsensical; warn loudly.
if CHAOS_MALFORMED_PCT + CHAOS_LATE_PCT > 100:
    print(f"⚠ CHAOS config error: malformed ({CHAOS_MALFORMED_PCT}%) + late "
          f"({CHAOS_LATE_PCT}%) > 100%. Late will be ignored beyond the cap.",
          file=sys.stderr)


# ══════════════════════════════════════════════════════════════════════════════
# CHAOS GENERATORS — five corruption strategies, one per consumer reason code
# ══════════════════════════════════════════════════════════════════════════════
# Each generator takes a valid event dict and returns a (corrupted_payload,
# reason_string) tuple. The reason_string matches the consumer's validator
# vocabulary exactly — so when we inject `missing_field` here, the consumer
# should report `missing_field` in its run summary. Mismatches between these
# two sides indicate either a producer bug or a consumer bug.

def _corrupt_missing_field(event):
    """
    Remove one required field at random. The consumer's validator checks
    event_type, event_ts, city, hotel_id — drop any one of them and it
    should classify the event as 'missing_field'.
    """
    field = random.choice(["event_type", "event_ts", "city", "hotel_id"])
    bad = dict(event)        # shallow copy so we don't mutate the original
    bad.pop(field, None)     # silently ignore if the field is already gone
    return bad, "missing_field"


def _corrupt_unknown_event_type(event):
    """
    Replace event_type with a string outside the valid set
    {BOOKING, CANCELLATION, CHECKIN, PRICE_CHANGE}.
    """
    # NOTE: "" removed — the consumer's fail-fast validator catches empty
    # strings as missing_field before reaching the event_type allow-list
    # check, causing per-reason label divergence. The empty-string case is
    # already covered by _corrupt_missing_field.
    bad = dict(event)
    bad["event_type"] = random.choice(["book", "BOOK", "RESERVED", "checkout"])
    return bad, "unknown_event_type"


def _corrupt_unknown_city(event):
    """
    Replace city with a typo, a wrong-language name, or a fictional one.
    Tests the consumer's KNOWN_CITIES lookup (loaded from hotel_master).
    The Hindi 'गोवा' tests that the validator handles unicode correctly
    rather than crashing on non-ASCII input.
    """
    bad = dict(event)
    bad["city"] = random.choice(["Mumbay", "DELHI_TYPO", "Atlantis", "गोवा"])
    return bad, "unknown_city"


def _corrupt_unparseable_ts(event):
    """
    Make event_ts a string that isn't ISO 8601.
    """
    # NOTE: "" removed — empty event_ts is caught as missing_field by the
    # consumer's fail-fast required-field check before reaching the
    # timestamp-parse gate, causing per-reason label divergence. The
    # empty-string case is already covered by _corrupt_missing_field.
    bad = dict(event)
    bad["event_ts"] = random.choice(["yesterday", "2026/05/19", "1747590000"])
    return bad, "unparseable_event_ts"


def _corrupt_unparseable_json(event):
    """
    Return raw bytes that don't deserialize to JSON. This is the only
    generator that returns bytes instead of a dict — the producer's
    value_serializer is patched (below) to pass bytes through unchanged
    so they hit Kafka raw, and the consumer's deserializer should catch
    the json.loads exception and route to 'unparseable_json'.

    The byte sequence below is intentionally a truncated JSON fragment —
    it looks JSON-ish but is missing the closing braces.
    """
    return b'{"event_type": "BOOKING", "city": ', "unparseable_json"


# Registered generators. Picking from this list with random.choice gives
# a roughly uniform distribution across the 5 reason codes.
_MALFORMED_GENERATORS = [
    _corrupt_missing_field,
    _corrupt_unknown_event_type,
    _corrupt_unknown_city,
    _corrupt_unparseable_ts,
    _corrupt_unparseable_json,
]


# ══════════════════════════════════════════════════════════════════════════════
# LATE EVENT GENERATOR
# ══════════════════════════════════════════════════════════════════════════════

def _make_late(event, min_minutes_late=70, max_minutes_late=180):
    """
    Shift event_ts back into the past far enough that the consumer's
    late-event guard fires.

    The consumer guard is:
        if window_end_secs < (max_event_ts - WATERMARK_GRACE_SECONDS):
            quarantine as late

    With WINDOW_SIZE_MINUTES=60 and WATERMARK_GRACE_SECONDS=300:
      - A window for hour H closes at event-time (H + 1 hour + 5 min).
      - So an event with event_ts >= 65 minutes in the past relative to
        the current max event-time is late.
      - We default to 70-180 minutes back to give a comfortable margin.

    Edge case: if the original event_ts is unparseable (shouldn't happen
    on the late path because chaos branches are exclusive, but defensive
    just in case), fall back to wall-clock now() and shift from there.
    """
    bad = dict(event)
    try:
        ts = datetime.fromisoformat(bad["event_ts"].replace("Z", "+00:00"))
    except Exception:
        ts = datetime.now(timezone.utc)
    minutes_back = random.uniform(min_minutes_late, max_minutes_late)
    bad["event_ts"] = (ts - timedelta(minutes=minutes_back)).isoformat()
    return bad


# ══════════════════════════════════════════════════════════════════════════════
# CHAOS DISPATCHER
# ══════════════════════════════════════════════════════════════════════════════

def maybe_corrupt_or_delay(event):
    """
    Decide what to do with this event based on the chaos config.

    Returns a (payload, mode) tuple:
      - payload: dict (normal or late) OR bytes (unparseable_json case)
      - mode:    "normal" | "late" | "malformed:<reason>" — used for the
                 producer's own chaos accounting, separate from what
                 actually hits the wire.

    The decision uses a single uniform random draw in [0, 100):
      [0, MALFORMED_PCT)                    → malformed
      [MALFORMED_PCT, MALFORMED + LATE_PCT) → late
      [MALFORMED + LATE_PCT, 100)           → normal

    This gives independent, additive probabilities. With both pcts set to 0,
    every event takes the normal branch — production-safe by default.
    """
    r = random.random() * 100.0   # uniform draw in [0, 100)

    # Branch 1: corrupt the event
    if r < CHAOS_MALFORMED_PCT:
        generator = random.choice(_MALFORMED_GENERATORS)
        payload, reason = generator(event)
        # payload may be a dict (4 of 5 generators) or bytes (json one)
        return payload, f"malformed:{reason}"

    # Branch 2: delay the event into a closed window
    if r < CHAOS_MALFORMED_PCT + CHAOS_LATE_PCT:
        return _make_late(event), "late"

    # Branch 3: pass through unchanged
    return event, "normal"


# ══════════════════════════════════════════════════════════════════════════════
# DOMAIN HELPERS (unchanged from your original)
# ══════════════════════════════════════════════════════════════════════════════

def seasonal_multiplier(city, month):
    """
    Return a multiplier (relative to baseline) for booking volume in a
    given city + month. Captures the rough shape of Indian tourism
    seasonality so the synthetic stream has realistic peaks/troughs
    rather than uniform load.
    """
    if city in GOA_CITIES:
        if month in (12, 1, 2): return 3.0      # winter peak
        if month in (7, 8):      return 0.25    # monsoon trough
    elif city in HILL_CITIES:
        if month in (5, 6, 10):  return 2.5     # summer + post-monsoon
        if month in (12, 1):     return 1.6     # snow season
    elif city in HERITAGE_CITIES:
        if month in (10, 11, 2, 3): return 2.2  # cool-weather peak
        if month in (5, 6):         return 0.5  # summer trough
    return 1.0


def make_event(hotel, month):
    """
    Build one event dict from a hotel seed record. The event type is
    drawn from EVENT_TYPES with EVENT_WEIGHTS, and BOOKING / PRICE_CHANGE
    get additional fields filled in. event_ts is always set to wall-clock
    now (chaos may overwrite it later if late injection fires).
    """
    etype = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
    now_utc = datetime.now(timezone.utc).isoformat()

    event = {
        "event_id":   str(uuid.uuid4()),
        "event_type": etype,
        "hotel_id":   hotel["hotel_id"],
        "city":       hotel["city"],
        "event_ts":   now_utc,
    }

    if etype == "BOOKING":
        nights  = random.randint(1, 7)
        base    = hotel.get("base_daily_bookings", 100) * seasonal_multiplier(hotel["city"], month)
        nightly = int(base * random.uniform(0.8, 1.4))
        event.update({
            "room_type_id":   str(uuid.uuid4()),
            "revenue_inr":    nightly * nights,
            "nights":         nights,
            "booking_source": random.choice(BOOKING_SOURCES),
        })
    elif etype == "PRICE_CHANGE":
        base_price = hotel.get("base_daily_bookings", 2000)
        old_price  = int(base_price * random.uniform(0.8, 1.0))
        new_price  = int(old_price  * random.uniform(0.9, 1.2))
        event.update({
            "room_type_id":  str(uuid.uuid4()),
            "old_price_inr": old_price,
            "new_price_inr": new_price,
        })

    return event


# ══════════════════════════════════════════════════════════════════════════════
# MAIN PRODUCER LOOP
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="TravelLens Kafka event producer")
    parser.add_argument("--rate",     type=float, default=50.0,
                        help="Events per second")
    parser.add_argument("--duration", type=float, default=0.0,
                        help="Run seconds (0=forever)")
    args = parser.parse_args()

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic     = os.getenv("KAFKA_TOPIC",     "booking-events")

    hotels = json.loads(SEED_FILE.read_text(encoding="utf-8"))
    print(f"Loaded {len(hotels)} hotels from seed file.")

    # ── KafkaProducer setup ─────────────────────────────────────────────────
    # The value_serializer normally JSON-encodes the dict. We need it to
    # PASS BYTES THROUGH UNCHANGED so the unparseable_json chaos generator
    # works — it produces raw broken bytes that must reach Kafka un-touched.
    # The `if isinstance(v, bytes)` check makes the serializer transparent
    # to bytes input while preserving JSON encoding for dicts.
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        linger_ms=20,
    )

    # ── Graceful shutdown ──────────────────────────────────────────────────
    running = True
    def _stop(sig, frame):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT,  _stop)
    signal.signal(signal.SIGTERM, _stop)

    # ── Run state ──────────────────────────────────────────────────────────
    interval   = 1.0 / args.rate          # target seconds per event
    total      = 0                         # events sent (any mode)
    t_start    = time.time()
    month      = datetime.now().month

    # Producer-side chaos accounting. After the run, these counts should
    # match what the consumer reports in its shutdown summary — that's the
    # whole correctness check.
    chaos_counts = defaultdict(int)

    # ── Startup banner ─────────────────────────────────────────────────────
    print(f"Publishing to {bootstrap}/{topic} at {args.rate:.0f} evt/s"
          + (f" for {args.duration:.0f}s" if args.duration else " (Ctrl-C to stop)"))
    if CHAOS_MALFORMED_PCT > 0 or CHAOS_LATE_PCT > 0:
        print(f"  CHAOS ENABLED — malformed: {CHAOS_MALFORMED_PCT}%  |  late: {CHAOS_LATE_PCT}%"
              + (f"  |  seed: {CHAOS_SEED}" if CHAOS_SEED else ""))
    else:
        print(f"  CHAOS disabled (CHAOS_MALFORMED_PCT and CHAOS_LATE_PCT both 0)")

    # ── Main publish loop ──────────────────────────────────────────────────
    while running:
        if args.duration and (time.time() - t_start) >= args.duration:
            break

        # Pick a random hotel from the seed list; generate a clean event.
        hotel = random.choice(hotels)
        event = make_event(hotel, month)

        # Apply chaos. payload may be a dict (normal/late/most malformed
        # cases) or bytes (unparseable_json case). The producer's
        # value_serializer handles both transparently.
        payload, mode = maybe_corrupt_or_delay(event)
        chaos_counts[mode] += 1

        # Kafka partition key. For chaos modes that corrupted `city`, we
        # still send under the *original* city as the key so partition
        # distribution stays even — the corruption is in the value, not
        # the routing. (Using `hotel["city"]` from the clean record, not
        # `event["city"]` which may have been overwritten.)
        producer.send(topic, key=hotel["city"], value=payload)
        total += 1

        # Periodic progress log every 100 events.
        if total % 100 == 0:
            now     = time.time()
            elapsed = now - t_start
            rate    = total / elapsed
            print(f"  Produced {total:,} events  |  actual rate {rate:.1f}/s  |  elapsed {elapsed:.1f}s")

        # Rate limiting — sleep to match the requested events/sec. Uses
        # absolute scheduling rather than per-iteration sleep so cumulative
        # drift stays bounded.
        next_tick = t_start + total * interval
        drift = next_tick - time.time()
        if drift > 0:
            time.sleep(drift)

    # ── Shutdown ───────────────────────────────────────────────────────────
    elapsed = time.time() - t_start
    producer.flush()
    producer.close()

    print(f"\nProduced {total:,} events in {elapsed:.1f}s  ({total/elapsed:.1f}/s avg). Producer closed.")

    # Chaos summary — print only if chaos was actually enabled, to keep
    # the normal-run output unchanged from your original script.
    if CHAOS_MALFORMED_PCT > 0 or CHAOS_LATE_PCT > 0:
        normal_count    = chaos_counts.get("normal", 0)
        late_count      = chaos_counts.get("late", 0)
        malformed_total = sum(v for k, v in chaos_counts.items() if k.startswith("malformed:"))

        print(f"\nProducer chaos summary:")
        print(f"  Normal              : {normal_count:,}  ({normal_count/total*100:.2f}%)")
        print(f"  Late                : {late_count:,}  ({late_count/total*100:.2f}%)")
        print(f"  Malformed (total)   : {malformed_total:,}  ({malformed_total/total*100:.2f}%)")
        # Per-reason breakdown so you can sanity-check the consumer's
        # malformed_by_reason output against these numbers directly.
        for mode, count in sorted(chaos_counts.items(), key=lambda x: -x[1]):
            if mode.startswith("malformed:"):
                reason = mode.split(":", 1)[1]
                print(f"     └─ {reason:25s}: {count:,}")
        print(f"\n  These counts should match the consumer's run summary"
              f" (within Kafka offset-commit jitter, usually ±5).")


if __name__ == "__main__":
    main()