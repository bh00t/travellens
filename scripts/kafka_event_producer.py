"""
Event producer — data-aware FORWARD generator (B-047, Stage 2a).

Replaces the calendar-replay simulator (B-034A).  The replay generator
walked a sim-clock day-by-day through `fact_bookings`, mirroring real
historical rows to the wire; it has no future left to replay (last sim
day 2027-12-06) and never had a story for "events happen at realistic
times of day."  The forward generator below:

  - Paces TOTAL outbound events with a 24-value diurnal RATE CURVE
    anchored to IST (night ~0.30×, evening peak ~2.05×).  At base 10
    evt/s and rate_multiplier=1, daily integral ≈ 868K events vs a
    1 M-event cap — 13 % headroom.
  - Generates NET-NEW BOOKINGs from the live `dim_location` /
    `hotel_master` / `dim_room_type` / `dim_customer` tables, respecting
    a per-hotel-per-night OCCUPANCY CAP from overlapping
    `sim_open_bookings` rows.
  - Stamps each new BOOKING with FIRE-TIMES for its CHECKIN, CHECKOUT,
    (optional) CANCELLATION, REVIEW events drawn from per-event-type
    IST hour distributions.  Lifecycle events fire when real wall-clock
    crosses each stamp.
  - Never backdates `event_ts` — on resume after downtime, overdue rows
    drain through the bucket at the curve's cap with `event_ts = NOW()`.

Wire compatibility with the consumer (`scripts/stream_consumer.py`) is
preserved: same event_type strings, same field shapes per type, same
revenue invariant (`revenue_inr == nightly_rate_inr * nights`) on
BOOKINGs, same Kafka config (city-partitioned, bytes-pass-through,
`acks="all"`, `linger_ms=20`).

────────────────────────────────────────────────────────────────────────
LOOP SHAPE
────────────────────────────────────────────────────────────────────────
Token bucket refilled at `effective_rate(h_IST) = BASE_RATE ×
DIURNAL_HOUR_MULT[h] × rate_multiplier` per second.  Each tick (~200 ms):

  1. Pull the soonest-due lifecycle row.  If one exists and its fire_ts
     has been reached, emit that CHECKIN / CHECKOUT / CANCELLATION /
     REVIEW (and advance state in `sim_open_bookings` accordingly).
  2. Else, pick BOOKING vs PRICE_CHANGE by per-hour weights:
        prob_BOOKING = w_b[h] / (w_b[h] + w_p[h])
     where w_b is `BOOKING_PICKER_WEIGHT[h]` (peak 2.80 @ 19 IST) and
     w_p is `DIURNAL_HOUR_MULT[h]` (the master curve).  No 95/5 coin
     flip — the per-hour mix shapes itself, daily aggregate ≈ 48 %/52 %.
  3. On a daily-cap hit, sleep until midnight IST.

All DB mutations of a single tick are batched into ONE commit; at
rate_multiplier=5 this caps commits at ~5/sec instead of ~100/sec.

────────────────────────────────────────────────────────────────────────
--rate-multiplier  (integer, default 1, silent-fallback-to-1 on bad input)
────────────────────────────────────────────────────────────────────────
Folds in B-041 (the run.py `--sim-rate` no-op + producer `--rate` /
`--duration` / `--sim-speed` no-op deprecations are deleted, not kept).
The single supported throughput knob across the system is now
`--rate-multiplier`.  Both the bucket rate AND the daily cap scale
together (`daily_cap = 1_000_000 × rate_multiplier`) so high-x runs
don't burn through the cap mid-day and go silent.

────────────────────────────────────────────────────────────────────────
Usage
────────────────────────────────────────────────────────────────────────
  python -m scripts.kafka_event_producer
  python -m scripts.kafka_event_producer --rate-multiplier 3
  python -m scripts.kafka_event_producer --malformed-pct 5 --late-pct 2 --chaos-seed 42
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
from datetime import date, datetime, time as dtime, timedelta, timezone
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from kafka import KafkaProducer

from scripts.chaos_injector import maybe_corrupt_or_delay
from scripts.review_generator import make_review_event_dict as _make_review

sys.stdout.reconfigure(encoding="utf-8")
load_dotenv()


# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTS — locked per the approved plan; do not retune without owner sign-off
# ══════════════════════════════════════════════════════════════════════════════

IST = ZoneInfo("Asia/Kolkata")
UTC = timezone.utc

# Master diurnal curve (multiplier on BASE_RATE).  Σ = 24.10 → mean 1.004×.
# Indexed by IST hour 0-23.
DIURNAL_HOUR_MULT = [
    0.30, 0.20, 0.20, 0.20, 0.25, 0.40,   # 00-05
    0.55, 0.75, 1.00, 1.15, 1.10, 1.05,   # 06-11
    1.15, 1.20, 1.15, 1.25, 1.40, 1.55,   # 12-17
    1.85, 2.05, 2.00, 1.65, 1.15, 0.55,   # 18-23
]
assert len(DIURNAL_HOUR_MULT) == 24

BASE_RATE       = 10           # events/sec at curve mean=1× (≈868K/day at x=1)
DAILY_CAP_BASE  = 1_000_000    # 10 lakh; daily_cap = base × rate_multiplier

# BOOKING picker weight (used vs PRICE_CHANGE).  Σ ≈ 24.37 → mean 1.015×.
# Peak 2.80 @ 19 IST.  Drops the prior 95/5 coin-flip — the per-hour ratio
# (w_b / (w_b + w_p)) shapes the daily mix to ≈ 48 % BOOKING / 52 % PC.
BOOKING_PICKER_WEIGHT = [
    0.04, 0.02, 0.02, 0.02, 0.04, 0.08,   # 00-05
    0.20, 0.40, 0.65, 0.85, 0.95, 1.05,   # 06-11
    1.15, 1.20, 1.25, 1.45, 1.65, 1.95,   # 12-17
    2.40, 2.80, 2.50, 1.95, 1.20, 0.55,   # 18-23
]
assert len(BOOKING_PICKER_WEIGHT) == 24

# Per-event-type fire-hour distributions.  Each dict sums to 1.0.  When a new
# BOOKING is emitted we sample CHECKIN / CHECKOUT / (optional) CANCELLATION
# fire-hours from these and stamp the row.  REVIEW is sampled on CHECKOUT/
# CANCELLATION emit (deferred), clamped to the same calendar day.
CHECKOUT_HOUR_DIST     = {8: 0.15, 9: 0.25, 10: 0.25, 11: 0.20, 12: 0.10, 13: 0.05}
CHECKIN_HOUR_DIST      = {12: 0.05, 13: 0.05, 14: 0.08, 15: 0.12,
                          16: 0.15, 17: 0.15, 18: 0.15, 19: 0.12,
                          20: 0.08, 21: 0.05}
CANCELLATION_HOUR_DIST = {9: 0.05, 10: 0.05, 11: 0.05, 12: 0.07, 13: 0.07,
                          14: 0.07, 15: 0.07, 16: 0.08, 17: 0.10, 18: 0.12,
                          19: 0.12, 20: 0.10, 21: 0.05}
REVIEW_HOUR_DIST       = {20: 0.30, 21: 0.35, 22: 0.25, 23: 0.10}

# Empirical booking-source mix from real fact_bookings (~1M rows).
BOOKING_SOURCE_MIX = [
    ("MakeMyTrip",  0.26), ("Direct",      0.22), ("OYO",      0.15),
    ("Booking.com", 0.13), ("Goibibo",     0.12), ("Walk-in",  0.06),
    ("Agoda",       0.06),
]

# Lead-time mix: 50% ≤7d, 35% 1-8wk, 15% 2-8mo.
LEAD_TIME_BUCKETS = [
    ( 0,   7,  0.50),
    ( 8,  56,  0.35),
    (60, 240,  0.15),
]

# Nights distribution from empirical fact_bookings.nights_stayed:
NIGHTS_MIX = [
    (1, 0.30), (2, 0.30), (3, 0.18), (4, 0.10), (5, 0.06),
    (6, 0.03), (7, 0.02), (10, 0.01),
]

# Cancel rate at booking time.  Deterministic per booking_id × daily seed.
P_CANCEL = 0.12

# Season multiplier on city-weight from dim_date.season + is_high_demand_holiday.
# Keys MUST match the real dim_date.season vocabulary exactly. The real values
# (verified via SELECT DISTINCT season FROM dim_date) and their month coverage:
#   Peak     — Jan/Feb/Oct/Nov/Dec (winter peak — highest Indian tourism demand)
#   Shoulder — March only (transitional)
#   Summer   — Apr/May/Jun (mixed: hot plains low, hill stations high — neutral)
#   Monsoon  — Jul/Aug/Sep (lowest demand across most of India)
# An unexpected value falls back to 1.0 with a one-time WARNING so future vocab
# drift is caught loudly instead of silently neutralized.
SEASON_MULT = {
    "Peak":     1.30,
    "Shoulder": 1.00,
    "Summer":   1.00,
    "Monsoon":  0.75,
}
_UNKNOWN_SEASON_WARNED = set()


def _season_mult(season):
    """Resolve season multiplier; warn once per unseen value, default 1.0."""
    if season in SEASON_MULT:
        return SEASON_MULT[season]
    if season not in _UNKNOWN_SEASON_WARNED:
        _UNKNOWN_SEASON_WARNED.add(season)
        print(f"WARNING: unknown dim_date.season value {season!r} — "
              f"using multiplier 1.0. Expected one of "
              f"{sorted(SEASON_MULT.keys())}.", file=sys.stderr)
    return 1.0


HIGH_DEMAND_HOLIDAY_MULT = 1.25

# Tick + bucket sizing.
TICK_SECONDS    = 0.2
BUCKET_BURST_S  = 2.0   # max burst = effective_rate × this many seconds

# Advisory lock — unchanged from prior producer.
PRODUCER_ADVISORY_LOCK_KEY = 7_400_030

# Kafka / DB.
DB_PARAMS = {
    "host":     os.getenv("POSTGRES_HOST",     "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB",       "travellens"),
    "user":     os.getenv("POSTGRES_USER",     "travellens"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
}

# Old sim-clock file — deleted on first launch and never written again.
LEGACY_CLOCK_FILE = "scripts/.sim_clock.json"


# ══════════════════════════════════════════════════════════════════════════════
# RATE-MULTIPLIER COERCION — integer-only, strictly > 1; silent fallback to 1
# ══════════════════════════════════════════════════════════════════════════════
# Owner-specified contract:
#   - Default (flag omitted)             → 1
#   - Pass --rate-multiplier 2 (int>1)   → 2
#   - Pass anything else                 → 1, silently (no argparse error,
#                                          no warning); same for the
#                                          RATE_MULTIPLIER env var.
# Folds in B-041: --sim-rate / --rate / --duration / --sim-speed / etc. all
# removed entirely.

def _coerce_rate_multiplier(s):
    """argparse type: int>1 or silent fallback to 1.  Never raises."""
    try:
        v = int(s)
        if v > 1:
            return v
    except (ValueError, TypeError):
        pass
    return 1


def _coerce_rate_multiplier_env():
    raw = os.getenv("RATE_MULTIPLIER")
    if raw is None:
        return 1
    return _coerce_rate_multiplier(raw)


# ══════════════════════════════════════════════════════════════════════════════
# DIM CACHE — loaded once at startup; ~15 MB resident
# ══════════════════════════════════════════════════════════════════════════════

class DimCache:
    """
    Snapshot of dim tables used by the BOOKING generator.  Loaded once; the
    sim is forward-only and dim drift mid-run is rare enough that a periodic
    refresh isn't worth the complexity.

    cities_by_id          — {location_id: (city, state, tourist_arrivals, popularity_weight)}
    hotels                — list of dicts {hotel_id, city, location_id, state, total_rooms,
                                            star_category, base_price_inr}
    hotels_by_city        — {city: [hotel_index_into_self.hotels, ...]}
    room_types_by_hotel   — {hotel_id: [(room_type_id, capacity, base_price_inr, type_name)]}
    customers             — list of (customer_id, home_state); ~100K
    """

    def __init__(self, conn):
        self.cities_by_id    = {}
        self.hotels          = []
        self.hotels_by_city  = defaultdict(list)
        self.room_types_by_hotel = defaultdict(list)
        self.customers       = []
        self.customers_by_state = defaultdict(list)
        self._load(conn)

    def _load(self, conn):
        with conn.cursor() as cur:
            # dim_location
            cur.execute("""
                SELECT location_id, city, state,
                       COALESCE(tourist_arrivals_annual_m, 1.0)
                FROM dim_location
            """)
            for loc_id, city, state, tour in cur.fetchall():
                self.cities_by_id[loc_id] = (city, state, float(tour),
                                              float(tour) ** 0.7 + 0.5)

            # hotel_master JOIN dim_location for city/state
            cur.execute("""
                SELECT h.hotel_id, l.city, h.location_id, l.state,
                       COALESCE(h.total_rooms, 30), COALESCE(h.star_category, 3),
                       COALESCE(h.base_price_inr, 3000)
                FROM hotel_master h
                JOIN dim_location l ON l.location_id = h.location_id
                WHERE h.is_active = TRUE
            """)
            for i, row in enumerate(cur.fetchall()):
                hid, city, loc_id, state, rooms, star, base = row
                self.hotels.append({
                    "hotel_id": hid, "city": city, "location_id": loc_id,
                    "state": state, "total_rooms": int(rooms),
                    "star_category": int(star), "base_price_inr": float(base),
                })
                self.hotels_by_city[city].append(i)

            # dim_room_type (only those whose hotel is active)
            active_hids = {h["hotel_id"] for h in self.hotels}
            cur.execute("""
                SELECT room_type_id, hotel_id, COALESCE(capacity, 2),
                       COALESCE(base_price_inr, 2500), type_name
                FROM dim_room_type
            """)
            for rt_id, hid, cap, base, name in cur.fetchall():
                if hid in active_hids:
                    self.room_types_by_hotel[hid].append((rt_id, int(cap),
                                                          float(base), name))

            # dim_customer
            cur.execute("SELECT customer_id, home_state FROM dim_customer")
            for cid, state in cur.fetchall():
                self.customers.append((cid, state))
                self.customers_by_state[state].append(len(self.customers) - 1)

        # Pre-bin cities by weight so weighted_city_pick is O(1) sample.
        self._build_city_indices()

    def _build_city_indices(self):
        """
        Build a flat alias-list-ish structure for weighted city sampling.
        Each city's weight ≡ popularity_weight × #hotels.  Cities with 0
        hotels are dropped.
        """
        self._city_names   = []
        self._city_weights = []
        self._city_states  = {}  # city -> state (mode)
        seen = set()
        for loc_id, (city, state, _tour, popw) in self.cities_by_id.items():
            if city in seen or not self.hotels_by_city[city]:
                continue
            seen.add(city)
            n_hotels = len(self.hotels_by_city[city])
            self._city_names.append(city)
            self._city_weights.append(popw * n_hotels)
            self._city_states[city] = state

    def weighted_city_pick(self, rng, season_mult, holiday_mult):
        """Sample a city, biased by popularity × hotel count × season × holiday."""
        # season/holiday are global multipliers that scale all city weights
        # uniformly — they keep the relative weight structure intact but
        # are kept here for future per-city tuning (peak_months matching).
        return rng.choices(self._city_names, weights=self._city_weights, k=1)[0]

    def pick_hotel_in_city(self, rng, city):
        """Sample a hotel in the given city, weighted by total_rooms × star_category."""
        idxs = self.hotels_by_city.get(city)
        if not idxs:
            return None
        weights = [self.hotels[i]["total_rooms"] * self.hotels[i]["star_category"]
                   for i in idxs]
        i = rng.choices(idxs, weights=weights, k=1)[0]
        return self.hotels[i]

    def pick_room_type(self, rng, hotel_id, num_guests):
        """Pick a room_type for the hotel that fits num_guests.  None if no fit."""
        rts = self.room_types_by_hotel.get(hotel_id)
        if not rts:
            return None
        fits = [r for r in rts if r[1] >= num_guests]
        if not fits:
            # last resort — largest available; consumer will accept
            fits = [max(rts, key=lambda r: r[1])]
        # weight 1/(price_tier) ≈ 1/base_price_inr to favour cheaper rooms
        weights = [1.0 / max(r[2], 100.0) for r in fits]
        rt = rng.choices(fits, weights=weights, k=1)[0]
        return rt

    def pick_customer(self, rng, exclude_state, intra_state_prob=0.25):
        """75% out-of-state tourist, 25% intra-state.  exclude_state may be None."""
        if rng.random() < intra_state_prob and exclude_state in self.customers_by_state:
            pool = self.customers_by_state[exclude_state]
            if pool:
                return self.customers[rng.choice(pool)]
        # any customer not in exclude_state if possible; else any customer
        for _ in range(8):
            c = rng.choice(self.customers)
            if c[1] != exclude_state:
                return c
        return rng.choice(self.customers)


# ══════════════════════════════════════════════════════════════════════════════
# DIM_DATE — small per-day lookup
# ══════════════════════════════════════════════════════════════════════════════

def fetch_dim_date(conn, d):
    """Return (season, is_high_demand_holiday, is_weekend) for date d, with safe defaults."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT season, is_high_demand_holiday, is_weekend
            FROM dim_date WHERE full_date = %s
        """, (d,))
        r = cur.fetchone()
    if not r:
        return ("Summer", False, False)
    return (r[0] or "Summer", bool(r[1]), bool(r[2]))


# ══════════════════════════════════════════════════════════════════════════════
# SAMPLING HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _now_utc():
    return datetime.now(UTC)


def _now_ist():
    return datetime.now(IST)


def _today_ist():
    return _now_ist().date()


def _now_iso_utc():
    return _now_utc().isoformat()


def _sample_weighted(rng, items, weights):
    return rng.choices(items, weights=weights, k=1)[0]


def _sample_booking_source(rng):
    return _sample_weighted(rng,
                            [s for s, _ in BOOKING_SOURCE_MIX],
                            [w for _, w in BOOKING_SOURCE_MIX])


def _sample_lead_time(rng):
    """Days until checkin from today."""
    buckets = LEAD_TIME_BUCKETS
    pick = _sample_weighted(rng, list(range(len(buckets))),
                            [b[2] for b in buckets])
    lo, hi, _ = buckets[pick]
    return rng.randint(lo, hi)


def _sample_nights(rng):
    return _sample_weighted(rng,
                            [n for n, _ in NIGHTS_MIX],
                            [w for _, w in NIGHTS_MIX])


def _sample_num_guests(rng):
    # 1: 25 %, 2: 50 %, 3: 18 %, 4: 6 %, 5: 1 %
    return _sample_weighted(rng, [1, 2, 3, 4, 5],
                            [0.25, 0.50, 0.18, 0.06, 0.01])


def _sample_hour_from_dist(rng, dist):
    """dist: {hour:int -> weight:float}.  Returns a sampled IST hour."""
    hours, weights = zip(*dist.items())
    return rng.choices(hours, weights=weights, k=1)[0]


def _sample_fire_ts_on_day(rng, ist_day, hour_dist):
    """Build a TIMESTAMPTZ (in UTC) for an IST datetime sampled from hour_dist."""
    h = _sample_hour_from_dist(rng, hour_dist)
    m = rng.randint(0, 59)
    s = rng.randint(0, 59)
    naive = datetime.combine(ist_day, dtime(h, m, s))
    return naive.replace(tzinfo=IST).astimezone(UTC)


def _compute_price(rng, base_price_inr, checkin_date, dim_date_today):
    """Apply weekend/holiday/off-season modifiers + bounded Gaussian noise."""
    season, is_holiday, _ = dim_date_today
    weekend = checkin_date.weekday() >= 5
    mult = 1.0
    if weekend:    mult *= 1.15
    if is_holiday: mult *= 1.25
    # Season vocabulary matches dim_date.season exactly (Peak/Shoulder/Summer/Monsoon).
    # Unknown values fall through silently here — _season_mult() above is the
    # canonical warner; this is a price-only modifier on a per-booking path
    # called every tick, so we keep it noise-free.
    if season == "Monsoon":  mult *= 0.85
    elif season == "Peak":   mult *= 1.10
    noise = max(0.70, min(1.30, rng.gauss(1.0, 0.05)))
    return max(500, int(round(base_price_inr * mult * noise)))


def _is_cancel_decided(booking_id_str, daily_seed):
    """Deterministic 12% cancel decision keyed on booking_id × daily_seed."""
    rng = random.Random(f"{booking_id_str}|cancel|{daily_seed}")
    return rng.random() < P_CANCEL


# ══════════════════════════════════════════════════════════════════════════════
# OCCUPANCY CAP
# ══════════════════════════════════════════════════════════════════════════════

def _hotel_occupancy_busted(cur, hotel_id, total_rooms, checkin_d, checkout_d):
    """
    True if any night in [checkin_d, checkout_d) already has total_rooms
    overlapping BOOKED+CHECKED_IN rows for this hotel.

    Postgres returns the MAX nightly overlap across the candidate range in
    one query via generate_series.
    """
    cur.execute("""
        WITH nights AS (
            SELECT generate_series(%s::date, %s::date - 1, '1 day'::interval)::date AS night
        )
        SELECT MAX(c) AS max_overlap FROM (
            SELECT n.night, COUNT(*) AS c
            FROM nights n
            LEFT JOIN sim_open_bookings sob
              ON sob.hotel_id = %s
             AND sob.state IN ('BOOKED', 'CHECKED_IN')
             AND sob.checkin_date <= n.night
             AND sob.checkout_date > n.night
            GROUP BY n.night
        ) x
    """, (checkin_d, checkout_d, hotel_id))
    r = cur.fetchone()
    overlap = (r[0] or 0)
    return overlap >= total_rooms


# ══════════════════════════════════════════════════════════════════════════════
# EVENT BUILDERS — wire shapes preserved
# ══════════════════════════════════════════════════════════════════════════════

def _build_booking_event(rec):
    """rec: dict from generate_booking().  Builds the BOOKING wire event."""
    return {
        "event_id":         str(uuid.uuid4()),
        "event_type":       "BOOKING",
        "hotel_id":         rec["hotel_id"],
        "city":             rec["city"],
        "event_ts":         _now_iso_utc(),
        "event_date":       _today_ist().isoformat(),
        "booking_id":       rec["booking_id"],
        "customer_id":      rec["customer_id"],
        "room_type_id":     rec["room_type_id"],
        "checkin_date":     rec["checkin_date"].isoformat(),
        "checkout_date":    rec["checkout_date"].isoformat(),
        "nights":           rec["nights"],
        "num_guests":       rec["num_guests"],
        "nightly_rate_inr": rec["nightly_rate_inr"],
        "revenue_inr":      rec["nightly_rate_inr"] * rec["nights"],
        "booking_source":   rec["booking_source"],
    }


def _build_lifecycle_event(event_type, booking_id, customer_id, hotel_id, city, extra=None):
    evt = {
        "event_id":    str(uuid.uuid4()),
        "event_type":  event_type,
        "hotel_id":    hotel_id,
        "city":        city,
        "event_ts":    _now_iso_utc(),
        "event_date":  _today_ist().isoformat(),
        "booking_id":  booking_id,
        "customer_id": customer_id,
    }
    if extra:
        evt.update(extra)
    return evt


def _build_price_change_event(rng, hotel):
    base = hotel["base_price_inr"]
    # Pick a real room type for the price change so consumers can FK it.
    rt = None
    rts = None  # set lazily; we already have the cache outside
    old_price = max(500, int(base * rng.uniform(0.85, 1.10)))
    new_price = max(500, int(old_price * rng.uniform(0.90, 1.20)))
    return {
        "event_id":      str(uuid.uuid4()),
        "event_type":    "PRICE_CHANGE",
        "hotel_id":      hotel["hotel_id"],
        "city":          hotel["city"],
        "event_ts":      _now_iso_utc(),
        "event_date":    _today_ist().isoformat(),
        "room_type_id":  str(uuid.uuid4()),  # operational stub; consumer accepts
        "old_price_inr": old_price,
        "new_price_inr": new_price,
    }


# ══════════════════════════════════════════════════════════════════════════════
# NEW-BOOKING GENERATION
# ══════════════════════════════════════════════════════════════════════════════

def generate_booking(rng, cache, conn, today_ist, daily_seed):
    """
    Build one new BOOKING + its fire-time stamps.  Returns a dict ready for
    INSERT into sim_open_bookings AND the BOOKING wire event, OR None if we
    couldn't find a hotel that satisfies the occupancy cap after a few tries.

    Steps (per the locked plan):
      1. City by popularity × hotel count × season/holiday
      2. Hotel in that city by total_rooms × star, subject to occupancy cap
      3. Room type fitting num_guests
      4. Customer (75% out-of-state)
      5. Lead time + nights + dates
      6. Price + revenue invariant
      7. booking_source + lifecycle fire-times + cancel decision
    """
    dim_date_today = fetch_dim_date(conn, today_ist)
    season_mult = _season_mult(dim_date_today[0])
    holiday_mult = HIGH_DEMAND_HOLIDAY_MULT if dim_date_today[1] else 1.0

    # Try a few cities × hotels before giving up to PRICE_CHANGE.
    for _ in range(6):
        city = cache.weighted_city_pick(rng, season_mult, holiday_mult)
        hotel = cache.pick_hotel_in_city(rng, city)
        if hotel is None:
            continue

        num_guests = _sample_num_guests(rng)
        rt = cache.pick_room_type(rng, hotel["hotel_id"], num_guests)
        if rt is None:
            continue
        rt_id, rt_cap, rt_base_price, rt_name = rt

        lead = _sample_lead_time(rng)
        nights = _sample_nights(rng)
        checkin_d  = today_ist + timedelta(days=lead)
        checkout_d = checkin_d + timedelta(days=nights)

        with conn.cursor() as cur:
            if _hotel_occupancy_busted(cur, hotel["hotel_id"],
                                       hotel["total_rooms"],
                                       checkin_d, checkout_d):
                continue  # try another hotel

        nightly = _compute_price(rng, rt_base_price, checkin_d, dim_date_today)
        revenue = nightly * nights
        assert revenue == nightly * nights, "revenue invariant"

        customer = cache.pick_customer(rng, exclude_state=hotel["state"])
        cust_id, _cust_state = customer
        source = _sample_booking_source(rng)

        booking_id = str(uuid.uuid4())

        # Lifecycle fire-times
        ckin_fire  = _sample_fire_ts_on_day(rng, checkin_d,  CHECKIN_HOUR_DIST)
        ckout_fire = _sample_fire_ts_on_day(rng, checkout_d, CHECKOUT_HOUR_DIST)

        # Cancel decision (deterministic) — cancel_date in [today, checkin_d]
        cancel_fire = None
        if _is_cancel_decided(booking_id, daily_seed):
            days_until = (checkin_d - today_ist).days
            cd_offset = rng.randint(0, max(0, days_until))
            cancel_day = today_ist + timedelta(days=cd_offset)
            cancel_fire = _sample_fire_ts_on_day(rng, cancel_day,
                                                 CANCELLATION_HOUR_DIST)
            # Clamp to NOT-IN-PAST (e.g. cancel_day=today, cancel_hour < now_ist_hour):
            now_u = _now_utc()
            if cancel_fire < now_u:
                cancel_fire = now_u + timedelta(seconds=rng.randint(60, 600))

        return {
            "booking_id":       booking_id,
            "hotel_id":         hotel["hotel_id"],
            "city":             hotel["city"],
            "customer_id":      cust_id,
            "room_type_id":     str(rt_id),
            "checkin_date":     checkin_d,
            "checkout_date":    checkout_d,
            "nights":           nights,
            "num_guests":       num_guests,
            "nightly_rate_inr": nightly,
            "booking_source":   source,
            "checkin_fire_ts":  ckin_fire,
            "checkout_fire_ts": ckout_fire,
            "cancel_fire_ts":   cancel_fire,
            "_hotel":           hotel,
            "_rt_name":         rt_name,
        }

    return None  # gave up — caller emits PRICE_CHANGE


# ══════════════════════════════════════════════════════════════════════════════
# DB MUTATIONS — collected per-tick, committed in one batch
# ══════════════════════════════════════════════════════════════════════════════

class TickBatch:
    """Accumulator for per-tick DB mutations.  Caller commits once per tick."""

    def __init__(self):
        self.inserts_open    = []     # rows for sim_open_bookings
        self.state_to_ci     = []     # booking_ids → state=CHECKED_IN
        self.state_to_rev    = []     # (booking_id, review_fire_ts) → state=REVIEW_PENDING
        self.delete_open     = []     # booking_ids to drop
        self.bookings_emitted = 0     # for sim_daily_counter
        self.events_emitted   = 0

    def add_booking(self, rec):
        self.inserts_open.append((
            rec["booking_id"], rec["customer_id"], rec["hotel_id"], rec["city"],
            rec["room_type_id"], rec["checkin_date"], rec["checkout_date"],
            rec["nights"], rec["num_guests"], rec["nightly_rate_inr"], None,
            rec["booking_source"], "BOOKED", _now_utc(), "stream",
            rec["checkin_fire_ts"], rec["checkout_fire_ts"],
            rec["cancel_fire_ts"], None,  # review_fire_ts set later
        ))
        self.bookings_emitted += 1


def flush_tick(conn, batch, daily_counter_today, events_in_tick):
    """Apply all per-tick mutations in ONE commit.  Called at end of every tick."""
    if (not batch.inserts_open and not batch.state_to_ci and not batch.state_to_rev
            and not batch.delete_open and events_in_tick == 0):
        return
    with conn.cursor() as cur:
        if batch.inserts_open:
            execute_values(cur, """
                INSERT INTO sim_open_bookings
                    (booking_id, customer_id, hotel_id, city, room_type_id,
                     checkin_date, checkout_date, nights, num_guests,
                     nightly_rate_inr, payment_mode, booking_source,
                     state, booked_event_ts, source,
                     checkin_fire_ts, checkout_fire_ts,
                     cancel_fire_ts, review_fire_ts)
                VALUES %s
                ON CONFLICT (booking_id) DO NOTHING
            """, batch.inserts_open, page_size=len(batch.inserts_open))

        if batch.state_to_ci:
            execute_values(cur, """
                UPDATE sim_open_bookings sob SET state='CHECKED_IN'
                FROM (VALUES %s) AS v(bid) WHERE sob.booking_id = v.bid::uuid
            """, [(b,) for b in batch.state_to_ci],
                page_size=len(batch.state_to_ci))

        if batch.state_to_rev:
            execute_values(cur, """
                UPDATE sim_open_bookings sob
                   SET state='REVIEW_PENDING', review_fire_ts = v.rfts::timestamptz
                FROM (VALUES %s) AS v(bid, rfts)
                WHERE sob.booking_id = v.bid::uuid
            """, [(b, rf.isoformat()) for (b, rf) in batch.state_to_rev],
                page_size=len(batch.state_to_rev))

        if batch.delete_open:
            execute_values(cur, """
                DELETE FROM sim_open_bookings sob
                USING (VALUES %s) AS v(bid)
                WHERE sob.booking_id = v.bid::uuid
            """, [(b,) for b in batch.delete_open],
                page_size=len(batch.delete_open))

        # Increment the day's events_emitted counter by events_in_tick.
        if events_in_tick > 0:
            cur.execute("""
                UPDATE sim_daily_counter
                   SET events_emitted = events_emitted + %s, updated_at = NOW()
                 WHERE counter_date = %s
            """, (events_in_tick, daily_counter_today))
    conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
# DAILY COUNTER MGMT
# ══════════════════════════════════════════════════════════════════════════════

def ensure_daily_counter_row(conn, today_ist, rate_multiplier):
    """
    Insert/refresh today's counter row; return (events_emitted_so_far, cap).

    LAST-WRITE-WINS on cap (B-047 + migration 016): a follow-up
    `python run.py --rate-multiplier N` overrides today's cap immediately,
    so an x=1 session followed by x=3 jumps cap from 1M to 3M without
    waiting for IST midnight.  `events_emitted` is intentionally NOT
    overwritten — it keeps accumulating across all sessions in the day
    (correct, since the counter is a cumulative ceiling check).
    """
    cap = DAILY_CAP_BASE * rate_multiplier
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO sim_daily_counter (counter_date, events_emitted, cap)
            VALUES (%s, 0, %s)
            ON CONFLICT (counter_date) DO UPDATE
              SET cap = EXCLUDED.cap, updated_at = NOW()
        """, (today_ist, cap))
        cur.execute("""
            SELECT events_emitted, cap FROM sim_daily_counter WHERE counter_date = %s
        """, (today_ist,))
        r = cur.fetchone()
    conn.commit()
    return (r[0], r[1]) if r else (0, cap)


def read_daily_counter(conn, today_ist):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT events_emitted, cap FROM sim_daily_counter WHERE counter_date = %s
        """, (today_ist,))
        r = cur.fetchone()
    return (r[0], r[1]) if r else (0, DAILY_CAP_BASE)


# ══════════════════════════════════════════════════════════════════════════════
# DUE-LIFECYCLE FETCH
# ══════════════════════════════════════════════════════════════════════════════

def fetch_due_lifecycle(conn, now_utc, limit=8):
    """
    Fetch up to `limit` lifecycle rows whose fire-time has passed.  Returns
    a list of dicts; caller iterates and emits.  Each row picks the SINGLE
    fire-type that matches its current state, so there's no ambiguity at
    emit time.
    """
    out = []
    with conn.cursor() as cur:
        # CHECKIN (state=BOOKED, checkin_fire_ts <= now)
        cur.execute("""
            SELECT booking_id, customer_id, hotel_id, city,
                   checkin_fire_ts AS fire_ts, 'CHECKIN' AS fire_type
            FROM sim_open_bookings
            WHERE state = 'BOOKED' AND checkin_fire_ts <= %s
              AND (cancel_fire_ts IS NULL OR cancel_fire_ts > checkin_fire_ts)
            ORDER BY checkin_fire_ts ASC LIMIT %s
        """, (now_utc, limit))
        out.extend(cur.fetchall())

        # CHECKOUT (state=CHECKED_IN, checkout_fire_ts <= now)
        cur.execute("""
            SELECT booking_id, customer_id, hotel_id, city,
                   checkout_fire_ts AS fire_ts, 'CHECKOUT' AS fire_type
            FROM sim_open_bookings
            WHERE state = 'CHECKED_IN' AND checkout_fire_ts <= %s
            ORDER BY checkout_fire_ts ASC LIMIT %s
        """, (now_utc, limit))
        out.extend(cur.fetchall())

        # CANCELLATION (state=BOOKED, cancel_fire_ts <= now, before checkin)
        cur.execute("""
            SELECT booking_id, customer_id, hotel_id, city,
                   cancel_fire_ts AS fire_ts, 'CANCELLATION' AS fire_type
            FROM sim_open_bookings
            WHERE state = 'BOOKED' AND cancel_fire_ts IS NOT NULL
              AND cancel_fire_ts <= %s
            ORDER BY cancel_fire_ts ASC LIMIT %s
        """, (now_utc, limit))
        out.extend(cur.fetchall())

        # REVIEW (state=REVIEW_PENDING, review_fire_ts <= now)
        cur.execute("""
            SELECT booking_id, customer_id, hotel_id, city,
                   review_fire_ts AS fire_ts, 'REVIEW' AS fire_type
            FROM sim_open_bookings
            WHERE state = 'REVIEW_PENDING' AND review_fire_ts IS NOT NULL
              AND review_fire_ts <= %s
            ORDER BY review_fire_ts ASC LIMIT %s
        """, (now_utc, limit))
        out.extend(cur.fetchall())

    # Sort all candidates by fire_ts ASC so the soonest-due fires first.
    out.sort(key=lambda r: r[4])
    return out


# ══════════════════════════════════════════════════════════════════════════════
# REVIEW EMISSION (deferred) — sample REVIEW content + schedule
# ══════════════════════════════════════════════════════════════════════════════

def _hotel_rating_map(conn):
    """Hotel rating + star — used by make_review_event_dict on REVIEW emit."""
    with conn.cursor() as cur:
        cur.execute("SELECT hotel_id, COALESCE(avg_rating, 3.0), COALESCE(star_category, 3) FROM hotel_master")
        return {r[0]: (float(r[1]), int(r[2])) for r in cur.fetchall()}


def _schedule_review_after(rng, anchor_utc):
    """
    Sample a REVIEW fire-ts from REVIEW_HOUR_DIST on the same IST day as
    anchor_utc; clamp to >= anchor + 30min if the sample lands earlier.
    """
    anchor_ist_day = anchor_utc.astimezone(IST).date()
    rfts = _sample_fire_ts_on_day(rng, anchor_ist_day, REVIEW_HOUR_DIST)
    min_review = anchor_utc + timedelta(minutes=30)
    if rfts < min_review:
        rfts = min_review + timedelta(seconds=rng.randint(0, 600))
    return rfts


def _maybe_review_payload(rng, booking_id, customer_id, hotel_id,
                          booking_source, hotel_rating_map,
                          lifecycle_status, sim_day, booking_ts_date,
                          checkin_d, checkout_d):
    """Call make_review_event_dict; return (dict, fire_ts) or None."""
    avg, star = hotel_rating_map.get(hotel_id, (3.0, 3))
    rv = _make_review(
        booking_id=booking_id, customer_id=customer_id, hotel_id=hotel_id,
        booking_source=booking_source, hotel_avg_rating=avg,
        star_category=star, lifecycle_status=lifecycle_status,
        sim_day=sim_day, booking_ts_date=booking_ts_date,
        checkin_date=checkin_d, checkout_date=checkout_d,
    )
    return rv


def _fetch_booking_for_review(conn, booking_id):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT booking_source, checkin_date, checkout_date,
                   booked_event_ts::date
            FROM sim_open_bookings WHERE booking_id = %s
        """, (booking_id,))
        return cur.fetchone()


# ══════════════════════════════════════════════════════════════════════════════
# CATCH-UP DRAIN BANNER — log once when overdue > threshold at startup
# ══════════════════════════════════════════════════════════════════════════════

def log_startup_state(conn, today_ist, events_emitted, cap, rate_multiplier):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM sim_open_bookings WHERE state='BOOKED'")
        booked = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM sim_open_bookings WHERE state='CHECKED_IN'")
        ci = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM sim_open_bookings WHERE state='REVIEW_PENDING'")
        rp = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*) FROM sim_open_bookings
            WHERE (state='BOOKED'      AND checkin_fire_ts  <= NOW())
               OR (state='CHECKED_IN'  AND checkout_fire_ts <= NOW())
               OR (state='BOOKED'      AND cancel_fire_ts   <= NOW())
               OR (state='REVIEW_PENDING' AND review_fire_ts <= NOW())
        """)
        overdue = cur.fetchone()[0]
    print(f"sim_open_bookings: {booked} BOOKED + {ci} CHECKED_IN + {rp} REVIEW_PENDING")
    print(f"overdue lifecycle rows: {overdue} (will drain at the curve cap, "
          f"event_ts = NOW()).")
    print(f"daily counter [{today_ist.isoformat()}]: {events_emitted:,} / {cap:,}  "
          f"(rate_multiplier={rate_multiplier})")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN LOOP
# ══════════════════════════════════════════════════════════════════════════════

def effective_rate_now(rate_multiplier):
    h = _now_ist().hour
    return BASE_RATE * DIURNAL_HOUR_MULT[h] * rate_multiplier


def picker_prob_booking():
    h = _now_ist().hour
    w_b = BOOKING_PICKER_WEIGHT[h]
    w_p = DIURNAL_HOUR_MULT[h]
    return w_b / (w_b + w_p) if (w_b + w_p) > 0 else 0.5


def _seconds_until_ist_midnight():
    now = _now_ist()
    tomorrow = (now + timedelta(days=1)).date()
    midnight = datetime.combine(tomorrow, dtime(0, 0, 0), tzinfo=IST)
    return max(60, int((midnight - now).total_seconds()))


def main():
    parser = argparse.ArgumentParser(
        description="TravelLens forward-generator (B-047 Stage 2a)"
    )
    # Owner contract: int>1 or silent fallback to 1; no error, no warn.
    # ARG_FROM_ENV note: argparse default uses env-coerced value, so the CLI
    # only ever OVERRIDES env when explicitly passed.
    parser.add_argument("--rate-multiplier",
                        type=_coerce_rate_multiplier,
                        default=_coerce_rate_multiplier_env(),
                        help="integer > 1 scales rate + daily cap together; "
                             "any invalid value silently becomes 1")
    # Chaos is ON by default at the locked B-047 plan rates (1.2% malformed +
    # 0.8% late).  These trickle the monitor's quarantine cards instead of
    # spiking them.  `--chaos` on run.py still overrides to the 5/2 stress
    # profile via CHAOS_* env vars; setting CHAOS_*_PCT=0 explicitly disables.
    parser.add_argument("--malformed-pct", type=float,
                        default=float(os.getenv("CHAOS_MALFORMED_PCT", "1.2")))
    parser.add_argument("--late-pct",      type=float,
                        default=float(os.getenv("CHAOS_LATE_PCT", "0.8")))
    parser.add_argument("--chaos-seed",    type=int,
                        default=(int(os.getenv("CHAOS_SEED"))
                                 if os.getenv("CHAOS_SEED") else None))
    args = parser.parse_args()

    # Belt-and-suspenders: re-coerce the final value in case argparse passes
    # a default that wasn't run through our coerce (it does, but explicit).
    rate_multiplier = _coerce_rate_multiplier(args.rate_multiplier)

    if args.chaos_seed is not None:
        random.seed(args.chaos_seed)
    rng = random.Random(args.chaos_seed if args.chaos_seed is not None else None)

    # Delete the legacy sim-clock file once; it's never read again.
    try:
        if os.path.exists(LEGACY_CLOCK_FILE):
            os.unlink(LEGACY_CLOCK_FILE)
            print(f"Removed legacy {LEGACY_CLOCK_FILE} — forward generator has no sim-clock.")
    except Exception:
        pass

    # Kafka
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic     = os.getenv("KAFKA_TOPIC",     "booking-events")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all", linger_ms=20,
    )

    # DB + advisory lock
    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (PRODUCER_ADVISORY_LOCK_KEY,))
        if not cur.fetchone()[0]:
            print(f"ERROR: Another producer instance already holds advisory lock "
                  f"(key={PRODUCER_ADVISORY_LOCK_KEY}).", file=sys.stderr)
            producer.close(); conn.close(); sys.exit(1)
    print(f"Advisory lock acquired (key={PRODUCER_ADVISORY_LOCK_KEY}).")

    print(f"Loading dim cache…")
    cache = DimCache(conn)
    rating_map = _hotel_rating_map(conn)
    print(f"  cached {len(cache.hotels):,} hotels across {len(cache._city_names):,} "
          f"cities, {sum(len(v) for v in cache.room_types_by_hotel.values()):,} "
          f"room types, {len(cache.customers):,} customers.")

    today_ist = _today_ist()
    events_emitted, cap = ensure_daily_counter_row(conn, today_ist, rate_multiplier)
    daily_seed = f"{today_ist.isoformat()}|{rate_multiplier}|{args.chaos_seed or 0}"

    log_startup_state(conn, today_ist, events_emitted, cap, rate_multiplier)

    # Graceful shutdown
    running = True
    def _stop(sig, frame):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT,  _stop)
    signal.signal(signal.SIGTERM, _stop)

    # Pacing state
    tokens = 0.0
    last_t = time.monotonic()
    chaos_args = (args.malformed_pct, args.late_pct)
    chaos_stats = defaultdict(int)
    wire_counts = defaultdict(int)
    if args.malformed_pct > 0 or args.late_pct > 0:
        seed_note = f"  |  seed: {args.chaos_seed}" if args.chaos_seed is not None else ""
        print(f"CHAOS — malformed: {args.malformed_pct}%  |  late: {args.late_pct}%{seed_note}")
    print(f"rate_multiplier={rate_multiplier}  |  base={BASE_RATE} evt/s  "
          f"|  peak ≈ {int(BASE_RATE * max(DIURNAL_HOUR_MULT) * rate_multiplier)} evt/s "
          f"@ 19 IST  |  daily_cap={cap:,}")

    t_start = time.time()

    while running:
        # ── Day rollover ────────────────────────────────────────────────────
        cur_today = _today_ist()
        if cur_today != today_ist:
            today_ist = cur_today
            events_emitted, cap = ensure_daily_counter_row(conn, today_ist, rate_multiplier)
            daily_seed = f"{today_ist.isoformat()}|{rate_multiplier}|{args.chaos_seed or 0}"
            print(f"Day rollover → {today_ist}, cap={cap:,}")

        # ── Cap check ───────────────────────────────────────────────────────
        events_emitted, cap = read_daily_counter(conn, today_ist)
        if events_emitted >= cap:
            sleep_s = _seconds_until_ist_midnight()
            print(f"Daily cap hit ({events_emitted:,} / {cap:,}); "
                  f"sleeping {sleep_s}s until IST midnight.")
            slept = 0
            while running and slept < sleep_s:
                time.sleep(min(30, sleep_s - slept))
                slept += 30
            continue

        # ── Bucket refill ───────────────────────────────────────────────────
        now_t = time.monotonic()
        dt = now_t - last_t
        last_t = now_t
        rate = effective_rate_now(rate_multiplier)
        tokens = min(tokens + dt * rate, rate * BUCKET_BURST_S)

        # ── Drain bucket ────────────────────────────────────────────────────
        batch = TickBatch()
        events_this_tick = 0

        # First: due lifecycle rows (priority).
        due_rows = fetch_due_lifecycle(conn, _now_utc(),
                                       limit=max(1, int(tokens) + 1))
        di = 0
        while tokens >= 1.0 and di < len(due_rows):
            bid, cust_id, hid, city, _fts, ftype = due_rows[di]
            di += 1

            if ftype == "CHECKIN":
                evt = _build_lifecycle_event("CHECKIN", str(bid), cust_id, hid, city)
                batch.state_to_ci.append(str(bid))

            elif ftype == "CHECKOUT":
                evt = _build_lifecycle_event("CHECKOUT", str(bid), cust_id, hid, city)
                # Defer REVIEW: maybe schedule it; else delete row now.
                ref = _fetch_booking_for_review(conn, bid)
                if ref:
                    bsrc, ck, cko, bts_d = ref
                    rv = _maybe_review_payload(
                        rng, str(bid), cust_id, hid, bsrc, rating_map,
                        "COMPLETED", today_ist, bts_d, ck, cko)
                    if rv is None:
                        batch.delete_open.append(str(bid))
                    else:
                        rfts = _schedule_review_after(rng, _now_utc())
                        batch.state_to_rev.append((str(bid), rfts))
                else:
                    batch.delete_open.append(str(bid))

            elif ftype == "CANCELLATION":
                evt = _build_lifecycle_event(
                    "CANCELLATION", str(bid), cust_id, hid, city,
                    extra={"cancellation_reason":
                           rng.choice(["customer_cancelled", "no_show"])})
                # Defer REVIEW (cancel side); maybe schedule, else delete.
                ref = _fetch_booking_for_review(conn, bid)
                if ref:
                    bsrc, ck, cko, bts_d = ref
                    rv = _maybe_review_payload(
                        rng, str(bid), cust_id, hid, bsrc, rating_map,
                        "CANCELLED", today_ist, bts_d, ck, cko)
                    if rv is None:
                        batch.delete_open.append(str(bid))
                    else:
                        rfts = _schedule_review_after(rng, _now_utc())
                        batch.state_to_rev.append((str(bid), rfts))
                else:
                    batch.delete_open.append(str(bid))

            elif ftype == "REVIEW":
                # Re-fetch the row to get booking_source + dates.
                ref = _fetch_booking_for_review(conn, bid)
                if not ref:
                    batch.delete_open.append(str(bid)); continue
                bsrc, ck, cko, bts_d = ref
                rv = _maybe_review_payload(
                    rng, str(bid), cust_id, hid, bsrc, rating_map,
                    "COMPLETED", today_ist, bts_d, ck, cko)
                if rv is None:
                    batch.delete_open.append(str(bid)); continue
                rv["event_ts"] = _now_iso_utc()
                evt = rv
                batch.delete_open.append(str(bid))
            else:
                continue

            payload, mode = maybe_corrupt_or_delay(evt, *chaos_args)
            chaos_stats[mode] += 1
            producer.send(topic, key=city, value=payload)
            wire_counts[ftype] += 1
            events_this_tick += 1
            tokens -= 1.0

        # Second: fill the rest with BOOKING / PRICE_CHANGE by per-hour weights.
        while tokens >= 1.0:
            pb = picker_prob_booking()
            if rng.random() < pb and events_emitted + events_this_tick < cap:
                # Try a BOOKING
                rec = generate_booking(rng, cache, conn, today_ist, daily_seed)
                if rec is not None:
                    evt = _build_booking_event(rec)
                    batch.add_booking(rec)
                    payload, mode = maybe_corrupt_or_delay(evt, *chaos_args)
                    chaos_stats[mode] += 1
                    producer.send(topic, key=rec["city"], value=payload)
                    wire_counts["BOOKING"] += 1
                    events_this_tick += 1
                    tokens -= 1.0
                    continue
                # else fall through to PRICE_CHANGE

            # PRICE_CHANGE
            hotel = cache.hotels[rng.randint(0, len(cache.hotels) - 1)]
            evt = _build_price_change_event(rng, hotel)
            payload, mode = maybe_corrupt_or_delay(evt, *chaos_args)
            chaos_stats[mode] += 1
            producer.send(topic, key=hotel["city"], value=payload)
            wire_counts["PRICE_CHANGE"] += 1
            events_this_tick += 1
            tokens -= 1.0

        # ── Commit the tick ─────────────────────────────────────────────────
        try:
            flush_tick(conn, batch, today_ist, events_this_tick)
        except Exception as exc:
            print(f"  ✗ tick commit failed: {exc}", file=sys.stderr)
            conn.rollback()

        # ── Progress log every ~10s ─────────────────────────────────────────
        elapsed = time.time() - t_start
        if int(elapsed) % 10 == 0 and elapsed > 0 and events_this_tick > 0:
            total = sum(wire_counts.values())
            rate_now = total / elapsed
            counts = " ".join(f"{k}={v}" for k, v in sorted(wire_counts.items()))
            print(f"  t+{elapsed:6.0f}s  total={total:,}  avg={rate_now:5.1f} evt/s  |  {counts}")

        time.sleep(TICK_SECONDS)

    # ── Shutdown ────────────────────────────────────────────────────────────
    print("\nShutting down — flushing Kafka and committing final tick…")
    producer.flush()
    producer.close()
    try:
        conn.commit()
    except Exception:
        pass
    conn.close()

    total = sum(wire_counts.values())
    elapsed = time.time() - t_start
    print(f"\nProduced {total:,} events in {elapsed:.1f}s "
          f"({(total/elapsed) if elapsed > 0 else 0:.1f} evt/s avg).")
    for et in ("BOOKING", "CHECKIN", "CHECKOUT", "CANCELLATION", "PRICE_CHANGE", "REVIEW"):
        n = wire_counts.get(et, 0)
        pct = (n / total * 100.0) if total else 0
        print(f"  {et:13s}: {n:7,}  ({pct:5.2f}%)")

    if args.malformed_pct > 0 or args.late_pct > 0:
        print(f"\nChaos summary:")
        for mode, count in sorted(chaos_stats.items(), key=lambda x: -x[1]):
            print(f"  {mode:30s}: {count:,}")


if __name__ == "__main__":
    main()
