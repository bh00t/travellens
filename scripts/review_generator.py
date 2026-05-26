"""
Shared review-generation utilities for B-030.

Used by:
  scripts/generate_review_backfill.py   — history backfill (record_source='history')
  scripts/kafka_event_producer.py       — live stream emission (record_source='stream')

All functions are pure (no DB or Kafka I/O) and deterministically seeded via
caller-supplied random.Random instances so results are reproducible across
restarts.

Generation rules (locked per B-030 spec):
  - hotel_id + customer_id stamped FROM the referenced booking
  - review_stage constrained to stages the booking actually reached
  - Negativity bias: p(review|dissatisfied) ~3.5× p(review|satisfied)
  - Overall review rate ≈ 15% at REVIEW_PROPENSITY_SCALE=0.47 (B-030a)
  - Sentiment anchored on hotel avg_rating + star_category + per-booking noise
  - channel OTA must equal booking_source; direct channels are free
  - Text: India-aware reason bank blended with sampled real Kaggle review texts
"""

import random as _random_module

# ── Review propensity by generated rating ─────────────────────────────────────
# Not every guest writes a review. Dissatisfied guests are far more likely to
# bother. These constants encode the SHAPE of the negativity bias; the overall
# rate is controlled by REVIEW_PROPENSITY_SCALE below.
#
# To change the overall review rate without touching the shape, adjust
# REVIEW_PROPENSITY_SCALE only. P_REVIEW_BY_RATING is derived from it.
#   scale=1.00 → ~32%  (original corpus rate, too high vs real-world ~5–15%)
#   scale=0.47 → ~15%  (B-030a target — realistic for Indian OTA segment)
_P_REVIEW_BASE = {
    1: 0.75,
    2: 0.65,
    3: 0.40,
    4: 0.25,
    5: 0.20,
}
REVIEW_PROPENSITY_SCALE = 0.47
P_REVIEW_BY_RATING = {k: v * REVIEW_PROPENSITY_SCALE for k, v in _P_REVIEW_BASE.items()}

# ── Stage selection weights (for bookings where multiple stages are reachable) ─
# COMPLETED → {booked, checked_in, checked_out}
STAGE_WEIGHTS_COMPLETED = [
    ("checked_out",  65),   # post-stay is the norm
    ("checked_in",   20),   # mid-stay complaint / note
    ("booked",       15),   # pre-stay anticipation / confirmation note
]
# CANCELLED → {booked, cancelled}
STAGE_WEIGHTS_CANCELLED = [
    ("cancelled",    70),   # frustration after cancellation
    ("booked",       30),   # pre-cancellation note
]
# IN_PROGRESS → {booked, checked_in}
STAGE_WEIGHTS_IN_PROGRESS = [
    ("checked_in",   70),
    ("booked",       30),
]
# BOOKED → {booked} only
STAGE_WEIGHTS_BOOKED = [("booked", 100)]

# ── OTA booking sources (channel must match booking_source when OTA) ───────────
OTA_SOURCES = {"MakeMyTrip", "OYO", "Booking.com", "Goibibo", "Agoda"}

# Direct channels (independent of booking_source)
DIRECT_CHANNELS = [
    "Email",
    "SMS",
    "WhatsApp",
    "Phone/Call",
    "Reception",
]

# ── India-aware reason bank ────────────────────────────────────────────────────
# Themes per the spec: location/access, parking+driver-quarter, cleanliness,
# utilities (toiletries/kettle/towels/TV/room-phone/hot-water/AC), power+wifi,
# staff/service, food, value, pre-arrival friction, mountain access, positive.
REASON_BANK = {
    "positive": [
        "The location was excellent — very close to the main market and railway station.",
        "Staff were incredibly helpful and went out of their way to assist.",
        "Room was spotlessly clean with fresh linen changed daily.",
        "Food quality at the in-house restaurant was outstanding, especially breakfast.",
        "Value for money is very good for this area and category.",
        "Hot water was available round the clock — no waiting in the morning.",
        "AC worked perfectly even during peak summer heat.",
        "Wi-Fi was surprisingly fast and stable throughout the stay.",
        "Free parking available; driver accommodation was provided without extra charge.",
        "Check-in was smooth and the receptionist was courteous.",
        "Wonderful view from the room, especially at sunrise.",
        "The neighbourhood was quiet — great for a peaceful night's rest.",
        "In-house restaurant served excellent South Indian and North Indian options.",
        "Lift was functional, luggage handling was prompt.",
        "Security staff were attentive and I felt safe throughout.",
        "Room phone worked and room service responded within minutes.",
        "Toiletries, towels, and kettle were all well-stocked.",
        "Proximity to the main attraction made sightseeing effortless.",
        "Very clean bathrooms with no issues at all.",
        "Overall a comfortable and pleasant stay — will definitely return.",
    ],
    "negative": [
        "AC was not working properly; the room was uncomfortably hot all night.",
        "Bathroom was not clean — tiles had stains and the floor smelled musty.",
        "Reception staff were rude and dismissive when I raised a complaint.",
        "Hot water was not available in the morning despite repeated requests.",
        "Wi-Fi was extremely slow and barely functional — unusable for work.",
        "Power cuts were frequent during the stay, sometimes for over an hour.",
        "Towels were not replaced for two days even after asking housekeeping.",
        "The kettle in the room was broken and was never replaced.",
        "There was no room phone, making it very difficult to reach the front desk.",
        "Heavy road and market noise made it impossible to sleep before midnight.",
        "Parking was extremely limited — had to park on the street far from the hotel.",
        "No accommodation for our driver, which is a basic expectation at this price.",
        "Food quality was very poor — the dal was watery and the roti was cold.",
        "Room had a musty smell throughout; needs proper ventilation.",
        "TV remote was missing and no replacement was provided despite asking twice.",
        "The mountain road to the hotel is very steep; shared autos refuse to go up.",
        "Lift was out of order for our entire stay — exhausting with heavy luggage.",
        "Bedsheets had visible stains; housekeeping standards were disappointing.",
        "Check-out process was unnecessarily slow with a long queue at reception.",
        "Hotel photos were completely misleading — actual room was much smaller.",
        "Toiletries were not replenished after the first day.",
        "TV channels were limited and picture quality was poor.",
        "Room service was extremely slow — order took over 45 minutes.",
        "No hot-water kettle despite it being listed in the amenities.",
    ],
    "cancelled": [
        "Had to cancel due to a family emergency — the cancellation process was a nightmare.",
        "Travel plans changed unexpectedly and the hotel offered no flexibility on the refund.",
        "The cancellation policy was not clearly communicated at booking time.",
        "Charged a heavy cancellation fee even though I cancelled 10 days in advance.",
        "Could not reach anyone at the hotel to discuss the cancellation — phone unanswered.",
        "Refund process was slow — still waiting after two weeks.",
        "Communication from the property was poor throughout the cancellation.",
        "Cancelled because a better-reviewed property opened nearby at the same price.",
        "Pre-arrival communication was so poor that I lost confidence and cancelled.",
        "Cancelled after reading recent reviews that mentioned cleanliness issues.",
    ],
    "no_show": [
        "Flight was severely delayed and the hotel had already allocated the room to someone else.",
        "Missed the check-in window due to an unexpected delay; hotel was unhelpful.",
    ],
}

# Closing sentences appended to assembled reason-bank texts for naturalism
_CLOSERS = [
    "Overall a mixed experience.",
    "Would not recommend at this price point.",
    "Management needs to address these issues urgently.",
    "I will give it one more chance on my next trip.",
    "Would consider returning if these issues are resolved.",
    "Not worth the price compared to alternatives nearby.",
    "The positives were real but the negatives were hard to ignore.",
    "Hope the management reads these reviews and acts.",
    "Decent stay overall despite the minor issues.",
    "Would recommend to budget travellers who can overlook minor inconveniences.",
]


# ── Core generation functions ─────────────────────────────────────────────────

def generate_rating(rng, hotel_avg_rating, stage, star_category):
    """
    Generate a 1-5 integer rating for a booking-tied review.

    Anchored on the hotel's real avg_rating with per-booking Gaussian noise
    and a stage-based adjustment reflecting when the review was written.
    Stage 'cancelled' and 'checked_in' skew more negative; 'checked_out'
    is neutral/mixed reflecting the full stay.
    """
    stage_adj = {
        "checked_out": 0.0,
        "checked_in":  -0.5,
        "booked":      -0.2,
        "cancelled":   -1.0,
    }
    base = float(hotel_avg_rating) + stage_adj.get(stage, 0.0)
    base += rng.gauss(0.0, 0.9)
    return max(1, min(5, round(base)))


def should_review(rng, rating):
    """
    Return True if the guest writes a review given the generated rating.
    Implements the negativity bias: dissatisfied guests review ~3x more often.
    """
    threshold = P_REVIEW_BY_RATING.get(rating, 0.30)
    return rng.random() < threshold


def pick_stage(rng, lifecycle_status):
    """
    Randomly select a review_stage consistent with the booking's lifecycle.

    lifecycle_status: one of 'COMPLETED', 'CANCELLED', 'IN_PROGRESS', 'BOOKED'.
    Returns a stage string from the appropriate weight table.
    """
    tables = {
        "COMPLETED":    STAGE_WEIGHTS_COMPLETED,
        "CANCELLED":    STAGE_WEIGHTS_CANCELLED,
        "IN_PROGRESS":  STAGE_WEIGHTS_IN_PROGRESS,
        "BOOKED":       STAGE_WEIGHTS_BOOKED,
    }
    weights_list = tables.get(lifecycle_status, STAGE_WEIGHTS_COMPLETED)
    stages, weights = zip(*weights_list)
    return rng.choices(stages, weights=weights, k=1)[0]


def pick_channel(rng, booking_source):
    """
    Pick a review_channel respecting the channel constraint:
    - OTA channels MUST equal the booking's booking_source.
    - Direct channels (email/SMS/WhatsApp/call/reception) are always free.
    - For non-OTA bookings (Direct / Walk-in), only direct channels are used.
    """
    if booking_source in OTA_SOURCES:
        # 50 % use the booking OTA, 50 % use a direct channel
        if rng.random() < 0.50:
            return booking_source
    return rng.choice(DIRECT_CHANNELS)


def pick_event_date(rng, stage, booking_ts_date, checkin_date, checkout_date):
    """
    Pick a calendar date consistent with when the stage-review would be written.
      booked     → [booking_ts_date, checkin_date - 1]
      checked_in → [checkin_date, checkout_date - 1]
      checked_out→ [checkout_date, checkout_date + 30]
      cancelled  → [booking_ts_date, checkin_date + 7]
    Falls back to the stage start date if the window is empty.
    """
    from datetime import date as _date, timedelta as _td

    def _rand_in(lo, hi):
        if hi < lo:
            return lo
        delta = (hi - lo).days
        return lo + _td(days=rng.randint(0, delta))

    if stage == "booked":
        lo = booking_ts_date
        hi = checkin_date - _td(days=1)
        return _rand_in(lo, hi)
    elif stage == "checked_in":
        lo = checkin_date
        hi = checkout_date - _td(days=1)
        return _rand_in(lo, hi)
    elif stage == "checked_out":
        lo = checkout_date
        hi = checkout_date + _td(days=30)
        return _rand_in(lo, hi)
    elif stage in ("cancelled", "no_show"):
        lo = booking_ts_date
        hi = checkin_date + _td(days=7)
        return _rand_in(lo, hi)
    return booking_ts_date


def _pick_sampled_text(rng, rating, seed_texts_by_bucket):
    """
    Sample a real Kaggle review text from the bucket closest to `rating`.
    seed_texts_by_bucket: dict with keys 'low' (1-2), 'mid' (3), 'high' (4-5).
    Returns None if the bucket is empty.
    """
    if rating <= 2:
        bucket = "low"
    elif rating == 3:
        bucket = "mid"
    else:
        bucket = "high"
    texts = seed_texts_by_bucket.get(bucket, [])
    if not texts:
        return None
    return rng.choice(texts)


def _assemble_reason_text(rng, rating, stage):
    """
    Build a review text from the India-aware reason bank.
    Picks 2–3 snippets appropriate for the sentiment and stage, then appends
    a closing sentence.
    """
    if stage in ("cancelled", "no_show"):
        pool = REASON_BANK["cancelled"] + (REASON_BANK["no_show"] if stage == "no_show" else [])
    elif rating <= 2:
        pool = REASON_BANK["negative"]
    elif rating >= 4:
        pool = REASON_BANK["positive"]
    else:
        # neutral: mix positive and negative
        pool = REASON_BANK["positive"][:6] + REASON_BANK["negative"][:8]

    count = rng.randint(2, 3)
    snippets = rng.sample(pool, min(count, len(pool)))
    closer = rng.choice(_CLOSERS)
    return " ".join(snippets) + " " + closer


def pick_text(rng, rating, stage, seed_texts_by_bucket=None):
    """
    Generate review text by blending the India reason bank with real Kaggle text.

    Blend strategy:
      40% — pure sampled real Kaggle text (if available, else reason bank)
      35% — pure India reason bank assembly
      25% — blended: 1 reason snippet + tail of a sampled real review

    All choices are seeded through `rng` for reproducibility.
    """
    mode_r = rng.random()

    if seed_texts_by_bucket is None:
        # No real text available — always use reason bank
        return _assemble_reason_text(rng, rating, stage)

    sampled = _pick_sampled_text(rng, rating, seed_texts_by_bucket)

    if mode_r < 0.40 and sampled:
        # Pure sampled real text
        return sampled
    elif mode_r < 0.75:
        # Pure reason bank
        return _assemble_reason_text(rng, rating, stage)
    else:
        # Blended: one reason snippet as opener, then sampled text
        if stage in ("cancelled", "no_show"):
            pool = REASON_BANK["cancelled"]
        elif rating <= 2:
            pool = REASON_BANK["negative"]
        elif rating >= 4:
            pool = REASON_BANK["positive"]
        else:
            pool = REASON_BANK["positive"][:5] + REASON_BANK["negative"][:6]
        opener = rng.choice(pool)
        body = sampled if sampled else _assemble_reason_text(rng, rating, stage)
        return opener + " " + body


def make_review_event_dict(booking_id, customer_id, hotel_id,
                           booking_source, hotel_avg_rating, star_category,
                           lifecycle_status, sim_day,
                           booking_ts_date, checkin_date, checkout_date,
                           seed_texts_by_bucket=None):
    """
    Attempt to build a REVIEW wire-event dict for the given booking.

    Returns a dict ready to send to Kafka (or None if the negativity-bias
    draw decides not to generate a review for this booking).

    The RNG is seeded on `f"{booking_id}|review"` so the same booking always
    produces the same review (or non-review) across restarts.
    """
    import uuid as _uuid
    rng = _random_module.Random(f"{booking_id}|review")

    stage  = pick_stage(rng, lifecycle_status)
    rating = generate_rating(rng, hotel_avg_rating, stage, star_category)

    if not should_review(rng, rating):
        return None

    channel    = pick_channel(rng, booking_source)
    text       = pick_text(rng, rating, stage, seed_texts_by_bucket)
    event_date = pick_event_date(rng, stage, booking_ts_date, checkin_date, checkout_date)

    # Deterministic review_id so ON CONFLICT (review_id) DO NOTHING is idempotent.
    REVIEW_NS = _uuid.UUID("b030cafe-feed-4ead-babe-0000deadbeef")
    review_id = _uuid.uuid5(REVIEW_NS, str(booking_id))

    return {
        "event_type":     "REVIEW",
        "review_id":      str(review_id),
        "booking_id":     str(booking_id),
        "customer_id":    customer_id,
        "hotel_id":       hotel_id,
        "review_stage":   stage,
        "rating":         rating,
        "review_text":    text,
        "review_channel": channel,
        "event_date":     event_date.isoformat() if hasattr(event_date, "isoformat") else str(event_date),
    }
