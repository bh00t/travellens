"""
TravelLens India — Stage 3: Customer Behavior Correlation Fix
==============================================================
Regenerates fact_bookings.csv with customer segments actually driving:
  - Room tier preference (backpacker → budget, honeymoon → luxury, etc.)
  - Nights stayed (business 1-2, honeymoon 5-7, family 3-4)
  - Number of guests (solo 1, couple 2, family 3-4)
  - Travel purpose ↔ destination affinity (Religious customers → Varanasi/Rishikesh)
  - Cancellation rate (business cancels more than honeymoon)

ALSO adds:
  ref_state_centroids.csv — lat/lng per state for geo-flow visualizations

Run AFTER generate_stage2.py.
"""

import json
import os
import random
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

SEED = 42
OUT_DIR = Path(os.getenv("DATA_DIR", "data"))

HISTORY_START = date(2024, 1, 1)
HISTORY_END = date(2026, 5, 16)
NUM_BOOKINGS = 1_000_000

random.seed(SEED)
np.random.seed(SEED)

# ---------------------------------------------------------------------------
# Load Stage 1 + 2 outputs
# ---------------------------------------------------------------------------
print("Loading existing datasets...")
hotels = pd.read_csv(OUT_DIR / "hotel_master.csv")
locations = pd.read_csv(OUT_DIR / "dim_location.csv")
room_types = pd.read_csv(OUT_DIR / "dim_room_type.csv")
customers = pd.read_csv(OUT_DIR / "dim_customer.csv")
dim_date = pd.read_csv(OUT_DIR / "dim_date.csv")
holidays = pd.read_csv(OUT_DIR / "public_holidays.csv")

date_to_id = {d: i for i, d in enumerate(
    pd.to_datetime(dim_date["full_date"]).dt.date.tolist(), start=1
)}
holiday_dates = set(pd.to_datetime(holidays["holiday_date"]).dt.date)
high_impact_dates = set(
    pd.to_datetime(holidays[holidays["demand_impact"] == "HIGH"]["holiday_date"]).dt.date
)

loc_by_id = locations.set_index("location_id").to_dict("index")
hotel_to_city = {}
hotel_to_zone = {}
hotel_to_state = {}
hotel_to_loc = {}
for _, h in hotels.iterrows():
    loc = loc_by_id[h["location_id"]]
    hotel_to_city[h["hotel_id"]] = loc["city"]
    hotel_to_zone[h["hotel_id"]] = loc["tourism_zone"]
    hotel_to_state[h["hotel_id"]] = loc["state"]
    hotel_to_loc[h["hotel_id"]] = h["location_id"]

# Group room types by hotel AND by price tier within hotel
rt_by_hotel_tier = {}  # hotel_id -> {tier: [room records]}
for _, rt in room_types.iterrows():
    hid = rt["hotel_id"]
    rt_by_hotel_tier.setdefault(hid, {}).setdefault(rt["price_tier"], []).append({
        "room_type_id": rt["room_type_id"],
        "type_name": rt["type_name"],
        "base_price_inr": rt["base_price_inr"],
        "capacity": rt["capacity"],
    })


# ===========================================================================
# 1. ref_state_centroids.csv — geographic centroid per Indian state
# ===========================================================================
def gen_state_centroids():
    # Real centroids (approximate geographic center of each state/UT)
    centroids = [
        ("Andhra Pradesh",     15.91, 79.74),
        ("Arunachal Pradesh",  28.22, 94.73),
        ("Assam",              26.20, 92.94),
        ("Bihar",              25.10, 85.31),
        ("Chhattisgarh",       21.28, 81.87),
        ("Goa",                15.30, 74.12),
        ("Gujarat",            22.26, 71.19),
        ("Haryana",            29.06, 76.09),
        ("Himachal Pradesh",   31.10, 77.17),
        ("Jharkhand",          23.61, 85.28),
        ("Karnataka",          15.32, 75.71),
        ("Kerala",             10.85, 76.27),
        ("Madhya Pradesh",     22.97, 78.66),
        ("Maharashtra",        19.75, 75.71),
        ("Manipur",            24.66, 93.91),
        ("Meghalaya",          25.47, 91.37),
        ("Mizoram",            23.16, 92.94),
        ("Nagaland",           26.16, 94.56),
        ("Odisha",             20.95, 85.10),
        ("Punjab",             31.15, 75.34),
        ("Rajasthan",          27.02, 74.22),
        ("Sikkim",             27.53, 88.51),
        ("Tamil Nadu",         11.13, 78.66),
        ("Telangana",          18.11, 79.02),
        ("Tripura",            23.94, 91.99),
        ("Uttar Pradesh",      26.85, 80.95),
        ("Uttarakhand",        30.07, 79.02),
        ("West Bengal",        22.99, 87.86),
        # UTs
        ("Delhi NCR",          28.61, 77.21),
        ("Jammu & Kashmir",    33.78, 76.57),
        ("Ladakh",             34.21, 77.65),
        ("Chandigarh",         30.73, 76.78),
        ("Puducherry",         11.91, 79.81),
        ("Andaman & Nicobar",  11.74, 92.65),
        ("Dadra & Nagar Haveli", 20.18, 73.02),
        ("Daman & Diu",        20.40, 72.83),
        ("Lakshadweep",        10.57, 72.64),
        ("Other",              20.59, 78.96),  # India centroid for "Other"
    ]
    rows = [{"state_name": n, "latitude": lat, "longitude": lng} for n, lat, lng in centroids]
    df = pd.DataFrame(rows)
    df.to_csv(OUT_DIR / "ref_state_centroids.csv", index=False)
    return df


# ===========================================================================
# 2. Customer behavior matrices
# ===========================================================================
# Segment → price tier preference (probabilities)
SEGMENT_TIER_PREF = {
    "Backpacker":   {"BUDGET": 0.75, "MID": 0.20, "PREMIUM": 0.04, "LUXURY": 0.01},
    "Leisure_FIT":  {"BUDGET": 0.30, "MID": 0.40, "PREMIUM": 0.20, "LUXURY": 0.10},
    "Family":       {"BUDGET": 0.25, "MID": 0.45, "PREMIUM": 0.22, "LUXURY": 0.08},
    "Business":     {"BUDGET": 0.05, "MID": 0.30, "PREMIUM": 0.45, "LUXURY": 0.20},
    "Group_Tour":   {"BUDGET": 0.40, "MID": 0.40, "PREMIUM": 0.15, "LUXURY": 0.05},
    "Honeymoon":    {"BUDGET": 0.05, "MID": 0.20, "PREMIUM": 0.40, "LUXURY": 0.35},
}

# Segment → nights stayed distribution (weighted choices)
SEGMENT_NIGHTS = {
    "Backpacker":   ([1, 2, 3, 4, 5, 7, 10],     [0.10, 0.25, 0.25, 0.15, 0.10, 0.10, 0.05]),
    "Leisure_FIT":  ([1, 2, 3, 4, 5, 7],          [0.10, 0.30, 0.30, 0.15, 0.10, 0.05]),
    "Family":       ([2, 3, 4, 5, 6, 7],          [0.10, 0.30, 0.30, 0.15, 0.10, 0.05]),
    "Business":     ([1, 2, 3, 4],                [0.40, 0.35, 0.18, 0.07]),
    "Group_Tour":   ([2, 3, 4, 5, 7],             [0.15, 0.30, 0.30, 0.15, 0.10]),
    "Honeymoon":    ([3, 4, 5, 6, 7, 8, 10],      [0.10, 0.20, 0.25, 0.20, 0.15, 0.07, 0.03]),
}

# Segment → number of guests distribution
SEGMENT_GUESTS = {
    "Backpacker":   ([1, 2],          [0.70, 0.30]),
    "Leisure_FIT":  ([1, 2, 3],       [0.30, 0.55, 0.15]),
    "Family":       ([2, 3, 4],       [0.10, 0.45, 0.45]),
    "Business":     ([1, 2],          [0.80, 0.20]),
    "Group_Tour":   ([2, 3, 4],       [0.30, 0.40, 0.30]),
    "Honeymoon":    ([2],             [1.00]),
}

# Segment → cancellation rate
SEGMENT_CANCEL_RATE = {
    "Backpacker":   0.18,   # plans change often
    "Leisure_FIT":  0.10,
    "Family":       0.08,   # commit early, rarely cancel
    "Business":     0.22,   # meetings shift constantly
    "Group_Tour":   0.06,   # group commitments hard to cancel
    "Honeymoon":    0.04,   # most committed booking
}

# Travel purpose → preferred zones (probability)
PURPOSE_ZONE_PREF = {
    "Heritage":     {"Heritage": 0.65, "Metro": 0.20, "Pilgrimage": 0.10, "_other": 0.05},
    "Beach":        {"Beach": 0.70, "Backwater": 0.15, "Metro": 0.10, "_other": 0.05},
    "Hill_Station": {"Hill Station": 0.75, "Metro": 0.10, "_other": 0.15},
    "Religious":    {"Pilgrimage": 0.75, "Heritage": 0.10, "Metro": 0.05, "_other": 0.10},
    "Business":     {"Metro": 0.80, "_other": 0.20},
    "Wildlife":     {"Wildlife": 0.55, "Hill Station": 0.15, "Backwater": 0.10, "_other": 0.20},
    "Wellness":     {"Hill Station": 0.40, "Backwater": 0.25, "Beach": 0.20, "_other": 0.15},
    "Adventure":    {"Hill Station": 0.50, "Wildlife": 0.20, "Beach": 0.15, "_other": 0.15},
    "Shopping":     {"Metro": 0.85, "_other": 0.15},
}

# OTA preference by tier (same as Stage 2)
OTA_DIST_BY_TIER = {
    "BUDGET":  [("OYO",0.45),("MakeMyTrip",0.25),("Goibibo",0.08),("Direct",0.12),("Walk-in",0.10)],
    "MID":     [("MakeMyTrip",0.32),("Goibibo",0.18),("Booking.com",0.15),("OYO",0.05),("Direct",0.18),("Walk-in",0.07),("Agoda",0.05)],
    "PREMIUM": [("MakeMyTrip",0.25),("Booking.com",0.22),("Goibibo",0.12),("Direct",0.25),("Agoda",0.10),("Walk-in",0.06)],
    "LUXURY":  [("Direct",0.42),("Booking.com",0.22),("MakeMyTrip",0.16),("Agoda",0.12),("Goibibo",0.05),("Walk-in",0.03)],
}

# Segment-specific OTA bias (overrides tier default for some)
SEGMENT_OTA_BIAS = {
    "Backpacker":  {"OYO": 1.5, "Direct": 0.3},  # OYO heavy, rarely Direct
    "Business":    {"Direct": 1.5, "MakeMyTrip": 1.2, "Walk-in": 0.3},
    "Honeymoon":   {"Direct": 1.4, "Booking.com": 1.2},
}

ZONE_SEASONALITY = {
    "Beach":        {1:2.5, 2:2.4, 3:1.2, 4:0.8, 5:0.6, 6:0.25, 7:0.2, 8:0.25, 9:0.4, 10:1.3, 11:2.6, 12:3.0},
    "Heritage":     {1:2.5, 2:2.4, 3:1.4, 4:0.5, 5:0.4, 6:0.4, 7:0.6, 8:0.7, 9:0.9, 10:2.0, 11:2.6, 12:2.8},
    "Hill Station": {1:0.7, 2:0.8, 3:1.0, 4:1.8, 5:2.5, 6:2.4, 7:1.2, 8:1.0, 9:1.3, 10:2.0, 11:1.0, 12:0.8},
    "Pilgrimage":   {1:1.0, 2:1.0, 3:2.5, 4:2.5, 5:1.2, 6:0.8, 7:0.8, 8:0.9, 9:1.3, 10:2.5, 11:2.6, 12:1.2},
    "Wildlife":     {1:2.4, 2:2.4, 3:2.0, 4:1.0, 5:0.6, 6:0.3, 7:0.3, 8:0.3, 9:0.6, 10:1.4, 11:2.4, 12:2.5},
    "Metro":        {1:1.2, 2:1.3, 3:1.3, 4:1.0, 5:0.95, 6:0.9, 7:0.9, 8:0.95, 9:1.0, 10:1.3, 11:1.3, 12:1.4},
    "Backwater":    {1:2.4, 2:1.8, 3:1.0, 4:0.6, 5:0.4, 6:0.3, 7:0.4, 8:0.5, 9:1.6, 10:2.2, 11:2.6, 12:2.6},
}


def weighted_choice(pairs):
    items, weights = zip(*pairs)
    return random.choices(items, weights=weights, k=1)[0]


# ===========================================================================
# 3. REGENERATE fact_bookings — customer-driven
# ===========================================================================

# Pre-compute: customers grouped by segment for fast sampling
customer_records = customers.to_dict("records")
customers_by_segment = {}
for c in customer_records:
    customers_by_segment.setdefault(c["customer_segment"], []).append(c)

# Pre-compute: hotels grouped by (zone, tier) for fast lookup
hotels_by_zone_tier = {}
hotel_records = hotels.to_dict("records")
for h in hotel_records:
    if h["is_active"] is False or h["is_active"] == "FALSE":
        continue
    zone = hotel_to_zone[h["hotel_id"]]
    tier = h["price_tier_id"]
    hotels_by_zone_tier.setdefault((zone, tier), []).append(h)

# Also: tier-only fallback (for when zone preference doesn't match any hotel)
hotels_by_tier = {}
for h in hotel_records:
    if h["is_active"] is False or h["is_active"] == "FALSE":
        continue
    hotels_by_tier.setdefault(h["price_tier_id"], []).append(h)

# City popularity (within a zone, weight hotels by city)
city_popularity = {
    "Mumbai":2.5,"Delhi":2.5,"Bangalore":2.2,"Goa":2.4,"Jaipur":2.0,
    "Agra":1.8,"Udaipur":1.6,"Chennai":1.6,"Hyderabad":1.6,"Manali":1.5,
    "Kochi":1.4,"Pune":1.4,"Shimla":1.4,"Varanasi":1.4,"Rishikesh":1.3,
    "Calangute":1.3,"Kolkata":1.3,"Ahmedabad":1.3,
}


def pick_zone_for_purpose(purpose):
    """Sample a zone based on customer's travel purpose."""
    prefs = PURPOSE_ZONE_PREF.get(purpose, {"_other": 1.0})
    zones, probs = [], []
    for z, p in prefs.items():
        if z == "_other":
            # Distribute "other" across remaining zones
            other_zones = [zz for zz in ZONE_SEASONALITY if zz not in prefs]
            per_zone = p / len(other_zones) if other_zones else 0
            for oz in other_zones:
                zones.append(oz)
                probs.append(per_zone)
        else:
            zones.append(z)
            probs.append(p)
    return random.choices(zones, weights=probs, k=1)[0]


HOTEL_WEIGHTS_CACHE = {}  # (zone, tier) -> (hotels, normalized_weights)


def precompute_hotel_weights():
    for key, candidates in hotels_by_zone_tier.items():
        weights = np.array([
            h["total_rooms"] * city_popularity.get(hotel_to_city[h["hotel_id"]], 1.0)
            for h in candidates
        ], dtype=float)
        HOTEL_WEIGHTS_CACHE[key] = (candidates, weights / weights.sum())
    # Tier-only fallback
    for tier, candidates in hotels_by_tier.items():
        weights = np.array([
            h["total_rooms"] * city_popularity.get(hotel_to_city[h["hotel_id"]], 1.0)
            for h in candidates
        ], dtype=float)
        HOTEL_WEIGHTS_CACHE[("_TIER_ONLY", tier)] = (candidates, weights / weights.sum())


def pick_hotel_for_customer(customer):
    """Pick a hotel matching customer's segment preference and travel purpose."""
    segment = customer["customer_segment"]
    purpose = customer["travel_purpose"]

    tier_prefs = SEGMENT_TIER_PREF[segment]
    tier = weighted_choice(list(tier_prefs.items()))
    zone = pick_zone_for_purpose(purpose)

    cache_key = (zone, tier)
    if cache_key not in HOTEL_WEIGHTS_CACHE:
        cache_key = ("_TIER_ONLY", tier)
    if cache_key not in HOTEL_WEIGHTS_CACHE:
        # absolute fallback — uniform across all active hotels
        return random.choice(hotel_records)

    candidates, weights = HOTEL_WEIGHTS_CACHE[cache_key]
    idx = np.random.choice(len(candidates), p=weights)
    return candidates[idx]


def precompute_zone_date_weights():
    """Build once: zone -> (dates_array, normalized_weights_array)."""
    history_dates = sorted([d for d in date_to_id if HISTORY_START <= d <= HISTORY_END])
    zone_weights = {}
    for zone in ZONE_SEASONALITY:
        weights = []
        for d in history_dates:
            base = ZONE_SEASONALITY[zone].get(d.month, 1.0)
            is_weekend = d.weekday() >= 5
            if zone == "Metro":
                base *= 1.1 if not is_weekend else 0.85
            else:
                base *= 1.3 if is_weekend else 1.0
            if d in high_impact_dates:
                base *= 2.2
            elif d in holiday_dates:
                base *= 1.4
            weights.append(base)
        w = np.array(weights)
        zone_weights[zone] = (history_dates, w / w.sum())
    return zone_weights


ZONE_DATE_CACHE = None  # populated in main


def pick_date_for_zone(zone):
    """Sample a check-in date using precomputed weights."""
    dates, weights = ZONE_DATE_CACHE[zone]
    idx = np.random.choice(len(dates), p=weights)
    return dates[idx]


def pick_room_for_hotel_tier(hotel_id, tier):
    """Pick a room type at this hotel, preferring the requested tier band."""
    rt_options = rt_by_hotel_tier.get(hotel_id, {})
    if tier in rt_options:
        return random.choice(rt_options[tier])
    # Fallback: any room at this hotel
    all_rooms = [r for tier_rooms in rt_options.values() for r in tier_rooms]
    return random.choice(all_rooms) if all_rooms else None


def pick_ota_source(tier, segment, hotel):
    """OTA selection with segment bias."""
    base_pairs = dict(OTA_DIST_BY_TIER[tier])
    # Apply segment bias
    bias = SEGMENT_OTA_BIAS.get(segment, {})
    for ota, mult in bias.items():
        if ota in base_pairs:
            base_pairs[ota] *= mult
    # OYO chain hotels skew toward OYO
    if hotel.get("chain_name") == "OYO":
        base_pairs["OYO"] = base_pairs.get("OYO", 0.1) * 3
    # Normalize
    total = sum(base_pairs.values())
    normalized = [(k, v/total) for k, v in base_pairs.items()]
    return weighted_choice(normalized)


def gen_fact_bookings_v3():
    print(f"  Generating {NUM_BOOKINGS:,} customer-driven bookings...")

    # Determine each booking's customer first
    # Customer activity: repeat customers book more (3-8x), one-timers book once
    customer_weights = np.array([
        (5.0 if c["is_repeat_customer"] in (True, "TRUE") else 1.0)
        for c in customer_records
    ])
    customer_weights = customer_weights / customer_weights.sum()

    booking_customer_idx = np.random.choice(
        len(customer_records), size=NUM_BOOKINGS, p=customer_weights
    )

    rows = []
    for i, ci in enumerate(booking_customer_idx):
        customer = customer_records[ci]
        segment = customer["customer_segment"]

        # 1. Pick hotel matching customer profile
        hotel = pick_hotel_for_customer(customer)
        hotel_id = hotel["hotel_id"]
        tier = hotel["price_tier_id"]
        zone = hotel_to_zone[hotel_id]

        # 2. Pick room (preferring customer's tier band)
        rt = pick_room_for_hotel_tier(hotel_id, tier)
        if rt is None:
            continue

        # 3. Pick check-in date with seasonality
        checkin = pick_date_for_zone(zone)

        # 4. Nights stayed — segment-driven
        nights_options, nights_probs = SEGMENT_NIGHTS[segment]
        nights = int(np.random.choice(nights_options, p=nights_probs))

        # 5. Number of guests — segment-driven, capped by room capacity
        guest_options, guest_probs = SEGMENT_GUESTS[segment]
        guests = int(np.random.choice(guest_options, p=guest_probs))
        guests = min(guests, rt["capacity"])

        # 6. Price — base × seasonality × holiday surge × noise
        seasonality_mult = ZONE_SEASONALITY.get(zone, ZONE_SEASONALITY["Metro"]).get(checkin.month, 1.0)
        price_mult = 0.7 + 0.45 * seasonality_mult / 3.0
        if checkin in high_impact_dates:
            price_mult *= 1.5
        nightly_rate = int(rt["base_price_inr"] * price_mult * random.uniform(0.92, 1.08))
        revenue = nightly_rate * nights

        # 7. Cancellation — segment-driven
        cancel_prob = SEGMENT_CANCEL_RATE[segment]
        # Boost cancellation slightly during holidays (more uncertainty in plans)
        if checkin in high_impact_dates:
            cancel_prob *= 1.15
        is_cancelled = random.random() < cancel_prob

        # 8. OTA source
        source = pick_ota_source(tier, segment, hotel)

        # 9. Lead time — business books late, honeymoon books early
        if segment == "Business":
            lead_days = int(np.random.choice([0,1,2,3,7,14], p=[0.20,0.20,0.20,0.15,0.15,0.10]))
        elif segment == "Honeymoon":
            lead_days = int(np.random.choice([14,30,60,90,120], p=[0.05,0.20,0.40,0.25,0.10]))
        elif segment == "Family":
            lead_days = int(np.random.choice([7,14,30,45,60], p=[0.15,0.25,0.30,0.20,0.10]))
        else:
            lead_days = int(np.random.choice([0,1,3,7,14,30,60], p=[0.08,0.12,0.15,0.25,0.20,0.15,0.05]))

        event_ts = datetime.combine(checkin - timedelta(days=lead_days), datetime.min.time()) \
                   + timedelta(hours=random.randint(8,23), minutes=random.randint(0,59))

        checkout = checkin + timedelta(days=nights)

        rows.append({
            "booking_id":       str(uuid.uuid4()),
            "hotel_id":         hotel_id,
            "customer_id":      customer["customer_id"],
            "room_type_id":     rt["room_type_id"],
            "location_id":      hotel_to_loc[hotel_id],
            "date_id":          date_to_id[checkin],
            "checkin_date":     checkin.isoformat(),
            "checkout_date":    checkout.isoformat(),
            "nights_stayed":    nights,
            "num_guests":       guests,
            "nightly_rate_inr": nightly_rate,
            "revenue_inr":      revenue,
            "booking_source":   source,
            "is_cancelled":     "TRUE" if is_cancelled else "FALSE",
            "booking_ts":       event_ts.isoformat(sep=" ", timespec="seconds"),
            "event_ts":         event_ts.isoformat(sep=" ", timespec="seconds"),
        })

        if (i+1) % 100_000 == 0:
            print(f"    {i+1:,} / {NUM_BOOKINGS:,}")

    df = pd.DataFrame(rows)
    df.to_csv(OUT_DIR / "fact_bookings.csv", index=False)
    return df


# ===========================================================================
# Validation
# ===========================================================================
def validate():
    print("\n" + "=" * 65)
    print("VALIDATION — Stage 3 (customer behavior correlation)")
    print("=" * 65)

    fb = pd.read_csv(OUT_DIR / "fact_bookings.csv")
    rt = pd.read_csv(OUT_DIR / "dim_room_type.csv")
    cust = pd.read_csv(OUT_DIR / "dim_customer.csv")
    loc = pd.read_csv(OUT_DIR / "dim_location.csv")

    df = fb.merge(rt[["room_type_id","type_name","price_tier"]], on="room_type_id") \
           .merge(cust[["customer_id","customer_segment","travel_purpose"]], on="customer_id") \
           .merge(loc[["location_id","tourism_zone","city"]], on="location_id")

    print(f"\nFinal row count: {len(fb):,}")

    print("\nTIER PREFERENCE BY SEGMENT (expect strong differences now):")
    for seg in ["Backpacker", "Business", "Honeymoon", "Family"]:
        s = df[df["customer_segment"] == seg]
        tiers = s["price_tier"].value_counts(normalize=True) * 100
        line = f"  {seg:12s}"
        for t in ["BUDGET", "MID", "PREMIUM", "LUXURY"]:
            line += f"  {t}:{tiers.get(t,0):4.0f}%"
        print(line)

    print("\nAVG NIGHTS BY SEGMENT (expect different durations):")
    by_seg = df.groupby("customer_segment")["nights_stayed"].agg(["mean", "median"]).round(2)
    for seg, row in by_seg.iterrows():
        print(f"  {seg:12s}  avg={row['mean']}  median={int(row['median'])}")

    print("\nAVG GUESTS BY SEGMENT:")
    by_seg = df.groupby("customer_segment")["num_guests"].mean().round(2)
    for seg, val in by_seg.items():
        print(f"  {seg:12s}  {val}")

    print("\nCANCELLATION RATE BY SEGMENT:")
    by_seg = df.groupby("customer_segment")["is_cancelled"].apply(
        lambda s: (s == "TRUE").mean() * 100
    ).round(1)
    for seg, val in by_seg.items():
        print(f"  {seg:12s}  {val}%")

    print("\nTRAVEL PURPOSE → ZONE (top zone per purpose):")
    for purpose in ["Heritage","Beach","Religious","Business","Hill_Station","Wildlife"]:
        p = df[df["travel_purpose"] == purpose]
        if len(p) == 0:
            continue
        top_zone = p["tourism_zone"].value_counts(normalize=True).head(2)
        line = f"  {purpose:14s}"
        for zone, pct in top_zone.items():
            line += f"  {zone}:{pct*100:4.0f}%"
        print(line)

    print("\nBOOKING SOURCE BY SEGMENT (top source):")
    for seg in df["customer_segment"].unique():
        s = df[df["customer_segment"] == seg]
        top = s["booking_source"].value_counts(normalize=True).head(1)
        for src, pct in top.items():
            print(f"  {seg:12s}  top source: {src} ({pct*100:.0f}%)")

    print(f"\nFINANCIAL RECAP:")
    cancelled = (fb["is_cancelled"] == "TRUE") | (fb["is_cancelled"] == True)
    print(f"  Overall cancellation rate: {cancelled.mean()*100:.1f}%")
    print(f"  Total revenue (cr ₹):      {fb[~cancelled]['revenue_inr'].sum()/1e7:.0f}")
    print(f"  Avg booking value:         ₹{fb[~cancelled]['revenue_inr'].mean():.0f}")


# ===========================================================================
# Run
# ===========================================================================
if __name__ == "__main__":
    print(f"Stage 3 generator → {OUT_DIR}\n")

    print("Precomputing caches...", end=" ", flush=True)
    ZONE_DATE_CACHE = precompute_zone_date_weights()
    precompute_hotel_weights()
    print("done")

    print("[1/2] ref_state_centroids.csv ...", end=" ", flush=True)
    gen_state_centroids()
    print("done")

    print("[2/2] fact_bookings.csv (regenerating with customer behavior) ...")
    gen_fact_bookings_v3()
    print("       done")

    validate()
    print("\n✓ Stage 3 complete.")
