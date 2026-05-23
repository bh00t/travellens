"""
TravelLens India — Stage 2 Dataset Generator
============================================
Reads the Stage 1 outputs from OUT_DIR and adds:

NEW datasets:
  fact_bookings.csv         (1M rows, Jan 2024 → 2026-05-16)
  fact_price_events.csv     (price change history)
  dim_date.csv              (date spine 2020-2026 with India seasonality)
  dim_customer.csv          (20K customers with segments)
  ref_price_tiers.csv       (4 rows — Budget/Mid/Premium/Luxury bands)

REGENERATED with quality fixes:
  reviews_raw.csv           (power-law hotel distribution, seasonal dates,
                             amenity correlation, verbose reviewers, larger name pool)

Run AFTER generate_datasets.py.
"""

import json
import os
import random
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
SEED = 42
OUT_DIR = Path(os.getenv("DATA_DIR", "data"))

# Fact bookings: Heavy volume, full history
HISTORY_START = date(2024, 1, 1)
HISTORY_END   = date(2026, 5, 16)         # "today"
NUM_BOOKINGS  = 1_000_000

# Customers
NUM_CUSTOMERS = 20_000

# Price events: roughly one per (hotel, room_type, week)
PRICE_EVENT_WEEKS = 30                     # randomly sample ~30 weeks per hotel-room

# Reviews: keep at 30K but redistribute with power law + seasonality
NUM_REVIEWS = 30_000
IRRELEVANT_REVIEW_FRAC = 0.05
VERBOSE_REVIEW_FRAC = 0.05                 # 5% are 200+ words

random.seed(SEED)
np.random.seed(SEED)

# ---------------------------------------------------------------------------
# Load Stage 1 outputs
# ---------------------------------------------------------------------------
print("Loading Stage 1 datasets...")
hotels = pd.read_csv(OUT_DIR / "hotel_master.csv")
locations = pd.read_csv(OUT_DIR / "dim_location.csv")
room_types = pd.read_csv(OUT_DIR / "dim_room_type.csv")
holidays = pd.read_csv(OUT_DIR / "public_holidays.csv")

# Build lookup tables
loc_by_id = locations.set_index("location_id").to_dict("index")
hotel_to_city = {}
hotel_to_zone = {}
hotel_to_state = {}
for _, h in hotels.iterrows():
    loc = loc_by_id[h["location_id"]]
    hotel_to_city[h["hotel_id"]] = loc["city"]
    hotel_to_zone[h["hotel_id"]] = loc["tourism_zone"]
    hotel_to_state[h["hotel_id"]] = loc["state"]

rt_by_hotel = room_types.groupby("hotel_id").apply(
    lambda g: g[["room_type_id", "type_name", "base_price_inr", "capacity"]].to_dict("records"),
    include_groups=False
).to_dict()

holiday_dates = set(pd.to_datetime(holidays["holiday_date"]).dt.date)
high_impact_dates = set(
    pd.to_datetime(holidays[holidays["demand_impact"] == "HIGH"]["holiday_date"]).dt.date
)


# ===========================================================================
# 1. ref_price_tiers.csv — tiny reference table
# ===========================================================================
def gen_ref_price_tiers():
    rows = [
        {"tier_id": "BUDGET",  "tier_name": "Budget",  "min_price_inr":   349, "max_price_inr":  1999, "description": "Affordable stays under 2000 INR/night — OYO, budget independent hotels"},
        {"tier_id": "MID",     "tier_name": "Mid",     "min_price_inr":  2000, "max_price_inr":  4999, "description": "Mid-range 3-star properties — branded chains and quality independents"},
        {"tier_id": "PREMIUM", "tier_name": "Premium", "min_price_inr":  5000, "max_price_inr":  9999, "description": "Upscale 4-star hotels — Lemon Tree Premier, ITC, Marriott"},
        {"tier_id": "LUXURY",  "tier_name": "Luxury",  "min_price_inr": 10000, "max_price_inr": 35000, "description": "5-star and heritage luxury — Taj, Oberoi, Leela"},
    ]
    df = pd.DataFrame(rows)
    df.to_csv(OUT_DIR / "ref_price_tiers.csv", index=False)
    return df


# ===========================================================================
# 2. dim_date.csv — date spine 2020-01-01 → 2026-12-31 with India seasonality
# ===========================================================================
def get_season(month: int) -> str:
    """India tourism season bands."""
    if month in (10, 11, 12, 1, 2):  return "Peak"
    if month in (3,):                return "Shoulder"
    if month in (4, 5, 6):           return "Summer"
    if month in (7, 8, 9):           return "Monsoon"
    return "Shoulder"


def gen_dim_date():
    start = date(2020, 1, 1)
    end = date(2026, 12, 31)
    rows = []
    date_id = 1
    cur = start
    while cur <= end:
        weekday = cur.weekday()  # 0=Mon
        rows.append({
            "date_id":      date_id,
            "full_date":    cur.isoformat(),
            "day_of_month": cur.day,
            "day_of_week":  ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"][weekday],
            "day_num":      weekday + 1,
            "week_of_year": cur.isocalendar()[1],
            "month":        cur.month,
            "month_name":   cur.strftime("%B"),
            "quarter":      f"Q{(cur.month-1)//3 + 1}",
            "year":         cur.year,
            "is_weekend":   "TRUE" if weekday >= 5 else "FALSE",
            "is_holiday":   "TRUE" if cur in holiday_dates else "FALSE",
            "is_high_demand_holiday": "TRUE" if cur in high_impact_dates else "FALSE",
            "season":       get_season(cur.month),
        })
        date_id += 1
        cur += timedelta(days=1)
    df = pd.DataFrame(rows)
    df.to_csv(OUT_DIR / "dim_date.csv", index=False)
    return df


# ===========================================================================
# 3. dim_customer.csv — customer master with segments
# ===========================================================================
INDIAN_HOME_STATES = [
    ("Maharashtra", 0.14), ("Karnataka", 0.10), ("Delhi NCR", 0.10),
    ("Tamil Nadu", 0.09), ("Gujarat", 0.08), ("Uttar Pradesh", 0.08),
    ("Telangana", 0.06), ("West Bengal", 0.06), ("Kerala", 0.05),
    ("Punjab", 0.04), ("Rajasthan", 0.04), ("Andhra Pradesh", 0.04),
    ("Haryana", 0.03), ("Madhya Pradesh", 0.03), ("Bihar", 0.02),
    ("Other", 0.04),
]

# Expanded name pool (50 → ~140 each)
INDIAN_FIRST_NAMES = [
    "Rahul","Priya","Amit","Sunita","Arjun","Meera","Vikram","Anjali","Rohit","Neha",
    "Karan","Pooja","Sandeep","Kavita","Nikhil","Divya","Aditya","Riya","Manish","Shruti",
    "Tarun","Aishwarya","Gaurav","Sneha","Vivek","Ananya","Suresh","Lakshmi","Mohan","Geeta",
    "Rakesh","Anita","Sanjay","Rekha","Ravi","Sushma","Deepak","Smita","Prakash","Bhavna",
    "Sachin","Kiran","Ajay","Komal","Vishal","Nisha","Akash","Pallavi","Harsh","Ritika",
    "Yash","Tanya","Siddharth","Megha","Varun","Swati","Naveen","Anuradha","Hemant","Jyoti",
    "Mukesh","Lata","Ramesh","Sarita","Vinod","Madhuri","Sunil","Usha","Dinesh","Asha",
    "Arvind","Shilpa","Bhupesh","Preeti","Devendra","Vandana","Pankaj","Rashmi","Yogesh","Charu",
    "Anand","Vasudha","Raghav","Sanya","Kabir","Ishita","Aryan","Mahima","Dhruv","Khushi",
    "Vihaan","Aanya","Aarav","Saanvi","Ayaan","Pari","Reyansh","Aadhya","Arnav","Myra",
    "Krish","Diya","Atharv","Kavya","Ishaan","Anaya","Aarush","Avni","Shaurya","Ira",
    "Ranbir","Alia","Shahrukh","Deepika","Salman","Kareena","Hrithik","Madhuri","Imran","Vidya",
    "Faiz","Ayesha","Imran","Zara","Aslam","Fatima","Rizwan","Sana","Tariq","Nadia",
    "Joseph","Mary","Thomas","Elizabeth","Anthony","Rosy","Francis","Maria","Xavier","Anna",
]
INDIAN_LAST_NAMES = [
    "Sharma","Verma","Gupta","Singh","Patel","Reddy","Iyer","Nair","Menon","Krishnan",
    "Mehta","Shah","Jain","Agarwal","Chopra","Kapoor","Bhat","Kulkarni","Desai","Rao",
    "Pillai","Mukherjee","Banerjee","Das","Khanna","Malhotra","Bose","Chatterjee","Pandey","Trivedi",
    "Joshi","Saxena","Dubey","Tiwari","Mishra","Yadav","Chauhan","Rana","Bhatt","Goswami",
    "Naidu","Acharya","Srinivasan","Subramanian","Raman","Krishnamurthy","Venkataraman","Iyengar",
    "Pai","Hegde","Shenoy","Kamath","Bhandari","Karnik","Joglekar","Phadke","Apte","Ranade",
    "Sinha","Prasad","Jha","Thakur","Roy","Sen","Sengupta","Ghosh","Basu","Chowdhury",
    "Khan","Ahmed","Hussain","Siddiqui","Ansari","Sheikh","Pathan","Qureshi","Mirza","Khatri",
    "Singh","Kaur","Gill","Sidhu","Brar","Sandhu","Dhillon","Cheema",
    "D'Souza","Fernandes","Pereira","Pinto","Rodrigues","Mascarenhas","Coutinho","Almeida",
    "Aggarwal","Bansal","Goel","Mittal","Garg","Singhal","Khurana","Sethi","Arora","Kohli",
]

CUSTOMER_SEGMENTS = [
    ("Leisure_FIT",   0.45),   # Free Independent Traveller
    ("Family",        0.25),
    ("Business",      0.15),
    ("Group_Tour",    0.08),
    ("Honeymoon",     0.04),
    ("Backpacker",    0.03),
]

TRAVEL_PURPOSES = [
    ("Heritage",      0.18),
    ("Beach",         0.16),
    ("Hill_Station",  0.18),
    ("Religious",     0.14),
    ("Business",      0.15),
    ("Wildlife",      0.06),
    ("Wellness",      0.05),
    ("Adventure",     0.04),
    ("Shopping",      0.04),
]

LOYALTY_TIERS = [("None", 0.65), ("Silver", 0.20), ("Gold", 0.10), ("Platinum", 0.05)]


def weighted_choice(pairs):
    items, weights = zip(*pairs)
    return random.choices(items, weights=weights, k=1)[0]


def gen_dim_customer():
    rows = []
    for i in range(1, NUM_CUSTOMERS + 1):
        # Age affects segment somewhat
        age = int(np.random.normal(35, 12))
        age = max(18, min(75, age))

        # Segment biased by age
        if age < 25:
            seg = random.choices(["Backpacker","Leisure_FIT","Group_Tour"], weights=[3,5,2])[0]
        elif age < 35:
            seg = random.choices(["Leisure_FIT","Honeymoon","Business","Family"], weights=[4,2,3,3])[0]
        elif age < 55:
            seg = random.choices(["Family","Business","Leisure_FIT"], weights=[5,3,3])[0]
        else:
            seg = random.choices(["Leisure_FIT","Group_Tour","Family"], weights=[4,3,3])[0]

        rows.append({
            "customer_id":       f"CUST-{i:06d}",
            "first_name":        random.choice(INDIAN_FIRST_NAMES),
            "last_name":         random.choice(INDIAN_LAST_NAMES),
            "age":               age,
            "gender":            random.choices(["M","F"], weights=[52,48])[0],
            "home_state":        weighted_choice(INDIAN_HOME_STATES),
            "customer_segment":  seg,
            "travel_purpose":    weighted_choice(TRAVEL_PURPOSES),
            "loyalty_tier":      weighted_choice(LOYALTY_TIERS),
            "is_repeat_customer": "TRUE" if random.random() < 0.35 else "FALSE",
        })
    df = pd.DataFrame(rows)
    df.to_csv(OUT_DIR / "dim_customer.csv", index=False)
    return df


# ===========================================================================
# 4. fact_bookings.csv — THE BIG ONE. 1M rows with realistic seasonality.
# ===========================================================================
ZONE_SEASONALITY = {
    # month -> multiplier on base daily bookings
    "Beach":        {1:2.5, 2:2.4, 3:1.2, 4:0.8, 5:0.6, 6:0.25, 7:0.2, 8:0.25, 9:0.4, 10:1.3, 11:2.6, 12:3.0},
    "Heritage":     {1:2.5, 2:2.4, 3:1.4, 4:0.5, 5:0.4, 6:0.4, 7:0.6, 8:0.7, 9:0.9, 10:2.0, 11:2.6, 12:2.8},
    "Hill Station": {1:0.7, 2:0.8, 3:1.0, 4:1.8, 5:2.5, 6:2.4, 7:1.2, 8:1.0, 9:1.3, 10:2.0, 11:1.0, 12:0.8},
    "Pilgrimage":   {1:1.0, 2:1.0, 3:2.5, 4:2.5, 5:1.2, 6:0.8, 7:0.8, 8:0.9, 9:1.3, 10:2.5, 11:2.6, 12:1.2},
    "Wildlife":     {1:2.4, 2:2.4, 3:2.0, 4:1.0, 5:0.6, 6:0.3, 7:0.3, 8:0.3, 9:0.6, 10:1.4, 11:2.4, 12:2.5},
    "Metro":        {1:1.2, 2:1.3, 3:1.3, 4:1.0, 5:0.95, 6:0.9, 7:0.9, 8:0.95, 9:1.0, 10:1.3, 11:1.3, 12:1.4},
    "Backwater":    {1:2.4, 2:1.8, 3:1.0, 4:0.6, 5:0.4, 6:0.3, 7:0.4, 8:0.5, 9:1.6, 10:2.2, 11:2.6, 12:2.6},
}

OTA_DIST_BY_TIER = {
    "BUDGET":  [("OYO",0.45),("MakeMyTrip",0.25),("Goibibo",0.08),("Direct",0.12),("Walk-in",0.10)],
    "MID":     [("MakeMyTrip",0.32),("Goibibo",0.18),("Booking.com",0.15),("OYO",0.05),("Direct",0.18),("Walk-in",0.07),("Agoda",0.05)],
    "PREMIUM": [("MakeMyTrip",0.25),("Booking.com",0.22),("Goibibo",0.12),("Direct",0.25),("Agoda",0.10),("Walk-in",0.06)],
    "LUXURY":  [("Direct",0.42),("Booking.com",0.22),("MakeMyTrip",0.16),("Agoda",0.12),("Goibibo",0.05),("Walk-in",0.03)],
}


def gen_fact_bookings(customers_df, dim_date_df):
    """Generate ~1M bookings spread across hotels with realistic seasonality."""
    customer_ids = customers_df["customer_id"].tolist()
    repeat_customers = customers_df[customers_df["is_repeat_customer"] == "TRUE"]["customer_id"].tolist()

    # Date_id lookup for FK
    date_to_id = {d: i for i, d in enumerate(
        pd.to_datetime(dim_date_df["full_date"]).dt.date.tolist(), start=1
    )}

    # Per-hotel weight ∝ (total_rooms × tier_revenue_factor × city_popularity)
    # This produces a power law: top hotels get most bookings
    city_popularity = {
        "Mumbai":2.5,"Delhi":2.5,"Bangalore":2.2,"Goa":2.4,"Jaipur":2.0,
        "Agra":1.8,"Udaipur":1.6,"Chennai":1.6,"Hyderabad":1.6,"Manali":1.5,
        "Kochi":1.4,"Pune":1.4,"Shimla":1.4,"Varanasi":1.4,"Rishikesh":1.3,
        "Calangute":1.3,"Kolkata":1.3,"Ahmedabad":1.3,
    }
    tier_factor = {"BUDGET":1.0, "MID":1.5, "PREMIUM":2.0, "LUXURY":2.5}

    hotel_weights = []
    hotel_records = hotels.to_dict("records")
    for h in hotel_records:
        # is_active may load as bool (TRUE/FALSE) or string depending on pandas version
        is_inactive = h["is_active"] is False or h["is_active"] == "FALSE" or h["is_active"] is None
        if is_inactive:
            hotel_weights.append(0.0)
            continue
        city = hotel_to_city[h["hotel_id"]]
        w = h["total_rooms"] * tier_factor[h["price_tier_id"]] * city_popularity.get(city, 1.0)
        hotel_weights.append(w)

    # Build date weights: weighted by seasonality + holiday boost
    all_dates = sorted(date_to_id.keys())
    history_dates = [d for d in all_dates if HISTORY_START <= d <= HISTORY_END]

    # Pre-compute zone seasonality per (zone, month)
    rows = []
    print(f"  Generating {NUM_BOOKINGS:,} bookings — this takes ~60s...")

    # Vectorized approach: sample hotels first, then assign dates with hotel-aware seasonality
    hotel_idx = np.random.choice(len(hotel_records), size=NUM_BOOKINGS, p=np.array(hotel_weights)/sum(hotel_weights))

    # For each booking, pick a date with seasonality weight for that hotel's zone
    # Pre-compute per-zone day weights once
    zone_day_weights = {}
    for zone in ZONE_SEASONALITY:
        weights = []
        for d in history_dates:
            base = ZONE_SEASONALITY[zone].get(d.month, 1.0)
            # Weekend boost for leisure zones, weekday boost for metro
            is_weekend = d.weekday() >= 5
            if zone == "Metro":
                base *= 1.1 if not is_weekend else 0.85
            else:
                base *= 1.3 if is_weekend else 1.0
            # Holiday boost
            if d in high_impact_dates:
                base *= 2.2
            elif d in holiday_dates:
                base *= 1.4
            weights.append(base)
        zone_day_weights[zone] = np.array(weights) / sum(weights)

    # Now batch-sample dates per zone (much faster than per-row)
    bookings_by_zone = {}
    for bi, hi in enumerate(hotel_idx):
        zone = hotel_to_zone[hotel_records[hi]["hotel_id"]]
        bookings_by_zone.setdefault(zone, []).append((bi, hi))

    # Pre-allocate result arrays
    result = [None] * NUM_BOOKINGS
    for zone, items in bookings_by_zone.items():
        if zone not in zone_day_weights:
            zone_day_weights[zone] = zone_day_weights["Metro"]
        n = len(items)
        sampled_date_idx = np.random.choice(len(history_dates), size=n, p=zone_day_weights[zone])
        for (bi, hi), didx in zip(items, sampled_date_idx):
            result[bi] = (hi, didx)

    # Now build records
    print("  Building booking records...")
    booking_id_counter = 1
    for bi, (hi, didx) in enumerate(result):
        h = hotel_records[hi]
        booking_date = history_dates[didx]
        hotel_id = h["hotel_id"]
        tier = h["price_tier_id"]

        # Room type + price
        rts = rt_by_hotel.get(hotel_id, [])
        if not rts:
            continue
        rt = random.choice(rts)
        nights = max(1, int(np.random.choice([1,1,2,2,2,3,3,4,5,7,10], p=[0.10,0.15,0.20,0.15,0.10,0.10,0.08,0.05,0.04,0.02,0.01])))

        # Price = base × seasonality multiplier × small noise
        seasonality_mult = ZONE_SEASONALITY.get(hotel_to_zone[hotel_id], ZONE_SEASONALITY["Metro"]).get(booking_date.month, 1.0)
        price_mult = 0.7 + 0.45 * seasonality_mult / 3.0  # range ~0.7-1.3
        if booking_date in high_impact_dates:
            price_mult *= 1.5
        nightly_rate = int(rt["base_price_inr"] * price_mult * random.uniform(0.92, 1.08))
        revenue = nightly_rate * nights

        # Cancellation (8-18% base, higher for budget/OTA bookings)
        cancel_prob = 0.13 if tier in ("BUDGET","MID") else 0.08
        is_cancelled = random.random() < cancel_prob

        # OTA source
        ota_pairs = OTA_DIST_BY_TIER[tier]
        source = weighted_choice(ota_pairs)
        # OYO chain hotels skew toward OYO
        if h.get("chain_name") == "OYO" and random.random() < 0.6:
            source = "OYO"

        # Check-in/out
        checkin = booking_date
        checkout = booking_date + timedelta(days=nights)

        # Event timestamp (booking made 1-90 days before checkin)
        lead_days = int(np.random.choice([0,1,2,3,7,14,30,60,90], p=[0.10,0.12,0.10,0.13,0.20,0.18,0.10,0.05,0.02]))
        event_ts = datetime.combine(booking_date - timedelta(days=lead_days), datetime.min.time()) \
                   + timedelta(hours=random.randint(8,23), minutes=random.randint(0,59))

        # Customer — biased toward home_state matching travel destination occasionally
        cust = random.choice(customer_ids)

        # Guest count
        guests = random.randint(1, min(4, rt["capacity"]))

        rows.append({
            "booking_id":      str(uuid.uuid4()),
            "hotel_id":        hotel_id,
            "customer_id":     cust,
            "room_type_id":    rt["room_type_id"],
            "location_id":     h["location_id"],
            "date_id":         date_to_id[booking_date],
            "checkin_date":    checkin.isoformat(),
            "checkout_date":   checkout.isoformat(),
            "nights_stayed":   nights,
            "num_guests":      guests,
            "nightly_rate_inr": nightly_rate,
            "revenue_inr":     revenue,
            "booking_source":  source,
            "is_cancelled":    "TRUE" if is_cancelled else "FALSE",
            "booking_ts":      event_ts.isoformat(sep=" ", timespec="seconds"),
            "event_ts":        event_ts.isoformat(sep=" ", timespec="seconds"),
        })
        booking_id_counter += 1

        if (bi+1) % 200_000 == 0:
            print(f"    {bi+1:,} / {NUM_BOOKINGS:,}")

    df = pd.DataFrame(rows)
    df.to_csv(OUT_DIR / "fact_bookings.csv", index=False)
    return df


# ===========================================================================
# 5. fact_price_events.csv — price change history per hotel-room
# ===========================================================================
PRICE_CHANGE_REASONS = [
    ("SEASONAL",          0.40),
    ("DEMAND_SURGE",      0.25),
    ("COMPETITOR_MATCH",  0.15),
    ("MANUAL_OVERRIDE",   0.10),
    ("EVENT_NEARBY",      0.10),
]


def gen_fact_price_events():
    """One price change per (hotel, room_type) roughly every 2-4 weeks."""
    rows = []
    history_days = (HISTORY_END - HISTORY_START).days

    for _, h in hotels.iterrows():
        if h["is_active"] is False or h["is_active"] == "FALSE":
            continue
        for rt in rt_by_hotel.get(h["hotel_id"], []):
            base = rt["base_price_inr"]
            current_price = base
            # Generate ~20-40 price changes over 28 months
            n_changes = random.randint(15, 35)
            change_days = sorted(random.sample(range(history_days), min(n_changes, history_days)))

            for cd in change_days:
                event_date = HISTORY_START + timedelta(days=cd)
                # Determine direction: seasonal pattern
                zone = hotel_to_zone[h["hotel_id"]]
                seasonal_target = base * ZONE_SEASONALITY.get(zone, ZONE_SEASONALITY["Metro"]).get(event_date.month, 1.0) ** 0.35
                # Holiday-driven spike
                if event_date in high_impact_dates or event_date + timedelta(days=1) in high_impact_dates:
                    seasonal_target *= 1.5

                # Pull current_price toward seasonal_target
                new_price = int(current_price + (seasonal_target - current_price) * random.uniform(0.3, 0.7))
                new_price = max(int(base * 0.65), min(int(base * 2.2), new_price))

                if abs(new_price - current_price) < 50:
                    continue  # skip tiny changes

                reason = weighted_choice(PRICE_CHANGE_REASONS)
                event_ts = datetime.combine(event_date, datetime.min.time()) + timedelta(
                    hours=random.randint(0,23), minutes=random.randint(0,59)
                )
                rows.append({
                    "event_id":       str(uuid.uuid4()),
                    "hotel_id":       h["hotel_id"],
                    "room_type_id":   rt["room_type_id"],
                    "event_ts":       event_ts.isoformat(sep=" ", timespec="seconds"),
                    "old_price_inr":  current_price,
                    "new_price_inr":  new_price,
                    "delta_inr":      new_price - current_price,
                    "pct_change":     round((new_price - current_price) / current_price * 100, 2),
                    "change_reason":  reason,
                    "flink_window_id": f"win_{event_date.strftime('%Y%m%d')}_{event_ts.hour:02d}",
                })
                current_price = new_price

    df = pd.DataFrame(rows)
    df.to_csv(OUT_DIR / "fact_price_events.csv", index=False)
    return df


# ===========================================================================
# 6. REGENERATE reviews_raw.csv with quality + polish fixes
# ===========================================================================
# Theme bank — same as Stage 1 but with theme-specific amenity gating
PHRASES = {
    "cleanliness_neg": [
        "Bathroom was not clean, there were stains on the tiles.",
        "Housekeeping did not change the bedsheets during our 3-night stay.",
        "The room had a foul smell, seemed like it wasn't cleaned properly.",
        "Found hair in the bathroom, very unhygienic.",
        "The carpet looked like it hadn't been vacuumed in weeks.",
        "Bedsheets had visible stains, had to ask for a change at midnight.",
        "Towels were dirty and smelled musty.",
        "Cobwebs in the corners of the room, clearly housekeeping is neglected.",
    ],
    "cleanliness_pos": [
        "The room was spotlessly clean throughout our stay.",
        "Housekeeping was prompt and the bathroom was always fresh.",
        "Bed linen was crisp and clean, towels changed daily.",
        "Maintained high standards of cleanliness, very impressive.",
    ],
    "wifi_neg": [
        "WiFi was extremely slow, couldn't even load WhatsApp.",
        "Internet kept disconnecting throughout the stay.",
        "WiFi only worked in the lobby, not in the room.",
        "Tried to attend a Zoom call, WiFi failed every 5 minutes.",
        "Free WiFi is a joke, speed was less than 0.5 Mbps.",
    ],
    "wifi_pos": [
        "WiFi was fast and stable throughout the stay, perfect for work.",
        "Internet speeds were excellent, could stream Netflix without issues.",
        "Good WiFi coverage across the entire property.",
    ],
    "food_neg": [
        "Breakfast was good but lunch options were very limited.",
        "Room service took over 45 minutes for a simple order.",
        "No veg options available at night which was disappointing.",
        "Food was bland and overpriced for the quality.",
        "The buffet was repetitive, same items every meal.",
        "Cold food was served, complained twice with no improvement.",
    ],
    "food_pos": [
        "The dal makhani was excellent, authentic North Indian flavours.",
        "Breakfast spread was extensive — South Indian, continental, fresh juices.",
        "In-house restaurant served amazing seafood, fresh and well-prepared.",
        "Loved the Rajasthani thali, very authentic and unlimited servings.",
        "Chef came out to ask about preferences, lovely personal touch.",
    ],
    "pool_pos": [
        "Swimming pool was clean and well-maintained, kids loved it.",
        "Pool area was beautiful with comfortable loungers and fresh towels.",
        "The infinity pool with city view was the highlight of our stay.",
    ],
    "pool_neg": [
        "Pool was tiny and crowded throughout the day.",
        "Swimming pool was closed for maintenance during our entire stay.",
    ],
    "ac_pos": [
        "AC worked perfectly even in peak summer heat, cooled the room fast.",
    ],
    "ac_neg": [
        "AC was barely functional, room felt humid all night.",
        "AC was making strange noises, had to switch it off to sleep.",
    ],
    "parking_pos": [
        "Ample parking space available, very convenient with our car.",
    ],
    "parking_neg": [
        "No parking available, had to park 500m away on the street.",
    ],
    "staff_neg": [
        "Rude behaviour at the front desk, very disappointing.",
        "Staff was unresponsive when we needed help with luggage.",
        "Reception staff didn't speak English or Hindi properly, hard to communicate.",
        "Manager was dismissive when we raised concerns about the room.",
    ],
    "staff_pos": [
        "Staff was very helpful and cooperative throughout the stay.",
        "Check-in process was smooth and staff upgraded our room.",
        "The manager personally ensured our stay was comfortable.",
        "Front desk team went above and beyond to help us with local tours.",
        "Housekeeping ladies were so warm, almost like family.",
    ],
    "location_pos": [
        "Hotel is very close to the main market, walking distance.",
        "The property is in a quiet area, perfect for relaxation.",
        "10 minutes from the railway station, very convenient.",
        "Easy access to all main attractions, great location.",
    ],
    "location_neg": [
        "Difficult to find, Google Maps shows wrong location.",
        "Very far from the main city, had to take a cab for everything.",
        "Located on a narrow lane, hard for cabs to reach.",
        "Far from tourist spots, wasted hours commuting daily.",
    ],
    "value_pos": [
        "Excellent value for ₹{price} per night, would definitely return.",
        "Great budget option, much better than expected for the price.",
        "Worth every rupee, you get more than what you pay for.",
    ],
    "value_neg": [
        "Overpriced for what they offer, found better options nearby.",
        "Not worth ₹{price} per night, very basic amenities.",
        "Hidden charges at checkout, total bill was much higher than booked.",
    ],
    "goa": [
        "Beach was just a 2-minute walk from the property.",
        "Party noise from neighbouring shacks till 2am, couldn't sleep.",
        "Checkout time of 10am was too early for a Goa holiday.",
        "Seafood thali at the in-house restaurant was outstanding, fresh prawns and pomfret.",
    ],
    "rajasthan": [
        "Heritage room decor was stunning, felt like royalty.",
        "Rooftop view of the city palace was breathtaking at sunset.",
        "Organised a camel safari for us, very well managed.",
        "AC was inadequate in May, room got too hot in the afternoons.",
        "The haveli architecture was beautifully preserved.",
    ],
    "hills": [
        "Mountain view from the balcony was simply magical.",
        "Heating was insufficient in December, freezing nights.",
        "Hot water was inconsistent, had to wait 20 minutes some mornings.",
        "Road access is difficult in monsoon, plan accordingly.",
        "Snow on the peaks visible from the room — unforgettable.",
    ],
    "pilgrimage": [
        "Pure vegetarian food only, perfect for our pilgrimage.",
        "5-minute walk to the ghats, very convenient for morning aarti.",
        "Noise from religious processions till late, but that's expected here.",
        "Hotel arranged a pandit for the puja, very helpful.",
    ],
    "metro": [
        "Excellent business centre, conducted my meetings without any issues.",
        "Airport shuttle on time, smooth transfer.",
        "Allowed early check-in at 9am, much appreciated.",
        "City view from the 15th floor was beautiful at night.",
    ],
    "hinglish": [
        "Mast experience tha, definitely coming back!",
        "Very nice property, staff bhi bahut achha hai.",
        "Khaana acha tha but service slow thi.",
        "Room theek-thaak, value for money.",
        "Bahut accha stay raha, recommend karunga.",
    ],
}

# Amenity → theme gating
# Only allow theme if hotel has the amenity (for positive themes)
# Negative themes are about the amenity FAILING — only valid if amenity is claimed but bad
AMENITY_GATE = {
    "pool_pos":    "Swimming Pool",
    "pool_neg":    "Swimming Pool",
    "ac_pos":      "AC",
    "ac_neg":      "AC",
    "parking_pos": "Parking facility",
    "parking_neg": "Parking facility",
    "wifi_pos":    "WiFi",
    "wifi_neg":    "WiFi",
    "food_pos":    "In-house Restaurant",
    "food_neg":    "In-house Restaurant",
}

IRRELEVANT_REVIEWS = [
    "The taxi driver from the airport was extremely rude, charged us extra for luggage. Otherwise the trip was fine. The hotel — I don't remember much, we slept and left early in the morning. Mainly writing this to warn people about the taxi situation in this city.",
    "Our flight got delayed by 4 hours which ruined the first day of our trip. By the time we reached the hotel we were too tired to even notice. Stayed two nights, was okay I guess. Indigo really needs to fix their scheduling.",
    "Don't have much to say about the hotel honestly. Came here for a wedding and barely spent time in the room. The function hall in the city was beautiful though. The wedding food was amazing.",
    "I am writing this review because nobody asked me how my trip was. My boss made me work even on vacation. The hotel was there. The room had a bed. I slept. Now I am back to work.",
    "This place was okay. Actually let me tell you about something else — the cab service in this city is a complete mess. Ola and Uber kept cancelling. We waited 40 minutes once. Three star to the cab situation, not the hotel.",
    "Honestly forgot we even booked this hotel until the bill came through. Was visiting family. Did not really stay here. Three stars by default.",
    "My phone got stolen in the local market two days into the trip. Spent the rest of the holiday at the police station. Hotel staff was sympathetic but couldn't really help. Be careful with belongings.",
    "The weather was terrible. Heavy rain for three days straight. Could not step out at all. Watched Netflix in the room the whole time. Not the hotel's fault but the trip was a waste.",
    "Came for a medical procedure, stayed at this hotel because it was close to the hospital. Was not really in the mood to evaluate hospitality. Bed was fine. Food was fine. Recovery is going well.",
    "I generally don't write reviews but my dog passed away just before this trip and I was very emotional. Hotel staff was nice. That's all I can say.",
    "Came here for a job interview. Did not get the job. The hotel was fine but I'm in a bad mood writing this so take that into account. Three stars.",
    "Lost my luggage at the airport, spent the first 24 hours dealing with the airline. Hotel let me borrow some toiletries which was nice. Otherwise normal stay.",
]

REVIEW_SOURCES = ["MakeMyTrip", "OYO", "Booking.com", "Goibibo", "TripAdvisor", "Google"]
TRAVEL_TYPES = ["Family", "Couple", "Solo", "Business", "Friends", "Pilgrimage"]
RATING_DIST = [(1, 0.08), (2, 0.05), (3, 0.12), (4, 0.30), (5, 0.45)]


def gen_review_text(rating, city, zone, base_price, amenities, verbose=False):
    """Build review with amenity-correlated themes."""
    parts = []

    # Available themes based on hotel amenities
    def has(amen): return amen in amenities

    if rating <= 2:
        candidate_neg = ["cleanliness_neg", "staff_neg", "location_neg", "value_neg"]
        if has("WiFi"): candidate_neg.append("wifi_neg")
        if has("AC"): candidate_neg.append("ac_neg")
        if has("In-house Restaurant"): candidate_neg.append("food_neg")
        if has("Swimming Pool"): candidate_neg.append("pool_neg")
        if has("Parking facility"): candidate_neg.append("parking_neg")

        themes = random.sample(candidate_neg, min(3 if verbose else 2, len(candidate_neg)))
        for t in themes:
            parts.append(random.choice(PHRASES[t]).replace("{price}", str(base_price)))
        if random.random() < 0.3:
            parts.append(random.choice(PHRASES["staff_pos"] + PHRASES["location_pos"]))
    elif rating == 3:
        pos_candidates = ["staff_pos", "location_pos", "cleanliness_pos"]
        if has("In-house Restaurant"): pos_candidates.append("food_pos")
        if has("WiFi"): pos_candidates.append("wifi_pos")
        if has("Swimming Pool"): pos_candidates.append("pool_pos")
        neg_candidates = ["cleanliness_neg", "value_neg"]
        if has("WiFi"): neg_candidates.append("wifi_neg")
        if has("AC"): neg_candidates.append("ac_neg")
        if has("In-house Restaurant"): neg_candidates.append("food_neg")

        parts.append(random.choice(PHRASES[random.choice(pos_candidates)]))
        parts.append(random.choice(PHRASES[random.choice(neg_candidates)]).replace("{price}", str(base_price)))
        if random.random() < 0.5:
            parts.append("Overall an average experience, nothing exceptional.")
    else:
        # 4-5 star
        pos_candidates = ["staff_pos", "location_pos", "cleanliness_pos", "value_pos"]
        if has("In-house Restaurant"): pos_candidates.append("food_pos")
        if has("WiFi"): pos_candidates.append("wifi_pos")
        if has("Swimming Pool"): pos_candidates.append("pool_pos")
        if has("AC"): pos_candidates.append("ac_pos")
        if has("Parking facility"): pos_candidates.append("parking_pos")

        n_pos = (4 if verbose else 2)
        themes = random.sample(pos_candidates, min(n_pos, len(pos_candidates)))
        for t in themes:
            parts.append(random.choice(PHRASES[t]).replace("{price}", str(base_price)))
        if rating == 4 and random.random() < 0.4:
            minor_negs = ["wifi_neg", "food_neg"] if has("WiFi") or has("In-house Restaurant") else ["cleanliness_neg"]
            parts.append("One small issue — " + random.choice(PHRASES[random.choice(minor_negs)]).lower())

    # City flavor
    if random.random() < (0.7 if verbose else 0.5):
        if city in {"Goa", "Calangute", "Anjuna", "Panjim"}:
            parts.append(random.choice(PHRASES["goa"]))
        elif zone == "Heritage" and city in {"Jaipur","Jodhpur","Udaipur","Jaisalmer","Pushkar","Bikaner","Agra"}:
            parts.append(random.choice(PHRASES["rajasthan"]))
        elif zone == "Hill Station":
            parts.append(random.choice(PHRASES["hills"]))
        elif zone == "Pilgrimage":
            parts.append(random.choice(PHRASES["pilgrimage"]))
        elif zone == "Metro":
            parts.append(random.choice(PHRASES["metro"]))

    if rating >= 4 and random.random() < 0.2:
        parts.append(random.choice(PHRASES["hinglish"]))

    if rating >= 4:
        parts.append(random.choice([
            "Will definitely come back next time we visit.",
            "Highly recommend to fellow travellers.",
            "A memorable stay, thank you team.",
            "Worth the booking, no regrets.",
        ]))
    elif rating <= 2:
        parts.append(random.choice([
            "Will not be returning.",
            "Better alternatives available in the same area.",
            "Management needs to seriously look into these issues.",
        ]))

    review = " ".join(parts)
    pad = [
        " The location was decent for the most part and we managed to get around without too much difficulty.",
        " The check-in process took its time but nothing too bad in the grand scheme of things.",
        " Would consider this place again depending on the alternatives available at that time.",
        " The overall experience was in line with what we have come to expect from properties in this segment.",
        " There were a few small things here and there but nothing that significantly affected the stay.",
        " We had booked through one of the popular OTAs and the booking process itself was hassle-free.",
        " Parking was available though a bit limited during peak hours which is fairly standard.",
    ]
    target = 220 if verbose else 42
    while len(review.split()) < target:
        review += random.choice(pad)
    return review


def pick_review_date_for_city(zone):
    """Bias review dates to fall ~0-30 days after a peak month for that zone."""
    # Pick a year and a peak month
    year = random.choices([2024, 2025, 2026], weights=[1.0, 1.2, 0.7])[0]
    peak_months = [m for m, mult in ZONE_SEASONALITY.get(zone, ZONE_SEASONALITY["Metro"]).items() if mult >= 1.3]
    if not peak_months:
        peak_months = [10, 11, 12, 1, 2]
    month = random.choice(peak_months)
    if year == 2026 and month > 5:
        # No data past May 2026
        year = 2025
    day = random.randint(1, 28)
    base_date = date(year, month, day)
    # Add 0-30 day offset (review written after stay)
    offset = int(np.random.exponential(10))
    offset = min(offset, 60)
    review_date = base_date + timedelta(days=offset)
    if review_date > HISTORY_END:
        review_date = HISTORY_END - timedelta(days=random.randint(0, 30))
    if review_date < date(2022, 1, 1):
        review_date = date(2022, 1, 1)
    return review_date.isoformat()


def gen_reviews_v2(customers_df):
    """Regenerate reviews with quality fixes."""
    # Power-law hotel distribution: weights = review_count ** 1.8
    hotel_pool = hotels.to_dict("records")
    weights = np.array([h["review_count"] ** 1.8 for h in hotel_pool], dtype=float)
    weights = weights / weights.sum()

    rows = []
    n_irrelevant = int(NUM_REVIEWS * IRRELEVANT_REVIEW_FRAC)
    n_verbose = int(NUM_REVIEWS * VERBOSE_REVIEW_FRAC)

    # Reserve ~10% of reviews for floor distribution (every hotel gets 1-3 minimum)
    n_floor = min(int(NUM_REVIEWS * 0.10), len(hotel_pool) * 3)
    n_relevant = NUM_REVIEWS - n_irrelevant - n_floor

    # Floor: at least 1-3 reviews per hotel
    floor_hotel_idx = []
    for _ in range(n_floor // len(hotel_pool) + 1):
        for hi in range(len(hotel_pool)):
            floor_hotel_idx.append(hi)
            if len(floor_hotel_idx) >= n_floor:
                break
        if len(floor_hotel_idx) >= n_floor:
            break
    random.shuffle(floor_hotel_idx)

    # Power-law sample for the rest
    relevant_hotel_idx = np.random.choice(len(hotel_pool), size=n_relevant, p=weights)
    # Combine
    all_relevant_idx = list(relevant_hotel_idx) + floor_hotel_idx
    random.shuffle(all_relevant_idx)
    n_relevant = len(all_relevant_idx)

    verbose_set = set(random.sample(range(n_relevant), n_verbose))

    for i, hi in enumerate(all_relevant_idx):
        hotel = hotel_pool[hi]
        rating = random.choices([r for r, _ in RATING_DIST], weights=[w for _, w in RATING_DIST], k=1)[0]
        if random.random() < 0.25:
            target = round(hotel["avg_rating"])
            rating = max(1, min(5, target + random.choice([-1, 0, 1])))

        city = hotel_to_city[hotel["hotel_id"]]
        zone = hotel_to_zone[hotel["hotel_id"]]
        amenities = json.loads(hotel["amenities_json"])

        text = gen_review_text(rating, city, zone, hotel["base_price_inr"], amenities, verbose=(i in verbose_set))

        rows.append({
            "review_id": str(uuid.uuid4()),
            "hotel_id": hotel["hotel_id"],
            "reviewer_name": f"{random.choice(INDIAN_FIRST_NAMES)} {random.choice(INDIAN_LAST_NAMES)}",
            "review_text": text,
            "rating": rating,
            "review_date": pick_review_date_for_city(zone),
            "source": random.choice(REVIEW_SOURCES),
            "travel_type": random.choice(TRAVEL_TYPES),
        })

    # Irrelevant reviews
    irrelevant_pad = [
        " Anyway, sharing this so others know what to expect.",
        " The trip itself had its ups and downs but that's how it goes sometimes.",
        " Not the hotel's fault really, just sharing context about our stay.",
        " Two stars to the overall trip, three to the hotel I suppose.",
        " Take this review with a grain of salt given the circumstances.",
    ]
    for _ in range(n_irrelevant):
        hotel = random.choice(hotel_pool)
        rating = random.choices([1,2,3,3,3,4,5], k=1)[0]
        text = random.choice(IRRELEVANT_REVIEWS)
        while len(text.split()) < 42:
            text += random.choice(irrelevant_pad)
        zone = hotel_to_zone[hotel["hotel_id"]]
        rows.append({
            "review_id": str(uuid.uuid4()),
            "hotel_id": hotel["hotel_id"],
            "reviewer_name": f"{random.choice(INDIAN_FIRST_NAMES)} {random.choice(INDIAN_LAST_NAMES)}",
            "review_text": text,
            "rating": rating,
            "review_date": pick_review_date_for_city(zone),
            "source": random.choice(REVIEW_SOURCES),
            "travel_type": random.choice(TRAVEL_TYPES),
        })

    random.shuffle(rows)
    df = pd.DataFrame(rows)
    df.to_csv(OUT_DIR / "reviews_raw.csv", index=False)
    return df


# ===========================================================================
# Validation
# ===========================================================================
def validate_all():
    print("\n" + "=" * 65)
    print("VALIDATION — Stage 2")
    print("=" * 65)
    hm = pd.read_csv(OUT_DIR / "hotel_master.csv")
    loc = pd.read_csv(OUT_DIR / "dim_location.csv")
    rt = pd.read_csv(OUT_DIR / "dim_room_type.csv")
    rv = pd.read_csv(OUT_DIR / "reviews_raw.csv")
    cust = pd.read_csv(OUT_DIR / "dim_customer.csv")
    dd = pd.read_csv(OUT_DIR / "dim_date.csv")
    fb = pd.read_csv(OUT_DIR / "fact_bookings.csv")
    fpe = pd.read_csv(OUT_DIR / "fact_price_events.csv")

    print(f"\nFinal row counts:")
    print(f"  hotel_master.csv:       {len(hm):>10,}")
    print(f"  dim_location.csv:       {len(loc):>10,}")
    print(f"  dim_room_type.csv:      {len(rt):>10,}")
    print(f"  dim_customer.csv:       {len(cust):>10,}")
    print(f"  dim_date.csv:           {len(dd):>10,}")
    print(f"  reviews_raw.csv:        {len(rv):>10,}")
    print(f"  fact_bookings.csv:      {len(fb):>10,}")
    print(f"  fact_price_events.csv:  {len(fpe):>10,}")

    print(f"\nFK integrity (fact_bookings):")
    print(f"  Orphan hotel_id:        {len(fb[~fb['hotel_id'].isin(hm['hotel_id'])])}")
    print(f"  Orphan customer_id:     {len(fb[~fb['customer_id'].isin(cust['customer_id'])])}")
    print(f"  Orphan room_type_id:    {len(fb[~fb['room_type_id'].isin(rt['room_type_id'])])}")
    print(f"  Orphan location_id:     {len(fb[~fb['location_id'].isin(loc['location_id'])])}")
    print(f"  Orphan date_id:         {len(fb[~fb['date_id'].isin(dd['date_id'])])}")

    print(f"\nFact_bookings date range:")
    print(f"  Min checkin:            {fb['checkin_date'].min()}")
    print(f"  Max checkin:            {fb['checkin_date'].max()}")
    # is_cancelled may load as bool or string — normalize
    cancelled_mask = (fb['is_cancelled'] == True) | (fb['is_cancelled'] == "TRUE")
    print(f"  Cancellation rate:      {cancelled_mask.mean()*100:.1f}%")
    print(f"  Total revenue (cr ₹):   {fb[~cancelled_mask]['revenue_inr'].sum()/1e7:.0f}")

    print(f"\nFact_bookings — seasonality check (avg bookings/month):")
    fb["month"] = pd.to_datetime(fb["checkin_date"]).dt.month
    by_month = fb.groupby("month").size()
    for m, n in by_month.items():
        bar = "█" * int(n / by_month.max() * 30)
        print(f"  {m:2d}: {bar} {n:,}")

    print(f"\nReview hotel distribution (power law check):")
    rv_per_hotel = rv.groupby("hotel_id").size().sort_values(ascending=False)
    print(f"  Top 10% hotels capture: {rv_per_hotel.head(int(len(rv_per_hotel)*0.1)).sum()/len(rv)*100:.1f}% of reviews")
    print(f"  Max reviews/hotel:      {rv_per_hotel.max()}")
    print(f"  Min reviews/hotel:      {rv_per_hotel.min()}")
    print(f"  Hotels with 0 reviews:  {len(hm) - len(rv_per_hotel)}")

    rv["wc"] = rv["review_text"].str.split().str.len()
    print(f"\nReview text:")
    print(f"  Min words:              {rv['wc'].min()}")
    print(f"  Avg words:              {rv['wc'].mean():.1f}")
    print(f"  Max words:              {rv['wc'].max()}")
    print(f"  Verbose (>150 words):   {(rv['wc'] > 150).sum()}")


# ===========================================================================
# Run
# ===========================================================================
if __name__ == "__main__":
    print(f"Stage 2 generator → {OUT_DIR}")
    print(f"  History: {HISTORY_START} → {HISTORY_END}")
    print(f"  Bookings: {NUM_BOOKINGS:,} | Customers: {NUM_CUSTOMERS:,}\n")

    print("[1/6] ref_price_tiers.csv ...", end=" ", flush=True)
    gen_ref_price_tiers()
    print("done")

    print("[2/6] dim_date.csv ...", end=" ", flush=True)
    dd_df = gen_dim_date()
    print(f"{len(dd_df)} rows")

    print("[3/6] dim_customer.csv ...", end=" ", flush=True)
    cust_df = gen_dim_customer()
    print(f"{len(cust_df)} rows")

    print("[4/6] fact_bookings.csv ...")
    gen_fact_bookings(cust_df, dd_df)
    print("       done")

    print("[5/6] fact_price_events.csv ...", end=" ", flush=True)
    fpe_df = gen_fact_price_events()
    print(f"{len(fpe_df):,} rows")

    print("[6/6] reviews_raw.csv (regenerating with quality fixes) ...", end=" ", flush=True)
    rv_df = gen_reviews_v2(cust_df)
    print(f"{len(rv_df)} rows")

    validate_all()
    print("\n✓ Stage 2 complete.")
