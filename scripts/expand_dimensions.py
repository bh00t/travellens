"""
TravelLens India — Stage 1 dimension expansion (B-046).

ADDITIVE. Inserts ~950 new cities, ~18K new hotels, ~45K new room types,
and ~80K new customers. Existing 44 cities / 2,000 hotels / 5,542 room
types / 20,000 customers and ALL fact data are UNTOUCHED.

Reads:    seeds/cities_expansion.csv (committed, source of truth)
Inserts:  dim_location, hotel_master, dim_room_type, dim_customer
Writes:   nothing else — no fact, no review, no agg.

Idempotent:
  - Aborts if ANY catalog (city, state) already exists in dim_location
    (a previous expansion run is detected).
  - Aborts if hotel_master / dim_customer max-id are already past the
    expansion starting points.

Run:  python -m scripts.expand_dimensions
"""

import csv
import json
import os
import random
import sys
import uuid
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Force UTF-8 stdout on Windows so any non-ASCII city name prints cleanly
# ---------------------------------------------------------------------------
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

load_dotenv()

SEED = 42
random.seed(SEED)

REPO_ROOT = Path(__file__).resolve().parent.parent
CSV_PATH = REPO_ROOT / "seeds" / "cities_expansion.csv"

DB_PARAMS = {
    "host":     os.environ["POSTGRES_HOST"],
    "port":     int(os.environ["POSTGRES_PORT"]),
    "dbname":   os.environ["POSTGRES_DB"],
    "user":     os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
}

# ---------------------------------------------------------------------------
# Constants — reuse existing dataset rules
# ---------------------------------------------------------------------------
PRICE_TIER_BANDS = {
    "BUDGET":  (349, 1999),
    "MID":     (2000, 4999),
    "PREMIUM": (5000, 9999),
    "LUXURY":  (10000, 35000),
}

STAR_DIST = [(0, 0.35), (1, 0.12), (2, 0.10), (3, 0.20), (4, 0.15), (5, 0.08)]

STAR_TO_TIER = {0: "BUDGET", 1: "BUDGET", 2: "BUDGET",
                3: "MID", 4: "PREMIUM", 5: "LUXURY"}

STAR_TO_ROOM_RANGE = {0: (10, 40), 1: (10, 40), 2: (10, 40),
                      3: (30, 80), 4: (60, 150), 5: (80, 300)}

STAR_TO_RATING_RANGE = {0: (3.2, 3.8), 1: (3.3, 3.9), 2: (3.5, 4.0),
                        3: (3.8, 4.3), 4: (4.0, 4.6), 5: (4.3, 4.9)}

# review_count = 0 for all new hotels (per decision)

AMENITIES = ["WiFi", "AC", "TV", "Parking facility", "Daily housekeeping",
             "Geyser", "Power backup", "In-house Restaurant", "Swimming Pool",
             "Spa", "Elevator", "CCTV cameras", "24/7 check-in", "Card payment",
             "Kitchen", "Mini Fridge", "King Sized Bed", "Bath Tub",
             "Room service", "Laundry"]
STAR_TO_AMENITY_RANGE = {0: (3, 5), 1: (3, 5), 2: (4, 6),
                         3: (6, 10), 4: (8, 13), 5: (10, 15)}

LUXURY_CHAINS = ["Taj Hotels", "ITC Hotels", "Lemon Tree", "Marriott",
                 "Oberoi", "Radisson", "ibis"]
MID_CHAINS    = ["OYO", "Lemon Tree", "FabHotels", "Treebo", "ibis"]
BUDGET_CHAINS = ["OYO", "FabHotels", "Treebo"]

# Hotels per city by popularity_tier — (min, max) inclusive
HOTELS_PER_CITY = {
    "mega":    (120, 150),
    "major":   (40, 80),
    "mid":     (20, 40),
    "small":   (8, 15),
    "obscure": (3, 8),
}

# Property-type weights by zone — sums approx 1.0 per zone
PROPERTY_WEIGHTS_BY_ZONE = {
    "Metro":        [("Hotel", 0.55), ("Apartment", 0.12), ("Service Apartment", 0.08),
                     ("Hostel", 0.08), ("Guest House", 0.10), ("BnB", 0.04), ("Lodge", 0.03)],
    "Heritage":     [("Hotel", 0.30), ("Palace", 0.10), ("Resort", 0.10), ("Homestay", 0.15),
                     ("Guest House", 0.20), ("Lodge", 0.10), ("BnB", 0.05)],
    "Pilgrimage":   [("Hotel", 0.45), ("Lodge", 0.20), ("Guest House", 0.25),
                     ("Homestay", 0.05), ("BnB", 0.05)],
    "Hill Station": [("Hotel", 0.28), ("Resort", 0.22), ("Homestay", 0.20),
                     ("Cottage", 0.10), ("Tent", 0.10), ("Lodge", 0.05),
                     ("BnB", 0.05)],
    "Beach":        [("Resort", 0.35), ("Hotel", 0.28), ("Homestay", 0.10),
                     ("Villa", 0.10), ("Apartment", 0.10), ("Hostel", 0.05),
                     ("BnB", 0.02)],
    "Wildlife":     [("Resort", 0.25), ("Lodge", 0.20), ("Tent", 0.20),
                     ("Treehouse", 0.08), ("Hotel", 0.15), ("Homestay", 0.10),
                     ("Cottage", 0.02)],
    "Backwater":    [("Houseboat", 0.30), ("Hotel", 0.25), ("Resort", 0.20),
                     ("Homestay", 0.15), ("Villa", 0.05), ("Guest House", 0.05)],
}

# Room types by star (canonical) + zone bonus (sometimes appended)
ROOM_TYPES_BY_STAR = {
    0: ["Standard Non AC", "Standard AC"],
    1: ["Standard Non AC", "Standard AC", "Deluxe"],
    2: ["Standard Non AC", "Standard AC", "Deluxe"],
    3: ["Standard AC", "Deluxe", "Super Deluxe", "Executive"],
    4: ["Deluxe", "Super Deluxe", "Executive", "Suite", "Premium Suite"],
    5: ["Executive", "Suite", "Premium Suite", "Luxury Suite", "Heritage Room"],
}

# Zone-specific room types we may add to the candidate pool
ZONE_ROOM_TYPES = {
    "Beach":        ["Sea View", "Pool View"],
    "Hill Station": ["Valley View", "Cottage Room", "Tent"],
    "Heritage":     ["Heritage Room"],
    "Backwater":    ["Houseboat Suite"],
    "Wildlife":     ["Tent", "Treehouse"],
    "Pilgrimage":   [],
    "Metro":        [],
}

# Room type price-jitter relative to base tier price (same logic as gen_dim_room_type)
ROOM_PRICE_JITTER = {
    "Standard Non AC": 0.7, "Standard AC": 0.85, "Deluxe": 1.0,
    "Super Deluxe": 1.15, "Executive": 1.3, "Suite": 1.5,
    "Premium Suite": 1.7, "Luxury Suite": 2.0,
    "Heritage Room": 1.4, "Valley View": 1.1, "Sea View": 1.2,
    "Pool View": 1.15, "Cottage Room": 0.9,
    "Tent": 0.95, "Treehouse": 1.35, "Houseboat Suite": 1.45,
}

# Hotel name prefixes by tier
HOTEL_PREFIXES = {
    "branded":     ["OYO Flagship", "OYO Townhouse", "OYO Rooms", "Treebo Trend",
                    "FabHotel", "Lemon Tree Premier", "ibis", "Taj", "ITC",
                    "The Oberoi", "Radisson Blu", "Marriott", "Hyatt Regency",
                    "Novotel"],
    "independent": ["Hotel", "Hotel", "Grand Hotel", "Royal Hotel",
                    "Sunrise", "Sunset", "Comfort", "Galaxy"],
    "heritage":    ["Haveli", "Palace", "Mahal", "Rawla", "Garh"],
    "budget":      ["Lodge", "Guest House", "Inn", "Stay Inn", "Backpackers Hostel"],
    "resort":      ["Beach Resort", "Hill Resort", "Spa Resort", "Eco Resort"],
}

# ---------------------------------------------------------------------------
# Customer config
# ---------------------------------------------------------------------------
NUM_CUSTOMERS_NEW = 80_000
CUSTOMER_START = 20_001   # CUST-020001 onward
CUSTOMER_END   = 100_000  # inclusive

INDIAN_HOME_STATES = [
    ("Maharashtra", 0.14), ("Karnataka", 0.10), ("Delhi NCR", 0.10),
    ("Tamil Nadu", 0.09), ("Gujarat", 0.08), ("Uttar Pradesh", 0.08),
    ("Telangana", 0.06), ("West Bengal", 0.06), ("Kerala", 0.05),
    ("Punjab", 0.04), ("Rajasthan", 0.04), ("Andhra Pradesh", 0.04),
    ("Haryana", 0.03), ("Madhya Pradesh", 0.03), ("Bihar", 0.02),
    ("Other", 0.04),
]
INDIAN_FIRST_NAMES = [
    "Rahul","Priya","Amit","Sunita","Arjun","Meera","Vikram","Anjali","Rohit","Neha",
    "Karan","Pooja","Sandeep","Kavita","Nikhil","Divya","Aditya","Riya","Manish","Shruti",
    "Tarun","Aishwarya","Gaurav","Sneha","Vivek","Ananya","Suresh","Lakshmi","Mohan","Geeta",
    "Rakesh","Anita","Sanjay","Rekha","Ravi","Sushma","Deepak","Smita","Prakash","Bhavna",
    "Sachin","Kiran","Ajay","Komal","Vishal","Nisha","Akash","Pallavi","Harsh","Ritika",
    "Yash","Tanya","Siddharth","Megha","Varun","Swati","Naveen","Anuradha","Hemant","Jyoti",
    "Mukesh","Lata","Ramesh","Sarita","Vinod","Madhuri","Sunil","Usha","Dinesh","Asha",
    "Arvind","Shilpa","Preeti","Devendra","Vandana","Pankaj","Rashmi","Yogesh","Charu",
    "Anand","Vasudha","Raghav","Sanya","Kabir","Ishita","Aryan","Mahima","Dhruv","Khushi",
    "Vihaan","Aanya","Aarav","Saanvi","Ayaan","Pari","Reyansh","Aadhya","Arnav","Myra",
    "Krish","Diya","Atharv","Kavya","Ishaan","Anaya","Aarush","Avni","Shaurya","Ira",
    "Faiz","Ayesha","Zara","Aslam","Fatima","Rizwan","Sana","Tariq","Nadia",
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
    "Kaur","Gill","Sidhu","Brar","Sandhu","Dhillon","Cheema",
    "D'Souza","Fernandes","Pereira","Pinto","Rodrigues","Mascarenhas","Coutinho","Almeida",
    "Aggarwal","Bansal","Goel","Mittal","Garg","Singhal","Khurana","Sethi","Arora","Kohli",
]
CUSTOMER_SEGMENTS = [
    ("Leisure_FIT",   0.45), ("Family", 0.25), ("Business", 0.15),
    ("Group_Tour",    0.08), ("Honeymoon", 0.04), ("Backpacker", 0.03),
]
# 9 canonical purposes; Backwater intentionally NOT added (decision #1)
TRAVEL_PURPOSES = [
    ("Heritage", 0.18), ("Beach", 0.16), ("Hill_Station", 0.18),
    ("Religious", 0.14), ("Business", 0.15), ("Wildlife", 0.06),
    ("Wellness", 0.05), ("Adventure", 0.04), ("Shopping", 0.04),
]
LOYALTY_TIERS = [("None", 0.65), ("Silver", 0.20), ("Gold", 0.10), ("Platinum", 0.05)]


def weighted_choice(pairs):
    items, weights = zip(*pairs)
    return random.choices(items, weights=weights, k=1)[0]


def pick_star():
    stars, weights = zip(*STAR_DIST)
    return random.choices(stars, weights=weights, k=1)[0]


# ---------------------------------------------------------------------------
# Hotel-name builders (mirror existing style)
# ---------------------------------------------------------------------------
def gen_hotel_name(city, star, prop_type, zone):
    if prop_type == "Houseboat":
        return f"{random.choice(['Vembanad','Backwater','Kuttanad','Sunset','Lotus','Heron'])} Houseboat {city}"
    if prop_type == "Treehouse":
        return f"{random.choice(['Canopy','Jungle','Wild','Mist','Eco'])} Treehouse {city}"
    if prop_type == "Tent":
        return f"{random.choice(['Safari','Wilderness','Riverside','Hilltop','Eco'])} Tents {city}"
    if prop_type == "Palace":
        return f"{random.choice(['Royal','Grand','Heritage','Maharaja','Rajputana'])} Palace {city}"
    if prop_type == "Villa":
        return f"{random.choice(['Azure','Casa','Verde','Sunset','Palms'])} Villa {city}"
    if prop_type == "Cottage":
        return f"{random.choice(['Pine','Cedar','Oak','Maple','Misty'])} Cottage {city}"
    if star >= 4:
        if random.random() < 0.5:
            prefix = random.choice(HOTEL_PREFIXES["branded"])
            return f"{prefix} {city}"
        prefix = random.choice(HOTEL_PREFIXES["heritage"])
        return f"{city} {prefix}"
    if star == 3:
        if random.random() < 0.4:
            return f"{random.choice(HOTEL_PREFIXES['branded'])} {city}"
        return f"{random.choice(HOTEL_PREFIXES['independent'])} {city}"
    if prop_type == "Resort":
        return f"{city} {random.choice(HOTEL_PREFIXES['resort'])}"
    if random.random() < 0.5:
        return f"{city} {random.choice(HOTEL_PREFIXES['budget'])}"
    return f"OYO {random.randint(100, 99999)} {city}"


# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------
def build_location_rows(catalog):
    rows = []
    for r in catalog:
        rows.append({
            "location_id":  str(uuid.uuid4()),
            "city":         r["city"],
            "state":        r["state"],
            "region":       r["region"],
            "tourism_zone": r["tourism_zone"],
            "latitude":     float(r["latitude"]),
            "longitude":    float(r["longitude"]),
            "tourist_arrivals_annual_m": float(r["tourist_arrivals_annual_m"]),
            "peak_months":  r["peak_months"],
            "popularity_tier": r["popularity_tier"],
        })
    return rows


def gen_hotels_for_city(city_rec, next_hotel_seq):
    """Generate the hotel rows for one city, returning (rows, next_seq)."""
    tier = city_rec["popularity_tier"]
    zone = city_rec["tourism_zone"]
    n = random.randint(*HOTELS_PER_CITY[tier])
    rows = []
    for _ in range(n):
        star = pick_star()
        ptier = STAR_TO_TIER[star]
        plo, phi = PRICE_TIER_BANDS[ptier]
        base_price = random.randint(plo, phi)

        # Property type — zone-weighted
        prop_type = weighted_choice(PROPERTY_WEIGHTS_BY_ZONE[zone])

        # Chain — ~60% NULL
        chain = None
        if random.random() > 0.6:
            if star >= 4:
                chain = random.choice(LUXURY_CHAINS)
            elif star == 3:
                chain = random.choice(MID_CHAINS)
            else:
                chain = random.choice(BUDGET_CHAINS)

        rooms = random.randint(*STAR_TO_ROOM_RANGE[star])
        rlo, rhi = STAR_TO_RATING_RANGE[star]
        avg_rating = round(random.uniform(rlo, rhi), 2)

        alo, ahi = STAR_TO_AMENITY_RANGE[star]
        amenities = random.sample(AMENITIES, random.randint(alo, ahi))

        opened_year = random.randint(1975, 2026)

        is_active = random.random() < 0.95

        hotel_id = f"HTL-{next_hotel_seq:06d}"
        next_hotel_seq += 1

        rows.append({
            "hotel_id":       hotel_id,
            "hotel_name":     gen_hotel_name(city_rec["city"], star, prop_type, zone),
            "chain_name":     chain,
            "property_type":  prop_type,
            "star_category":  star,
            "total_rooms":    rooms,
            "location_id":    city_rec["location_id"],
            "avg_rating":     avg_rating,
            "amenities_json": json.dumps(amenities),
            "review_count":   0,
            "price_tier_id":  ptier,
            "base_price_inr": base_price,
            "is_active":      is_active,
            "opened_year":    opened_year,
            "_zone":          zone,  # internal helper for room types
            "_property_type": prop_type,
        })
    return rows, next_hotel_seq


def gen_room_types_for_hotel(hotel):
    star = hotel["star_category"]
    zone = hotel["_zone"]
    prop_type = hotel["_property_type"]
    ptier = hotel["price_tier_id"]
    plo, phi = PRICE_TIER_BANDS[ptier]

    candidate_pool = list(ROOM_TYPES_BY_STAR[star])
    candidate_pool += ZONE_ROOM_TYPES.get(zone, [])

    # Property-type drives unique room-type names where it makes sense
    if prop_type == "Houseboat":
        candidate_pool = ["Houseboat Suite"] + [t for t in candidate_pool if t != "Houseboat Suite"]
    elif prop_type == "Treehouse":
        candidate_pool = ["Treehouse"] + [t for t in candidate_pool if t != "Treehouse"]
    elif prop_type == "Tent":
        candidate_pool = ["Tent"] + [t for t in candidate_pool if t != "Tent"]

    candidate_pool = list(dict.fromkeys(candidate_pool))  # preserve order, dedup

    n_types = random.randint(2, 4)
    n_types = min(n_types, len(candidate_pool))
    chosen = random.sample(candidate_pool, n_types)

    rows = []
    for name in chosen:
        # has_ac rules
        if "Non AC" in name:
            has_ac = False
        elif name in ("Suite", "Premium Suite", "Luxury Suite", "Executive"):
            has_ac = True
        elif name in ("Tent", "Treehouse"):
            has_ac = random.random() < 0.3
        elif name == "Cottage Room":
            has_ac = random.random() < 0.5
        else:
            has_ac = True

        jitter = ROOM_PRICE_JITTER.get(name, 1.0)
        base = random.randint(plo, phi)
        price = int(min(phi, max(plo, base * jitter)))

        rows.append({
            "room_type_id":   str(uuid.uuid4()),
            "hotel_id":       hotel["hotel_id"],
            "type_name":      name,
            "capacity":       random.choice([1, 2, 2, 2, 3, 4]),
            "has_ac":         has_ac,
            "has_breakfast":  random.random() < 0.30,
            "price_tier":     ptier,
            "base_price_inr": price,
        })
    return rows


def gen_customers(start_seq, n):
    """Generate n new customers starting at CUST-{start_seq:06d}."""
    rows = []
    for i in range(n):
        cid = f"CUST-{start_seq + i:06d}"
        age = int(random.normalvariate(35, 12))
        age = max(18, min(75, age))

        if age < 25:
            seg = random.choices(["Backpacker","Leisure_FIT","Group_Tour"], weights=[3,5,2])[0]
        elif age < 35:
            seg = random.choices(["Leisure_FIT","Honeymoon","Business","Family"], weights=[4,2,3,3])[0]
        elif age < 55:
            seg = random.choices(["Family","Business","Leisure_FIT"], weights=[5,3,3])[0]
        else:
            seg = random.choices(["Leisure_FIT","Group_Tour","Family"], weights=[4,3,3])[0]

        rows.append({
            "customer_id":       cid,
            "first_name":        random.choice(INDIAN_FIRST_NAMES),
            "last_name":         random.choice(INDIAN_LAST_NAMES),
            "age":               age,
            "gender":            random.choices(["M","F"], weights=[52,48])[0],
            "home_state":        weighted_choice(INDIAN_HOME_STATES),
            "customer_segment":  seg,
            "travel_purpose":    weighted_choice(TRAVEL_PURPOSES),
            "loyalty_tier":      weighted_choice(LOYALTY_TIERS),
            "is_repeat_customer": random.random() < 0.35,
        })
    return rows


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def fetch_one(cur, sql, *args):
    cur.execute(sql, args or None)
    return cur.fetchone()


def fetchall(cur, sql, *args):
    cur.execute(sql, args or None)
    return cur.fetchall()


def parse_hotel_seq(s):
    """HTL-002000 -> 2000."""
    return int(s.split("-")[1])


def parse_customer_seq(s):
    """CUST-020000 -> 20000."""
    return int(s.split("-")[1])


def main():
    t0 = datetime.utcnow()
    print(f"[{t0.isoformat()}Z] Starting Stage 1 dimension expansion")

    # ------------------------------------------------------------------ catalog
    if not CSV_PATH.exists():
        raise SystemExit(f"Catalog CSV not found: {CSV_PATH}. Run build_cities_expansion_csv first.")
    with CSV_PATH.open(encoding="utf-8") as fh:
        catalog = list(csv.DictReader(fh))
    print(f"  catalog: {len(catalog)} cities loaded from {CSV_PATH.relative_to(REPO_ROOT)}")

    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # =========================================================
        # PRE-FLIGHT GUARDS
        # =========================================================
        # State validation: every catalog state must exist in india_states_zones
        valid_states = {r[0] for r in fetchall(cur, "SELECT state_name FROM india_states_zones")}
        catalog_states = {r["state"] for r in catalog}
        bad = catalog_states - valid_states
        if bad:
            raise SystemExit(f"State mismatch — these catalog states are NOT in india_states_zones: {sorted(bad)}")
        print(f"  state validation: {len(catalog_states)} catalog states all match india_states_zones ✓")

        # Idempotency: no catalog (city, state) may already exist in dim_location
        existing_pairs = {(r[0], r[1]) for r in fetchall(cur,
            "SELECT city, state FROM dim_location")}
        collisions = [(r["city"], r["state"]) for r in catalog
                      if (r["city"], r["state"]) in existing_pairs]
        if collisions:
            raise SystemExit(
                f"Idempotency abort: {len(collisions)} catalog cities already exist in dim_location. "
                f"First few: {collisions[:5]}. Expansion has already run."
            )
        print(f"  idempotency: 0 catalog cities collide with existing dim_location ✓")

        # Starting sequences for new hotel_id and customer_id
        (max_hotel,) = fetch_one(cur, "SELECT MAX(hotel_id) FROM hotel_master")
        (max_cust,)  = fetch_one(cur, "SELECT MAX(customer_id) FROM dim_customer")
        next_hotel_seq    = parse_hotel_seq(max_hotel) + 1 if max_hotel else 1
        next_customer_seq = parse_customer_seq(max_cust) + 1 if max_cust else 1

        if next_hotel_seq != 2001:
            raise SystemExit(
                f"Unexpected hotel_master state — next hotel seq would be {next_hotel_seq}, expected 2001 "
                f"(max existing was {max_hotel}). Aborting."
            )
        if next_customer_seq != 20001:
            raise SystemExit(
                f"Unexpected dim_customer state — next customer seq would be {next_customer_seq}, expected 20001 "
                f"(max existing was {max_cust}). Aborting."
            )
        print(f"  ID guards: next hotel = HTL-{next_hotel_seq:06d}, next customer = CUST-{next_customer_seq:06d} ✓")

        # Record before-state for all dim/fact tables (verification)
        (before_loc,)  = fetch_one(cur, "SELECT COUNT(*) FROM dim_location")
        (before_hot,)  = fetch_one(cur, "SELECT COUNT(*) FROM hotel_master")
        (before_rt,)   = fetch_one(cur, "SELECT COUNT(*) FROM dim_room_type")
        (before_cust,) = fetch_one(cur, "SELECT COUNT(*) FROM dim_customer")
        (before_fb,)   = fetch_one(cur, "SELECT COUNT(*) FROM fact_bookings")
        (before_fbe,)  = fetch_one(cur, "SELECT COUNT(*) FROM fact_booking_events")
        (before_fbl,)  = fetch_one(cur, "SELECT COUNT(*) FROM fact_booking_lifecycle")
        (before_rv,)   = fetch_one(cur, "SELECT COUNT(*) FROM reviews_raw")
        print(f"  before: dim_location={before_loc:,}  hotel_master={before_hot:,}  "
              f"dim_room_type={before_rt:,}  dim_customer={before_cust:,}")
        print(f"          fact_bookings={before_fb:,}  fact_booking_events={before_fbe:,}  "
              f"fact_booking_lifecycle={before_fbl:,}  reviews_raw={before_rv:,}")

        # =========================================================
        # GENERATE
        # =========================================================
        # 1) dim_location rows
        loc_rows = build_location_rows(catalog)
        print(f"\n  generating: {len(loc_rows)} dim_location rows")

        # 2) hotel_master rows
        hotel_rows = []
        for loc in loc_rows:
            hr, next_hotel_seq = gen_hotels_for_city(loc, next_hotel_seq)
            hotel_rows.extend(hr)
        print(f"  generating: {len(hotel_rows)} hotel_master rows")

        # 3) dim_room_type rows
        rt_rows = []
        for h in hotel_rows:
            rt_rows.extend(gen_room_types_for_hotel(h))
        print(f"  generating: {len(rt_rows)} dim_room_type rows")

        # 4) dim_customer rows
        cust_rows = gen_customers(next_customer_seq, NUM_CUSTOMERS_NEW)
        print(f"  generating: {len(cust_rows)} dim_customer rows")

        # =========================================================
        # INSERT (single transaction)
        # =========================================================
        # dim_location
        psycopg2.extras.execute_values(cur,
            """
            INSERT INTO dim_location
              (location_id, city, state, region, tourism_zone,
               latitude, longitude, tourist_arrivals_annual_m, peak_months)
            VALUES %s
            """,
            [(r["location_id"], r["city"], r["state"], r["region"],
              r["tourism_zone"], r["latitude"], r["longitude"],
              r["tourist_arrivals_annual_m"], r["peak_months"])
             for r in loc_rows],
            page_size=500,
        )
        print(f"\n  inserted: dim_location +{len(loc_rows):,}")

        # hotel_master
        psycopg2.extras.execute_values(cur,
            """
            INSERT INTO hotel_master
              (hotel_id, hotel_name, chain_name, property_type, star_category,
               total_rooms, location_id, avg_rating, amenities_json, review_count,
               price_tier_id, base_price_inr, is_active, opened_year)
            VALUES %s
            """,
            [(r["hotel_id"], r["hotel_name"], r["chain_name"], r["property_type"],
              r["star_category"], r["total_rooms"], r["location_id"], r["avg_rating"],
              r["amenities_json"], r["review_count"], r["price_tier_id"],
              r["base_price_inr"], r["is_active"], r["opened_year"])
             for r in hotel_rows],
            page_size=1000,
        )
        print(f"  inserted: hotel_master +{len(hotel_rows):,}")

        # dim_room_type
        psycopg2.extras.execute_values(cur,
            """
            INSERT INTO dim_room_type
              (room_type_id, hotel_id, type_name, capacity, has_ac,
               has_breakfast, price_tier, base_price_inr)
            VALUES %s
            """,
            [(r["room_type_id"], r["hotel_id"], r["type_name"], r["capacity"],
              r["has_ac"], r["has_breakfast"], r["price_tier"], r["base_price_inr"])
             for r in rt_rows],
            page_size=1000,
        )
        print(f"  inserted: dim_room_type +{len(rt_rows):,}")

        # dim_customer
        psycopg2.extras.execute_values(cur,
            """
            INSERT INTO dim_customer
              (customer_id, first_name, last_name, age, gender, home_state,
               customer_segment, travel_purpose, loyalty_tier, is_repeat_customer)
            VALUES %s
            """,
            [(r["customer_id"], r["first_name"], r["last_name"], r["age"],
              r["gender"], r["home_state"], r["customer_segment"],
              r["travel_purpose"], r["loyalty_tier"], r["is_repeat_customer"])
             for r in cust_rows],
            page_size=2000,
        )
        print(f"  inserted: dim_customer +{len(cust_rows):,}")

        # =========================================================
        # POST-CHECK (still in transaction — we commit only if all pass)
        # =========================================================
        (after_loc,)  = fetch_one(cur, "SELECT COUNT(*) FROM dim_location")
        (after_hot,)  = fetch_one(cur, "SELECT COUNT(*) FROM hotel_master")
        (after_rt,)   = fetch_one(cur, "SELECT COUNT(*) FROM dim_room_type")
        (after_cust,) = fetch_one(cur, "SELECT COUNT(*) FROM dim_customer")
        (after_fb,)   = fetch_one(cur, "SELECT COUNT(*) FROM fact_bookings")
        (after_fbe,)  = fetch_one(cur, "SELECT COUNT(*) FROM fact_booking_events")
        (after_fbl,)  = fetch_one(cur, "SELECT COUNT(*) FROM fact_booking_lifecycle")
        (after_rv,)   = fetch_one(cur, "SELECT COUNT(*) FROM reviews_raw")

        # Fact rows MUST be unchanged
        if (after_fb, after_fbe, after_fbl, after_rv) != (before_fb, before_fbe, before_fbl, before_rv):
            raise SystemExit("FAIL — fact table counts changed; rolling back.")

        # FK orphan checks
        (orphan_hotel_loc,) = fetch_one(cur, """
            SELECT COUNT(*) FROM hotel_master h
            LEFT JOIN dim_location l ON h.location_id = l.location_id
            WHERE l.location_id IS NULL
        """)
        (orphan_hotel_pt,) = fetch_one(cur, """
            SELECT COUNT(*) FROM hotel_master h
            LEFT JOIN ref_price_tiers p ON h.price_tier_id = p.tier_id
            WHERE p.tier_id IS NULL
        """)
        (orphan_rt_hotel,) = fetch_one(cur, """
            SELECT COUNT(*) FROM dim_room_type r
            LEFT JOIN hotel_master h ON r.hotel_id = h.hotel_id
            WHERE h.hotel_id IS NULL
        """)
        (orphan_rt_pt,) = fetch_one(cur, """
            SELECT COUNT(*) FROM dim_room_type r
            LEFT JOIN ref_price_tiers p ON r.price_tier = p.tier_id
            WHERE p.tier_id IS NULL
        """)
        (hotels_without_rt,) = fetch_one(cur, """
            SELECT COUNT(*) FROM hotel_master h
            WHERE NOT EXISTS (SELECT 1 FROM dim_room_type r WHERE r.hotel_id = h.hotel_id)
        """)
        if any([orphan_hotel_loc, orphan_hotel_pt, orphan_rt_hotel, orphan_rt_pt, hotels_without_rt]):
            raise SystemExit(
                f"FAIL — FK / integrity violation; rolling back.\n"
                f"  hotel→location orphans:  {orphan_hotel_loc}\n"
                f"  hotel→price_tier orphans:{orphan_hotel_pt}\n"
                f"  room_type→hotel orphans: {orphan_rt_hotel}\n"
                f"  room_type→tier  orphans: {orphan_rt_pt}\n"
                f"  hotels without ≥1 room:  {hotels_without_rt}"
            )

        # No state drift — re-verify (defensive)
        (state_drift,) = fetch_one(cur, """
            SELECT COUNT(*) FROM dim_location d
            LEFT JOIN india_states_zones z ON d.state = z.state_name
            WHERE z.state_name IS NULL
        """)
        if state_drift:
            raise SystemExit(f"FAIL — {state_drift} dim_location rows have a state not in india_states_zones.")

        # COMMIT
        conn.commit()
        t1 = datetime.utcnow()
        print(f"\n[{t1.isoformat()}Z] COMMITTED in {(t1 - t0).total_seconds():.1f}s")

        # =========================================================
        # FINAL REPORT
        # =========================================================
        print("\n" + "=" * 70)
        print("EXPANSION COMPLETE")
        print("=" * 70)
        print(f"  dim_location:           {before_loc:>9,} -> {after_loc:>9,}   (+{after_loc - before_loc:,})")
        print(f"  hotel_master:           {before_hot:>9,} -> {after_hot:>9,}   (+{after_hot - before_hot:,})")
        print(f"  dim_room_type:          {before_rt:>9,} -> {after_rt:>9,}   (+{after_rt - before_rt:,})")
        print(f"  dim_customer:           {before_cust:>9,} -> {after_cust:>9,}   (+{after_cust - before_cust:,})")
        print(f"  fact_bookings:          {before_fb:>9,}   (unchanged)")
        print(f"  fact_booking_events:    {before_fbe:>9,}   (unchanged)")
        print(f"  fact_booking_lifecycle: {before_fbl:>9,}   (unchanged)")
        print(f"  reviews_raw:            {before_rv:>9,}   (unchanged)")
        print(f"\n  FK orphans:  0   |   hotels-with-≥1-room: 100%   |   state-drift: 0")

    except Exception as e:
        conn.rollback()
        print(f"\nROLLBACK — {e}", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
