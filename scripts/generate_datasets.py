"""
TravelLens India — Dataset Generator
Generates all 7 datasets per spec with referential integrity.

Outputs to OUT_DIR:
  hotel_master.csv         (dim_hotel feed)
  dim_location.csv         (dim_location feed)
  dim_room_type.csv        (dim_room_type feed)
  reviews_raw.csv          (reviews_raw feed)
  booking_events_seed.json (Kafka simulator seed)
  public_holidays.csv      (ref_india_holidays feed)
  india_states_zones.csv   (ref_state_tourism_zones feed)
"""

import json
import os
import random
import uuid
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

# ---------------------------------------------------------------------------
# CONFIG — change these to scale
# ---------------------------------------------------------------------------
SEED = 42
NUM_HOTELS = 2000
NUM_REVIEWS = 30000
IRRELEVANT_REVIEW_FRAC = 0.05          # 5% off-topic noise
NUM_BOOKING_SEEDS = 500
OUT_DIR = Path(os.getenv("DATA_DIR", "data"))
OUT_DIR.mkdir(parents=True, exist_ok=True)

random.seed(SEED)
np.random.seed(SEED)
fake = Faker("en_IN")
Faker.seed(SEED)

# ---------------------------------------------------------------------------
# REFERENCE DATA — city/state/region/zone mapping (exact from spec)
# ---------------------------------------------------------------------------
CITY_MAP = {
    # Rajasthan — Heritage
    "Jaipur":      ("Rajasthan",        "North India", "Heritage",       26.9124, 75.7873, 4.2),
    "Jodhpur":     ("Rajasthan",        "North India", "Heritage",       26.2389, 73.0243, 2.1),
    "Udaipur":     ("Rajasthan",        "North India", "Heritage",       24.5854, 73.7125, 2.5),
    "Jaisalmer":   ("Rajasthan",        "North India", "Heritage",       26.9157, 70.9083, 1.6),
    "Pushkar":     ("Rajasthan",        "North India", "Heritage",       26.4899, 74.5511, 1.2),
    "Bikaner":     ("Rajasthan",        "North India", "Heritage",       28.0229, 73.3119, 0.9),
    "Ajmer":       ("Rajasthan",        "North India", "Heritage",       26.4499, 74.6399, 1.4),
    # Goa — Beach
    "Goa":         ("Goa",              "West India",  "Beach",          15.2993, 74.1240, 8.5),
    "Calangute":   ("Goa",              "West India",  "Beach",          15.5440, 73.7553, 3.2),
    "Anjuna":      ("Goa",              "West India",  "Beach",          15.5736, 73.7407, 2.1),
    "Panjim":      ("Goa",              "West India",  "Beach",          15.4909, 73.8278, 1.8),
    # Himachal — Hill
    "Manali":      ("Himachal Pradesh", "North India", "Hill Station",   32.2396, 77.1887, 3.0),
    "Shimla":      ("Himachal Pradesh", "North India", "Hill Station",   31.1048, 77.1734, 2.8),
    "Dharamshala": ("Himachal Pradesh", "North India", "Hill Station",   32.2190, 76.3234, 1.5),
    "Dalhousie":   ("Himachal Pradesh", "North India", "Hill Station",   32.5448, 75.9712, 0.8),
    "Kasauli":     ("Himachal Pradesh", "North India", "Hill Station",   30.8979, 76.9647, 0.6),
    # Uttarakhand — Hill / Pilgrimage
    "Rishikesh":   ("Uttarakhand",      "North India", "Pilgrimage",     30.0869, 78.2676, 2.4),
    "Haridwar":    ("Uttarakhand",      "North India", "Pilgrimage",     29.9457, 78.1642, 3.5),
    "Mussoorie":   ("Uttarakhand",      "North India", "Hill Station",   30.4598, 78.0644, 1.4),
    "Nainital":    ("Uttarakhand",      "North India", "Hill Station",   29.3919, 79.4542, 1.6),
    "Dehradun":    ("Uttarakhand",      "North India", "Metro",          30.3165, 78.0322, 2.0),
    # Kerala — Beach / Backwater
    "Munnar":      ("Kerala",           "South India", "Hill Station",   10.0889, 77.0595, 2.2),
    "Alleppey":    ("Kerala",           "South India", "Backwater",      9.4981,  76.3388, 2.8),
    "Kovalam":     ("Kerala",           "South India", "Beach",          8.4004,  76.9787, 1.5),
    "Kochi":       ("Kerala",           "South India", "Metro",          9.9312,  76.2673, 3.6),
    "Wayanad":     ("Kerala",           "South India", "Wildlife",       11.6854, 76.1320, 1.1),
    # Maharashtra
    "Mumbai":      ("Maharashtra",      "West India",  "Metro",          19.0760, 72.8777, 12.5),
    "Pune":        ("Maharashtra",      "West India",  "Metro",          18.5204, 73.8567, 5.4),
    "Nashik":      ("Maharashtra",      "West India",  "Pilgrimage",     19.9975, 73.7898, 2.6),
    "Lonavala":    ("Maharashtra",      "West India",  "Hill Station",   18.7546, 73.4068, 1.8),
    "Mahabaleshwar": ("Maharashtra",    "West India",  "Hill Station",   17.9307, 73.6477, 1.4),
    # Metro
    "Delhi":       ("Delhi NCR",        "North India", "Metro",          28.6139, 77.2090, 14.2),
    "Bangalore":   ("Karnataka",        "South India", "Metro",          12.9716, 77.5946, 9.8),
    "Chennai":     ("Tamil Nadu",       "South India", "Metro",          13.0827, 80.2707, 7.5),
    "Hyderabad":   ("Telangana",        "South India", "Metro",          17.3850, 78.4867, 6.9),
    "Kolkata":     ("West Bengal",      "East India",  "Metro",          22.5726, 88.3639, 6.2),
    "Ahmedabad":   ("Gujarat",          "West India",  "Metro",          23.0225, 72.5714, 4.8),
    # Others
    "Agra":        ("Uttar Pradesh",    "North India", "Heritage",       27.1767, 78.0081, 6.5),
    "Varanasi":    ("Uttar Pradesh",    "North India", "Pilgrimage",     25.3176, 82.9739, 7.0),
    "Khajuraho":   ("Madhya Pradesh",   "North India", "Heritage",       24.8318, 79.9199, 0.7),
    "Ooty":        ("Tamil Nadu",       "South India", "Hill Station",   11.4102, 76.6950, 2.5),
    "Coorg":       ("Karnataka",        "South India", "Hill Station",   12.3375, 75.8069, 1.6),
    "Guwahati":    ("Assam",            "East India",  "Wildlife",       26.1445, 91.7362, 1.9),
    "Chandigarh":  ("Punjab",           "North India", "Metro",          30.7333, 76.7794, 1.7),
}

PEAK_MONTHS_BY_ZONE = {
    "Heritage":     "Oct|Nov|Dec|Jan|Feb",
    "Beach":        "Nov|Dec|Jan|Feb",
    "Hill Station": "Apr|May|Jun|Sep|Oct",
    "Pilgrimage":   "Oct|Nov|Mar|Apr",
    "Wildlife":     "Nov|Dec|Jan|Feb|Mar",
    "Metro":        "Jan|Feb|Mar|Oct|Nov|Dec",
    "Backwater":    "Sep|Oct|Nov|Dec|Jan",
}

# Indian states + UTs for india_states_zones.csv
INDIA_STATES = [
    # (code, name, zone, arrivals_m, peak_months, primary_attractions)
    ("RJ", "Rajasthan",         "Heritage",     54.3, "Oct|Nov|Dec|Jan|Feb",  "Jaipur|Udaipur|Jaisalmer|Hawa Mahal|Amber Fort"),
    ("MH", "Maharashtra",       "Mixed",        120.5,"Oct|Nov|Dec|Jan|Feb",  "Gateway of India|Ajanta Caves|Lonavala|Shirdi"),
    ("DL", "Delhi NCR",         "Metro",        29.8, "Oct|Nov|Dec|Jan|Feb",  "Red Fort|Qutub Minar|India Gate|Lotus Temple"),
    ("KA", "Karnataka",         "Mixed",        81.2, "Oct|Nov|Dec|Jan|Feb",  "Mysore Palace|Hampi|Coorg|Bangalore Palace"),
    ("TN", "Tamil Nadu",        "Pilgrimage",   115.4,"Oct|Nov|Dec|Jan|Feb",  "Meenakshi Temple|Marina Beach|Ooty|Mahabalipuram"),
    ("KL", "Kerala",            "Backwater",    18.4, "Sep|Oct|Nov|Dec|Jan",  "Alleppey Backwaters|Munnar|Kovalam|Kochi"),
    ("GJ", "Gujarat",           "Heritage",     46.2, "Nov|Dec|Jan|Feb|Mar",  "Statue of Unity|Rann of Kutch|Somnath|Dwarka"),
    ("UP", "Uttar Pradesh",     "Pilgrimage",   234.5,"Oct|Nov|Dec|Jan|Feb",  "Taj Mahal|Varanasi Ghats|Agra Fort|Ayodhya"),
    ("HR", "Haryana",           "Metro",        14.3, "Oct|Nov|Dec|Jan|Feb",  "Kurukshetra|Sultanpur Bird Sanctuary|Pinjore Gardens"),
    ("HP", "Himachal Pradesh",  "Hill Station", 17.2, "Apr|May|Jun|Sep|Oct",  "Manali|Shimla|Dharamshala|Spiti Valley"),
    ("UK", "Uttarakhand",       "Pilgrimage",   38.5, "Mar|Apr|May|Sep|Oct",  "Rishikesh|Haridwar|Char Dham|Jim Corbett"),
    ("GA", "Goa",               "Beach",        8.5,  "Nov|Dec|Jan|Feb",      "Calangute Beach|Anjuna|Old Goa Churches|Dudhsagar"),
    ("WB", "West Bengal",       "Mixed",        89.1, "Oct|Nov|Dec|Jan|Feb",  "Victoria Memorial|Darjeeling|Sundarbans|Howrah Bridge"),
    ("AP", "Andhra Pradesh",    "Pilgrimage",   178.3,"Oct|Nov|Dec|Jan|Feb",  "Tirupati|Araku Valley|Borra Caves|Visakhapatnam"),
    ("TG", "Telangana",         "Heritage",     74.8, "Oct|Nov|Dec|Jan|Feb",  "Charminar|Golconda Fort|Ramoji Film City|Warangal"),
    ("OD", "Odisha",            "Pilgrimage",   23.7, "Oct|Nov|Dec|Jan|Feb",  "Jagannath Temple|Konark Sun Temple|Chilika Lake"),
    ("JH", "Jharkhand",         "Wildlife",     35.4, "Oct|Nov|Dec|Jan|Feb",  "Betla National Park|Hundru Falls|Deoghar"),
    ("BR", "Bihar",             "Pilgrimage",   28.4, "Oct|Nov|Dec|Jan|Feb",  "Bodh Gaya|Nalanda|Rajgir|Vaishali"),
    ("MP", "Madhya Pradesh",    "Wildlife",     91.4, "Oct|Nov|Dec|Jan|Feb",  "Khajuraho|Kanha National Park|Sanchi|Bandhavgarh"),
    ("CG", "Chhattisgarh",      "Wildlife",     17.6, "Oct|Nov|Dec|Jan|Feb",  "Chitrakote Falls|Bastar|Kanger Valley"),
    ("AS", "Assam",             "Wildlife",     6.2,  "Nov|Dec|Jan|Feb|Mar",  "Kaziranga|Majuli|Kamakhya Temple"),
    ("MN", "Manipur",           "Wildlife",     1.7,  "Oct|Nov|Dec|Jan|Feb",  "Loktak Lake|Kangla Fort|Imphal"),
    ("ML", "Meghalaya",         "Hill Station", 1.2,  "Oct|Nov|Dec|Mar|Apr",  "Cherrapunji|Shillong|Living Root Bridges"),
    ("MZ", "Mizoram",           "Hill Station", 0.3,  "Oct|Nov|Dec|Jan|Feb",  "Aizawl|Reiek|Phawngpui"),
    ("NL", "Nagaland",          "Wildlife",     0.4,  "Oct|Nov|Dec|Jan|Feb",  "Kohima|Hornbill Festival|Dzukou Valley"),
    ("TR", "Tripura",           "Mixed",        0.6,  "Oct|Nov|Dec|Jan|Feb",  "Ujjayanta Palace|Neermahal|Unakoti"),
    ("SK", "Sikkim",            "Hill Station", 1.5,  "Mar|Apr|May|Sep|Oct",  "Gangtok|Tsomgo Lake|Nathula Pass|Pelling"),
    ("AR", "Arunachal Pradesh", "Wildlife",     0.7,  "Oct|Nov|Dec|Mar|Apr",  "Tawang|Ziro Valley|Namdapha"),
    # UTs
    ("PB", "Punjab",            "Pilgrimage",   42.5, "Oct|Nov|Dec|Jan|Feb",  "Golden Temple|Wagah Border|Jallianwala Bagh"),
    ("JK", "Jammu & Kashmir",   "Hill Station", 16.2, "Apr|May|Jun|Sep|Oct",  "Dal Lake|Gulmarg|Pahalgam|Vaishno Devi"),
    ("LA", "Ladakh",            "Hill Station", 0.5,  "Jun|Jul|Aug|Sep",      "Pangong Lake|Leh|Nubra Valley|Magnetic Hill"),
    ("CH", "Chandigarh",        "Metro",        1.7,  "Oct|Nov|Dec|Jan|Feb",  "Rock Garden|Sukhna Lake|Capitol Complex"),
    ("DN", "Dadra & Nagar Haveli","Mixed",      0.9,  "Oct|Nov|Dec|Jan|Feb",  "Vanganga Lake|Hirwa Van Garden"),
    ("DD", "Daman & Diu",       "Beach",        0.8,  "Oct|Nov|Dec|Jan|Feb",  "Diu Fort|Nagoa Beach|Devka Beach"),
    ("AN", "Andaman & Nicobar", "Beach",        0.6,  "Nov|Dec|Jan|Feb|Mar",  "Radhanagar Beach|Cellular Jail|Havelock"),
    ("PY", "Puducherry",        "Beach",        1.8,  "Oct|Nov|Dec|Jan|Feb",  "Auroville|Promenade Beach|French Quarter"),
    ("LD", "Lakshadweep",       "Beach",        0.1,  "Oct|Nov|Dec|Jan|Feb",  "Agatti Island|Bangaram|Kavaratti"),
]


# ---------------------------------------------------------------------------
# 1. dim_location.csv — 43 cities (one row each)
# ---------------------------------------------------------------------------
def gen_dim_location():
    rows = []
    for city, (state, region, zone, lat, lng, arrivals) in CITY_MAP.items():
        rows.append({
            "location_id": str(uuid.uuid4()),
            "city": city,
            "state": state,
            "region": region,
            "tourism_zone": zone,
            "latitude": round(lat, 6),
            "longitude": round(lng, 6),
            "tourist_arrivals_annual_m": arrivals,
            "peak_months": PEAK_MONTHS_BY_ZONE.get(zone, "Oct|Nov|Dec|Jan|Feb"),
        })
    df = pd.DataFrame(rows)
    df.to_csv(OUT_DIR / "dim_location.csv", index=False)
    return df


# ---------------------------------------------------------------------------
# 2. hotel_master.csv — NUM_HOTELS hotels
# ---------------------------------------------------------------------------
HOTEL_PREFIXES = {
    "branded":     ["OYO Flagship", "OYO Townhouse", "OYO Rooms", "Treebo Trend", "FabHotel",
                    "Lemon Tree Premier", "ibis", "Taj", "ITC", "The Oberoi", "Radisson Blu",
                    "Marriott", "Hyatt Regency", "Novotel"],
    "independent": ["Hotel", "Hotel", "Hotel", "Grand Hotel", "Royal Hotel", "Hotel",
                    "Sunrise", "Sunset", "Comfort", "Galaxy"],
    "heritage":    ["Haveli", "Palace", "Mahal", "Rawla", "Garh"],
    "budget":      ["Lodge", "Guest House", "Inn", "Stay Inn", "Backpackers Hostel"],
    "resort":      ["Beach Resort", "Hill Resort", "Spa Resort", "Eco Resort"],
}

CITY_NAME_FLAVOR = {
    "Goa": ["Sunset", "Beach", "Coconut", "Palms", "Lagoon", "Tropical"],
    "Calangute": ["Beach", "Sands", "Waves"],
    "Anjuna": ["Hippie", "Beach", "Sunset"],
    "Jaipur": ["Pink", "Royal", "Rajputana", "Hawa", "Amer"],
    "Udaipur": ["Lake", "Pichola", "Sajjan", "City Palace", "Aravalli"],
    "Jodhpur": ["Blue", "Mehrangarh", "Marwar"],
    "Jaisalmer": ["Golden", "Thar", "Desert"],
    "Manali": ["Snow", "Mountain", "Solang", "Beas"],
    "Shimla": ["Hill", "Ridge", "Mall", "Toy Train"],
    "Varanasi": ["Ganga", "Kashi", "Banaras", "Ghats"],
    "Rishikesh": ["Ganga", "Yoga", "Laxman", "Trayambakeshwar"],
    "Agra": ["Taj View", "Mughal", "Yamuna"],
    "Mumbai": ["Marine", "Bandra", "Andheri", "Juhu", "Powai"],
    "Delhi": ["Connaught", "Karol Bagh", "Paharganj", "Aerocity"],
}

CHAIN_OPTIONS = ["OYO", "Taj Hotels", "ITC Hotels", "Lemon Tree", "FabHotels",
                 "Treebo", "ibis", "Marriott", "Oberoi", "Radisson"]

PROPERTY_TYPES = ["Hotel", "Resort", "Guest House", "Homestay", "Lodge", "Cottage",
                  "Houseboat", "Palace", "Apartment", "Hostel", "BnB", "Villa",
                  "Tent", "Service Apartment"]

# Star distribution per spec — sums to 1.0
STAR_DIST = [(0, 0.35), (1, 0.12), (2, 0.10), (3, 0.20), (4, 0.15), (5, 0.08)]

AMENITIES = ["WiFi", "AC", "TV", "Parking facility", "Daily housekeeping", "Geyser",
             "Power backup", "In-house Restaurant", "Swimming Pool", "Spa", "Elevator",
             "CCTV cameras", "24/7 check-in", "Card payment", "Kitchen", "Mini Fridge",
             "King Sized Bed", "Bath Tub", "Room service", "Laundry"]

PRICE_TIER_BANDS = {
    "BUDGET":  (349, 1999),
    "MID":     (2000, 4999),
    "PREMIUM": (5000, 9999),
    "LUXURY":  (10000, 35000),
}

STAR_TO_TIER = {
    0: "BUDGET", 1: "BUDGET", 2: "BUDGET",
    3: "MID",
    4: "PREMIUM",
    5: "LUXURY",
}

STAR_TO_ROOM_RANGE = {
    0: (10, 40), 1: (10, 40), 2: (10, 40),
    3: (30, 80),
    4: (60, 150),
    5: (80, 300),
}

STAR_TO_RATING_RANGE = {
    0: (3.2, 3.8),
    1: (3.3, 3.9),
    2: (3.5, 4.0),
    3: (3.8, 4.3),
    4: (4.0, 4.6),
    5: (4.3, 4.9),
}

STAR_TO_REVIEW_COUNT = {
    0: (5, 200), 1: (5, 200), 2: (5, 200),
    3: (50, 500),
    4: (200, 2000),
    5: (500, 7000),
}


def pick_star():
    stars, weights = zip(*STAR_DIST)
    return random.choices(stars, weights=weights, k=1)[0]


def gen_hotel_name(city, star, prop_type):
    """Build a realistic Indian hotel name."""
    flavor = CITY_NAME_FLAVOR.get(city, [city])
    if star >= 4:
        # Branded or heritage
        if random.random() < 0.5:
            prefix = random.choice(HOTEL_PREFIXES["branded"])
            return f"{prefix} {random.choice(flavor)} {city}"
        else:
            prefix = random.choice(HOTEL_PREFIXES["heritage"])
            return f"{random.choice(flavor)} {prefix}"
    elif star == 3:
        if random.random() < 0.4:
            prefix = random.choice(HOTEL_PREFIXES["branded"])
            return f"{prefix} {city}"
        else:
            prefix = random.choice(HOTEL_PREFIXES["independent"])
            return f"{prefix} {random.choice(flavor)}"
    else:
        # Budget
        if prop_type == "Resort":
            return f"{random.choice(flavor)} {random.choice(HOTEL_PREFIXES['resort'])}"
        if random.random() < 0.5:
            prefix = random.choice(HOTEL_PREFIXES["budget"])
            return f"{random.choice(flavor)} {prefix}"
        else:
            return f"OYO {random.randint(100, 99999)} {random.choice(flavor)} {city}"


def gen_hotel_master(loc_df):
    loc_lookup = {row.city: row.location_id for row in loc_df.itertuples()}
    cities = list(loc_lookup.keys())

    rows = []
    for i in range(1, NUM_HOTELS + 1):
        hotel_id = f"HTL-{i:06d}"
        city = random.choices(cities, weights=[
            # weight metros + heritage higher (more hotels in reality)
            8 if c in {"Mumbai", "Delhi", "Bangalore", "Goa", "Jaipur", "Agra", "Varanasi"}
            else 5 if c in {"Udaipur", "Chennai", "Hyderabad", "Pune", "Kochi", "Manali", "Shimla", "Rishikesh"}
            else 2
            for c in cities
        ], k=1)[0]

        star = pick_star()
        tier = STAR_TO_TIER[star]
        price_min, price_max = PRICE_TIER_BANDS[tier]
        base_price = random.randint(price_min, price_max)

        prop_type = random.choices(
            PROPERTY_TYPES,
            weights=[40, 8, 12, 8, 10, 4, 1, 2, 3, 5, 2, 3, 1, 1],
            k=1
        )[0]

        # Chain assignment: ~60% NULL (independent)
        chain = None
        if random.random() > 0.6:
            if star >= 4:
                chain = random.choice(["Taj Hotels", "ITC Hotels", "Lemon Tree", "Marriott", "Oberoi", "Radisson", "ibis"])
            elif star == 3:
                chain = random.choice(["OYO", "Lemon Tree", "FabHotels", "Treebo", "ibis"])
            else:
                chain = random.choice(["OYO", "FabHotels", "Treebo"])

        rooms = random.randint(*STAR_TO_ROOM_RANGE[star])
        rating_lo, rating_hi = STAR_TO_RATING_RANGE[star]
        avg_rating = round(random.uniform(rating_lo, rating_hi), 2)
        review_count = random.randint(*STAR_TO_REVIEW_COUNT[star])

        # Amenities: budget 3-5, luxury 10-15
        amen_count = {0: (3, 5), 1: (3, 5), 2: (4, 6), 3: (6, 10), 4: (8, 13), 5: (10, 15)}[star]
        amenities = random.sample(AMENITIES, random.randint(*amen_count))

        rows.append({
            "hotel_id": hotel_id,
            "hotel_name": gen_hotel_name(city, star, prop_type),
            "chain_name": chain if chain else "",
            "property_type": prop_type,
            "star_category": star,
            "total_rooms": rooms,
            "location_id": loc_lookup[city],
            "_city": city,  # internal helper, removed before write
            "avg_rating": avg_rating,
            "amenities_json": json.dumps(amenities),
            "review_count": review_count,
            "price_tier_id": tier,
            "base_price_inr": base_price,
            "is_active": "TRUE" if random.random() < 0.95 else "FALSE",
        })

    df = pd.DataFrame(rows)
    df_out = df.drop(columns=["_city"])
    df_out.to_csv(OUT_DIR / "hotel_master.csv", index=False)
    return df  # keep _city for downstream use


# ---------------------------------------------------------------------------
# 3. dim_room_type.csv — 2-4 room types per hotel
# ---------------------------------------------------------------------------
ROOM_TYPE_NAMES = [
    "Standard Non AC", "Standard AC", "Deluxe", "Super Deluxe", "Executive",
    "Suite", "Premium Suite", "Cottage Room", "Valley View", "Sea View",
    "Pool View", "Heritage Room", "Luxury Suite",
]

ROOM_TYPES_BY_STAR = {
    0: ["Standard Non AC", "Standard AC"],
    1: ["Standard Non AC", "Standard AC", "Deluxe"],
    2: ["Standard Non AC", "Standard AC", "Deluxe"],
    3: ["Standard AC", "Deluxe", "Super Deluxe", "Executive"],
    4: ["Deluxe", "Super Deluxe", "Executive", "Suite", "Premium Suite"],
    5: ["Executive", "Suite", "Premium Suite", "Luxury Suite", "Heritage Room"],
}


def gen_dim_room_type(hotels_df):
    """Map of zone-specific bonus room types."""
    zone_extras = {
        "Beach": ["Sea View", "Pool View"],
        "Hill Station": ["Valley View", "Cottage Room"],
        "Heritage": ["Heritage Room"],
        "Backwater": ["Sea View"],
    }
    # build location -> zone lookup
    loc_zone = {row.location_id: row.tourism_zone for row in pd.read_csv(OUT_DIR / "dim_location.csv").itertuples()}

    rows = []
    for h in hotels_df.itertuples():
        star = h.star_category
        tier = h.price_tier_id
        price_min, price_max = PRICE_TIER_BANDS[tier]
        zone = loc_zone[h.location_id]

        # Number of room types: 2-4
        n_types = random.randint(2, 4)
        candidate_pool = list(ROOM_TYPES_BY_STAR[star])
        if zone in zone_extras:
            candidate_pool += zone_extras[zone]
        candidate_pool = list(set(candidate_pool))
        chosen = random.sample(candidate_pool, min(n_types, len(candidate_pool)))

        # Hill-station budget bias toward non-AC
        is_hill_budget = (zone == "Hill Station" and star <= 2)

        for rt_name in chosen:
            has_ac = "AC" in rt_name or rt_name not in ("Standard Non AC", "Cottage Room")
            if is_hill_budget:
                has_ac = random.random() < 0.4
            if "Non AC" in rt_name:
                has_ac = False
            elif rt_name in ("Suite", "Premium Suite", "Luxury Suite", "Executive"):
                has_ac = True

            # Suites have higher prices within tier
            price_jitter = {"Standard Non AC": 0.7, "Standard AC": 0.85, "Deluxe": 1.0,
                            "Super Deluxe": 1.15, "Executive": 1.3, "Suite": 1.5,
                            "Premium Suite": 1.7, "Luxury Suite": 2.0,
                            "Heritage Room": 1.4, "Valley View": 1.1, "Sea View": 1.2,
                            "Pool View": 1.15, "Cottage Room": 0.9}.get(rt_name, 1.0)
            base = random.randint(price_min, price_max)
            price = int(min(price_max, max(price_min, base * price_jitter)))

            rows.append({
                "room_type_id": str(uuid.uuid4()),
                "hotel_id": h.hotel_id,
                "type_name": rt_name,
                "capacity": random.choice([1, 2, 2, 2, 3, 4]),
                "has_ac": "TRUE" if has_ac else "FALSE",
                "has_breakfast": "TRUE" if random.random() < 0.30 else "FALSE",
                "price_tier": tier,
                "base_price_inr": price,
            })

    df = pd.DataFrame(rows)
    df.to_csv(OUT_DIR / "dim_room_type.csv", index=False)
    return df


# ---------------------------------------------------------------------------
# 4. reviews_raw.csv — the hardest one. Templated phrase bank.
# ---------------------------------------------------------------------------
INDIAN_FIRST_NAMES = [
    "Rahul", "Priya", "Amit", "Sunita", "Arjun", "Meera", "Vikram", "Anjali",
    "Rohit", "Neha", "Karan", "Pooja", "Sandeep", "Kavita", "Nikhil", "Divya",
    "Aditya", "Riya", "Manish", "Shruti", "Tarun", "Aishwarya", "Gaurav", "Sneha",
    "Vivek", "Ananya", "Suresh", "Lakshmi", "Mohan", "Geeta", "Rakesh", "Anita",
    "Sanjay", "Rekha", "Ravi", "Sushma", "Deepak", "Smita", "Prakash", "Bhavna",
    "Sachin", "Kiran", "Ajay", "Komal", "Vishal", "Nisha",
]
INDIAN_LAST_NAMES = [
    "Sharma", "Verma", "Gupta", "Singh", "Patel", "Reddy", "Iyer", "Nair",
    "Menon", "Krishnan", "Mehta", "Shah", "Jain", "Agarwal", "Chopra", "Kapoor",
    "Bhat", "Kulkarni", "Desai", "Rao", "Pillai", "Mukherjee", "Banerjee", "Das",
    "Khanna", "Malhotra", "Bose", "Chatterjee", "Pandey", "Trivedi", "Joshi",
    "Saxena", "Dubey", "Tiwari",
]

REVIEW_SOURCES = ["MakeMyTrip", "OYO", "Booking.com", "Goibibo", "TripAdvisor", "Google"]
TRAVEL_TYPES = ["Family", "Couple", "Solo", "Business", "Friends", "Pilgrimage"]
RATING_DIST = [(1, 0.08), (2, 0.05), (3, 0.12), (4, 0.30), (5, 0.45)]

# Phrase bank: theme -> list of phrases (drawn from spec, expanded)
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
    # City-specific
    "goa": [
        "Beach was just a 2-minute walk from the property.",
        "Party noise from neighbouring shacks till 2am, couldn't sleep.",
        "Checkout time of 10am was too early for a Goa holiday.",
        "Seafood thali at the in-house restaurant was outstanding, fresh prawns and pomfret.",
        "Pool was small but clean, kids loved it.",
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

# Irrelevant / off-topic templates (5% of reviews)
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


def gen_review_text(rating, city, zone, base_price):
    """Build a realistic review from theme phrases, weighted by rating."""
    parts = []

    # Choose theme weights based on rating
    if rating <= 2:
        neg_themes = random.sample(["cleanliness_neg", "wifi_neg", "food_neg", "staff_neg", "location_neg", "value_neg"], 2)
        for t in neg_themes:
            parts.append(random.choice(PHRASES[t]).replace("{price}", str(base_price)))
        # Sometimes one positive among complaints
        if random.random() < 0.3:
            parts.append(random.choice(PHRASES["staff_pos"] + PHRASES["location_pos"]))
    elif rating == 3:
        # Mixed
        pos = random.choice(["staff_pos", "location_pos", "food_pos", "cleanliness_pos"])
        neg = random.choice(["wifi_neg", "food_neg", "cleanliness_neg", "value_neg"])
        parts.append(random.choice(PHRASES[pos]))
        parts.append(random.choice(PHRASES[neg]).replace("{price}", str(base_price)))
        # Add one neutral observation
        if random.random() < 0.5:
            parts.append("Overall an average experience, nothing exceptional.")
    else:
        # 4-5 star: mostly positive
        pos_themes = random.sample(["staff_pos", "location_pos", "cleanliness_pos", "food_pos", "value_pos", "wifi_pos"], 2)
        for t in pos_themes:
            parts.append(random.choice(PHRASES[t]).replace("{price}", str(base_price)))
        if rating == 4 and random.random() < 0.4:
            # Add one minor complaint for realism
            parts.append("One small issue — " + random.choice(PHRASES["wifi_neg"] + PHRASES["food_neg"]).lower())

    # City/zone-specific flavor (50% chance)
    if random.random() < 0.5:
        if city in {"Goa", "Calangute", "Anjuna", "Panjim"}:
            parts.append(random.choice(PHRASES["goa"]))
        elif zone == "Heritage" and city in {"Jaipur", "Jodhpur", "Udaipur", "Jaisalmer", "Pushkar", "Bikaner", "Agra"}:
            parts.append(random.choice(PHRASES["rajasthan"]))
        elif zone == "Hill Station":
            parts.append(random.choice(PHRASES["hills"]))
        elif zone == "Pilgrimage":
            parts.append(random.choice(PHRASES["pilgrimage"]))
        elif zone == "Metro":
            parts.append(random.choice(PHRASES["metro"]))

    # Hinglish flavor (20% chance, mostly positive reviews)
    if rating >= 4 and random.random() < 0.2:
        parts.append(random.choice(PHRASES["hinglish"]))

    # Closing sentence
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

    # Robust 40-word minimum guarantee — keep adding context until threshold met
    padding_options = [
        " The location was decent for the most part and we managed to get around without too much difficulty.",
        " The check-in process took its time but nothing too bad in the grand scheme of things.",
        " Would consider this place again depending on the alternatives available at that time.",
        " The overall experience was in line with what we have come to expect from properties in this segment.",
        " There were a few small things here and there but nothing that significantly affected the stay.",
        " We had booked through one of the popular OTAs and the booking process itself was hassle-free.",
        " Parking was available though a bit limited during peak hours which is fairly standard.",
    ]
    while len(review.split()) < 42:
        review += random.choice(padding_options)

    return review


def gen_reviews(hotels_df, loc_df):
    loc_zone = {row.location_id: row.tourism_zone for row in loc_df.itertuples()}

    # weighted hotel pool — more reviews for hotels with higher review_count
    hotel_pool = hotels_df.to_dict("records")
    weights = [h["review_count"] for h in hotel_pool]

    rows = []
    n_irrelevant = int(NUM_REVIEWS * IRRELEVANT_REVIEW_FRAC)
    n_relevant = NUM_REVIEWS - n_irrelevant

    # Relevant reviews
    for _ in range(n_relevant):
        hotel = random.choices(hotel_pool, weights=weights, k=1)[0]
        rating = random.choices([r for r, _ in RATING_DIST], weights=[w for _, w in RATING_DIST], k=1)[0]
        # Mild bias toward hotel's avg_rating — but keep target distribution mostly intact
        if random.random() < 0.25:
            target = round(hotel["avg_rating"])
            rating = max(1, min(5, target + random.choice([-1, 0, 1])))

        zone = loc_zone[hotel["location_id"]]
        text = gen_review_text(rating, hotel["_city"], zone, hotel["base_price_inr"])

        rows.append({
            "review_id": str(uuid.uuid4()),
            "hotel_id": hotel["hotel_id"],
            "reviewer_name": f"{random.choice(INDIAN_FIRST_NAMES)} {random.choice(INDIAN_LAST_NAMES)}",
            "review_text": text,
            "rating": rating,
            "review_date": (date(2022, 1, 1) + timedelta(days=random.randint(0, 1094))).isoformat(),
            "source": random.choice(REVIEW_SOURCES),
            "travel_type": random.choice(TRAVEL_TYPES),
        })

    # Irrelevant reviews — random hotels, random ratings (often 3)
    irrelevant_pad = [
        " Anyway, sharing this so others know what to expect.",
        " The trip itself had its ups and downs but that's how it goes sometimes.",
        " Not the hotel's fault really, just sharing context about our stay.",
        " Two stars to the overall trip, three to the hotel I suppose.",
        " Take this review with a grain of salt given the circumstances.",
    ]
    for _ in range(n_irrelevant):
        hotel = random.choice(hotel_pool)
        rating = random.choices([1, 2, 3, 3, 3, 4, 5], k=1)[0]
        text = random.choice(IRRELEVANT_REVIEWS)
        while len(text.split()) < 42:
            text += random.choice(irrelevant_pad)

        rows.append({
            "review_id": str(uuid.uuid4()),
            "hotel_id": hotel["hotel_id"],
            "reviewer_name": f"{random.choice(INDIAN_FIRST_NAMES)} {random.choice(INDIAN_LAST_NAMES)}",
            "review_text": text,
            "rating": rating,
            "review_date": (date(2022, 1, 1) + timedelta(days=random.randint(0, 1094))).isoformat(),
            "source": random.choice(REVIEW_SOURCES),
            "travel_type": random.choice(TRAVEL_TYPES),
        })

    random.shuffle(rows)
    df = pd.DataFrame(rows)
    df.to_csv(OUT_DIR / "reviews_raw.csv", index=False)
    return df


# ---------------------------------------------------------------------------
# 5. booking_events_seed.json — Kafka simulator config
# ---------------------------------------------------------------------------
def gen_booking_seeds(hotels_df):
    # Pick NUM_BOOKING_SEEDS hotels, prefer active ones across all cities/tiers
    active = hotels_df[hotels_df["is_active"] == "TRUE"].copy()
    sample = active.sample(min(NUM_BOOKING_SEEDS, len(active)), random_state=SEED)

    loc_df = pd.read_csv(OUT_DIR / "dim_location.csv")
    loc_zone = {row.location_id: row.tourism_zone for row in loc_df.itertuples()}

    rt_df = pd.read_csv(OUT_DIR / "dim_room_type.csv")
    rt_by_hotel = rt_df.groupby("hotel_id")["type_name"].apply(list).to_dict()

    # OTA distributions per spec
    ota_budget   = {"OYO": 0.55, "MakeMyTrip": 0.25, "Direct": 0.10, "Walk-in": 0.10}
    ota_mid      = {"MakeMyTrip": 0.40, "Goibibo": 0.20, "Booking.com": 0.15, "Direct": 0.15, "Walk-in": 0.10}
    ota_luxury   = {"Direct": 0.40, "Booking.com": 0.25, "MakeMyTrip": 0.20, "Agoda": 0.15}

    # Zone-based peak/off multipliers + months
    zone_pattern = {
        "Beach":        {"peak": ["November","December","January","February"], "peak_mult": 2.8, "off_mult": 0.25},
        "Heritage":     {"peak": ["October","November","December","January","February"], "peak_mult": 2.5, "off_mult": 0.40},
        "Hill Station": {"peak": ["April","May","June","October"], "peak_mult": 2.2, "off_mult": 0.60},
        "Pilgrimage":   {"peak": ["October","November","March","April"], "peak_mult": 3.0, "off_mult": 0.55},
        "Wildlife":     {"peak": ["November","December","January","February","March"], "peak_mult": 2.4, "off_mult": 0.50},
        "Metro":        {"peak": ["January","February","March","October","November","December"], "peak_mult": 1.4, "off_mult": 0.85},
        "Backwater":    {"peak": ["September","October","November","December","January"], "peak_mult": 2.6, "off_mult": 0.35},
    }

    records = []
    for _, h in sample.iterrows():
        h_city = h["_city"]
        zone = loc_zone[h["location_id"]]
        pattern = zone_pattern.get(zone, zone_pattern["Metro"])
        tier = h.price_tier_id

        if tier == "BUDGET":
            ota_mix = ota_budget
            base_daily = random.randint(4, 12)
        elif tier == "MID":
            ota_mix = ota_mid
            base_daily = random.randint(8, 20)
        elif tier == "PREMIUM":
            ota_mix = ota_mid
            base_daily = random.randint(15, 35)
        else:
            ota_mix = ota_luxury
            base_daily = random.randint(20, 50)

        # Adjust OYO chains
        if h.get("chain_name") == "OYO":
            ota_mix = ota_budget

        price_min, price_max = PRICE_TIER_BANDS[tier]

        records.append({
            "hotel_id": h["hotel_id"],
            "city": h_city,
            "tier": tier,
            "base_daily_bookings": base_daily,
            "peak_multiplier": pattern["peak_mult"],
            "off_season_multiplier": pattern["off_mult"],
            "peak_months": pattern["peak"],
            "cancellation_rate_base": round(random.uniform(0.08, 0.18), 3),
            "avg_stay_nights": round(random.uniform(1.5, 4.5), 1),
            "booking_sources": ota_mix,
            "room_types": rt_by_hotel.get(h["hotel_id"], ["Standard AC"]),
            "price_range_inr": {"min": price_min, "max": price_max},
        })

    with open(OUT_DIR / "booking_events_seed.json", "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# 6. public_holidays.csv — Indian holidays 2020-2026
# ---------------------------------------------------------------------------
def gen_public_holidays():
    """Manually curated real Indian holiday dates."""
    # (date, name, type, states_applicable, demand_impact)
    base_holidays = [
        # National holidays — same date every year
        ("01-26", "Republic Day", "National", "ALL", "MEDIUM"),
        ("08-15", "Independence Day", "National", "ALL", "MEDIUM"),
        ("10-02", "Gandhi Jayanti", "National", "ALL", "MEDIUM"),
        ("12-25", "Christmas", "Religious", "ALL", "HIGH"),
        ("01-01", "New Year", "National", "ALL", "HIGH"),
    ]
    # Year-specific real dates for movable holidays
    year_specific = {
        2020: [("11-14","Diwali","Religious","ALL","HIGH"),("10-25","Dussehra","Religious","ALL","HIGH"),("03-10","Holi","Religious","ALL","HIGH"),("05-24","Eid ul-Fitr","Religious","ALL","HIGH"),("07-31","Eid ul-Adha","Religious","ALL","HIGH"),("10-17","Navratri","Religious","ALL","HIGH"),("10-26","Durga Puja","Religious","WB|AS|OD|BR","HIGH"),("01-15","Pongal","Religious","TN","MEDIUM"),("09-01","Onam","Religious","KL","HIGH"),("04-13","Baisakhi","Religious","PB|HR","MEDIUM"),("04-02","Ram Navami","Religious","ALL","MEDIUM"),("08-11","Janmashtami","Religious","ALL","MEDIUM"),("02-21","Maha Shivratri","Religious","ALL","MEDIUM"),("04-10","Good Friday","Religious","ALL","MEDIUM"),("05-07","Buddha Purnima","Religious","ALL","LOW"),("11-30","Guru Nanak Jayanti","Religious","PB|HR|DL","MEDIUM")],
        2021: [("11-04","Diwali","Religious","ALL","HIGH"),("10-15","Dussehra","Religious","ALL","HIGH"),("03-29","Holi","Religious","ALL","HIGH"),("05-13","Eid ul-Fitr","Religious","ALL","HIGH"),("07-20","Eid ul-Adha","Religious","ALL","HIGH"),("10-07","Navratri","Religious","ALL","HIGH"),("10-15","Durga Puja","Religious","WB|AS|OD|BR","HIGH"),("01-14","Pongal","Religious","TN","MEDIUM"),("08-21","Onam","Religious","KL","HIGH"),("04-13","Baisakhi","Religious","PB|HR","MEDIUM"),("04-21","Ram Navami","Religious","ALL","MEDIUM"),("08-30","Janmashtami","Religious","ALL","MEDIUM"),("03-11","Maha Shivratri","Religious","ALL","MEDIUM"),("04-02","Good Friday","Religious","ALL","MEDIUM"),("05-26","Buddha Purnima","Religious","ALL","LOW"),("11-19","Guru Nanak Jayanti","Religious","PB|HR|DL","MEDIUM")],
        2022: [("10-24","Diwali","Religious","ALL","HIGH"),("10-05","Dussehra","Religious","ALL","HIGH"),("03-18","Holi","Religious","ALL","HIGH"),("05-03","Eid ul-Fitr","Religious","ALL","HIGH"),("07-10","Eid ul-Adha","Religious","ALL","HIGH"),("09-26","Navratri","Religious","ALL","HIGH"),("10-05","Durga Puja","Religious","WB|AS|OD|BR","HIGH"),("01-14","Pongal","Religious","TN","MEDIUM"),("09-08","Onam","Religious","KL","HIGH"),("04-14","Baisakhi","Religious","PB|HR","MEDIUM"),("04-10","Ram Navami","Religious","ALL","MEDIUM"),("08-19","Janmashtami","Religious","ALL","MEDIUM"),("03-01","Maha Shivratri","Religious","ALL","MEDIUM"),("04-15","Good Friday","Religious","ALL","MEDIUM"),("05-16","Buddha Purnima","Religious","ALL","LOW"),("11-08","Guru Nanak Jayanti","Religious","PB|HR|DL","MEDIUM")],
        2023: [("11-12","Diwali","Religious","ALL","HIGH"),("10-24","Dussehra","Religious","ALL","HIGH"),("03-08","Holi","Religious","ALL","HIGH"),("04-22","Eid ul-Fitr","Religious","ALL","HIGH"),("06-29","Eid ul-Adha","Religious","ALL","HIGH"),("10-15","Navratri","Religious","ALL","HIGH"),("10-24","Durga Puja","Religious","WB|AS|OD|BR","HIGH"),("01-15","Pongal","Religious","TN","MEDIUM"),("08-29","Onam","Religious","KL","HIGH"),("04-14","Baisakhi","Religious","PB|HR","MEDIUM"),("03-30","Ram Navami","Religious","ALL","MEDIUM"),("09-07","Janmashtami","Religious","ALL","MEDIUM"),("02-18","Maha Shivratri","Religious","ALL","MEDIUM"),("04-07","Good Friday","Religious","ALL","MEDIUM"),("05-05","Buddha Purnima","Religious","ALL","LOW"),("11-27","Guru Nanak Jayanti","Religious","PB|HR|DL","MEDIUM")],
        2024: [("11-01","Diwali","Religious","ALL","HIGH"),("10-12","Dussehra","Religious","ALL","HIGH"),("03-25","Holi","Religious","ALL","HIGH"),("04-10","Eid ul-Fitr","Religious","ALL","HIGH"),("06-17","Eid ul-Adha","Religious","ALL","HIGH"),("10-03","Navratri","Religious","ALL","HIGH"),("10-12","Durga Puja","Religious","WB|AS|OD|BR","HIGH"),("01-15","Pongal","Religious","TN","MEDIUM"),("09-15","Onam","Religious","KL","HIGH"),("04-13","Baisakhi","Religious","PB|HR","MEDIUM"),("04-17","Ram Navami","Religious","ALL","MEDIUM"),("08-26","Janmashtami","Religious","ALL","MEDIUM"),("03-08","Maha Shivratri","Religious","ALL","MEDIUM"),("03-29","Good Friday","Religious","ALL","MEDIUM"),("05-23","Buddha Purnima","Religious","ALL","LOW"),("11-15","Guru Nanak Jayanti","Religious","PB|HR|DL","MEDIUM")],
        2025: [("10-21","Diwali","Religious","ALL","HIGH"),("10-02","Dussehra","Religious","ALL","HIGH"),("03-14","Holi","Religious","ALL","HIGH"),("03-31","Eid ul-Fitr","Religious","ALL","HIGH"),("06-07","Eid ul-Adha","Religious","ALL","HIGH"),("09-22","Navratri","Religious","ALL","HIGH"),("10-02","Durga Puja","Religious","WB|AS|OD|BR","HIGH"),("01-14","Pongal","Religious","TN","MEDIUM"),("09-05","Onam","Religious","KL","HIGH"),("04-13","Baisakhi","Religious","PB|HR","MEDIUM"),("04-06","Ram Navami","Religious","ALL","MEDIUM"),("08-15","Janmashtami","Religious","ALL","MEDIUM"),("02-26","Maha Shivratri","Religious","ALL","MEDIUM"),("04-18","Good Friday","Religious","ALL","MEDIUM"),("05-12","Buddha Purnima","Religious","ALL","LOW"),("11-05","Guru Nanak Jayanti","Religious","PB|HR|DL","MEDIUM")],
        2026: [("11-08","Diwali","Religious","ALL","HIGH"),("10-20","Dussehra","Religious","ALL","HIGH"),("03-04","Holi","Religious","ALL","HIGH"),("03-20","Eid ul-Fitr","Religious","ALL","HIGH"),("05-27","Eid ul-Adha","Religious","ALL","HIGH"),("10-11","Navratri","Religious","ALL","HIGH"),("10-20","Durga Puja","Religious","WB|AS|OD|BR","HIGH"),("01-14","Pongal","Religious","TN","MEDIUM"),("08-26","Onam","Religious","KL","HIGH"),("04-14","Baisakhi","Religious","PB|HR","MEDIUM"),("03-26","Ram Navami","Religious","ALL","MEDIUM"),("09-04","Janmashtami","Religious","ALL","MEDIUM"),("02-15","Maha Shivratri","Religious","ALL","MEDIUM"),("04-03","Good Friday","Religious","ALL","MEDIUM"),("05-31","Buddha Purnima","Religious","ALL","LOW"),("11-24","Guru Nanak Jayanti","Religious","PB|HR|DL","MEDIUM")],
    }

    rows = []
    for year in range(2020, 2027):
        for mmdd, name, htype, states, impact in base_holidays:
            rows.append({
                "holiday_date": f"{year}-{mmdd}",
                "holiday_name": name,
                "holiday_type": htype,
                "states_applicable": states,
                "demand_impact": impact,
            })
        for mmdd, name, htype, states, impact in year_specific[year]:
            rows.append({
                "holiday_date": f"{year}-{mmdd}",
                "holiday_name": name,
                "holiday_type": htype,
                "states_applicable": states,
                "demand_impact": impact,
            })

    df = pd.DataFrame(rows).sort_values("holiday_date").reset_index(drop=True)
    df.to_csv(OUT_DIR / "public_holidays.csv", index=False)


# ---------------------------------------------------------------------------
# 7. india_states_zones.csv
# ---------------------------------------------------------------------------
def gen_india_states_zones():
    rows = [{
        "state_code": code, "state_name": name, "zone": zone,
        "tourist_arrivals_m": arrivals, "peak_months": peak,
        "primary_attractions": attr,
    } for code, name, zone, arrivals, peak, attr in INDIA_STATES]
    df = pd.DataFrame(rows)
    df.to_csv(OUT_DIR / "india_states_zones.csv", index=False)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def validate(hotels_df):
    """Print validation summary."""
    print("\n" + "=" * 60)
    print("VALIDATION REPORT")
    print("=" * 60)

    hm = pd.read_csv(OUT_DIR / "hotel_master.csv")
    loc = pd.read_csv(OUT_DIR / "dim_location.csv")
    rt = pd.read_csv(OUT_DIR / "dim_room_type.csv")
    rv = pd.read_csv(OUT_DIR / "reviews_raw.csv")
    hol = pd.read_csv(OUT_DIR / "public_holidays.csv")
    sz = pd.read_csv(OUT_DIR / "india_states_zones.csv")

    print(f"\nRow counts:")
    print(f"  hotel_master.csv:        {len(hm):>7,}")
    print(f"  dim_location.csv:        {len(loc):>7,}")
    print(f"  dim_room_type.csv:       {len(rt):>7,}")
    print(f"  reviews_raw.csv:         {len(rv):>7,}")
    print(f"  public_holidays.csv:     {len(hol):>7,}")
    print(f"  india_states_zones.csv:  {len(sz):>7,}")

    # FK integrity
    print(f"\nFK integrity:")
    orphan_rt = rt[~rt["hotel_id"].isin(hm["hotel_id"])]
    orphan_rv = rv[~rv["hotel_id"].isin(hm["hotel_id"])]
    orphan_loc = hm[~hm["location_id"].isin(loc["location_id"])]
    print(f"  Orphan room types:       {len(orphan_rt)}")
    print(f"  Orphan reviews:          {len(orphan_rv)}")
    print(f"  Orphan hotel→location:   {len(orphan_loc)}")

    # Star distribution
    print(f"\nStar distribution (target: 35/12/10/20/15/8):")
    star_dist = hm["star_category"].value_counts(normalize=True).sort_index() * 100
    for star, pct in star_dist.items():
        print(f"  {int(star)}-star: {pct:5.1f}%")

    # Price tier check
    print(f"\nPrice band consistency:")
    for tier, (lo, hi) in PRICE_TIER_BANDS.items():
        subset = hm[hm["price_tier_id"] == tier]
        out_of_band = subset[(subset["base_price_inr"] < lo) | (subset["base_price_inr"] > hi)]
        print(f"  {tier:7s} ({lo}-{hi}): {len(subset)} hotels, {len(out_of_band)} out of band")

    # Rating distribution
    print(f"\nReview rating distribution (target: 8/5/12/30/45):")
    rating_dist = rv["rating"].value_counts(normalize=True).sort_index() * 100
    for r, pct in rating_dist.items():
        print(f"  {int(r)}-star: {pct:5.1f}%")

    # Review text length check
    rv["word_count"] = rv["review_text"].str.split().str.len()
    short_reviews = rv[rv["word_count"] < 40]
    print(f"\nReview text quality:")
    print(f"  Avg word count:          {rv['word_count'].mean():.1f}")
    print(f"  Reviews under 40 words:  {len(short_reviews)}")
    print(f"  Min words:               {rv['word_count'].min()}")
    print(f"  Max words:               {rv['word_count'].max()}")


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(f"Generating TravelLens datasets → {OUT_DIR}")
    print(f"  Hotels: {NUM_HOTELS:,}, Reviews: {NUM_REVIEWS:,} ({IRRELEVANT_REVIEW_FRAC*100:.0f}% off-topic)")

    print("\n[1/7] dim_location.csv ...", end=" ")
    loc_df = gen_dim_location()
    print(f"{len(loc_df)} rows")

    print("[2/7] hotel_master.csv ...", end=" ")
    hotels_df = gen_hotel_master(loc_df)
    print(f"{len(hotels_df)} rows")

    print("[3/7] dim_room_type.csv ...", end=" ")
    rt_df = gen_dim_room_type(hotels_df)
    print(f"{len(rt_df)} rows")

    print("[4/7] reviews_raw.csv ...", end=" ")
    rv_df = gen_reviews(hotels_df, loc_df)
    print(f"{len(rv_df)} rows")

    print("[5/7] booking_events_seed.json ...", end=" ")
    gen_booking_seeds(hotels_df)
    print(f"{NUM_BOOKING_SEEDS} seeds")

    print("[6/7] public_holidays.csv ...", end=" ")
    gen_public_holidays()
    print("done")

    print("[7/7] india_states_zones.csv ...", end=" ")
    gen_india_states_zones()
    print(f"{len(INDIA_STATES)} rows")

    validate(hotels_df)
    print("\n✓ All datasets generated successfully.")
