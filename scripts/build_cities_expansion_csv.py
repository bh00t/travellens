"""
TravelLens India — Stage 1 dimension expansion: city catalog builder (B-046).

Emits a curated catalog of net-new Indian cities to seeds/cities_expansion.csv.
Deterministic — seed=42 + fixed iteration order over the inline catalog.

The CSV is the source of truth for the additive expansion. This script is
how it was generated and is committed alongside it.

Schema (matches dim_location):
  city, state, region, tourism_zone, latitude, longitude,
  tourist_arrivals_annual_m, peak_months, popularity_tier

`popularity_tier` is an extra column (not in dim_location). The expansion
script uses it to size per-city hotel counts (mega/major/mid/small/obscure).

Run:  python -m scripts.build_cities_expansion_csv
"""

import csv
import random
from pathlib import Path

SEED = 42
random.seed(SEED)

REPO_ROOT = Path(__file__).resolve().parent.parent
OUT_PATH = REPO_ROOT / "seeds" / "cities_expansion.csv"
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# State → region (must match india_states_zones values exactly)
# ---------------------------------------------------------------------------
STATE_REGION = {
    "Andhra Pradesh": "South India",
    "Arunachal Pradesh": "East India",
    "Assam": "East India",
    "Bihar": "East India",
    "Chhattisgarh": "East India",
    "Goa": "West India",
    "Gujarat": "West India",
    "Haryana": "North India",
    "Himachal Pradesh": "North India",
    "Jharkhand": "East India",
    "Karnataka": "South India",
    "Kerala": "South India",
    "Madhya Pradesh": "North India",
    "Maharashtra": "West India",
    "Manipur": "East India",
    "Meghalaya": "East India",
    "Mizoram": "East India",
    "Nagaland": "East India",
    "Odisha": "East India",
    "Punjab": "North India",
    "Rajasthan": "North India",
    "Sikkim": "East India",
    "Tamil Nadu": "South India",
    "Telangana": "South India",
    "Tripura": "East India",
    "Uttar Pradesh": "North India",
    "Uttarakhand": "North India",
    "West Bengal": "East India",
    "Andaman & Nicobar": "East India",
    "Chandigarh": "North India",
    "Dadra & Nagar Haveli": "West India",
    "Daman & Diu": "West India",
    "Delhi NCR": "North India",
    "Jammu & Kashmir": "North India",
    "Ladakh": "North India",
    "Lakshadweep": "South India",
    "Puducherry": "South India",
}

# State centroid (lat, lng) — used to seed per-city coordinates with bounded jitter
STATE_CENTROID = {
    "Andhra Pradesh":      (15.9100, 79.7400),
    "Arunachal Pradesh":   (28.2200, 94.7300),
    "Assam":               (26.2000, 92.9400),
    "Bihar":               (25.1000, 85.3100),
    "Chhattisgarh":        (21.2800, 81.8700),
    "Goa":                 (15.3000, 74.1200),
    "Gujarat":             (22.2600, 71.1900),
    "Haryana":             (29.0600, 76.0900),
    "Himachal Pradesh":    (31.1000, 77.1700),
    "Jharkhand":           (23.6100, 85.2800),
    "Karnataka":           (15.3200, 75.7100),
    "Kerala":              (10.8500, 76.2700),
    "Madhya Pradesh":      (22.9700, 78.6600),
    "Maharashtra":         (19.7500, 75.7100),
    "Manipur":             (24.6600, 93.9100),
    "Meghalaya":           (25.4700, 91.3700),
    "Mizoram":             (23.1600, 92.9400),
    "Nagaland":            (26.1600, 94.5600),
    "Odisha":              (20.9500, 85.1000),
    "Punjab":              (31.1500, 75.3400),
    "Rajasthan":           (27.0200, 74.2200),
    "Sikkim":              (27.5300, 88.5100),
    "Tamil Nadu":          (11.1300, 78.6600),
    "Telangana":           (18.1100, 79.0200),
    "Tripura":             (23.9400, 91.9900),
    "Uttar Pradesh":       (26.8500, 80.9500),
    "Uttarakhand":         (30.0700, 79.0200),
    "West Bengal":         (22.9900, 87.8600),
    "Andaman & Nicobar":   (11.7400, 92.6500),
    "Chandigarh":          (30.7300, 76.7800),
    "Dadra & Nagar Haveli":(20.1800, 73.0200),
    "Daman & Diu":         (20.4000, 72.8300),
    "Delhi NCR":           (28.6100, 77.2100),
    "Jammu & Kashmir":     (33.7800, 76.5700),
    "Ladakh":              (34.2100, 77.6500),
    "Lakshadweep":         (10.5700, 72.6400),
    "Puducherry":          (11.9100, 79.8100),
}

# Per-state jitter half-width in degrees — small UTs get tight bounds
SMALL = 0.30
MEDIUM = 0.80
LARGE = 1.80

STATE_JITTER = {
    # Small footprints
    "Goa": SMALL, "Chandigarh": SMALL, "Delhi NCR": SMALL,
    "Dadra & Nagar Haveli": SMALL, "Daman & Diu": SMALL,
    "Lakshadweep": SMALL, "Puducherry": SMALL,
    "Andaman & Nicobar": MEDIUM, "Sikkim": SMALL, "Tripura": SMALL,
    # Medium
    "Kerala": MEDIUM, "Punjab": MEDIUM, "Haryana": MEDIUM,
    "Himachal Pradesh": MEDIUM, "Uttarakhand": MEDIUM,
    "Manipur": MEDIUM, "Meghalaya": MEDIUM, "Mizoram": MEDIUM,
    "Nagaland": MEDIUM, "Jharkhand": MEDIUM, "Bihar": MEDIUM,
    "Chhattisgarh": MEDIUM, "Jammu & Kashmir": MEDIUM, "Assam": MEDIUM,
    "Arunachal Pradesh": MEDIUM, "Ladakh": MEDIUM,
    # Large by default — see fallback below
}

# Zone definitions per the approved plan
PEAK_MONTHS_BY_ZONE = {
    "Heritage":     "Oct|Nov|Dec|Jan|Feb",
    "Beach":        "Nov|Dec|Jan|Feb",
    "Hill Station": "Apr|May|Jun|Sep|Oct",
    "Pilgrimage":   "Oct|Nov|Mar|Apr",
    "Wildlife":     "Nov|Dec|Jan|Feb|Mar",
    "Metro":        "Jan|Feb|Mar|Oct|Nov|Dec",
    "Backwater":    "Sep|Oct|Nov|Dec|Jan",
}

ZONE_FACTOR = {
    "Heritage":     1.4,
    "Pilgrimage":   2.0,
    "Beach":        1.5,
    "Hill Station": 1.2,
    "Wildlife":     0.7,
    "Backwater":    1.0,
    "Metro":        1.0,
}

POP_TIER_BASE = {
    "mega":     8.0,
    "major":    2.5,
    "mid":      0.8,
    "small":    0.25,
    "obscure":  0.08,
}

ARRIVALS_CAP = 24.0  # demand proxy cap (decision #4)

# ---------------------------------------------------------------------------
# CURATED CATALOG — (city, state, zone, popularity_tier)
# Order is stable; do NOT shuffle — the iteration order seeds lat/lng jitter.
# Excludes the existing 44 cities by (city, state).
# ---------------------------------------------------------------------------
CITIES = [
    # =====================================================================
    # HERITAGE (~150)
    # =====================================================================
    # Rajasthan
    ("Bharatpur", "Rajasthan", "Heritage", "mid"),
    ("Alwar", "Rajasthan", "Heritage", "mid"),
    ("Mandawa", "Rajasthan", "Heritage", "mid"),
    ("Nawalgarh", "Rajasthan", "Heritage", "small"),
    ("Sikar", "Rajasthan", "Heritage", "small"),
    ("Karauli", "Rajasthan", "Heritage", "small"),
    ("Tonk", "Rajasthan", "Heritage", "small"),
    ("Bundi", "Rajasthan", "Heritage", "mid"),
    ("Chittorgarh", "Rajasthan", "Heritage", "mid"),
    ("Banswara", "Rajasthan", "Heritage", "small"),
    ("Dungarpur", "Rajasthan", "Heritage", "small"),
    ("Sirohi", "Rajasthan", "Heritage", "small"),
    ("Pali", "Rajasthan", "Heritage", "small"),
    ("Jalore", "Rajasthan", "Heritage", "small"),
    ("Barmer", "Rajasthan", "Heritage", "small"),
    ("Osian", "Rajasthan", "Heritage", "obscure"),
    ("Phalodi", "Rajasthan", "Heritage", "obscure"),
    ("Deogarh", "Rajasthan", "Heritage", "small"),
    ("Kumbhalgarh", "Rajasthan", "Heritage", "mid"),
    ("Ranakpur", "Rajasthan", "Heritage", "mid"),
    ("Nagaur", "Rajasthan", "Heritage", "small"),
    ("Bayana", "Rajasthan", "Heritage", "obscure"),
    ("Roopangarh", "Rajasthan", "Heritage", "obscure"),
    ("Lakshmangarh", "Rajasthan", "Heritage", "obscure"),
    # Uttar Pradesh (Heritage)
    ("Fatehpur Sikri", "Uttar Pradesh", "Heritage", "major"),
    ("Sarnath", "Uttar Pradesh", "Heritage", "mid"),
    ("Mahoba", "Uttar Pradesh", "Heritage", "small"),
    ("Kalinjar", "Uttar Pradesh", "Heritage", "small"),
    ("Bithoor", "Uttar Pradesh", "Heritage", "small"),
    ("Jhansi", "Uttar Pradesh", "Heritage", "mid"),
    ("Lalitpur", "Uttar Pradesh", "Heritage", "small"),
    ("Hastinapur", "Uttar Pradesh", "Heritage", "small"),
    ("Kannauj", "Uttar Pradesh", "Heritage", "small"),
    ("Mainpuri", "Uttar Pradesh", "Heritage", "obscure"),
    ("Etah", "Uttar Pradesh", "Heritage", "obscure"),
    ("Bahraich", "Uttar Pradesh", "Heritage", "small"),
    ("Sankisa", "Uttar Pradesh", "Heritage", "obscure"),
    ("Etawah", "Uttar Pradesh", "Heritage", "obscure"),
    ("Pratapgarh", "Uttar Pradesh", "Heritage", "obscure"),
    # Madhya Pradesh (Heritage)
    ("Orchha", "Madhya Pradesh", "Heritage", "mid"),
    ("Mandu", "Madhya Pradesh", "Heritage", "mid"),
    ("Sanchi", "Madhya Pradesh", "Heritage", "mid"),
    ("Gwalior", "Madhya Pradesh", "Heritage", "major"),
    ("Maheshwar", "Madhya Pradesh", "Heritage", "small"),
    ("Bhojpur", "Madhya Pradesh", "Heritage", "small"),
    ("Vidisha", "Madhya Pradesh", "Heritage", "small"),
    ("Burhanpur", "Madhya Pradesh", "Heritage", "small"),
    ("Chanderi", "Madhya Pradesh", "Heritage", "small"),
    ("Datia", "Madhya Pradesh", "Heritage", "small"),
    ("Shivpuri", "Madhya Pradesh", "Heritage", "small"),
    ("Mandsaur", "Madhya Pradesh", "Heritage", "obscure"),
    ("Bagh", "Madhya Pradesh", "Heritage", "obscure"),
    ("Asirgarh", "Madhya Pradesh", "Heritage", "obscure"),
    # Gujarat (Heritage)
    ("Champaner", "Gujarat", "Heritage", "small"),
    ("Patan", "Gujarat", "Heritage", "small"),
    ("Modhera", "Gujarat", "Heritage", "small"),
    ("Junagadh", "Gujarat", "Heritage", "mid"),
    ("Vadnagar", "Gujarat", "Heritage", "small"),
    ("Lothal", "Gujarat", "Heritage", "obscure"),
    ("Dholavira", "Gujarat", "Heritage", "small"),
    ("Vijaynagar", "Gujarat", "Heritage", "obscure"),
    ("Idar", "Gujarat", "Heritage", "obscure"),
    ("Sidhpur", "Gujarat", "Heritage", "obscure"),
    ("Halvad", "Gujarat", "Heritage", "obscure"),
    ("Halol", "Gujarat", "Heritage", "obscure"),
    # Karnataka (Heritage)
    ("Mysuru", "Karnataka", "Heritage", "major"),
    ("Hampi", "Karnataka", "Heritage", "major"),
    ("Badami", "Karnataka", "Heritage", "mid"),
    ("Aihole", "Karnataka", "Heritage", "small"),
    ("Pattadakal", "Karnataka", "Heritage", "small"),
    ("Bidar", "Karnataka", "Heritage", "mid"),
    ("Vijayapura", "Karnataka", "Heritage", "mid"),
    ("Halebidu", "Karnataka", "Heritage", "small"),
    ("Belur", "Karnataka", "Heritage", "small"),
    ("Sravanabelagola", "Karnataka", "Heritage", "small"),
    ("Lakkundi", "Karnataka", "Heritage", "obscure"),
    ("Anegundi", "Karnataka", "Heritage", "obscure"),
    # Tamil Nadu (Heritage)
    ("Mahabalipuram", "Tamil Nadu", "Heritage", "major"),
    ("Kanchipuram", "Tamil Nadu", "Heritage", "mid"),
    ("Thanjavur", "Tamil Nadu", "Heritage", "mid"),
    ("Gangaikonda Cholapuram", "Tamil Nadu", "Heritage", "small"),
    ("Kumbakonam", "Tamil Nadu", "Heritage", "small"),
    ("Darasuram", "Tamil Nadu", "Heritage", "obscure"),
    ("Chettinad", "Tamil Nadu", "Heritage", "mid"),
    ("Karaikudi", "Tamil Nadu", "Heritage", "small"),
    ("Padmanabhapuram", "Tamil Nadu", "Heritage", "small"),
    ("Thirumayam", "Tamil Nadu", "Heritage", "obscure"),
    ("Pudukkottai", "Tamil Nadu", "Heritage", "small"),
    # Telangana (Heritage)
    ("Warangal", "Telangana", "Heritage", "mid"),
    ("Bhongir", "Telangana", "Heritage", "small"),
    ("Mahbubnagar", "Telangana", "Heritage", "small"),
    ("Karimnagar", "Telangana", "Heritage", "small"),
    ("Nizamabad", "Telangana", "Heritage", "small"),
    ("Suryapet", "Telangana", "Heritage", "obscure"),
    ("Pillalamarri", "Telangana", "Heritage", "obscure"),
    ("Adilabad", "Telangana", "Heritage", "obscure"),
    # West Bengal (Heritage)
    ("Murshidabad", "West Bengal", "Heritage", "mid"),
    ("Bishnupur", "West Bengal", "Heritage", "small"),
    ("Malda", "West Bengal", "Heritage", "small"),
    ("Cooch Behar", "West Bengal", "Heritage", "small"),
    ("Hazarduari", "West Bengal", "Heritage", "obscure"),
    ("Adina", "West Bengal", "Heritage", "obscure"),
    ("Gaur", "West Bengal", "Heritage", "obscure"),
    ("Pandua", "West Bengal", "Heritage", "obscure"),
    # Odisha (Heritage)
    ("Konark", "Odisha", "Heritage", "major"),
    ("Khandagiri", "Odisha", "Heritage", "small"),
    ("Udayagiri", "Odisha", "Heritage", "small"),
    ("Lalitgiri", "Odisha", "Heritage", "obscure"),
    ("Ratnagiri", "Odisha", "Heritage", "obscure"),
    ("Pipili", "Odisha", "Heritage", "obscure"),
    ("Dhauli", "Odisha", "Heritage", "small"),
    # Bihar (Heritage)
    ("Nalanda", "Bihar", "Heritage", "mid"),
    ("Vaishali", "Bihar", "Heritage", "small"),
    ("Vikramshila", "Bihar", "Heritage", "small"),
    ("Kesariya", "Bihar", "Heritage", "obscure"),
    ("Lauriya Nandangarh", "Bihar", "Heritage", "obscure"),
    ("Rohtas", "Bihar", "Heritage", "obscure"),
    # Andhra Pradesh (Heritage)
    ("Lepakshi", "Andhra Pradesh", "Heritage", "small"),
    ("Penukonda", "Andhra Pradesh", "Heritage", "small"),
    ("Chandragiri", "Andhra Pradesh", "Heritage", "small"),
    ("Gandikota", "Andhra Pradesh", "Heritage", "small"),
    ("Belum", "Andhra Pradesh", "Heritage", "small"),
    ("Anupu", "Andhra Pradesh", "Heritage", "obscure"),
    ("Phanigiri", "Andhra Pradesh", "Heritage", "obscure"),
    ("Amaravati", "Andhra Pradesh", "Heritage", "mid"),
    # Maharashtra (Heritage)
    ("Ajanta", "Maharashtra", "Heritage", "major"),
    ("Ellora", "Maharashtra", "Heritage", "major"),
    ("Daulatabad", "Maharashtra", "Heritage", "mid"),
    ("Lonar", "Maharashtra", "Heritage", "small"),
    ("Raigad", "Maharashtra", "Heritage", "small"),
    ("Pratapgad", "Maharashtra", "Heritage", "small"),
    ("Shivneri", "Maharashtra", "Heritage", "small"),
    ("Sindhudurg", "Maharashtra", "Heritage", "small"),
    ("Janjira", "Maharashtra", "Heritage", "small"),
    ("Murud", "Maharashtra", "Heritage", "small"),
    # Goa (Heritage)
    ("Old Goa", "Goa", "Heritage", "mid"),
    ("Reis Magos", "Goa", "Heritage", "small"),
    ("Cabo de Rama", "Goa", "Heritage", "obscure"),
    # Kerala (Heritage)
    ("Tripunithura", "Kerala", "Heritage", "small"),
    ("Bekal", "Kerala", "Heritage", "small"),
    ("Thalassery", "Kerala", "Heritage", "small"),
    ("Anjarakkandy", "Kerala", "Heritage", "obscure"),
    ("Krishnapuram", "Kerala", "Heritage", "obscure"),
    # Punjab (Heritage)
    ("Patiala", "Punjab", "Heritage", "mid"),
    ("Kapurthala", "Punjab", "Heritage", "small"),
    ("Sangrur", "Punjab", "Heritage", "small"),
    ("Faridkot", "Punjab", "Heritage", "small"),
    # Haryana (Heritage)
    ("Pinjore", "Haryana", "Heritage", "small"),
    ("Pataudi", "Haryana", "Heritage", "small"),
    ("Madhuban", "Haryana", "Heritage", "obscure"),

    # =====================================================================
    # PILGRIMAGE (~175)
    # =====================================================================
    # Uttar Pradesh
    ("Mathura", "Uttar Pradesh", "Pilgrimage", "major"),
    ("Ayodhya", "Uttar Pradesh", "Pilgrimage", "mega"),
    ("Chitrakoot", "Uttar Pradesh", "Pilgrimage", "mid"),
    ("Naimisharanya", "Uttar Pradesh", "Pilgrimage", "small"),
    ("Garhmukteshwar", "Uttar Pradesh", "Pilgrimage", "small"),
    ("Govardhan", "Uttar Pradesh", "Pilgrimage", "mid"),
    ("Prayagraj", "Uttar Pradesh", "Pilgrimage", "mega"),
    ("Sambhal", "Uttar Pradesh", "Pilgrimage", "small"),
    ("Mirzapur", "Uttar Pradesh", "Pilgrimage", "small"),
    ("Soron", "Uttar Pradesh", "Pilgrimage", "obscure"),
    ("Vindhyachal", "Uttar Pradesh", "Pilgrimage", "small"),
    ("Misrikh", "Uttar Pradesh", "Pilgrimage", "obscure"),
    ("Barsana", "Uttar Pradesh", "Pilgrimage", "small"),
    ("Nandgaon", "Uttar Pradesh", "Pilgrimage", "small"),
    ("Gokul", "Uttar Pradesh", "Pilgrimage", "small"),
    ("Bateshwar", "Uttar Pradesh", "Pilgrimage", "obscure"),
    ("Phulpur", "Uttar Pradesh", "Pilgrimage", "obscure"),
    ("Kushinagar", "Uttar Pradesh", "Pilgrimage", "mid"),
    ("Devipatan", "Uttar Pradesh", "Pilgrimage", "obscure"),
    ("Shringverpur", "Uttar Pradesh", "Pilgrimage", "obscure"),
    ("Devban", "Uttar Pradesh", "Pilgrimage", "obscure"),
    ("Ballia", "Uttar Pradesh", "Pilgrimage", "small"),
    ("Faizabad", "Uttar Pradesh", "Pilgrimage", "small"),
    ("Triveni", "Uttar Pradesh", "Pilgrimage", "obscure"),
    # Karnataka (Pilgrimage)
    ("Sringeri", "Karnataka", "Pilgrimage", "mid"),
    ("Udupi", "Karnataka", "Pilgrimage", "mid"),
    ("Sirsi", "Karnataka", "Pilgrimage", "small"),
    ("Murudeshwar", "Karnataka", "Pilgrimage", "mid"),
    ("Dharmasthala", "Karnataka", "Pilgrimage", "mid"),
    ("Subramanya", "Karnataka", "Pilgrimage", "small"),
    ("Hornadu", "Karnataka", "Pilgrimage", "small"),
    ("Kateel", "Karnataka", "Pilgrimage", "small"),
    ("Kollur", "Karnataka", "Pilgrimage", "small"),
    ("Saundatti", "Karnataka", "Pilgrimage", "small"),
    ("Nanjangud", "Karnataka", "Pilgrimage", "small"),
    ("Melukote", "Karnataka", "Pilgrimage", "small"),
    ("Kukke Subramanya", "Karnataka", "Pilgrimage", "small"),
    ("Talakad", "Karnataka", "Pilgrimage", "small"),
    ("Tirumakudal Narasipura", "Karnataka", "Pilgrimage", "obscure"),
    ("Mahakuta", "Karnataka", "Pilgrimage", "obscure"),
    ("Banashankari", "Karnataka", "Pilgrimage", "small"),
    ("Bhadravati", "Karnataka", "Pilgrimage", "small"),
    ("Mookambika", "Karnataka", "Pilgrimage", "obscure"),
    # Tamil Nadu (Pilgrimage)
    ("Madurai", "Tamil Nadu", "Pilgrimage", "mega"),
    ("Rameshwaram", "Tamil Nadu", "Pilgrimage", "major"),
    ("Tiruvannamalai", "Tamil Nadu", "Pilgrimage", "major"),
    ("Palani", "Tamil Nadu", "Pilgrimage", "mid"),
    ("Srirangam", "Tamil Nadu", "Pilgrimage", "mid"),
    ("Tiruchendur", "Tamil Nadu", "Pilgrimage", "small"),
    ("Chidambaram", "Tamil Nadu", "Pilgrimage", "small"),
    ("Tiruvarur", "Tamil Nadu", "Pilgrimage", "small"),
    ("Velankanni", "Tamil Nadu", "Pilgrimage", "mid"),
    ("Tirukkalukundram", "Tamil Nadu", "Pilgrimage", "small"),
    ("Tirukkadaiyur", "Tamil Nadu", "Pilgrimage", "obscure"),
    ("Sirkali", "Tamil Nadu", "Pilgrimage", "small"),
    ("Mailam", "Tamil Nadu", "Pilgrimage", "obscure"),
    ("Samayapuram", "Tamil Nadu", "Pilgrimage", "obscure"),
    ("Marudhamalai", "Tamil Nadu", "Pilgrimage", "obscure"),
    ("Sankarankoil", "Tamil Nadu", "Pilgrimage", "small"),
    ("Tirukoshtiyur", "Tamil Nadu", "Pilgrimage", "obscure"),
    ("Tirukurungudi", "Tamil Nadu", "Pilgrimage", "obscure"),
    ("Tiruparankundram", "Tamil Nadu", "Pilgrimage", "small"),
    ("Thiruvotriyur", "Tamil Nadu", "Pilgrimage", "obscure"),
    # Andhra Pradesh (Pilgrimage)
    ("Tirupati", "Andhra Pradesh", "Pilgrimage", "mega"),
    ("Srisailam", "Andhra Pradesh", "Pilgrimage", "major"),
    ("Annavaram", "Andhra Pradesh", "Pilgrimage", "small"),
    ("Pithapuram", "Andhra Pradesh", "Pilgrimage", "small"),
    ("Simhachalam", "Andhra Pradesh", "Pilgrimage", "small"),
    ("Ahobilam", "Andhra Pradesh", "Pilgrimage", "small"),
    ("Mahanandi", "Andhra Pradesh", "Pilgrimage", "small"),
    ("Antarvedi", "Andhra Pradesh", "Pilgrimage", "obscure"),
    ("Kanipakam", "Andhra Pradesh", "Pilgrimage", "small"),
    ("Mangalagiri", "Andhra Pradesh", "Pilgrimage", "small"),
    ("Kalahasti", "Andhra Pradesh", "Pilgrimage", "mid"),
    ("Penuganchiprolu", "Andhra Pradesh", "Pilgrimage", "obscure"),
    ("Yaganti", "Andhra Pradesh", "Pilgrimage", "obscure"),
    ("Kasapuram", "Andhra Pradesh", "Pilgrimage", "obscure"),
    ("Bhattiprolu", "Andhra Pradesh", "Pilgrimage", "obscure"),
    ("Vontimitta", "Andhra Pradesh", "Pilgrimage", "obscure"),
    ("Kotappakonda", "Andhra Pradesh", "Pilgrimage", "obscure"),
    ("Bhimavaram", "Andhra Pradesh", "Pilgrimage", "small"),
    ("Kuchipudi", "Andhra Pradesh", "Pilgrimage", "obscure"),
    # Telangana (Pilgrimage)
    ("Bhadrachalam", "Telangana", "Pilgrimage", "mid"),
    ("Vemulawada", "Telangana", "Pilgrimage", "small"),
    ("Basara", "Telangana", "Pilgrimage", "small"),
    ("Komuravelli", "Telangana", "Pilgrimage", "small"),
    ("Yadagirigutta", "Telangana", "Pilgrimage", "mid"),
    ("Medak", "Telangana", "Pilgrimage", "small"),
    # Maharashtra (Pilgrimage)
    ("Shirdi", "Maharashtra", "Pilgrimage", "mega"),
    ("Pandharpur", "Maharashtra", "Pilgrimage", "major"),
    ("Trimbakeshwar", "Maharashtra", "Pilgrimage", "mid"),
    ("Bhimashankar", "Maharashtra", "Pilgrimage", "mid"),
    ("Jejuri", "Maharashtra", "Pilgrimage", "small"),
    ("Tuljapur", "Maharashtra", "Pilgrimage", "mid"),
    ("Aundha Nagnath", "Maharashtra", "Pilgrimage", "small"),
    ("Parli Vaijnath", "Maharashtra", "Pilgrimage", "small"),
    ("Akkalkot", "Maharashtra", "Pilgrimage", "small"),
    ("Alandi", "Maharashtra", "Pilgrimage", "small"),
    ("Dehu", "Maharashtra", "Pilgrimage", "small"),
    ("Audumbar", "Maharashtra", "Pilgrimage", "obscure"),
    ("Saswad", "Maharashtra", "Pilgrimage", "obscure"),
    ("Lenyadri", "Maharashtra", "Pilgrimage", "obscure"),
    ("Theur", "Maharashtra", "Pilgrimage", "obscure"),
    # West Bengal (Pilgrimage)
    ("Tarapith", "West Bengal", "Pilgrimage", "mid"),
    ("Mayapur", "West Bengal", "Pilgrimage", "mid"),
    ("Nabadwip", "West Bengal", "Pilgrimage", "small"),
    ("Tarakeshwar", "West Bengal", "Pilgrimage", "small"),
    ("Belur", "West Bengal", "Pilgrimage", "small"),
    ("Gangasagar", "West Bengal", "Pilgrimage", "mid"),
    ("Bakreshwar", "West Bengal", "Pilgrimage", "small"),
    ("Furfura Sharif", "West Bengal", "Pilgrimage", "small"),
    # Odisha (Pilgrimage)
    ("Puri", "Odisha", "Pilgrimage", "major"),
    ("Joranda", "Odisha", "Pilgrimage", "small"),
    ("Ghatgaon", "Odisha", "Pilgrimage", "obscure"),
    # Bihar (Pilgrimage)
    ("Bodhgaya", "Bihar", "Pilgrimage", "major"),
    ("Patna Sahib", "Bihar", "Pilgrimage", "mid"),
    ("Sitamarhi", "Bihar", "Pilgrimage", "small"),
    ("Singhwahini", "Bihar", "Pilgrimage", "obscure"),
    ("Maner Sharif", "Bihar", "Pilgrimage", "small"),
    ("Sabour", "Bihar", "Pilgrimage", "obscure"),
    ("Mokama", "Bihar", "Pilgrimage", "obscure"),
    # Punjab (Pilgrimage)
    ("Amritsar", "Punjab", "Pilgrimage", "mega"),
    ("Anandpur Sahib", "Punjab", "Pilgrimage", "mid"),
    ("Talwandi Sabo", "Punjab", "Pilgrimage", "small"),
    ("Goindwal", "Punjab", "Pilgrimage", "small"),
    ("Khadur Sahib", "Punjab", "Pilgrimage", "small"),
    ("Sultanpur Lodhi", "Punjab", "Pilgrimage", "small"),
    ("Damdama Sahib", "Punjab", "Pilgrimage", "small"),
    # Haryana (Pilgrimage)
    ("Kurukshetra", "Haryana", "Pilgrimage", "mid"),
    ("Pehowa", "Haryana", "Pilgrimage", "small"),
    ("Pundri", "Haryana", "Pilgrimage", "obscure"),
    # Uttarakhand (Pilgrimage)
    ("Kedarnath", "Uttarakhand", "Pilgrimage", "major"),
    ("Badrinath", "Uttarakhand", "Pilgrimage", "major"),
    ("Gangotri", "Uttarakhand", "Pilgrimage", "mid"),
    ("Yamunotri", "Uttarakhand", "Pilgrimage", "mid"),
    ("Devprayag", "Uttarakhand", "Pilgrimage", "small"),
    ("Joshimath", "Uttarakhand", "Pilgrimage", "small"),
    ("Rudraprayag", "Uttarakhand", "Pilgrimage", "small"),
    ("Karnaprayag", "Uttarakhand", "Pilgrimage", "small"),
    ("Hemkund Sahib", "Uttarakhand", "Pilgrimage", "small"),
    ("Triyuginarayan", "Uttarakhand", "Pilgrimage", "obscure"),
    ("Ukhimath", "Uttarakhand", "Pilgrimage", "small"),
    ("Guptkashi", "Uttarakhand", "Pilgrimage", "small"),
    # Himachal Pradesh (Pilgrimage)
    ("Manimahesh", "Himachal Pradesh", "Pilgrimage", "small"),
    ("Chintpurni", "Himachal Pradesh", "Pilgrimage", "small"),
    ("Jwalamukhi", "Himachal Pradesh", "Pilgrimage", "small"),
    ("Naina Devi", "Himachal Pradesh", "Pilgrimage", "small"),
    ("Baijnath", "Himachal Pradesh", "Pilgrimage", "small"),
    ("Bajreshwari", "Himachal Pradesh", "Pilgrimage", "obscure"),
    # Jammu & Kashmir (Pilgrimage)
    ("Katra", "Jammu & Kashmir", "Pilgrimage", "major"),
    ("Amarnath", "Jammu & Kashmir", "Pilgrimage", "major"),
    ("Tulmulla", "Jammu & Kashmir", "Pilgrimage", "small"),
    ("Shankaracharya", "Jammu & Kashmir", "Pilgrimage", "small"),
    ("Charar-i-Sharief", "Jammu & Kashmir", "Pilgrimage", "small"),
    # Gujarat (Pilgrimage)
    ("Dwarka", "Gujarat", "Pilgrimage", "major"),
    ("Somnath", "Gujarat", "Pilgrimage", "major"),
    ("Palitana", "Gujarat", "Pilgrimage", "mid"),
    ("Ambaji", "Gujarat", "Pilgrimage", "mid"),
    ("Becharaji", "Gujarat", "Pilgrimage", "small"),
    ("Shamlaji", "Gujarat", "Pilgrimage", "small"),
    ("Dakor", "Gujarat", "Pilgrimage", "small"),
    ("Bhadreshwar", "Gujarat", "Pilgrimage", "obscure"),
    ("Nageshwar", "Gujarat", "Pilgrimage", "small"),
    # Kerala (Pilgrimage)
    ("Sabarimala", "Kerala", "Pilgrimage", "mega"),
    ("Guruvayur", "Kerala", "Pilgrimage", "major"),
    ("Chottanikkara", "Kerala", "Pilgrimage", "small"),
    ("Vaikom", "Kerala", "Pilgrimage", "small"),
    ("Ettumanoor", "Kerala", "Pilgrimage", "small"),
    ("Bharananganam", "Kerala", "Pilgrimage", "obscure"),
    ("Kalady", "Kerala", "Pilgrimage", "small"),
    # Madhya Pradesh (Pilgrimage)
    ("Ujjain", "Madhya Pradesh", "Pilgrimage", "major"),
    ("Omkareshwar", "Madhya Pradesh", "Pilgrimage", "mid"),
    ("Amarkantak", "Madhya Pradesh", "Pilgrimage", "small"),
    ("Salkanpur", "Madhya Pradesh", "Pilgrimage", "obscure"),
    # Sikkim (Pilgrimage)
    ("Rumtek", "Sikkim", "Pilgrimage", "small"),
    # Northeast (Pilgrimage)
    ("Hajo", "Assam", "Pilgrimage", "small"),
    ("Mahabhairab", "Assam", "Pilgrimage", "obscure"),

    # =====================================================================
    # HILL STATION (~165)
    # =====================================================================
    # Himachal Pradesh
    ("Mcleodganj", "Himachal Pradesh", "Hill Station", "major"),
    ("Bir", "Himachal Pradesh", "Hill Station", "mid"),
    ("Palampur", "Himachal Pradesh", "Hill Station", "mid"),
    ("Kullu", "Himachal Pradesh", "Hill Station", "mid"),
    ("Kalpa", "Himachal Pradesh", "Hill Station", "small"),
    ("Sangla", "Himachal Pradesh", "Hill Station", "small"),
    ("Chitkul", "Himachal Pradesh", "Hill Station", "small"),
    ("Nako", "Himachal Pradesh", "Hill Station", "obscure"),
    ("Rampur Bushahr", "Himachal Pradesh", "Hill Station", "small"),
    ("Kufri", "Himachal Pradesh", "Hill Station", "small"),
    ("Naldehra", "Himachal Pradesh", "Hill Station", "small"),
    ("Narkanda", "Himachal Pradesh", "Hill Station", "small"),
    ("Sarahan", "Himachal Pradesh", "Hill Station", "small"),
    ("Mashobra", "Himachal Pradesh", "Hill Station", "small"),
    ("Chail", "Himachal Pradesh", "Hill Station", "small"),
    ("Khajjiar", "Himachal Pradesh", "Hill Station", "mid"),
    ("Chamba", "Himachal Pradesh", "Hill Station", "mid"),
    ("Bharmour", "Himachal Pradesh", "Hill Station", "small"),
    ("Tirthan Valley", "Himachal Pradesh", "Hill Station", "small"),
    ("Kasol", "Himachal Pradesh", "Hill Station", "mid"),
    ("Tosh", "Himachal Pradesh", "Hill Station", "small"),
    ("Malana", "Himachal Pradesh", "Hill Station", "small"),
    ("Kaza", "Himachal Pradesh", "Hill Station", "small"),
    ("Kibber", "Himachal Pradesh", "Hill Station", "obscure"),
    ("Tabo", "Himachal Pradesh", "Hill Station", "small"),
    ("Sissu", "Himachal Pradesh", "Hill Station", "obscure"),
    ("Dhauladhar", "Himachal Pradesh", "Hill Station", "obscure"),
    # Uttarakhand
    ("Auli", "Uttarakhand", "Hill Station", "mid"),
    ("Mukteshwar", "Uttarakhand", "Hill Station", "small"),
    ("Chopta", "Uttarakhand", "Hill Station", "small"),
    ("Munsiyari", "Uttarakhand", "Hill Station", "small"),
    ("Ranikhet", "Uttarakhand", "Hill Station", "mid"),
    ("Almora", "Uttarakhand", "Hill Station", "mid"),
    ("Bageshwar", "Uttarakhand", "Hill Station", "small"),
    ("Kausani", "Uttarakhand", "Hill Station", "small"),
    ("Pithoragarh", "Uttarakhand", "Hill Station", "small"),
    ("Lansdowne", "Uttarakhand", "Hill Station", "small"),
    ("Khirsu", "Uttarakhand", "Hill Station", "obscure"),
    ("Pauri", "Uttarakhand", "Hill Station", "small"),
    ("Chakrata", "Uttarakhand", "Hill Station", "small"),
    ("Pangot", "Uttarakhand", "Hill Station", "small"),
    ("Bhimtal", "Uttarakhand", "Hill Station", "small"),
    ("Sattal", "Uttarakhand", "Hill Station", "small"),
    ("Naukuchiatal", "Uttarakhand", "Hill Station", "small"),
    ("Binsar", "Uttarakhand", "Hill Station", "small"),
    ("Dhanaulti", "Uttarakhand", "Hill Station", "small"),
    ("Tehri", "Uttarakhand", "Hill Station", "small"),
    ("New Tehri", "Uttarakhand", "Hill Station", "obscure"),
    ("Dakpathar", "Uttarakhand", "Hill Station", "obscure"),
    # Jammu & Kashmir
    ("Pahalgam", "Jammu & Kashmir", "Hill Station", "major"),
    ("Gulmarg", "Jammu & Kashmir", "Hill Station", "major"),
    ("Sonamarg", "Jammu & Kashmir", "Hill Station", "mid"),
    ("Yusmarg", "Jammu & Kashmir", "Hill Station", "small"),
    ("Doodhpathri", "Jammu & Kashmir", "Hill Station", "small"),
    ("Aharbal", "Jammu & Kashmir", "Hill Station", "obscure"),
    ("Tangmarg", "Jammu & Kashmir", "Hill Station", "small"),
    ("Bhaderwah", "Jammu & Kashmir", "Hill Station", "small"),
    ("Patnitop", "Jammu & Kashmir", "Hill Station", "small"),
    ("Sanasar", "Jammu & Kashmir", "Hill Station", "obscure"),
    # Ladakh
    ("Leh", "Ladakh", "Hill Station", "mid"),
    ("Nubra", "Ladakh", "Hill Station", "mid"),
    ("Tso Moriri", "Ladakh", "Hill Station", "small"),
    ("Diskit", "Ladakh", "Hill Station", "small"),
    ("Hunder", "Ladakh", "Hill Station", "small"),
    ("Hemis", "Ladakh", "Hill Station", "small"),
    ("Alchi", "Ladakh", "Hill Station", "small"),
    ("Likir", "Ladakh", "Hill Station", "obscure"),
    ("Lamayuru", "Ladakh", "Hill Station", "small"),
    ("Kargil", "Ladakh", "Hill Station", "small"),
    ("Drass", "Ladakh", "Hill Station", "obscure"),
    ("Padum", "Ladakh", "Hill Station", "obscure"),
    # Sikkim
    ("Gangtok", "Sikkim", "Hill Station", "major"),
    ("Pelling", "Sikkim", "Hill Station", "mid"),
    ("Lachung", "Sikkim", "Hill Station", "mid"),
    ("Lachen", "Sikkim", "Hill Station", "small"),
    ("Yumthang", "Sikkim", "Hill Station", "small"),
    ("Ravangla", "Sikkim", "Hill Station", "small"),
    ("Namchi", "Sikkim", "Hill Station", "small"),
    ("Yuksom", "Sikkim", "Hill Station", "small"),
    ("Aritar", "Sikkim", "Hill Station", "small"),
    ("Zuluk", "Sikkim", "Hill Station", "obscure"),
    ("Mangan", "Sikkim", "Hill Station", "small"),
    # West Bengal
    ("Darjeeling", "West Bengal", "Hill Station", "major"),
    ("Kalimpong", "West Bengal", "Hill Station", "mid"),
    ("Mirik", "West Bengal", "Hill Station", "small"),
    ("Kurseong", "West Bengal", "Hill Station", "small"),
    ("Lava", "West Bengal", "Hill Station", "small"),
    ("Lolegaon", "West Bengal", "Hill Station", "small"),
    ("Tinchuley", "West Bengal", "Hill Station", "obscure"),
    ("Sandakphu", "West Bengal", "Hill Station", "small"),
    ("Sittong", "West Bengal", "Hill Station", "obscure"),
    ("Bijanbari", "West Bengal", "Hill Station", "obscure"),
    # Arunachal Pradesh
    ("Tawang", "Arunachal Pradesh", "Hill Station", "mid"),
    ("Bomdila", "Arunachal Pradesh", "Hill Station", "small"),
    ("Ziro", "Arunachal Pradesh", "Hill Station", "small"),
    ("Dirang", "Arunachal Pradesh", "Hill Station", "small"),
    ("Mechuka", "Arunachal Pradesh", "Hill Station", "small"),
    ("Bhalukpong", "Arunachal Pradesh", "Hill Station", "small"),
    ("Anini", "Arunachal Pradesh", "Hill Station", "obscure"),
    # Meghalaya
    ("Shillong", "Meghalaya", "Hill Station", "major"),
    ("Sohra", "Meghalaya", "Hill Station", "major"),
    ("Mawlynnong", "Meghalaya", "Hill Station", "mid"),
    ("Mawsynram", "Meghalaya", "Hill Station", "mid"),
    ("Dawki", "Meghalaya", "Hill Station", "mid"),
    ("Mawkdok", "Meghalaya", "Hill Station", "obscure"),
    ("Laitlum", "Meghalaya", "Hill Station", "small"),
    # Mizoram
    ("Champhai", "Mizoram", "Hill Station", "small"),
    ("Reiek", "Mizoram", "Hill Station", "small"),
    ("Phawngpui", "Mizoram", "Hill Station", "obscure"),
    # Nagaland
    ("Kohima", "Nagaland", "Hill Station", "mid"),
    ("Mokokchung", "Nagaland", "Hill Station", "small"),
    ("Wokha", "Nagaland", "Hill Station", "small"),
    ("Mon", "Nagaland", "Hill Station", "small"),
    ("Tuensang", "Nagaland", "Hill Station", "small"),
    # Manipur
    ("Ukhrul", "Manipur", "Hill Station", "small"),
    ("Senapati", "Manipur", "Hill Station", "small"),
    ("Moirang", "Manipur", "Hill Station", "small"),
    ("Tamenglong", "Manipur", "Hill Station", "small"),
    # Tripura
    ("Jampui Hills", "Tripura", "Hill Station", "small"),
    ("Atharamura", "Tripura", "Hill Station", "obscure"),
    ("Unakoti", "Tripura", "Hill Station", "small"),
    # Assam
    ("Haflong", "Assam", "Hill Station", "small"),
    ("Diphu", "Assam", "Hill Station", "small"),
    # Karnataka
    ("Chikmagalur", "Karnataka", "Hill Station", "mid"),
    ("Sakleshpur", "Karnataka", "Hill Station", "small"),
    ("Kemmangundi", "Karnataka", "Hill Station", "small"),
    ("Kudremukh", "Karnataka", "Hill Station", "small"),
    ("Agumbe", "Karnataka", "Hill Station", "small"),
    ("Talakaveri", "Karnataka", "Hill Station", "small"),
    ("Bababudangiri", "Karnataka", "Hill Station", "small"),
    ("Nandi Hills", "Karnataka", "Hill Station", "mid"),
    # Tamil Nadu
    ("Kodaikanal", "Tamil Nadu", "Hill Station", "major"),
    ("Yercaud", "Tamil Nadu", "Hill Station", "mid"),
    ("Kotagiri", "Tamil Nadu", "Hill Station", "small"),
    ("Coonoor", "Tamil Nadu", "Hill Station", "mid"),
    ("Yelagiri", "Tamil Nadu", "Hill Station", "small"),
    ("Valparai", "Tamil Nadu", "Hill Station", "small"),
    ("Topslip", "Tamil Nadu", "Hill Station", "small"),
    # Kerala
    ("Vagamon", "Kerala", "Hill Station", "small"),
    ("Ponmudi", "Kerala", "Hill Station", "small"),
    ("Idukki", "Kerala", "Hill Station", "small"),
    ("Devikulam", "Kerala", "Hill Station", "small"),
    ("Marayoor", "Kerala", "Hill Station", "small"),
    ("Peermade", "Kerala", "Hill Station", "small"),
    # Maharashtra
    ("Panchgani", "Maharashtra", "Hill Station", "mid"),
    ("Matheran", "Maharashtra", "Hill Station", "mid"),
    ("Bhandardara", "Maharashtra", "Hill Station", "small"),
    ("Igatpuri", "Maharashtra", "Hill Station", "small"),
    ("Khandala", "Maharashtra", "Hill Station", "mid"),
    ("Toranmal", "Maharashtra", "Hill Station", "obscure"),
    ("Chikhaldara", "Maharashtra", "Hill Station", "small"),
    ("Amboli", "Maharashtra", "Hill Station", "small"),
    ("Karjat", "Maharashtra", "Hill Station", "small"),
    ("Tamhini", "Maharashtra", "Hill Station", "obscure"),
    ("Karnala", "Maharashtra", "Hill Station", "obscure"),
    # Gujarat
    ("Saputara", "Gujarat", "Hill Station", "small"),
    ("Wilson Hills", "Gujarat", "Hill Station", "obscure"),
    ("Don", "Gujarat", "Hill Station", "obscure"),

    # =====================================================================
    # WILDLIFE (~167)
    # =====================================================================
    # Rajasthan
    ("Sawai Madhopur", "Rajasthan", "Wildlife", "mid"),
    ("Sariska", "Rajasthan", "Wildlife", "small"),
    ("Tal Chhapar", "Rajasthan", "Wildlife", "small"),
    ("Mount Abu WLS", "Rajasthan", "Wildlife", "obscure"),
    ("Mukundra", "Rajasthan", "Wildlife", "obscure"),
    ("Bandh Baretha", "Rajasthan", "Wildlife", "obscure"),
    ("Sajjangarh", "Rajasthan", "Wildlife", "obscure"),
    ("Ramgarh Vishdhari", "Rajasthan", "Wildlife", "obscure"),
    ("National Chambal", "Rajasthan", "Wildlife", "obscure"),
    # Madhya Pradesh
    ("Tala", "Madhya Pradesh", "Wildlife", "mid"),
    ("Mukki", "Madhya Pradesh", "Wildlife", "small"),
    ("Khawasa", "Madhya Pradesh", "Wildlife", "small"),
    ("Madla", "Madhya Pradesh", "Wildlife", "obscure"),
    ("Pachmarhi", "Madhya Pradesh", "Wildlife", "mid"),
    ("Pench Turia", "Madhya Pradesh", "Wildlife", "small"),
    ("Madhai", "Madhya Pradesh", "Wildlife", "small"),
    ("Kuno-Palpur", "Madhya Pradesh", "Wildlife", "mid"),
    ("Singhori", "Madhya Pradesh", "Wildlife", "obscure"),
    ("Karera", "Madhya Pradesh", "Wildlife", "obscure"),
    ("Sanjay-Dubri", "Madhya Pradesh", "Wildlife", "obscure"),
    ("Veerangana Durgavati", "Madhya Pradesh", "Wildlife", "obscure"),
    ("Ghatigaon", "Madhya Pradesh", "Wildlife", "obscure"),
    ("Ratapani", "Madhya Pradesh", "Wildlife", "small"),
    # Maharashtra
    ("Tadoba", "Maharashtra", "Wildlife", "mid"),
    ("Pench-Maharashtra", "Maharashtra", "Wildlife", "small"),
    ("Nagzira", "Maharashtra", "Wildlife", "small"),
    ("Navegaon", "Maharashtra", "Wildlife", "small"),
    ("Tipeshwar", "Maharashtra", "Wildlife", "small"),
    ("Sahyadri WLS", "Maharashtra", "Wildlife", "obscure"),
    ("Melghat", "Maharashtra", "Wildlife", "small"),
    ("Karnala WLS", "Maharashtra", "Wildlife", "obscure"),
    ("Phansad", "Maharashtra", "Wildlife", "obscure"),
    ("Mayureshwar", "Maharashtra", "Wildlife", "obscure"),
    ("Yedshi-Ramling", "Maharashtra", "Wildlife", "obscure"),
    ("Lonar Lake", "Maharashtra", "Wildlife", "small"),
    ("Radhanagari", "Maharashtra", "Wildlife", "small"),
    # Karnataka
    ("Bandipur", "Karnataka", "Wildlife", "mid"),
    ("Nagarhole", "Karnataka", "Wildlife", "mid"),
    ("BRT", "Karnataka", "Wildlife", "small"),
    ("Dandeli", "Karnataka", "Wildlife", "mid"),
    ("Bhadra", "Karnataka", "Wildlife", "small"),
    ("Anshi", "Karnataka", "Wildlife", "small"),
    ("Cauvery WLS", "Karnataka", "Wildlife", "small"),
    ("Kabini", "Karnataka", "Wildlife", "mid"),
    ("Anekere", "Karnataka", "Wildlife", "obscure"),
    ("Mookambika WLS", "Karnataka", "Wildlife", "obscure"),
    ("Sharavathi", "Karnataka", "Wildlife", "small"),
    ("Pushpagiri", "Karnataka", "Wildlife", "small"),
    ("Brahmagiri", "Karnataka", "Wildlife", "small"),
    # Kerala
    ("Thekkady", "Kerala", "Wildlife", "mid"),
    ("Silent Valley", "Kerala", "Wildlife", "mid"),
    ("Eravikulam", "Kerala", "Wildlife", "small"),
    ("Parambikulam", "Kerala", "Wildlife", "small"),
    ("Chinnar", "Kerala", "Wildlife", "small"),
    ("Aralam", "Kerala", "Wildlife", "small"),
    ("Neyyar", "Kerala", "Wildlife", "small"),
    ("Peppara", "Kerala", "Wildlife", "obscure"),
    ("Kurinjimala", "Kerala", "Wildlife", "obscure"),
    # Tamil Nadu
    ("Mudumalai", "Tamil Nadu", "Wildlife", "mid"),
    ("Mukurthi", "Tamil Nadu", "Wildlife", "small"),
    ("Anaimalai", "Tamil Nadu", "Wildlife", "small"),
    ("Sathyamangalam", "Tamil Nadu", "Wildlife", "small"),
    ("Kalakkad-Mundanthurai", "Tamil Nadu", "Wildlife", "small"),
    ("Point Calimere", "Tamil Nadu", "Wildlife", "small"),
    ("Vedanthangal", "Tamil Nadu", "Wildlife", "small"),
    ("Srivilliputhur", "Tamil Nadu", "Wildlife", "obscure"),
    ("Vallanadu", "Tamil Nadu", "Wildlife", "obscure"),
    ("Megamalai", "Tamil Nadu", "Wildlife", "small"),
    # Andhra Pradesh
    ("Nagarjunsagar Reserve", "Andhra Pradesh", "Wildlife", "small"),
    ("Sri Venkateswara", "Andhra Pradesh", "Wildlife", "small"),
    ("Coringa", "Andhra Pradesh", "Wildlife", "small"),
    ("Pulicat", "Andhra Pradesh", "Wildlife", "small"),
    ("Papikondalu", "Andhra Pradesh", "Wildlife", "small"),
    # Telangana
    ("Kawal", "Telangana", "Wildlife", "small"),
    ("Pranahita", "Telangana", "Wildlife", "small"),
    ("Amrabad", "Telangana", "Wildlife", "small"),
    ("Eturnagaram", "Telangana", "Wildlife", "small"),
    ("Pakhal", "Telangana", "Wildlife", "small"),
    ("Kinnerasani", "Telangana", "Wildlife", "obscure"),
    # Odisha
    ("Simlipal", "Odisha", "Wildlife", "mid"),
    ("Bhitarkanika", "Odisha", "Wildlife", "mid"),
    ("Satkosia", "Odisha", "Wildlife", "small"),
    ("Debrigarh", "Odisha", "Wildlife", "small"),
    ("Kuldiha", "Odisha", "Wildlife", "obscure"),
    ("Karlapat", "Odisha", "Wildlife", "obscure"),
    ("Chandaka", "Odisha", "Wildlife", "obscure"),
    ("Hadgarh", "Odisha", "Wildlife", "obscure"),
    # West Bengal
    ("Buxa", "West Bengal", "Wildlife", "mid"),
    ("Gorumara", "West Bengal", "Wildlife", "mid"),
    ("Jaldapara", "West Bengal", "Wildlife", "mid"),
    ("Mahananda", "West Bengal", "Wildlife", "small"),
    ("Singalila", "West Bengal", "Wildlife", "small"),
    ("Senchal", "West Bengal", "Wildlife", "small"),
    ("Neora Valley", "West Bengal", "Wildlife", "small"),
    ("Bibhutibhushan", "West Bengal", "Wildlife", "obscure"),
    ("Sajnekhali", "West Bengal", "Wildlife", "small"),
    # Assam
    ("Manas", "Assam", "Wildlife", "mid"),
    ("Nameri", "Assam", "Wildlife", "small"),
    ("Dibru-Saikhowa", "Assam", "Wildlife", "small"),
    ("Orang", "Assam", "Wildlife", "small"),
    ("Pobitora", "Assam", "Wildlife", "small"),
    ("Burachapori", "Assam", "Wildlife", "obscure"),
    ("Laokhowa", "Assam", "Wildlife", "obscure"),
    # Arunachal Pradesh
    ("Namdapha", "Arunachal Pradesh", "Wildlife", "mid"),
    ("Mouling", "Arunachal Pradesh", "Wildlife", "small"),
    ("Pakke", "Arunachal Pradesh", "Wildlife", "small"),
    ("D'Ering", "Arunachal Pradesh", "Wildlife", "small"),
    ("Eaglenest", "Arunachal Pradesh", "Wildlife", "small"),
    ("Dibang", "Arunachal Pradesh", "Wildlife", "small"),
    ("Tale Valley", "Arunachal Pradesh", "Wildlife", "obscure"),
    # Meghalaya
    ("Nokrek", "Meghalaya", "Wildlife", "small"),
    ("Balphakram", "Meghalaya", "Wildlife", "small"),
    ("Siju", "Meghalaya", "Wildlife", "obscure"),
    # Mizoram
    ("Dampa", "Mizoram", "Wildlife", "small"),
    ("Murlen", "Mizoram", "Wildlife", "obscure"),
    # Nagaland
    ("Intanki", "Nagaland", "Wildlife", "small"),
    ("Pulie Badze", "Nagaland", "Wildlife", "obscure"),
    # Manipur
    ("Keibul Lamjao", "Manipur", "Wildlife", "small"),
    ("Yangoupokpi", "Manipur", "Wildlife", "obscure"),
    # Tripura
    ("Sepahijala", "Tripura", "Wildlife", "small"),
    ("Trishna", "Tripura", "Wildlife", "small"),
    ("Gomati", "Tripura", "Wildlife", "small"),
    # Sikkim
    ("Kanchenjunga NP", "Sikkim", "Wildlife", "small"),
    ("Maenam", "Sikkim", "Wildlife", "small"),
    ("Pangolakha", "Sikkim", "Wildlife", "obscure"),
    ("Fambong Lho", "Sikkim", "Wildlife", "obscure"),
    # Himachal Pradesh
    ("Great Himalayan NP", "Himachal Pradesh", "Wildlife", "mid"),
    ("Inderkilla", "Himachal Pradesh", "Wildlife", "obscure"),
    ("Sechu Tuan", "Himachal Pradesh", "Wildlife", "obscure"),
    ("Rupi Bhaba", "Himachal Pradesh", "Wildlife", "obscure"),
    ("Tundah", "Himachal Pradesh", "Wildlife", "obscure"),
    ("Pong Dam", "Himachal Pradesh", "Wildlife", "small"),
    # Jammu & Kashmir
    ("Dachigam", "Jammu & Kashmir", "Wildlife", "small"),
    ("Hokersar", "Jammu & Kashmir", "Wildlife", "small"),
    ("Hirpora", "Jammu & Kashmir", "Wildlife", "obscure"),
    ("Surinsar", "Jammu & Kashmir", "Wildlife", "obscure"),
    # Uttarakhand
    ("Ramnagar", "Uttarakhand", "Wildlife", "major"),
    ("Rajaji", "Uttarakhand", "Wildlife", "mid"),
    ("Govind", "Uttarakhand", "Wildlife", "small"),
    ("Askot", "Uttarakhand", "Wildlife", "obscure"),
    ("Sonanadi", "Uttarakhand", "Wildlife", "small"),
    # Uttar Pradesh
    ("Dudhwa", "Uttar Pradesh", "Wildlife", "mid"),
    ("Katarniaghat", "Uttar Pradesh", "Wildlife", "small"),
    ("Kishanpur", "Uttar Pradesh", "Wildlife", "small"),
    ("Pilibhit", "Uttar Pradesh", "Wildlife", "mid"),
    ("Sohelwa", "Uttar Pradesh", "Wildlife", "small"),
    ("Suhelwa", "Uttar Pradesh", "Wildlife", "obscure"),
    ("Ranipur", "Uttar Pradesh", "Wildlife", "obscure"),
    # Bihar
    ("Valmiki", "Bihar", "Wildlife", "mid"),
    ("Kaimur Hills", "Bihar", "Wildlife", "small"),
    ("Bhimbandh", "Bihar", "Wildlife", "obscure"),
    # Jharkhand
    ("Betla", "Jharkhand", "Wildlife", "mid"),
    ("Hazaribagh WLS", "Jharkhand", "Wildlife", "small"),
    ("Dalma", "Jharkhand", "Wildlife", "small"),
    ("Saranda", "Jharkhand", "Wildlife", "small"),
    # Chhattisgarh
    ("Indravati", "Chhattisgarh", "Wildlife", "mid"),
    ("Achanakmar", "Chhattisgarh", "Wildlife", "small"),
    ("Kanger Valley", "Chhattisgarh", "Wildlife", "small"),
    ("Udanti", "Chhattisgarh", "Wildlife", "small"),
    ("Guru Ghasidas", "Chhattisgarh", "Wildlife", "small"),
    # Gujarat
    ("Sasan-Gir", "Gujarat", "Wildlife", "mid"),
    ("Velavadar", "Gujarat", "Wildlife", "small"),
    ("Jamnagar Marine", "Gujarat", "Wildlife", "small"),
    ("Dhrangadhra", "Gujarat", "Wildlife", "small"),
    ("Khijadiya", "Gujarat", "Wildlife", "obscure"),
    ("Vansda", "Gujarat", "Wildlife", "small"),
    ("Hingolgadh", "Gujarat", "Wildlife", "obscure"),

    # =====================================================================
    # BEACH (~77)
    # =====================================================================
    # Goa
    ("Vagator", "Goa", "Beach", "mid"),
    ("Baga", "Goa", "Beach", "major"),
    ("Candolim", "Goa", "Beach", "mid"),
    ("Sinquerim", "Goa", "Beach", "mid"),
    ("Dona Paula", "Goa", "Beach", "mid"),
    ("Colva", "Goa", "Beach", "mid"),
    ("Benaulim", "Goa", "Beach", "mid"),
    ("Varca", "Goa", "Beach", "mid"),
    ("Cavelossim", "Goa", "Beach", "small"),
    ("Palolem", "Goa", "Beach", "mid"),
    ("Morjim", "Goa", "Beach", "small"),
    ("Arambol", "Goa", "Beach", "mid"),
    # Maharashtra
    ("Alibag", "Maharashtra", "Beach", "mid"),
    ("Murud Janjira", "Maharashtra", "Beach", "small"),
    ("Ratnagiri", "Maharashtra", "Beach", "mid"),
    ("Ganpatipule", "Maharashtra", "Beach", "small"),
    ("Tarkarli", "Maharashtra", "Beach", "small"),
    ("Malvan", "Maharashtra", "Beach", "small"),
    ("Vengurla", "Maharashtra", "Beach", "small"),
    ("Diveagar", "Maharashtra", "Beach", "small"),
    # Karnataka
    ("Karwar", "Karnataka", "Beach", "small"),
    ("Maravanthe", "Karnataka", "Beach", "small"),
    ("Malpe", "Karnataka", "Beach", "small"),
    ("Kapu", "Karnataka", "Beach", "small"),
    ("Kundapur", "Karnataka", "Beach", "small"),
    ("Surathkal", "Karnataka", "Beach", "small"),
    ("Tannirbavi", "Karnataka", "Beach", "obscure"),
    # Kerala
    ("Varkala", "Kerala", "Beach", "mid"),
    ("Cherai", "Kerala", "Beach", "small"),
    ("Marari", "Kerala", "Beach", "small"),
    ("Muzhappilangad", "Kerala", "Beach", "small"),
    ("Kappad", "Kerala", "Beach", "small"),
    ("Payyambalam", "Kerala", "Beach", "small"),
    ("Thirumullavaram", "Kerala", "Beach", "obscure"),
    ("Poovar", "Kerala", "Beach", "small"),
    # Tamil Nadu
    ("Kanyakumari", "Tamil Nadu", "Beach", "major"),
    ("Tranquebar", "Tamil Nadu", "Beach", "small"),
    ("Marakkanam", "Tamil Nadu", "Beach", "obscure"),
    ("Poompuhar", "Tamil Nadu", "Beach", "small"),
    ("Silver Beach Cuddalore", "Tamil Nadu", "Beach", "small"),
    ("Gokarna", "Karnataka", "Beach", "mid"),
    # Andhra Pradesh
    ("Yarada", "Andhra Pradesh", "Beach", "small"),
    ("Suryalanka", "Andhra Pradesh", "Beach", "small"),
    ("Bheemili", "Andhra Pradesh", "Beach", "small"),
    ("Manginapudi", "Andhra Pradesh", "Beach", "obscure"),
    ("Mypadu", "Andhra Pradesh", "Beach", "obscure"),
    # Odisha
    ("Gopalpur", "Odisha", "Beach", "small"),
    ("Chandipur", "Odisha", "Beach", "small"),
    ("Talsari", "Odisha", "Beach", "obscure"),
    ("Astaranga", "Odisha", "Beach", "obscure"),
    ("Satapada", "Odisha", "Beach", "obscure"),
    # West Bengal
    ("Digha", "West Bengal", "Beach", "mid"),
    ("Mandarmani", "West Bengal", "Beach", "mid"),
    ("Tajpur", "West Bengal", "Beach", "small"),
    ("Bakkhali", "West Bengal", "Beach", "small"),
    ("Shankarpur", "West Bengal", "Beach", "small"),
    ("Frasergunj", "West Bengal", "Beach", "obscure"),
    # Gujarat
    ("Tithal", "Gujarat", "Beach", "small"),
    ("Dumas", "Gujarat", "Beach", "small"),
    ("Mandvi", "Gujarat", "Beach", "small"),
    ("Madhavpur", "Gujarat", "Beach", "small"),
    ("Chorwad", "Gujarat", "Beach", "small"),
    # Andaman & Nicobar
    ("Swaraj Dweep", "Andaman & Nicobar", "Beach", "major"),
    ("Shaheed Dweep", "Andaman & Nicobar", "Beach", "mid"),
    ("Diglipur", "Andaman & Nicobar", "Beach", "small"),
    ("Long Island", "Andaman & Nicobar", "Beach", "small"),
    ("Baratang", "Andaman & Nicobar", "Beach", "small"),
    ("Ross Island", "Andaman & Nicobar", "Beach", "small"),
    # Lakshadweep
    ("Agatti", "Lakshadweep", "Beach", "mid"),
    ("Bangaram", "Lakshadweep", "Beach", "small"),
    ("Kadmat", "Lakshadweep", "Beach", "small"),
    ("Minicoy", "Lakshadweep", "Beach", "small"),
    # Daman & Diu
    ("Diu", "Daman & Diu", "Beach", "mid"),
    ("Nagoa", "Daman & Diu", "Beach", "small"),
    ("Devka", "Daman & Diu", "Beach", "small"),
    ("Jampore", "Daman & Diu", "Beach", "small"),
    # Puducherry
    ("Pondicherry", "Puducherry", "Beach", "major"),
    ("Mahe", "Puducherry", "Beach", "small"),
    ("Auroville", "Puducherry", "Beach", "mid"),
    ("Yanam", "Puducherry", "Beach", "small"),

    # =====================================================================
    # BACKWATER (~46)
    # =====================================================================
    # Kerala
    ("Kumarakom", "Kerala", "Backwater", "major"),
    ("Kollam", "Kerala", "Backwater", "mid"),
    ("Vembanad", "Kerala", "Backwater", "small"),
    ("Pathiramanal", "Kerala", "Backwater", "small"),
    ("Punnamada", "Kerala", "Backwater", "small"),
    ("Champakulam", "Kerala", "Backwater", "small"),
    ("Kuttanad", "Kerala", "Backwater", "mid"),
    ("Nedumudi", "Kerala", "Backwater", "small"),
    ("Edathua", "Kerala", "Backwater", "obscure"),
    ("Thottappally", "Kerala", "Backwater", "small"),
    ("Kayamkulam", "Kerala", "Backwater", "small"),
    ("Karunagappally", "Kerala", "Backwater", "small"),
    ("Kainakary", "Kerala", "Backwater", "small"),
    ("Karuvatta", "Kerala", "Backwater", "obscure"),
    ("Aroor", "Kerala", "Backwater", "small"),
    ("Mannar", "Kerala", "Backwater", "small"),
    ("Chavara", "Kerala", "Backwater", "obscure"),
    ("Munroe Island", "Kerala", "Backwater", "small"),
    ("Ashtamudi", "Kerala", "Backwater", "mid"),
    ("Nileshwar", "Kerala", "Backwater", "small"),
    ("Valiyaparamba", "Kerala", "Backwater", "obscure"),
    ("Kanjirappally", "Kerala", "Backwater", "obscure"),
    ("Akkulam", "Kerala", "Backwater", "small"),
    ("Pamba", "Kerala", "Backwater", "obscure"),
    ("Mavelikkara", "Kerala", "Backwater", "small"),
    ("Ramankary", "Kerala", "Backwater", "obscure"),
    ("Pulinkunnu", "Kerala", "Backwater", "obscure"),
    ("Mannancherry", "Kerala", "Backwater", "obscure"),
    ("Pallippuram", "Kerala", "Backwater", "obscure"),
    ("Kavalam", "Kerala", "Backwater", "obscure"),
    ("Veeyapuram", "Kerala", "Backwater", "obscure"),
    # West Bengal
    ("Gosaba", "West Bengal", "Backwater", "small"),
    ("Pakhirala", "West Bengal", "Backwater", "obscure"),
    ("Bali", "West Bengal", "Backwater", "obscure"),
    ("Dayapur", "West Bengal", "Backwater", "obscure"),
    ("Jharkhali", "West Bengal", "Backwater", "obscure"),
    # Odisha
    ("Barkul", "Odisha", "Backwater", "small"),
    ("Rambha", "Odisha", "Backwater", "small"),
    ("Mangalajodi", "Odisha", "Backwater", "small"),
    ("Tampara", "Odisha", "Backwater", "obscure"),
    # Goa
    ("Chorao", "Goa", "Backwater", "small"),
    ("Divar", "Goa", "Backwater", "small"),
    # Karnataka
    ("Honavar", "Karnataka", "Backwater", "small"),
    ("Linganamakki", "Karnataka", "Backwater", "obscure"),
    # Andhra Pradesh
    ("Kolleru", "Andhra Pradesh", "Backwater", "small"),
    # Assam
    ("Majuli", "Assam", "Backwater", "mid"),
    # Andaman & Nicobar
    ("Mayabunder", "Andaman & Nicobar", "Backwater", "small"),

    # =====================================================================
    # METRO (~170)
    # =====================================================================
    # Maharashtra
    ("Nagpur", "Maharashtra", "Metro", "major"),
    ("Aurangabad", "Maharashtra", "Metro", "major"),
    ("Solapur", "Maharashtra", "Metro", "mid"),
    ("Kolhapur", "Maharashtra", "Metro", "mid"),
    ("Sangli", "Maharashtra", "Metro", "mid"),
    ("Akola", "Maharashtra", "Metro", "mid"),
    ("Latur", "Maharashtra", "Metro", "mid"),
    ("Nanded", "Maharashtra", "Metro", "mid"),
    ("Jalgaon", "Maharashtra", "Metro", "mid"),
    ("Dhule", "Maharashtra", "Metro", "small"),
    ("Ahmednagar", "Maharashtra", "Metro", "mid"),
    ("Amravati", "Maharashtra", "Metro", "mid"),
    # Uttar Pradesh
    ("Kanpur", "Uttar Pradesh", "Metro", "major"),
    ("Ghaziabad", "Uttar Pradesh", "Metro", "major"),
    ("Noida", "Uttar Pradesh", "Metro", "major"),
    ("Meerut", "Uttar Pradesh", "Metro", "major"),
    ("Aligarh", "Uttar Pradesh", "Metro", "mid"),
    ("Bareilly", "Uttar Pradesh", "Metro", "mid"),
    ("Moradabad", "Uttar Pradesh", "Metro", "mid"),
    ("Saharanpur", "Uttar Pradesh", "Metro", "mid"),
    ("Gorakhpur", "Uttar Pradesh", "Metro", "mid"),
    ("Lucknow", "Uttar Pradesh", "Metro", "major"),
    # West Bengal
    ("Howrah", "West Bengal", "Metro", "major"),
    ("Asansol", "West Bengal", "Metro", "mid"),
    ("Durgapur", "West Bengal", "Metro", "mid"),
    ("Siliguri", "West Bengal", "Metro", "mid"),
    ("Bardhaman", "West Bengal", "Metro", "mid"),
    ("Jalpaiguri", "West Bengal", "Metro", "small"),
    ("Berhampore", "West Bengal", "Metro", "small"),
    ("Kharagpur", "West Bengal", "Metro", "mid"),
    # Bihar
    ("Patna", "Bihar", "Metro", "major"),
    ("Gaya", "Bihar", "Metro", "major"),
    ("Muzaffarpur", "Bihar", "Metro", "mid"),
    ("Bhagalpur", "Bihar", "Metro", "mid"),
    ("Darbhanga", "Bihar", "Metro", "mid"),
    ("Begusarai", "Bihar", "Metro", "mid"),
    ("Purnia", "Bihar", "Metro", "mid"),
    ("Arrah", "Bihar", "Metro", "small"),
    ("Saharsa", "Bihar", "Metro", "small"),
    # Jharkhand
    ("Ranchi", "Jharkhand", "Metro", "major"),
    ("Jamshedpur", "Jharkhand", "Metro", "major"),
    ("Bokaro", "Jharkhand", "Metro", "mid"),
    ("Dhanbad", "Jharkhand", "Metro", "mid"),
    ("Hazaribagh", "Jharkhand", "Metro", "small"),
    ("Phusro", "Jharkhand", "Metro", "small"),
    ("Giridih", "Jharkhand", "Metro", "small"),
    # Odisha
    ("Bhubaneswar", "Odisha", "Metro", "major"),
    ("Cuttack", "Odisha", "Metro", "major"),
    ("Rourkela", "Odisha", "Metro", "mid"),
    ("Sambalpur", "Odisha", "Metro", "mid"),
    ("Berhampur", "Odisha", "Metro", "mid"),
    ("Balasore", "Odisha", "Metro", "small"),
    ("Paradip", "Odisha", "Metro", "small"),
    ("Jharsuguda", "Odisha", "Metro", "small"),
    # Chhattisgarh
    ("Raipur", "Chhattisgarh", "Metro", "major"),
    ("Bilaspur", "Chhattisgarh", "Metro", "mid"),
    ("Bhilai", "Chhattisgarh", "Metro", "mid"),
    ("Durg", "Chhattisgarh", "Metro", "mid"),
    ("Korba", "Chhattisgarh", "Metro", "mid"),
    ("Rajnandgaon", "Chhattisgarh", "Metro", "small"),
    ("Jagdalpur", "Chhattisgarh", "Metro", "small"),
    # Andhra Pradesh
    ("Visakhapatnam", "Andhra Pradesh", "Metro", "major"),
    ("Vijayawada", "Andhra Pradesh", "Metro", "major"),
    ("Guntur", "Andhra Pradesh", "Metro", "mid"),
    ("Nellore", "Andhra Pradesh", "Metro", "mid"),
    ("Kurnool", "Andhra Pradesh", "Metro", "mid"),
    ("Kakinada", "Andhra Pradesh", "Metro", "mid"),
    ("Rajamahendravaram", "Andhra Pradesh", "Metro", "mid"),
    ("Ananthapur", "Andhra Pradesh", "Metro", "small"),
    # Telangana
    ("Khammam", "Telangana", "Metro", "small"),
    ("Nalgonda", "Telangana", "Metro", "small"),
    ("Ramagundam", "Telangana", "Metro", "small"),
    ("Wanaparthy", "Telangana", "Metro", "small"),
    # Tamil Nadu
    ("Coimbatore", "Tamil Nadu", "Metro", "major"),
    ("Salem", "Tamil Nadu", "Metro", "mid"),
    ("Tiruchirappalli", "Tamil Nadu", "Metro", "major"),
    ("Vellore", "Tamil Nadu", "Metro", "mid"),
    ("Tirunelveli", "Tamil Nadu", "Metro", "mid"),
    ("Erode", "Tamil Nadu", "Metro", "mid"),
    ("Tiruppur", "Tamil Nadu", "Metro", "mid"),
    ("Hosur", "Tamil Nadu", "Metro", "mid"),
    ("Karur", "Tamil Nadu", "Metro", "small"),
    ("Thoothukudi", "Tamil Nadu", "Metro", "mid"),
    # Karnataka
    ("Mangalore", "Karnataka", "Metro", "major"),
    ("Hubli", "Karnataka", "Metro", "mid"),
    ("Belgaum", "Karnataka", "Metro", "mid"),
    ("Dharwad", "Karnataka", "Metro", "mid"),
    ("Davangere", "Karnataka", "Metro", "mid"),
    ("Tumkur", "Karnataka", "Metro", "mid"),
    ("Shimoga", "Karnataka", "Metro", "small"),
    ("Gulbarga", "Karnataka", "Metro", "mid"),
    ("Raichur", "Karnataka", "Metro", "small"),
    ("Bellary", "Karnataka", "Metro", "small"),
    # Kerala
    ("Thiruvananthapuram", "Kerala", "Metro", "major"),
    ("Thrissur", "Kerala", "Metro", "mid"),
    ("Kozhikode", "Kerala", "Metro", "mid"),
    ("Kannur", "Kerala", "Metro", "mid"),
    ("Palakkad", "Kerala", "Metro", "mid"),
    ("Malappuram", "Kerala", "Metro", "mid"),
    ("Kasaragod", "Kerala", "Metro", "small"),
    ("Kottayam", "Kerala", "Metro", "mid"),
    ("Pathanamthitta", "Kerala", "Metro", "small"),
    # Goa
    ("Vasco da Gama", "Goa", "Metro", "mid"),
    ("Margao", "Goa", "Metro", "mid"),
    ("Mapusa", "Goa", "Metro", "small"),
    # Gujarat
    ("Surat", "Gujarat", "Metro", "major"),
    ("Vadodara", "Gujarat", "Metro", "major"),
    ("Rajkot", "Gujarat", "Metro", "major"),
    ("Bhavnagar", "Gujarat", "Metro", "mid"),
    ("Jamnagar", "Gujarat", "Metro", "mid"),
    ("Anand", "Gujarat", "Metro", "mid"),
    ("Gandhinagar", "Gujarat", "Metro", "mid"),
    ("Bharuch", "Gujarat", "Metro", "small"),
    # Madhya Pradesh
    ("Bhopal", "Madhya Pradesh", "Metro", "major"),
    ("Indore", "Madhya Pradesh", "Metro", "major"),
    ("Jabalpur", "Madhya Pradesh", "Metro", "mid"),
    ("Sagar", "Madhya Pradesh", "Metro", "small"),
    ("Rewa", "Madhya Pradesh", "Metro", "small"),
    ("Satna", "Madhya Pradesh", "Metro", "small"),
    ("Dewas", "Madhya Pradesh", "Metro", "small"),
    # Rajasthan
    ("Kota", "Rajasthan", "Metro", "mid"),
    ("Beawar", "Rajasthan", "Metro", "small"),
    ("Hanumangarh", "Rajasthan", "Metro", "small"),
    ("Sri Ganganagar", "Rajasthan", "Metro", "small"),
    ("Bhilwara", "Rajasthan", "Metro", "mid"),
    ("Jhalawar", "Rajasthan", "Metro", "small"),
    # Uttarakhand
    ("Roorkee", "Uttarakhand", "Metro", "mid"),
    ("Haldwani", "Uttarakhand", "Metro", "mid"),
    ("Rudrapur", "Uttarakhand", "Metro", "mid"),
    ("Kashipur", "Uttarakhand", "Metro", "small"),
    # Himachal Pradesh
    ("Solan", "Himachal Pradesh", "Metro", "small"),
    ("Mandi", "Himachal Pradesh", "Metro", "small"),
    ("Una", "Himachal Pradesh", "Metro", "small"),
    ("Hamirpur", "Himachal Pradesh", "Metro", "small"),
    ("Nahan", "Himachal Pradesh", "Metro", "small"),
    # Punjab
    ("Ludhiana", "Punjab", "Metro", "major"),
    ("Jalandhar", "Punjab", "Metro", "major"),
    ("Pathankot", "Punjab", "Metro", "mid"),
    ("Bathinda", "Punjab", "Metro", "mid"),
    ("Mohali", "Punjab", "Metro", "mid"),
    # Haryana
    ("Faridabad", "Haryana", "Metro", "major"),
    ("Gurugram", "Haryana", "Metro", "major"),
    ("Panipat", "Haryana", "Metro", "mid"),
    ("Rohtak", "Haryana", "Metro", "mid"),
    ("Hisar", "Haryana", "Metro", "mid"),
    ("Karnal", "Haryana", "Metro", "mid"),
    ("Sonipat", "Haryana", "Metro", "mid"),
    # Jammu & Kashmir
    ("Srinagar", "Jammu & Kashmir", "Metro", "major"),
    ("Jammu", "Jammu & Kashmir", "Metro", "major"),
    ("Anantnag", "Jammu & Kashmir", "Metro", "mid"),
    ("Baramulla", "Jammu & Kashmir", "Metro", "mid"),
    ("Sopore", "Jammu & Kashmir", "Metro", "small"),
    # Assam
    ("Silchar", "Assam", "Metro", "mid"),
    ("Dibrugarh", "Assam", "Metro", "mid"),
    ("Tezpur", "Assam", "Metro", "mid"),
    ("Jorhat", "Assam", "Metro", "mid"),
    ("Tinsukia", "Assam", "Metro", "mid"),
    # Tripura
    ("Agartala", "Tripura", "Metro", "mid"),
    ("Udaipur Tripura", "Tripura", "Metro", "small"),
    ("Dharmanagar", "Tripura", "Metro", "small"),
    # Manipur
    ("Imphal", "Manipur", "Metro", "mid"),
    # Mizoram
    ("Aizawl", "Mizoram", "Metro", "mid"),
    # Nagaland
    ("Dimapur", "Nagaland", "Metro", "mid"),
    # Meghalaya
    ("Tura", "Meghalaya", "Metro", "mid"),
    # Arunachal Pradesh
    ("Itanagar", "Arunachal Pradesh", "Metro", "mid"),
    ("Naharlagun", "Arunachal Pradesh", "Metro", "small"),
    ("Pasighat", "Arunachal Pradesh", "Metro", "small"),
    # Puducherry
    ("Karaikal", "Puducherry", "Metro", "small"),
    # Andaman & Nicobar
    ("Port Blair", "Andaman & Nicobar", "Metro", "mid"),
    # Lakshadweep
    ("Kavaratti", "Lakshadweep", "Metro", "small"),
]


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
def make_lat_lng(state):
    base = STATE_CENTROID[state]
    jitter = STATE_JITTER.get(state, LARGE)
    return (
        round(base[0] + random.uniform(-jitter, jitter), 4),
        round(base[1] + random.uniform(-jitter, jitter), 4),
    )


def make_arrivals(zone, tier):
    base = POP_TIER_BASE[tier] * ZONE_FACTOR[zone]
    jittered = base * random.uniform(0.85, 1.15)
    return round(min(jittered, ARRIVALS_CAP), 2)


def main():
    # Sanity: every state must be in india_states_zones (37 valid)
    bad_states = {s for _, s, _, _ in CITIES} - STATE_REGION.keys()
    if bad_states:
        raise SystemExit(f"Unknown states in catalog: {bad_states}")

    # Dedup within catalog (city, state)
    seen = set()
    dupes = []
    for c, s, _, _ in CITIES:
        if (c, s) in seen:
            dupes.append((c, s))
        seen.add((c, s))
    if dupes:
        raise SystemExit(f"Duplicate (city, state) within catalog: {dupes}")

    with OUT_PATH.open("w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow([
            "city", "state", "region", "tourism_zone",
            "latitude", "longitude",
            "tourist_arrivals_annual_m", "peak_months", "popularity_tier",
        ])
        for city, state, zone, tier in CITIES:
            lat, lng = make_lat_lng(state)
            arr = make_arrivals(zone, tier)
            w.writerow([
                city, state, STATE_REGION[state], zone,
                lat, lng, arr, PEAK_MONTHS_BY_ZONE[zone], tier,
            ])

    # Summary
    from collections import Counter
    zone_counts = Counter(z for _, _, z, _ in CITIES)
    state_counts = Counter(s for _, s, _, _ in CITIES)
    print(f"Wrote {len(CITIES)} cities to {OUT_PATH}")
    print("\nBy zone:")
    for z, n in sorted(zone_counts.items(), key=lambda kv: -kv[1]):
        print(f"  {z:<14} {n}")
    print(f"\nStates covered: {len(state_counts)}")


if __name__ == "__main__":
    main()
