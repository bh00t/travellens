-- TravelLens Phase 1 Schema
-- Table creation order: extensions → reference → dimensions → facts → aggregates
-- No indexes here — created post-load in load_to_postgres.py

CREATE EXTENSION IF NOT EXISTS vector;

-- ── Reference tables (no foreign keys) ───────────────────────────────────────

CREATE TABLE ref_price_tiers (
    tier_id       VARCHAR(20)  PRIMARY KEY,
    tier_name     VARCHAR(50)  NOT NULL,
    min_price_inr INTEGER      NOT NULL,
    max_price_inr INTEGER      NOT NULL,
    description   TEXT
);

CREATE TABLE ref_state_centroids (
    state_name VARCHAR(100) PRIMARY KEY,
    latitude   NUMERIC(9,4) NOT NULL,
    longitude  NUMERIC(9,4) NOT NULL
);

CREATE TABLE india_states_zones (
    state_code          VARCHAR(10)  PRIMARY KEY,
    state_name          VARCHAR(100) NOT NULL,
    zone                VARCHAR(50)  NOT NULL,
    tourist_arrivals_m  NUMERIC(6,1),
    peak_months         TEXT,
    primary_attractions TEXT
);

CREATE TABLE public_holidays (
    holiday_date      DATE         NOT NULL,
    holiday_name      VARCHAR(100) NOT NULL,
    holiday_type      VARCHAR(50)  NOT NULL,
    states_applicable VARCHAR(100),
    demand_impact     VARCHAR(20),
    PRIMARY KEY (holiday_date, holiday_name)
);

-- ── Dimensions ────────────────────────────────────────────────────────────────

CREATE TABLE dim_date (
    date_id                INTEGER      PRIMARY KEY,
    full_date              DATE         NOT NULL,
    day_of_month           SMALLINT,
    day_of_week            VARCHAR(10),
    day_num                SMALLINT,
    week_of_year           SMALLINT,
    month                  SMALLINT,
    month_name             VARCHAR(20),
    quarter                VARCHAR(5),
    year                   SMALLINT,
    is_weekend             BOOLEAN,
    is_holiday             BOOLEAN,
    is_high_demand_holiday BOOLEAN,
    season                 VARCHAR(20)
);

CREATE TABLE dim_location (
    location_id               UUID         PRIMARY KEY,
    city                      VARCHAR(100) NOT NULL,
    state                     VARCHAR(100) NOT NULL,
    region                    VARCHAR(100),
    tourism_zone              VARCHAR(50),
    latitude                  NUMERIC(9,4),
    longitude                 NUMERIC(9,4),
    tourist_arrivals_annual_m NUMERIC(8,2),
    peak_months               TEXT
);

CREATE TABLE dim_customer (
    customer_id        VARCHAR(20)  PRIMARY KEY,
    first_name         VARCHAR(100),
    last_name          VARCHAR(100),
    age                SMALLINT,
    gender             VARCHAR(1),
    home_state         VARCHAR(100),
    customer_segment   VARCHAR(50),
    travel_purpose     VARCHAR(50),
    loyalty_tier       VARCHAR(20),
    is_repeat_customer BOOLEAN
);

CREATE TABLE hotel_master (
    hotel_id      VARCHAR(20)  PRIMARY KEY,
    hotel_name    VARCHAR(200) NOT NULL,
    chain_name    VARCHAR(100),
    property_type VARCHAR(50),
    star_category SMALLINT,
    total_rooms   INTEGER,
    location_id   UUID         REFERENCES dim_location(location_id),
    avg_rating    NUMERIC(3,2),
    amenities_json JSONB,
    review_count  INTEGER,
    price_tier_id VARCHAR(20)  REFERENCES ref_price_tiers(tier_id),
    base_price_inr NUMERIC(10,2),
    is_active     BOOLEAN
);

CREATE TABLE dim_room_type (
    room_type_id   UUID        PRIMARY KEY,
    hotel_id       VARCHAR(20) REFERENCES hotel_master(hotel_id),
    type_name      VARCHAR(100),
    capacity       SMALLINT,
    has_ac         BOOLEAN,
    has_breakfast  BOOLEAN,
    price_tier     VARCHAR(20) REFERENCES ref_price_tiers(tier_id),
    base_price_inr NUMERIC(10,2)
);

-- ── Facts ─────────────────────────────────────────────────────────────────────

CREATE TABLE fact_bookings (
    booking_id       UUID        PRIMARY KEY,
    hotel_id         VARCHAR(20) REFERENCES hotel_master(hotel_id),
    customer_id      VARCHAR(20) REFERENCES dim_customer(customer_id),
    room_type_id     UUID        REFERENCES dim_room_type(room_type_id),
    location_id      UUID        REFERENCES dim_location(location_id),
    date_id          INTEGER     REFERENCES dim_date(date_id),
    checkin_date     DATE,
    checkout_date    DATE,
    nights_stayed    SMALLINT,
    num_guests       SMALLINT,
    nightly_rate_inr NUMERIC(10,2),
    revenue_inr      NUMERIC(10,2),
    booking_source   VARCHAR(50),
    is_cancelled     BOOLEAN     NOT NULL DEFAULT FALSE,
    booking_ts       TIMESTAMP,
    event_ts         TIMESTAMP
);

CREATE TABLE fact_price_events (
    event_id        UUID        PRIMARY KEY,
    hotel_id        VARCHAR(20) REFERENCES hotel_master(hotel_id),
    room_type_id    UUID        REFERENCES dim_room_type(room_type_id),
    event_ts        TIMESTAMP,
    old_price_inr   NUMERIC(10,2),
    new_price_inr   NUMERIC(10,2),
    delta_inr       NUMERIC(10,2),
    pct_change      NUMERIC(6,2),
    change_reason   VARCHAR(50),
    flink_window_id VARCHAR(50)
);

CREATE TABLE reviews_raw (
    review_id     UUID        PRIMARY KEY,
    hotel_id      VARCHAR(20) REFERENCES hotel_master(hotel_id),
    reviewer_name VARCHAR(200),
    review_text   TEXT,
    rating        NUMERIC(3,1),
    review_date   DATE,
    source        VARCHAR(50),
    travel_type   VARCHAR(50),
    embedding     vector(384)
);

-- ── Aggregates (populated by Phase 2 Flink jobs) ──────────────────────────────

CREATE TABLE agg_hourly_city_stats (
    city              VARCHAR(100)  NOT NULL,
    window_start      TIMESTAMP     NOT NULL,
    window_end        TIMESTAMP     NOT NULL,
    total_bookings    INTEGER,
    total_revenue_inr NUMERIC(15,2),
    avg_occupancy_rate NUMERIC(5,2),
    PRIMARY KEY (city, window_start)
);

CREATE TABLE agg_daily_hotel_kpi (
    hotel_id             VARCHAR(20)   NOT NULL,
    kpi_date             DATE          NOT NULL,
    total_bookings       INTEGER,
    total_revenue_inr    NUMERIC(15,2),
    avg_nightly_rate_inr NUMERIC(10,2),
    cancellation_rate    NUMERIC(5,2),
    avg_rating           NUMERIC(3,2),
    PRIMARY KEY (hotel_id, kpi_date)
);
