-- =============================================
-- FLIGHT DATA PIPELINE - DATABASE SCHEMA
-- =============================================
-- Run order matters: dimensions first, then facts
-- All tables use INSERT ON CONFLICT for idempotency

-- =============================================
-- DIMENSION TABLES (no dependencies, load first)
-- =============================================

-- Carriers: extracted from flight CSVs
-- Reporting_Airline is the unique carrier code (AA, UA, DL, etc.)
CREATE TABLE IF NOT EXISTS carriers (
    carrier_code VARCHAR(10) PRIMARY KEY,
    carrier_name TEXT NOT NULL,
    dot_id INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Airports: loaded from OpenFlights dataset
-- IATA code is the link key (JFK, LAX, ORD, etc.)
CREATE TABLE IF NOT EXISTS airports (
    airport_code VARCHAR(10) PRIMARY KEY,
    airport_name TEXT NOT NULL,
    city TEXT,
    state TEXT,
    country TEXT,
    latitude FLOAT,
    longitude FLOAT,
    altitude INTEGER,
    timezone TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Date dimension: generated programmatically for full date range
-- Enables fast GROUP BY on year, month, day_of_week, season
CREATE TABLE IF NOT EXISTS date_dim (
    date_id DATE PRIMARY KEY,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    season VARCHAR(10) NOT NULL
);

-- =============================================
-- FACT TABLES (depend on dimension tables above)
-- =============================================

-- Flights: main fact table
-- Columns mapped directly from BTS On-Time Performance CSV
CREATE TABLE IF NOT EXISTS flights (
    flight_id BIGSERIAL PRIMARY KEY,
    
    -- Date & carrier
    flight_date DATE NOT NULL REFERENCES date_dim(date_id),
    carrier_code VARCHAR(10) NOT NULL REFERENCES carriers(carrier_code),
    tail_number VARCHAR(20),
    flight_number INTEGER,
    
    -- Origin
    origin_airport VARCHAR(10) NOT NULL REFERENCES airports(airport_code),
    origin_city TEXT,
    origin_state VARCHAR(5),
    
    -- Destination
    dest_airport VARCHAR(10) NOT NULL REFERENCES airports(airport_code),
    dest_city TEXT,
    dest_state VARCHAR(5),
    
    -- Departure times
    scheduled_dep VARCHAR(10),      -- CRSDepTime (hhmm format)
    actual_dep VARCHAR(10),         -- DepTime
    dep_delay FLOAT,                -- DepDelay (negative = early)
    dep_delay_minutes FLOAT,        -- DepDelayMinutes (early set to 0)
    dep_delay_15 BOOLEAN,           -- DepDel15 (1 = delayed 15+ min)
    
    -- Arrival times
    scheduled_arr VARCHAR(10),      -- CRSArrTime
    actual_arr VARCHAR(10),         -- ArrTime
    arr_delay FLOAT,                -- ArrDelay (negative = early)
    arr_delay_minutes FLOAT,        -- ArrDelayMinutes (early set to 0)
    arr_delay_15 BOOLEAN,           -- ArrDel15 (1 = delayed 15+ min)
    
    -- Flight details
    cancelled BOOLEAN DEFAULT FALSE,
    cancellation_code VARCHAR(5),
    diverted BOOLEAN DEFAULT FALSE,
    distance FLOAT,
    air_time FLOAT,
    scheduled_elapsed FLOAT,        -- CRSElapsedTime
    actual_elapsed FLOAT,           -- ActualElapsedTime
    
    -- Delay breakdown (only populated for delayed flights)
    carrier_delay FLOAT,
    weather_delay FLOAT,
    nas_delay FLOAT,
    security_delay FLOAT,
    late_aircraft_delay FLOAT,
    
    -- Dedup key: one flight per date+carrier+number+origin
    UNIQUE (flight_date, carrier_code, flight_number, origin_airport, scheduled_dep)
);

-- =============================================
-- WEATHER (Phase 2 - created now, populated later)
-- =============================================

CREATE TABLE IF NOT EXISTS weather_observations (
    observation_id BIGSERIAL PRIMARY KEY,
    airport_code VARCHAR(10) NOT NULL REFERENCES airports(airport_code),
    observation_date DATE NOT NULL REFERENCES date_dim(date_id),
    observation_time TIMESTAMP NOT NULL,
    avg_temperature FLOAT,
    max_temperature FLOAT,
    min_temperature FLOAT,
    avg_wind_speed FLOAT,
    max_wind_speed FLOAT,
    avg_visibility FLOAT,
    precipitation FLOAT,
    snow_depth FLOAT,
    conditions TEXT,
    UNIQUE (airport_code, observation_time)
);

-- =============================================
-- INDEXES (beyond PK and UNIQUE constraints)
-- =============================================

-- Flight lookups during querying
CREATE INDEX IF NOT EXISTS idx_flights_date ON flights(flight_date);
CREATE INDEX IF NOT EXISTS idx_flights_carrier ON flights(carrier_code);
CREATE INDEX IF NOT EXISTS idx_flights_origin ON flights(origin_airport);
CREATE INDEX IF NOT EXISTS idx_flights_dest ON flights(dest_airport);
CREATE INDEX IF NOT EXISTS idx_flights_arr_delay ON flights(arr_delay);

-- Composite: "delays from JFK in January"
CREATE INDEX IF NOT EXISTS idx_flights_origin_date ON flights(origin_airport, flight_date);
-- Composite: "carrier performance over time"
CREATE INDEX IF NOT EXISTS idx_flights_carrier_date ON flights(carrier_code, flight_date);

-- Weather joins
CREATE INDEX IF NOT EXISTS idx_weather_airport_date ON weather_observations(airport_code, observation_date);

-- =============================================
-- PIPELINE METADATA
-- =============================================

-- Tracks processed files for idempotency
-- If pipeline reruns, check here first: "did I already load this file?"
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id BIGSERIAL PRIMARY KEY,
    file_name TEXT NOT NULL,
    source TEXT NOT NULL,           -- 'flights', 'airports', 'weather'
    rows_processed INTEGER,
    rows_loaded INTEGER,
    rows_rejected INTEGER,
    status VARCHAR(20) NOT NULL,    -- 'running', 'completed', 'failed'
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    error_message TEXT,
    UNIQUE (file_name, source)
);

-- Rejected records audit trail
-- Never silently drop data — log it here with the reason
CREATE TABLE IF NOT EXISTS rejected_records (
    reject_id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    file_name TEXT NOT NULL,
    row_number INTEGER,
    raw_data TEXT,
    rejection_reason TEXT NOT NULL,
    rejected_at TIMESTAMP DEFAULT NOW()
);
