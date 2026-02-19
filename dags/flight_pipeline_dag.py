"""
Flight Data ETL Pipeline
========================
Orchestrates the full pipeline:
  Stage 1: Upload raw files to S3 (MinIO)
  Stage 2: Load airports (dimension) from OpenFlights
  Stage 3: Extract carriers from flight CSVs + generate date_dim
  Stage 4: Load flights (fact table — depends on carriers, airports, dates)
  Stage 5: Load weather observations for airports (Phase 2)
  Stage 6: Post-load data quality checks

DAG dependency graph:
  upload_to_s3 >> load_airports >> [extract_carriers, generate_dates] >> load_flights >> load_weather >> quality_checks

Multi-file support:
  - Processes ALL CSV files in data/raw/
  - Tracks processed files in pipeline_runs table
  - Skips already-processed files (idempotent)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from psycopg2.extras import execute_values

import sys
sys.path.insert(0, '/opt/airflow/scripts')


# =============================================
# DEFAULT ARGS (apply to all tasks in DAG)
# =============================================
default_args = {
    'owner': 'manoj',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2025, 1, 1),
}

# Bucket and file paths in MinIO
BUCKET = 'flight-data'
AIRPORTS_KEY = 'raw/airports.dat'


# =============================================
# TASK FUNCTIONS
# =============================================

def upload_raw_files_to_s3(**kwargs):
    """
    Upload raw data files from local /data/raw/ to MinIO.
    Uploads ALL CSV files found (multi-file support).
    """
    from s3_helper import upload_file, ensure_bucket
    import os

    ensure_bucket(BUCKET)

    raw_dir = '/opt/airflow/data/raw'
    uploaded = []

    # Upload ALL flight CSV files
    for f in os.listdir(raw_dir):
        if f.endswith('.csv') and 'airport' not in f.lower():
            key = f'raw/{f}'
            upload_file(BUCKET, key, os.path.join(raw_dir, f))
            uploaded.append(f)

    # Upload airports.dat
    airports_path = os.path.join(raw_dir, 'airports.dat')
    if os.path.exists(airports_path):
        upload_file(BUCKET, AIRPORTS_KEY, airports_path)
        uploaded.append('airports.dat')

    print(f"Uploaded {len(uploaded)} files to s3://{BUCKET}/")
    return uploaded


def load_airports(**kwargs):
    """
    Load airports from OpenFlights airports.dat into airports table.

    airports.dat has no headers. Column order:
    ID, Name, City, Country, IATA, ICAO, Lat, Lon, Altitude, TZ_offset, DST, Timezone, Type, Source

    We only load airports with valid IATA codes (3 letters).
    Uses INSERT ON CONFLICT for idempotency.
    """
    import pandas as pd
    import io
    from s3_helper import get_s3_client
    from db_helper import get_connection
    from validation_schemas import airports_schema
    from datetime import datetime as dt

    print("Loading airports from S3...")

    # Use shared S3 client
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=BUCKET, Key=AIRPORTS_KEY)

    col_names = [
        'id', 'airport_name', 'city', 'country', 'airport_code', 'icao',
        'latitude', 'longitude', 'altitude', 'tz_offset', 'dst', 'timezone',
        'type', 'source'
    ]

    df = pd.read_csv(io.BytesIO(obj['Body'].read()), header=None, names=col_names)

    print(f"Raw airports: {len(df)} rows")

    # Filter: only airports with valid IATA codes
    df = df[df['airport_code'].notna() & (df['airport_code'] != '\\N')]
    df = df[df['airport_code'].str.len() == 3]

    # Deduplicate by IATA code (keep first occurrence)
    df = df.drop_duplicates(subset='airport_code', keep='first')

    print(f"Valid IATA airports: {len(df)} rows")

    # Validate with Pandera
    try:
        airports_schema.validate(df, lazy=True)
        print("Pandera validation passed")
    except Exception as e:
        print(f"Pandera validation warnings: {e}")

    # Insert into database
    insert_query = """
        INSERT INTO airports (airport_code, airport_name, city, country,
                              latitude, longitude, altitude, timezone)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (airport_code) DO NOTHING
    """

    rows_loaded = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                cur.execute(insert_query, (
                    row['airport_code'],
                    row['airport_name'],
                    row['city'],
                    row['country'],
                    row['latitude'],
                    row['longitude'],
                    int(row['altitude']) if pd.notna(row['altitude']) else None,
                    row['timezone'] if pd.notna(row['timezone']) else None,
                ))
                rows_loaded += 1

    # Log pipeline run
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_runs (file_name, source, rows_processed, rows_loaded,
                                           rows_rejected, status, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (file_name, source) DO UPDATE SET
                    rows_loaded = EXCLUDED.rows_loaded,
                    status = EXCLUDED.status,
                    completed_at = EXCLUDED.completed_at
            """, ('airports.dat', 'airports', len(df), rows_loaded, 0,
                  'completed', dt.now(), dt.now()))

    print(f"Loaded {rows_loaded} airports into database")
    return rows_loaded


def extract_carriers(**kwargs):
    """
    Extract unique carriers from ALL flight CSV files and load into carriers table.
    Multi-file support: reads from all CSVs in the bucket.
    Uses INSERT ON CONFLICT for idempotency.
    """
    import pandas as pd
    import io
    from s3_helper import get_s3_client, list_files
    from db_helper import get_connection

    print("Extracting carriers from ALL flight CSV files...")

    # Known US carrier code -> name mapping
    carrier_names = {
        'AA': 'American Airlines', 'DL': 'Delta Air Lines',
        'UA': 'United Airlines', 'WN': 'Southwest Airlines',
        'B6': 'JetBlue Airways', 'AS': 'Alaska Airlines',
        'NK': 'Spirit Airlines', 'F9': 'Frontier Airlines',
        'G4': 'Allegiant Air', 'HA': 'Hawaiian Airlines',
        'SY': 'Sun Country Airlines', 'MX': 'MexicanaLink',
        'OH': 'PSA Airlines', 'OO': 'SkyWest Airlines',
        'YV': 'Mesa Airlines', 'YX': 'Republic Airways',
        'QX': 'Horizon Air', 'MQ': 'Envoy Air',
        '9E': 'Endeavor Air', 'EV': 'ExpressJet Airlines',
        'PT': 'Piedmont Airlines', 'ZW': 'Air Wisconsin',
        'CP': 'Compass Airlines', 'C5': 'CommutAir',
        'G7': 'GoJet Airlines', 'KS': 'Penair',
    }

    # Find all CSV files
    all_files = list_files(BUCKET, prefix='raw/')
    csv_files = [f for f in all_files if f.endswith('.csv') and 'airport' not in f.lower()]

    if not csv_files:
        print("No CSV files found!")
        return 0

    # Collect unique carriers from ALL files
    all_carriers = {}
    s3 = get_s3_client()

    for file_key in csv_files:
        print(f"Reading carriers from {file_key}...")
        obj = s3.get_object(Bucket=BUCKET, Key=file_key)
        df = pd.read_csv(
            io.BytesIO(obj['Body'].read()),
            usecols=['Reporting_Airline', 'DOT_ID_Reporting_Airline'],
            low_memory=False
        )

        for _, row in df.drop_duplicates().iterrows():
            code = row['Reporting_Airline']
            if code not in all_carriers:
                dot_id = int(row['DOT_ID_Reporting_Airline']) if pd.notna(row['DOT_ID_Reporting_Airline']) else None
                all_carriers[code] = dot_id

    print(f"Found {len(all_carriers)} unique carriers across all files")

    insert_query = """
        INSERT INTO carriers (carrier_code, carrier_name, dot_id)
        VALUES (%s, %s, %s)
        ON CONFLICT (carrier_code) DO NOTHING
    """

    rows_loaded = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for code, dot_id in all_carriers.items():
                name = carrier_names.get(code, f'Carrier {code}')
                cur.execute(insert_query, (code, name, dot_id))
                rows_loaded += 1

    print(f"Loaded {rows_loaded} carriers into database")
    return rows_loaded


def generate_date_dim(**kwargs):
    """
    Generate date dimension table for the date range in ALL flight CSV files.
    Uses the shared ensure_dates_exist function from db_helper.
    """
    import pandas as pd
    import io
    from s3_helper import list_files, get_s3_client
    from db_helper import ensure_dates_exist

    print("Generating date dimension...")

    # Find all flight CSV files
    all_files = list_files(BUCKET, prefix='raw/')
    csv_files = [f for f in all_files if f.endswith('.csv') and 'airport' not in f.lower()]

    if not csv_files:
        print("No CSV files found!")
        return 0

    # Collect all unique dates from all CSV files
    all_dates = set()
    s3 = get_s3_client()

    for file_key in csv_files:
        print(f"Reading dates from {file_key}...")
        obj = s3.get_object(Bucket=BUCKET, Key=file_key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()), usecols=['FlightDate'], low_memory=False)
        file_dates = set(pd.to_datetime(df['FlightDate']))
        all_dates.update(file_dates)
        print(f"  Found {len(file_dates)} unique dates")

    print(f"Total unique dates across all files: {len(all_dates)}")

    # Use the shared function to add all dates
    rows_added = ensure_dates_exist(all_dates)

    print(f"Date dimension complete: {rows_added} dates processed")
    return rows_added


def load_flights(**kwargs):
    """
    Load flights from BTS CSV files into flights fact table.

    MULTI-FILE VERSION:
    - Lists all CSV files in S3 bucket
    - Checks pipeline_runs to see which files are already processed
    - Only processes NEW files
    - Tracks each file in pipeline_runs when done

    Key design decisions:
    - Chunked processing (50K rows at a time) to limit memory
    - INSERT ON CONFLICT for idempotency (safe retries)
    - Rejected rows logged to rejected_records table
    """
    import pandas as pd
    import io
    from s3_helper import get_s3_client, list_files
    from db_helper import get_connection, ensure_dates_exist, safe_float, safe_int, safe_str, safe_bool
    from validation_schemas import flights_schema
    from datetime import datetime as dt

    CHUNK_SIZE = 50000

    print("=" * 50)
    print("MULTI-FILE FLIGHT LOADER")
    print("=" * 50)

    # Step 1: Get list of all CSV files in S3
    all_files = list_files(BUCKET, prefix='raw/')
    csv_files = [f for f in all_files if f.endswith('.csv') and 'airport' not in f.lower()]
    print(f"Found {len(csv_files)} CSV files in S3: {csv_files}")

    # Step 2: Check which files have already been processed
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT file_name FROM pipeline_runs
                WHERE source = 'flights' AND status = 'completed'
            """)
            processed_files = set(row[0] for row in cur.fetchall())

    print(f"Already processed: {processed_files}")

    # Step 3: Find NEW files (not yet processed)
    new_files = [f for f in csv_files if f.split('/')[-1] not in processed_files]

    if not new_files:
        print("No new files to process!")
        return {'files_processed': 0, 'total_loaded': 0}

    print(f"New files to process: {new_files}")

    # Step 4: Get valid foreign keys from database
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT airport_code FROM airports")
            valid_airports = set(row[0] for row in cur.fetchall())

            cur.execute("SELECT carrier_code FROM carriers")
            valid_carriers = set(row[0] for row in cur.fetchall())

            cur.execute("SELECT date_id FROM date_dim")
            valid_dates = set(str(row[0]) for row in cur.fetchall())

    print(f"Valid airports: {len(valid_airports)}, carriers: {len(valid_carriers)}, dates: {len(valid_dates)}")

    # Step 5: Process each new file
    grand_total_loaded = 0
    grand_total_rejected = 0
    files_processed = 0

    s3 = get_s3_client()

    for file_key in new_files:
        file_name = file_key.split('/')[-1]
        print(f"\n{'=' * 50}")
        print(f"Processing: {file_name}")
        print(f"{'=' * 50}")

        started_at = dt.now()

        # Read CSV from S3
        obj = s3.get_object(Bucket=BUCKET, Key=file_key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()), low_memory=False)

        total_rows = len(df)
        print(f"Total rows in {file_name}: {total_rows}")

        # Validate with Pandera
        try:
            flights_schema.validate(df, lazy=True)
            print("Pandera validation passed")
        except Exception as e:
            print(f"Pandera validation warnings (continuing): {e}")

        # Check if we need to add new dates to date_dim
        dates_in_file = set(str(d.date()) for d in pd.to_datetime(df['FlightDate']))
        missing_dates = dates_in_file - valid_dates
        if missing_dates:
            print(f"Adding {len(missing_dates)} new dates to date_dim...")
            ensure_dates_exist(missing_dates)
            valid_dates.update(missing_dates)

        file_loaded = 0
        file_rejected = 0

        # Process in chunks — validate row-by-row, then bulk insert
        for chunk_start in range(0, total_rows, CHUNK_SIZE):
            chunk = df.iloc[chunk_start:chunk_start + CHUNK_SIZE]
            chunk_num = chunk_start // CHUNK_SIZE + 1
            print(f"Processing chunk {chunk_num} ({len(chunk)} rows)...")

            valid_rows = []
            rejected_rows = []

            # Step A: Validate all rows in Python (fast — set lookups only)
            for idx, row in chunk.iterrows():
                origin = str(row['Origin']).strip()
                dest = str(row['Dest']).strip()
                carrier = str(row['Reporting_Airline']).strip()
                flight_date = str(row['FlightDate']).strip()

                rejection_reasons = []
                if origin not in valid_airports:
                    rejection_reasons.append(f"Unknown origin airport: {origin}")
                if dest not in valid_airports:
                    rejection_reasons.append(f"Unknown dest airport: {dest}")
                if carrier not in valid_carriers:
                    rejection_reasons.append(f"Unknown carrier: {carrier}")
                if flight_date not in valid_dates:
                    rejection_reasons.append(f"Unknown date: {flight_date}")

                if rejection_reasons:
                    rejected_rows.append((
                        'flights', file_name, int(idx),
                        f"{flight_date},{carrier},{origin},{dest}",
                        '; '.join(rejection_reasons)
                    ))
                    continue

                valid_rows.append((
                    flight_date,
                    carrier,
                    safe_str(row.get('Tail_Number')),
                    safe_int(row.get('Flight_Number_Reporting_Airline')),
                    origin,
                    safe_str(row.get('OriginCityName')),
                    safe_str(row.get('OriginState')),
                    dest,
                    safe_str(row.get('DestCityName')),
                    safe_str(row.get('DestState')),
                    safe_str(row.get('CRSDepTime')),
                    safe_str(row.get('DepTime')),
                    safe_float(row.get('DepDelay')),
                    safe_float(row.get('DepDelayMinutes')),
                    safe_bool(row.get('DepDel15')),
                    safe_str(row.get('CRSArrTime')),
                    safe_str(row.get('ArrTime')),
                    safe_float(row.get('ArrDelay')),
                    safe_float(row.get('ArrDelayMinutes')),
                    safe_bool(row.get('ArrDel15')),
                    safe_bool(row.get('Cancelled')),
                    safe_str(row.get('CancellationCode')),
                    safe_bool(row.get('Diverted')),
                    safe_float(row.get('Distance')),
                    safe_float(row.get('AirTime')),
                    safe_float(row.get('CRSElapsedTime')),
                    safe_float(row.get('ActualElapsedTime')),
                    safe_float(row.get('CarrierDelay')),
                    safe_float(row.get('WeatherDelay')),
                    safe_float(row.get('NASDelay')),
                    safe_float(row.get('SecurityDelay')),
                    safe_float(row.get('LateAircraftDelay')),
                ))

            # Step B: Bulk insert valid rows (one round-trip for entire chunk)
            with get_connection() as conn:
                with conn.cursor() as cur:
                    if valid_rows:
                        execute_values(cur, """
                            INSERT INTO flights (
                                flight_date, carrier_code, tail_number, flight_number,
                                origin_airport, origin_city, origin_state,
                                dest_airport, dest_city, dest_state,
                                scheduled_dep, actual_dep, dep_delay, dep_delay_minutes, dep_delay_15,
                                scheduled_arr, actual_arr, arr_delay, arr_delay_minutes, arr_delay_15,
                                cancelled, cancellation_code, diverted,
                                distance, air_time, scheduled_elapsed, actual_elapsed,
                                carrier_delay, weather_delay, nas_delay, security_delay, late_aircraft_delay
                            ) VALUES %s
                            ON CONFLICT (flight_date, carrier_code, flight_number, origin_airport) DO NOTHING
                        """, valid_rows, page_size=10000)
                        file_loaded += len(valid_rows)

                    if rejected_rows:
                        execute_values(cur, """
                            INSERT INTO rejected_records (source, file_name, row_number, raw_data, rejection_reason)
                            VALUES %s
                        """, rejected_rows)
                        file_rejected += len(rejected_rows)

            print(f"  Chunk {chunk_num}: loaded={file_loaded}, rejected={file_rejected}")

        # Log this file as processed
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO pipeline_runs (file_name, source, rows_processed, rows_loaded,
                                               rows_rejected, status, started_at, completed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (file_name, source) DO UPDATE SET
                        rows_loaded = EXCLUDED.rows_loaded,
                        rows_rejected = EXCLUDED.rows_rejected,
                        status = EXCLUDED.status,
                        completed_at = EXCLUDED.completed_at
                """, (file_name, 'flights', total_rows, file_loaded,
                      file_rejected, 'completed', started_at, dt.now()))

        print(f"\n{file_name} complete: loaded={file_loaded}, rejected={file_rejected}")

        grand_total_loaded += file_loaded
        grand_total_rejected += file_rejected
        files_processed += 1

    print(f"\n{'=' * 50}")
    print(f"ALL FILES COMPLETE")
    print(f"Files processed: {files_processed}")
    print(f"Total loaded: {grand_total_loaded}")
    print(f"Total rejected: {grand_total_rejected}")
    print(f"{'=' * 50}")

    return {'files_processed': files_processed, 'loaded': grand_total_loaded, 'rejected': grand_total_rejected}


def load_weather(**kwargs):
    """
    Load weather observations for airports in our flight data.

    PHASE 2: Weather Data Integration
    ---------------------------------
    - Generates realistic sample weather data for portfolio demo
    - In production, this would fetch from NOAA's LCD API
    - Weather data enables delay correlation analysis:
      "Do flights have more delays on snowy days?"

    Design decisions:
    - Only loads weather for airports in our airport table
    - Only loads weather for dates in our date_dim table
    - Uses INSERT ON CONFLICT for idempotency (safe to re-run)
    """
    import pandas as pd
    from db_helper import get_connection
    from weather_helper import generate_weather_data, get_mapped_airports
    from validation_schemas import weather_schema
    from datetime import datetime as dt

    print("=" * 50)
    print("WEATHER DATA LOADER (Phase 2)")
    print("=" * 50)

    started_at = dt.now()

    # Step 1: Get airports that exist in our database AND have weather mappings
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT airport_code FROM airports")
            db_airports = set(row[0] for row in cur.fetchall())

    mapped_airports = set(get_mapped_airports())
    valid_airports = list(db_airports & mapped_airports)

    print(f"Airports in DB: {len(db_airports)}")
    print(f"Airports with weather mappings: {len(mapped_airports)}")
    print(f"Airports to load weather for: {len(valid_airports)}")

    if not valid_airports:
        print("No valid airports found!")
        return {'loaded': 0, 'rejected': 0}

    # Step 2: Get dates from our date_dim table
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT date_id FROM date_dim ORDER BY date_id")
            dates = [row[0] for row in cur.fetchall()]

    print(f"Dates in date_dim: {len(dates)}")

    if not dates:
        print("No dates found in date_dim!")
        return {'loaded': 0, 'rejected': 0}

    # Step 3: Check what weather data we already have
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT airport_code, observation_time FROM weather_observations
            """)
            existing = set((row[0], str(row[1])) for row in cur.fetchall())

    print(f"Existing weather records: {len(existing)}")

    # Step 4: Generate weather data
    weather_data = generate_weather_data(valid_airports, dates)

    # Convert to DataFrame for validation
    df = pd.DataFrame(weather_data)

    # Step 5: Validate with Pandera
    try:
        weather_schema.validate(df, lazy=True)
        print("Pandera validation passed")
    except Exception as e:
        print(f"Pandera validation warnings (continuing): {e}")

    # Step 6: Insert into database
    insert_query = """
        INSERT INTO weather_observations (
            airport_code, observation_date, observation_time,
            avg_temperature, max_temperature, min_temperature,
            avg_wind_speed, max_wind_speed, avg_visibility,
            precipitation, snow_depth, conditions
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (airport_code, observation_time) DO NOTHING
    """

    loaded = 0
    skipped = 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            for obs in weather_data:
                # Skip if already exists
                key = (obs['airport_code'], obs['observation_time'])
                if key in existing:
                    skipped += 1
                    continue

                cur.execute(insert_query, (
                    obs['airport_code'],
                    obs['observation_date'],
                    obs['observation_time'],
                    obs['avg_temperature'],
                    obs['max_temperature'],
                    obs['min_temperature'],
                    obs['avg_wind_speed'],
                    obs['max_wind_speed'],
                    obs['avg_visibility'],
                    obs['precipitation'],
                    obs['snow_depth'],
                    obs['conditions'],
                ))
                loaded += 1

    # Step 7: Log pipeline run
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_runs (file_name, source, rows_processed, rows_loaded,
                                           rows_rejected, status, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (file_name, source) DO UPDATE SET
                    rows_loaded = EXCLUDED.rows_loaded,
                    status = EXCLUDED.status,
                    completed_at = EXCLUDED.completed_at
            """, ('iem_hourly_weather', 'weather', len(weather_data), loaded,
                  0, 'completed', started_at, dt.now()))

    print(f"\n{'=' * 50}")
    print(f"Weather loading complete!")
    print(f"  Generated: {len(weather_data)}")
    print(f"  Loaded: {loaded}")
    print(f"  Skipped (already exists): {skipped}")
    print(f"{'=' * 50}")

    return {'loaded': loaded, 'skipped': skipped}


def run_quality_checks(**kwargs):
    """
    Post-load data quality checks.
    Verifies data integrity after all tables are loaded.
    """
    from db_helper import get_connection

    checks_passed = 0
    checks_failed = 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            # Check 1: Airports table not empty
            cur.execute("SELECT COUNT(*) FROM airports")
            count = cur.fetchone()[0]
            if count > 0:
                print(f"✓ Airports: {count} rows")
                checks_passed += 1
            else:
                print(f"✗ Airports: EMPTY")
                checks_failed += 1

            # Check 2: Carriers table not empty
            cur.execute("SELECT COUNT(*) FROM carriers")
            count = cur.fetchone()[0]
            if count > 0:
                print(f"✓ Carriers: {count} rows")
                checks_passed += 1
            else:
                print(f"✗ Carriers: EMPTY")
                checks_failed += 1

            # Check 3: Flights loaded
            cur.execute("SELECT COUNT(*) FROM flights")
            count = cur.fetchone()[0]
            if count > 0:
                print(f"✓ Flights: {count} rows")
                checks_passed += 1
            else:
                print(f"✗ Flights: EMPTY")
                checks_failed += 1

            # Check 4: No orphan flights (FK integrity)
            cur.execute("""
                SELECT COUNT(*) FROM flights f
                LEFT JOIN airports a ON f.origin_airport = a.airport_code
                WHERE a.airport_code IS NULL
            """)
            orphans = cur.fetchone()[0]
            if orphans == 0:
                print(f"✓ No orphan origin airports")
                checks_passed += 1
            else:
                print(f"✗ {orphans} flights with unknown origin airports")
                checks_failed += 1

            # Check 5: No orphan destination airports
            cur.execute("""
                SELECT COUNT(*) FROM flights f
                LEFT JOIN airports a ON f.dest_airport = a.airport_code
                WHERE a.airport_code IS NULL
            """)
            orphans = cur.fetchone()[0]
            if orphans == 0:
                print(f"✓ No orphan destination airports")
                checks_passed += 1
            else:
                print(f"✗ {orphans} flights with unknown dest airports")
                checks_failed += 1

            # Check 6: Delay values in reasonable range
            cur.execute("""
                SELECT COUNT(*) FROM flights
                WHERE arr_delay IS NOT NULL AND (arr_delay < -150 OR arr_delay > 5000)
            """)
            bad_delays = cur.fetchone()[0]
            if bad_delays == 0:
                print(f"✓ All delay values in valid range")
                checks_passed += 1
            else:
                print(f"✗ {bad_delays} flights with out-of-range delays")
                checks_failed += 1

            # Check 7: Rejection rate
            cur.execute("""
                SELECT rows_loaded, rows_rejected FROM pipeline_runs
                WHERE source = 'flights'
                ORDER BY completed_at DESC LIMIT 1
            """)
            result = cur.fetchone()
            if result:
                loaded, rejected = result
                if loaded > 0:
                    reject_rate = rejected / (loaded + rejected) * 100
                    if reject_rate < 5:
                        print(f"✓ Rejection rate: {reject_rate:.2f}% ({rejected}/{loaded + rejected})")
                        checks_passed += 1
                    else:
                        print(f"✗ High rejection rate: {reject_rate:.2f}%")
                        checks_failed += 1

            # Check 8: Weather data loaded (Phase 2)
            cur.execute("SELECT COUNT(*) FROM weather_observations")
            count = cur.fetchone()[0]
            if count > 0:
                print(f"✓ Weather observations: {count} rows")
                checks_passed += 1
            else:
                print(f"✗ Weather observations: EMPTY")
                checks_failed += 1

            # Check 9: Weather covers our airports
            cur.execute("""
                SELECT COUNT(DISTINCT w.airport_code)
                FROM weather_observations w
                JOIN airports a ON w.airport_code = a.airport_code
            """)
            airports_with_weather = cur.fetchone()[0]
            if airports_with_weather > 0:
                print(f"✓ Airports with weather data: {airports_with_weather}")
                checks_passed += 1
            else:
                print(f"✗ No airports have weather data")
                checks_failed += 1

            # Check 10: Weather covers our flight dates
            cur.execute("""
                SELECT COUNT(DISTINCT w.observation_date)
                FROM weather_observations w
                JOIN date_dim d ON w.observation_date = d.date_id
            """)
            dates_with_weather = cur.fetchone()[0]
            if dates_with_weather > 0:
                print(f"✓ Dates with weather data: {dates_with_weather}")
                checks_passed += 1
            else:
                print(f"✗ No dates have weather data")
                checks_failed += 1

            # Summary stats
            print(f"\n{'=' * 50}")
            print(f"Quality checks: {checks_passed} passed, {checks_failed} failed")

            # Bonus: print some summary stats
            cur.execute("""
                SELECT
                    COUNT(*) as total_flights,
                    COUNT(DISTINCT carrier_code) as carriers,
                    COUNT(DISTINCT origin_airport) as origins,
                    COUNT(DISTINCT dest_airport) as dests,
                    AVG(arr_delay) as avg_delay,
                    SUM(CASE WHEN cancelled THEN 1 ELSE 0 END) as cancellations
                FROM flights
            """)
            stats = cur.fetchone()
            if stats:
                print(f"\nDataset Summary:")
                print(f"  Flights: {stats[0]:,}")
                print(f"  Carriers: {stats[1]}")
                print(f"  Origin airports: {stats[2]}")
                print(f"  Dest airports: {stats[3]}")
                print(f"  Avg arrival delay: {stats[4]:.1f} min" if stats[4] else "  Avg arrival delay: N/A")
                print(f"  Cancellations: {stats[5]:,}")

            # Weather summary
            cur.execute("""
                SELECT
                    COUNT(*) as total_obs,
                    COUNT(DISTINCT airport_code) as airports,
                    AVG(avg_temperature) as avg_temp,
                    SUM(CASE WHEN precipitation > 0 THEN 1 ELSE 0 END) as precip_days,
                    SUM(CASE WHEN snow_depth > 0 THEN 1 ELSE 0 END) as snow_days
                FROM weather_observations
            """)
            weather_stats = cur.fetchone()
            if weather_stats and weather_stats[0] > 0:
                print(f"\nWeather Summary:")
                print(f"  Observations: {weather_stats[0]:,}")
                print(f"  Airports covered: {weather_stats[1]}")
                print(f"  Avg temperature: {weather_stats[2]:.1f}°F" if weather_stats[2] else "  Avg temperature: N/A")
                print(f"  Days with precipitation: {weather_stats[3]:,}")
                print(f"  Days with snow: {weather_stats[4]:,}")

    if checks_failed > 0:
        raise Exception(f"{checks_failed} quality checks failed!")

    return {'passed': checks_passed, 'failed': checks_failed}


# =============================================
# DAG DEFINITION
# =============================================

with DAG(
    dag_id='flight_data_pipeline',
    default_args=default_args,
    description='ETL pipeline: BTS flight data + OpenFlights airports → PostgreSQL',
    schedule=None,  # Manual trigger (no auto-schedule)
    catchup=False,
    tags=['flights', 'etl', 'pipeline'],
) as dag:

    # Stage 1: Upload raw files to S3
    upload_task = PythonOperator(
        task_id='upload_raw_to_s3',
        python_callable=upload_raw_files_to_s3,
    )

    # Stage 2: Load dimensions (airports first — needed for FK validation)
    airports_task = PythonOperator(
        task_id='load_airports',
        python_callable=load_airports,
    )

    # Stage 3: Extract carriers + generate dates (parallel — independent of each other)
    carriers_task = PythonOperator(
        task_id='extract_carriers',
        python_callable=extract_carriers,
    )

    dates_task = PythonOperator(
        task_id='generate_date_dim',
        python_callable=generate_date_dim,
    )

    # Stage 4: Load flights (depends on ALL dimensions being ready)
    flights_task = PythonOperator(
        task_id='load_flights',
        python_callable=load_flights,
    )

    # Stage 5: Load weather data (Phase 2 — depends on airports + dates)
    weather_task = PythonOperator(
        task_id='load_weather',
        python_callable=load_weather,
    )

    # Stage 6: Quality checks
    quality_task = PythonOperator(
        task_id='quality_checks',
        python_callable=run_quality_checks,
    )

    # =============================================
    # TASK DEPENDENCIES (the DAG graph)
    # =============================================
    #
    #  upload_raw_to_s3
    #         |
    #    load_airports
    #       /    \
    # extract_   generate_
    # carriers   date_dim
    #       \    /
    #    load_flights
    #         |
    #    load_weather   <-- Phase 2: Weather data
    #         |
    #   quality_checks

    upload_task >> airports_task
    airports_task >> [carriers_task, dates_task]
    [carriers_task, dates_task] >> flights_task
    flights_task >> weather_task
    weather_task >> quality_task
