# Flight Data ETL Pipeline

End-to-end data pipeline that ingests US flight performance data from the Bureau of Transportation Statistics (BTS) and airport reference data from OpenFlights, transforms and validates the data, and loads it into a normalized PostgreSQL data warehouse.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│  BTS CSV     │────▶│   MinIO     │────▶│   Airflow    │────▶│ PostgreSQL  │
│  (flights)   │     │   (S3)      │     │   (DAGs)     │     │  (warehouse)│
├─────────────┤     │             │     │              │     │             │
│ OpenFlights  │────▶│  Raw data   │     │  Extract     │     │ carriers    │
│  (airports)  │     │  landing    │     │  Validate    │     │ airports    │
└─────────────┘     │  zone       │     │  Transform   │     │ date_dim    │
                    └─────────────┘     │  Load        │     │ flights     │
                                        └──────────────┘     └─────────────┘
```

## Tech Stack

| Component | Tool | Purpose |
|-----------|------|---------|
| Orchestration | Apache Airflow | DAG-based workflow scheduling, retries, monitoring |
| Storage | MinIO (S3-compatible) | Raw data landing zone, same API as AWS S3 |
| Database | PostgreSQL | Normalized data warehouse with star schema |
| Validation | Pandera | Pre-load schema validation (types, ranges, constraints) |
| Quality | Great Expectations | Post-load data quality checks (FK integrity, stats) |
| Containers | Docker Compose | Local dev environment matching production topology |

## Data Sources

1. **BTS On-Time Performance** — Every US domestic flight with departure/arrival times, delays, cancellations
2. **OpenFlights** — Airport reference data with IATA codes, coordinates, timezones

## Pipeline Stages

```
upload_raw_to_s3 → load_airports → [extract_carriers, generate_date_dim] → load_flights → quality_checks
```

1. **Upload**: Raw files → MinIO (S3 landing zone)
2. **Airports**: Load dimension table from OpenFlights
3. **Carriers + Dates**: Extract carriers from flight data, generate date dimension (parallel)
4. **Flights**: Load fact table with FK validation, chunked processing, rejected record logging
5. **Quality**: Post-load integrity checks, summary statistics

## Quick Start

```bash
# 1. Clone and enter project
cd flight-pipeline

# 2. Place raw data files
cp your_flights.csv data/raw/flights.csv
cp airports.dat data/raw/airports.dat

# 3. Start all services
docker-compose up -d

# 4. Wait for init to complete (~60 seconds)
docker-compose logs -f airflow-init

# 5. Open Airflow UI
open http://localhost:8080
# Login: admin / admin

# 6. Trigger the pipeline
# Click on 'flight_data_pipeline' → Trigger DAG

# 7. Monitor progress in Airflow UI
# Or check PostgreSQL directly:
psql -h localhost -p 5433 -U flights_user -d flights
```

## Project Structure

```
flight-pipeline/
├── dags/
│   └── flight_pipeline_dag.py    # Main DAG definition
├── scripts/
│   ├── init_db.sql               # Database schema
│   ├── db_helper.py              # PostgreSQL connection helper
│   ├── s3_helper.py              # MinIO/S3 helper
│   └── validation_schemas.py     # Pandera validation schemas
├── data/
│   └── raw/                      # Place raw CSV files here
├── great_expectations/           # GE configuration (Phase 2)
├── docker-compose.yml
├── requirements.txt
├── .env
└── README.md
```

## Key Design Decisions

- **Idempotent loads**: All INSERT statements use ON CONFLICT — safe to rerun without duplicates
- **FK validation**: Flights referencing unknown airports/carriers go to rejected_records table
- **Chunked processing**: 50K rows per batch to limit memory usage
- **Audit trail**: pipeline_runs and rejected_records tables track every load
- **Separated concerns**: Airflow metadata DB separate from project DB
