# NYC Smart Parking IoT Data Generator

A comprehensive IoT data simulation platform that generates realistic parking facility events across **50 NYC parking facilities** in **6 districts**. The system supports both real-time streaming via Snowpipe and historical batch data generation to Snowflake Iceberg tables.

## Overview

This project simulates a smart parking system for New York City, generating:
- **CAR_IN / CAR_OUT events** with realistic traffic patterns
- **Parking sessions** with calculated durations and costs
- **State-based license plates** reflecting real NYC-area traffic distribution
- **Borough-specific traffic patterns** (weekday/weekend, peak hours, seasonal)

## Key Features

| Feature | Description |
|---------|-------------|
| **50 Parking Facilities** | Across Manhattan, Brooklyn, Queens, Bronx, Staten Island, and Airports |
| **Realistic Traffic Patterns** | Borough-specific weekday/weekend multipliers and peak hours |
| **State License Plates** | NY (60%), NJ (15%), CT (8%), PA (7%), MA (3%), others (7%) |
| **Snowpipe Streaming** | Real-time sub-second latency ingestion to Iceberg tables |
| **Historical Backfill** | Batch generation for any date range with consistent patterns |
| **Iceberg Tables** | Native Snowflake Iceberg format with Delta metadata |

## Project Structure

```
DataGenerator/
├── app.py                      # Real-time Streamlit dashboard & event generator
├── generate_historical_data.py # Historical batch data generator (CLI)
├── snowflake_setup.sql         # Snowflake DDL (database, tables, pipes)
├── requirements.txt            # Python dependencies
├── Dockerfile                  # Container build definition
├── docker-compose.yml          # Docker orchestration
├── data/
│   ├── parking_iot.db          # Local SQLite state (auto-generated)
│   └── profile.json            # Snowpipe Streaming auth (auto-generated)
└── keys/
    └── rsa_key.p8              # RSA private key for Snowflake auth
```

## Quick Start

### Prerequisites

1. **Snowflake Account** with Iceberg table support
2. **RSA Key Pair** configured for your Snowflake user
3. **Python 3.8+** or Docker

### Option 1: Docker Compose (Recommended)

```bash
cd DataGenerator
cp .env.example .env  # Edit with your credentials
docker-compose up --build
```

### Option 2: Local Python

```bash
cd DataGenerator
pip install -r requirements.txt
cp .env.example .env  # Edit with your credentials

# Real-time dashboard
streamlit run app.py --server.port=8501

# Historical data generation
python generate_historical_data.py --start-date 2025-01-01 --end-date 2025-12-31
```

## Snowflake Setup

Run `snowflake_setup.sql` to create the required objects:

```sql
-- Creates:
-- Database: PARKING_IOT
-- Schema: RAW_DATA
-- Tables: PARKING_EVENTS, PARKING_SESSIONS, PARKING_FACILITIES (Iceberg)
-- Pipes: PARKING_EVENTS_PIPE, PARKING_SESSIONS_PIPE
```

### Database Schema

#### PARKING_EVENTS
| Column | Type | Description |
|--------|------|-------------|
| `event_id` | STRING | Unique event identifier (UUID) |
| `event_type` | STRING | `CAR_IN` or `CAR_OUT` |
| `session_id` | STRING | Links entry to exit event |
| `facility_id` | INTEGER | Facility identifier (1-50) |
| `facility_name` | STRING | Human-readable facility name |
| `district` | STRING | NYC borough/area |
| `license_plate` | STRING | Vehicle plate number |
| `license_plate_state` | STRING | State of registration (NY, NJ, CT, etc.) |
| `event_time` | TIMESTAMP_LTZ | When the event occurred |
| `available_spots_after` | INTEGER | Spots available after this event |
| `parking_duration_hours` | FLOAT | Duration in hours (CAR_OUT only) |
| `cost` | FLOAT | Parking cost (CAR_OUT only) |
| `traffic_pattern` | STRING | Pattern tag (e.g., `Manhattan\|weekday_busy+peak_entry_hour\|mult:1.4x`) |

#### PARKING_SESSIONS
| Column | Type | Description |
|--------|------|-------------|
| `session_id` | STRING | Unique session identifier (UUID) |
| `license_plate` | STRING | Vehicle plate number |
| `license_plate_state` | STRING | State of registration |
| `facility_id` | INTEGER | Facility identifier |
| `in_time` | TIMESTAMP_LTZ | Entry timestamp |
| `out_time` | TIMESTAMP_LTZ | Exit timestamp (NULL while active) |
| `actual_duration_hours` | FLOAT | Parking duration |
| `rate_per_hour` | FLOAT | Hourly rate charged |
| `cost` | FLOAT | Total parking cost |
| `status` | STRING | `active` or `completed` |

## NYC Parking Facilities

### Districts & Facility Distribution

| District | Facility IDs | Count | Characteristics |
|----------|-------------|-------|-----------------|
| **Manhattan** | 1-15 | 15 | Highest rates ($15-$40/hr), busiest weekdays |
| **Brooklyn** | 16-25 | 10 | Moderate rates ($4-$18/hr), busy weekends |
| **Queens** | 26-35 | 10 | Commuter-focused ($5-$9/hr) |
| **Bronx** | 36-40 | 5 | Event-driven (Yankee Stadium, Zoo) |
| **Staten Island** | 41-45 | 5 | Ferry commuter parking, low rates |
| **Airport** | 46-50 | 5 | JFK, LaGuardia, Newark - multi-day stays |

### Sample Facilities

| ID | Name | Spots | Rate/hr | District |
|----|------|-------|---------|----------|
| 1 | Times Square 44th St | 200 | $35.00 | Manhattan |
| 4 | Financial District Wall St | 180 | $40.00 | Manhattan |
| 17 | DUMBO Water St | 150 | $18.00 | Brooklyn |
| 26 | Long Island City Court Square | 476 | $9.00 | Queens |
| 37 | Yankee Stadium Lot A | 600 | $25.00 | Bronx |
| 46 | JFK Terminal 1 Garage | 500 | $18.00 | Airport |

## Traffic Patterns

### Borough-Specific Behavior

```python
DISTRICT_PATTERNS = {
    "Manhattan": {
        "weekday_mult": 1.4,      # Very busy weekdays
        "weekend_mult": 1.2,      # Still busy (tourists)
        "peak_entry_hours": [7, 8, 9, 10, 11],
        "peak_exit_hours": [17, 18, 19, 20, 21],
        "avg_stay_hours": 4,
    },
    "Brooklyn": {
        "weekday_mult": 1.1,
        "weekend_mult": 1.4,      # Busy weekends (brunch, shopping)
        "avg_stay_hours": 3,
    },
    "Airport": {
        "weekday_mult": 1.4,      # Business travel
        "weekend_mult": 0.8,
        "avg_stay_hours": 72,     # Multi-day trips
    },
    # ... more patterns
}
```

### License Plate Distribution

Reflects real NYC-area traffic:

| State | Percentage | Format Example |
|-------|------------|----------------|
| NY | 60% | `ABC-1234` |
| NJ | 15% | `D12-ABC` |
| CT | 8% | `AB-12345` |
| PA | 7% | `ABC-1234` |
| MA | 3% | `1AB-C23` |
| FL | 2% | `ABC-1234` |
| Others | 5% | Various |

## Historical Data Generator

### Usage

```bash
python generate_historical_data.py [OPTIONS]

Options:
  --start-date    Start date (YYYY-MM-DD), default: 2025-01-01
  --end-date      End date (YYYY-MM-DD), default: yesterday
  --batch-size    Records per batch insert, default: 1000
  --dry-run       Simulate without inserting to Snowflake
```

### Example: Generate Full Year

```bash
python -u generate_historical_data.py \
  --start-date 2025-04-19 \
  --end-date 2026-01-31 \
  --batch-size 1000
```

### Output

```
============================================================
Historical Data Generator for Smart Parking IoT
============================================================
Date Range: 2025-04-19 to 2026-01-31
Total Days: 288
Batch Size: 1000
============================================================

Connected to Snowflake: SFPSCOGS-CAPITALONE_AWS_1
2025-04-19 (Sat): Events=3,117, Pending=567, Total Events=3,117, Sessions=1,275
2025-04-20 (Sun): Events=3,230, Pending=945, Total Events=6,347, Sessions=2,701
...
2026-01-31 (Sat): Events=2,512, Pending=8,553, Total Events=823,605, Sessions=407,526

============================================================
Generation Complete!
============================================================
Total Events Generated: 823,605
Total Sessions Generated: 407,526
============================================================
```

### Data Volume Estimates

| Duration | Events | Sessions | Notes |
|----------|--------|----------|-------|
| 1 month | ~85K | ~42K | ~2,800/day |
| 6 months | ~500K | ~250K | |
| 1 year | ~1M | ~500K | |

## Real-Time Dashboard (Streamlit)

### Features

- **Live Metrics**: Total spots, available, occupied, cars in/out
- **District View**: Occupancy gauges per borough
- **Facility Details**: All 50 facilities with real-time status
- **Event Stream**: Live CAR_IN/CAR_OUT feed
- **Session History**: Duration, cost, and status tracking

### Controls

| Button | Action |
|--------|--------|
| **Start/Stop** | Toggle continuous event generation |
| **Burst Events** | Generate random burst across 10-20 facilities |
| **Reset DB** | Reinitialize local SQLite state |

Access at: http://localhost:8501

## Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `SNOWFLAKE_ACCOUNT` | Account identifier (e.g., `SFPSCOGS-CAPITALONE_AWS_1`) | Yes |
| `SNOWFLAKE_USER` | Username | Yes |
| `SNOWFLAKE_ROLE` | Role (default: `SYSADMIN`) | No |
| `SNOWFLAKE_WAREHOUSE` | Warehouse name | Yes |
| `SNOWFLAKE_DATABASE` | Database (default: `PARKING_IOT`) | No |
| `SNOWFLAKE_SCHEMA` | Schema (default: `RAW_DATA`) | No |
| `PRIVATE_KEY_PATH` | Path to RSA private key | Yes |
| `ENABLE_SNOWPIPE_STREAMING` | Enable real-time streaming | No |

### RSA Key Setup

```bash
# Generate key pair
openssl genrsa -out rsa_key.pem 2048
openssl pkcs8 -topk8 -inform PEM -outform PEM \
  -in rsa_key.pem -out keys/rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.pem -pubout -out rsa_key.pub

# Register with Snowflake
ALTER USER <your_user> SET RSA_PUBLIC_KEY='<public_key_contents>';
```

## Sample Queries

### Event Analysis

```sql
-- Events by type and district
SELECT 
    district,
    event_type,
    COUNT(*) as count,
    AVG(parking_duration_hours) as avg_duration
FROM PARKING_IOT.RAW_DATA.PARKING_EVENTS
GROUP BY district, event_type
ORDER BY district, event_type;

-- Traffic patterns analysis
SELECT 
    SPLIT_PART(traffic_pattern, '|', 1) as district,
    SPLIT_PART(traffic_pattern, '|', 2) as pattern_tags,
    COUNT(*) as event_count
FROM PARKING_IOT.RAW_DATA.PARKING_EVENTS
GROUP BY 1, 2
ORDER BY event_count DESC;
```

### Session Analysis

```sql
-- Revenue by district
SELECT 
    district,
    COUNT(*) as sessions,
    SUM(cost) as total_revenue,
    AVG(actual_duration_hours) as avg_duration,
    AVG(cost) as avg_cost
FROM PARKING_IOT.RAW_DATA.PARKING_SESSIONS
WHERE status = 'completed'
GROUP BY district
ORDER BY total_revenue DESC;

-- License plate state distribution
SELECT 
    license_plate_state,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pct
FROM PARKING_IOT.RAW_DATA.PARKING_SESSIONS
GROUP BY license_plate_state
ORDER BY count DESC;
```

### Facility Utilization

```sql
-- Current occupancy by facility
SELECT 
    f.name,
    f.district,
    f.total_spots,
    f.total_spots - COALESCE(active.count, 0) as available,
    ROUND((COALESCE(active.count, 0) / f.total_spots) * 100, 1) as occupancy_pct
FROM PARKING_IOT.RAW_DATA.PARKING_FACILITIES f
LEFT JOIN (
    SELECT facility_id, COUNT(*) as count
    FROM PARKING_IOT.RAW_DATA.PARKING_SESSIONS
    WHERE status = 'active'
    GROUP BY facility_id
) active ON f.facility_id = active.facility_id
ORDER BY occupancy_pct DESC;
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Data Generation                               │
├─────────────────────────────┬───────────────────────────────────────┤
│   Real-Time (app.py)        │   Historical (generate_historical)    │
│   - Streamlit Dashboard     │   - CLI batch generator               │
│   - Continuous simulation   │   - Date range backfill               │
│   - Interactive controls    │   - Progress tracking                 │
└──────────────┬──────────────┴──────────────────┬────────────────────┘
               │                                  │
               ▼                                  ▼
┌──────────────────────────────┐  ┌──────────────────────────────────┐
│   Snowpipe Streaming         │  │   Direct Snowflake Insert        │
│   - Sub-second latency       │  │   - Batch INSERT (1000 rows)     │
│   - PARKING_EVENTS_PIPE      │  │   - Transactional commits        │
│   - PARKING_SESSIONS_PIPE    │  │   - Progress per day             │
└──────────────┬───────────────┘  └──────────────┬───────────────────┘
               │                                  │
               └──────────────┬───────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        Snowflake                                     │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │                    PARKING_IOT Database                        │ │
│  │  ┌──────────────────────────────────────────────────────────┐  │ │
│  │  │                   RAW_DATA Schema                         │  │ │
│  │  │                                                           │  │ │
│  │  │  ┌─────────────────┐  ┌─────────────────┐                │  │ │
│  │  │  │ PARKING_EVENTS  │  │ PARKING_SESSIONS│                │  │ │
│  │  │  │ (Iceberg Table) │  │ (Iceberg Table) │                │  │ │
│  │  │  │                 │  │                 │                │  │ │
│  │  │  │ - 823K+ events  │  │ - 407K+ sessions│                │  │ │
│  │  │  │ - Delta metadata│  │ - Delta metadata│                │  │ │
│  │  │  └─────────────────┘  └─────────────────┘                │  │ │
│  │  │                                                           │  │ │
│  │  │  ┌───────────────────┐                                   │  │ │
│  │  │  │ PARKING_FACILITIES│                                   │  │ │
│  │  │  │ (Iceberg Table)   │                                   │  │ │
│  │  │  │ - 50 facilities   │                                   │  │ │
│  │  │  └───────────────────┘                                   │  │ │
│  │  └──────────────────────────────────────────────────────────┘  │ │
│  └────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

## Dependencies

```
streamlit>=1.28.0
pandas>=2.0.0
plotly>=5.18.0
python-dotenv>=1.0.0
snowflake-connector-python[pandas]>=3.6.0
pyarrow>=14.0.0
snowpipe-streaming>=0.1.0
cryptography
```

## Current Data Status

As of the last historical generation run:

| Metric | Value |
|--------|-------|
| Date Range | Apr 19, 2025 - Jan 31, 2026 |
| Total Days | 288 |
| Total Events | 823,605 |
| Total Sessions | 407,526 |
| Avg Events/Day | ~2,860 |
| Avg Sessions/Day | ~1,415 |

## License

Internal use only.
