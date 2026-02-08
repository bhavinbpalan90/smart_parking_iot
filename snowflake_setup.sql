-- ============================================================
-- Snowflake Iceberg Tables Setup for Parking IoT
-- with Snowpipe Streaming Support
-- ============================================================
-- Prerequisites:
-- 1. Create an external volume pointing to your cloud storage
-- 2. Update EXTERNAL_VOLUME_NAME below with your volume name
-- ============================================================

USE ROLE SYSADMIN;

-- ============================================================
-- STEP 1: Create Database and Schema
-- ============================================================

CREATE DATABASE IF NOT EXISTS PARKING_IOT;
USE DATABASE PARKING_IOT;
CREATE SCHEMA IF NOT EXISTS RAW_DATA;
USE SCHEMA RAW_DATA;

ALTER SCHEMA RAW_DATA 
SET 
CATALOG = 'SNOWFLAKE',
EXTERNAL_VOLUME = 'EV_S3_SNOWFLAKEDEMOS',
BASE_LOCATION_PREFIX = 'iot_application/smart_parking/nyc/raw_data';


-- ============================================================
-- STEP 2: Create Iceberg Tables with Snowpipe Streaming columns
-- ============================================================

-- Parking Events Iceberg Table
-- Stores CAR_IN and CAR_OUT events with traffic pattern tags
CREATE OR REPLACE ICEBERG TABLE PARKING_EVENTS (
    event_id STRING,
    event_type STRING,                -- CAR_IN or CAR_OUT
    session_id STRING,
    facility_id INTEGER,
    facility_name STRING,
    district STRING,
    license_plate STRING,
    event_time TIMESTAMP_LTZ,
    available_spots_after INTEGER,
    parking_duration_hours FLOAT,     -- NULL for CAR_IN, actual duration for CAR_OUT
    cost FLOAT,                       -- NULL for CAR_IN, calculated cost for CAR_OUT
    traffic_pattern STRING,           -- Traffic simulation pattern tag (e.g., "Downtown|weekday_busy+peak_entry_hour|mult:1.3x")
    license_plate_state STRING,
    ingested_at TIMESTAMP_LTZ,
    -- Snowpipe Streaming metadata columns
    row_timestamp TIMESTAMP_LTZ,
    offset_id INTEGER
)
GENERATE_DELTA_METADATA = TRUE;

-- Parking Sessions Iceberg Table
-- Stores session lifecycle (active → completed)
CREATE OR REPLACE ICEBERG TABLE PARKING_SESSIONS (
    session_id STRING,
    license_plate STRING,
    facility_id INTEGER,
    facility_name STRING,
    district STRING,
    in_time TIMESTAMP_LTZ,
    out_time TIMESTAMP_LTZ,           -- NULL while active
    actual_duration_hours FLOAT,      -- NULL while active, calculated at exit
    rate_per_hour FLOAT,
    cost FLOAT,                       -- NULL while active, calculated at exit
    status STRING,                    -- 'active' or 'completed'
    license_plate_state STRING,
    ingested_at TIMESTAMP_LTZ,
    -- Snowpipe Streaming metadata columns
    row_timestamp TIMESTAMP_LTZ,
    offset_id INTEGER
)
GENERATE_DELTA_METADATA = TRUE;

-- Parking Facilities Iceberg Table
-- Reference data for the 50 parking facilities
CREATE OR REPLACE ICEBERG TABLE PARKING_FACILITIES (
    facility_id INTEGER,
    name STRING,
    district STRING,
    total_spots INTEGER,
    rate_per_hour FLOAT,
    created_at TIMESTAMP_LTZ
)
GENERATE_DELTA_METADATA = TRUE;

-- ============================================================
-- STEP 3: Create Snowpipe Streaming Pipes
-- These pipes enable real-time streaming ingestion
-- ============================================================

-- Create Snowpipe Streaming pipe for PARKING_EVENTS table
CREATE OR REPLACE PIPE PARKING_EVENTS_PIPE 
AS COPY INTO PARKING_EVENTS
FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
CLUSTER_AT_INGEST_TIME = TRUE;

-- Create Snowpipe Streaming pipe for PARKING_SESSIONS table
CREATE OR REPLACE PIPE PARKING_SESSIONS_PIPE 
AS COPY INTO PARKING_SESSIONS
FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
CLUSTER_AT_INGEST_TIME = TRUE;

-- ============================================================
-- STEP 4: Verify Tables and Pipes Created
-- ============================================================

SHOW ICEBERG TABLES IN SCHEMA PARKING_IOT.RAW_DATA;
SHOW PIPES IN SCHEMA PARKING_IOT.RAW_DATA;

-- ============================================================
-- Sample Queries
-- ============================================================

-- Check event counts by type
-- SELECT event_type, COUNT(*) as count FROM PARKING_EVENTS GROUP BY event_type;

-- Analyze traffic patterns
-- SELECT 
--     traffic_pattern,
--     COUNT(*) as event_count,
--     AVG(parking_duration_hours) as avg_duration
-- FROM PARKING_EVENTS 
-- WHERE event_type = 'CAR_OUT'
-- GROUP BY traffic_pattern
-- ORDER BY event_count DESC;

-- Check events by district and pattern
-- SELECT 
--     district,
--     SPLIT_PART(traffic_pattern, '|', 2) as pattern_tags,
--     COUNT(*) as count
-- FROM PARKING_EVENTS
-- GROUP BY district, pattern_tags
-- ORDER BY district, count DESC;

-- Active sessions (cars currently parked)
-- SELECT * FROM PARKING_SESSIONS WHERE status = 'active';

-- Completed sessions with duration analysis
-- SELECT 
--     district,
--     AVG(actual_duration_hours) as avg_duration,
--     MIN(actual_duration_hours) as min_duration,
--     MAX(actual_duration_hours) as max_duration,
--     COUNT(*) as total_sessions
-- FROM PARKING_SESSIONS 
-- WHERE status = 'completed'
-- GROUP BY district;

-- Check streaming latency
-- SELECT 
--     AVG(TIMESTAMPDIFF(SECOND, event_time, ingested_at)) as avg_latency_seconds,
--     MAX(TIMESTAMPDIFF(SECOND, event_time, ingested_at)) as max_latency_seconds
-- FROM PARKING_EVENTS
-- WHERE ingested_at > DATEADD(HOUR, -1, CURRENT_TIMESTAMP());
