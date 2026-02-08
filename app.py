import streamlit as st
import pandas as pd
import random
import string
import uuid
import time
import os
import math
import base64
import logging
import threading
import subprocess
import json
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Snowflake configuration from .env
SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT", ""),
    "user": os.getenv("SNOWFLAKE_USER", ""),
    "role": os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", ""),
    "database": os.getenv("SNOWFLAKE_DATABASE", "PARKING_IOT"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "RAW_DATA"),
}

# Key pair authentication settings
PRIVATE_KEY_PATH = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "")
PRIVATE_KEY_BASE64 = os.getenv("SNOWFLAKE_PRIVATE_KEY_BASE64", "")
PRIVATE_KEY_PASSPHRASE = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "")

ENABLE_SNOWPIPE = os.getenv("ENABLE_SNOWPIPE_STREAMING", "false").lower() == "true"

# ============== NYC Parking Facilities ==============
# 50 Parking Facilities across NYC's 5 boroughs + airports

FACILITY_CONFIGS = [
    # Manhattan (ID 1-15) - Highest rates in the city
    {"id": 1, "name": "Times Square 44th St", "spots": 200, "rate": 35.00, "base_rate": 0.95, "peak_hours": [10,11,12,17,18,19,20]},
    {"id": 2, "name": "Penn Station 33rd St", "spots": 300, "rate": 28.00, "base_rate": 0.9, "peak_hours": [7,8,9,17,18,19]},
    {"id": 3, "name": "Grand Central 42nd St", "spots": 250, "rate": 32.00, "base_rate": 0.92, "peak_hours": [7,8,9,17,18,19]},
    {"id": 4, "name": "Financial District Wall St", "spots": 180, "rate": 40.00, "base_rate": 0.95, "peak_hours": [7,8,9,17,18]},
    {"id": 5, "name": "Midtown 5th Ave", "spots": 150, "rate": 38.00, "base_rate": 0.88, "peak_hours": [10,11,12,13,14,15]},
    {"id": 6, "name": "Chelsea Market 15th St", "spots": 120, "rate": 25.00, "base_rate": 0.75, "peak_hours": [11,12,13,18,19,20]},
    {"id": 7, "name": "Upper East Side 86th St", "spots": 100, "rate": 30.00, "base_rate": 0.7, "peak_hours": [9,10,11,14,15]},
    {"id": 8, "name": "Upper West Side 72nd St", "spots": 100, "rate": 28.00, "base_rate": 0.7, "peak_hours": [9,10,11,14,15]},
    {"id": 9, "name": "SoHo Broadway", "spots": 80, "rate": 35.00, "base_rate": 0.8, "peak_hours": [11,12,13,14,15,16]},
    {"id": 10, "name": "Tribeca Greenwich St", "spots": 90, "rate": 32.00, "base_rate": 0.75, "peak_hours": [11,12,13,18,19]},
    {"id": 11, "name": "East Village 2nd Ave", "spots": 70, "rate": 22.00, "base_rate": 0.65, "peak_hours": [18,19,20,21,22]},
    {"id": 12, "name": "West Village 7th Ave", "spots": 60, "rate": 25.00, "base_rate": 0.7, "peak_hours": [18,19,20,21,22]},
    {"id": 13, "name": "Harlem 125th St", "spots": 150, "rate": 15.00, "base_rate": 0.6, "peak_hours": [9,10,11,17,18]},
    {"id": 14, "name": "Lincoln Center 65th St", "spots": 180, "rate": 30.00, "base_rate": 0.5, "peak_hours": [18,19,20,21]},
    {"id": 15, "name": "Columbus Circle", "spots": 140, "rate": 35.00, "base_rate": 0.85, "peak_hours": [10,11,12,17,18,19]},
    # Brooklyn (ID 16-25)
    {"id": 16, "name": "Downtown Brooklyn Borough Hall", "spots": 205, "rate": 9.00, "base_rate": 0.8, "peak_hours": [8,9,10,17,18]},
    {"id": 17, "name": "DUMBO Water St", "spots": 150, "rate": 18.00, "base_rate": 0.75, "peak_hours": [10,11,12,18,19,20]},
    {"id": 18, "name": "Williamsburg Bedford Ave", "spots": 120, "rate": 15.00, "base_rate": 0.7, "peak_hours": [11,12,18,19,20,21]},
    {"id": 19, "name": "Bay Ridge 5th Ave", "spots": 205, "rate": 4.00, "base_rate": 0.65, "peak_hours": [10,11,12,17,18]},
    {"id": 20, "name": "Park Slope 7th Ave", "spots": 100, "rate": 12.00, "base_rate": 0.7, "peak_hours": [9,10,11,17,18,19]},
    {"id": 21, "name": "Brooklyn Heights Montague St", "spots": 90, "rate": 14.00, "base_rate": 0.7, "peak_hours": [8,9,17,18,19]},
    {"id": 22, "name": "Coney Island Boardwalk", "spots": 300, "rate": 8.00, "base_rate": 0.5, "peak_hours": [10,11,12,13,14,15]},
    {"id": 23, "name": "Bensonhurst 86th St", "spots": 120, "rate": 5.00, "base_rate": 0.6, "peak_hours": [10,11,17,18]},
    {"id": 24, "name": "Flatbush Junction", "spots": 180, "rate": 6.00, "base_rate": 0.65, "peak_hours": [9,10,11,17,18,19]},
    {"id": 25, "name": "Brighton Beach", "spots": 150, "rate": 7.00, "base_rate": 0.55, "peak_hours": [10,11,12,13,14]},
    # Queens (ID 26-35)
    {"id": 26, "name": "Long Island City Court Square", "spots": 476, "rate": 9.00, "base_rate": 0.8, "peak_hours": [7,8,9,17,18,19]},
    {"id": 27, "name": "Flushing Main St", "spots": 200, "rate": 8.00, "base_rate": 0.85, "peak_hours": [10,11,12,13,17,18,19]},
    {"id": 28, "name": "Jamaica Station", "spots": 250, "rate": 6.00, "base_rate": 0.75, "peak_hours": [6,7,8,17,18,19]},
    {"id": 29, "name": "Astoria Steinway St", "spots": 46, "rate": 7.00, "base_rate": 0.7, "peak_hours": [10,11,18,19,20]},
    {"id": 30, "name": "Forest Hills 71st Ave", "spots": 150, "rate": 8.00, "base_rate": 0.65, "peak_hours": [9,10,11,17,18]},
    {"id": 31, "name": "Bayside Bell Blvd", "spots": 120, "rate": 6.00, "base_rate": 0.6, "peak_hours": [10,11,12,17,18]},
    {"id": 32, "name": "Queens Center Mall", "spots": 400, "rate": 5.00, "base_rate": 0.85, "peak_hours": [11,12,13,14,15,16,17]},
    {"id": 33, "name": "Rego Park 63rd Dr", "spots": 180, "rate": 5.00, "base_rate": 0.7, "peak_hours": [10,11,12,17,18]},
    {"id": 34, "name": "Jackson Heights 37th Ave", "spots": 100, "rate": 6.00, "base_rate": 0.75, "peak_hours": [11,12,13,18,19,20]},
    {"id": 35, "name": "Queens Family Court", "spots": 100, "rate": 5.00, "base_rate": 0.6, "peak_hours": [8,9,10,14,15,16]},
    # Bronx (ID 36-40)
    {"id": 36, "name": "Jerome-190th St Garage", "spots": 416, "rate": 7.00, "base_rate": 0.7, "peak_hours": [8,9,10,17,18]},
    {"id": 37, "name": "Yankee Stadium Lot A", "spots": 600, "rate": 25.00, "base_rate": 0.3, "peak_hours": [17,18,19,20]},
    {"id": 38, "name": "Fordham Road Plaza", "spots": 150, "rate": 6.00, "base_rate": 0.75, "peak_hours": [10,11,12,17,18,19]},
    {"id": 39, "name": "Bronx Zoo Southern Blvd", "spots": 300, "rate": 18.00, "base_rate": 0.5, "peak_hours": [9,10,11,12,13,14]},
    {"id": 40, "name": "Bronxdale Municipal", "spots": 100, "rate": 5.00, "base_rate": 0.6, "peak_hours": [8,9,17,18]},
    # Staten Island (ID 41-45)
    {"id": 41, "name": "St George Ferry Terminal", "spots": 200, "rate": 5.00, "base_rate": 0.75, "peak_hours": [6,7,8,17,18,19]},
    {"id": 42, "name": "Staten Island Mall", "spots": 400, "rate": 0.00, "base_rate": 0.8, "peak_hours": [11,12,13,14,15,16,17]},
    {"id": 43, "name": "Staten Island Courthouse", "spots": 150, "rate": 5.00, "base_rate": 0.6, "peak_hours": [8,9,10,14,15]},
    {"id": 44, "name": "Great Kills Municipal", "spots": 100, "rate": 3.00, "base_rate": 0.5, "peak_hours": [10,11,12,13,14]},
    {"id": 45, "name": "New Dorp Municipal", "spots": 80, "rate": 3.00, "base_rate": 0.5, "peak_hours": [10,11,12,17,18]},
    # Airport District (ID 46-50)
    {"id": 46, "name": "JFK Terminal 1 Garage", "spots": 500, "rate": 18.00, "base_rate": 0.8, "peak_hours": [5,6,7,14,15,20,21]},
    {"id": 47, "name": "JFK Long-Term Lot", "spots": 1000, "rate": 8.00, "base_rate": 0.6, "peak_hours": [5,6,7,8]},
    {"id": 48, "name": "LaGuardia Terminal B", "spots": 400, "rate": 18.00, "base_rate": 0.8, "peak_hours": [5,6,7,14,15,20,21]},
    {"id": 49, "name": "LaGuardia Economy Lot", "spots": 600, "rate": 6.00, "base_rate": 0.55, "peak_hours": [5,6,7]},
    {"id": 50, "name": "Newark EWR Daily Lot", "spots": 800, "rate": 12.00, "base_rate": 0.7, "peak_hours": [5,6,7,14,15,20,21]},
]

# NYC Boroughs as districts
DISTRICTS = {
    "Manhattan": range(1, 16),
    "Brooklyn": range(16, 26),
    "Queens": range(26, 36),
    "Bronx": range(36, 41),
    "Staten_Island": range(41, 46),
    "Airport": range(46, 51),
}

# NYC Borough-specific traffic patterns for realistic simulation
DISTRICT_PATTERNS = {
    "Manhattan": {
        "weekday_mult": 1.4,      # Very busy weekdays (commuters + tourists)
        "weekend_mult": 1.2,      # Still busy weekends (tourists, shopping, shows)
        "peak_entry_hours": [7, 8, 9, 10, 11],
        "peak_exit_hours": [17, 18, 19, 20, 21],
        "entry_boost": 2.0,
        "exit_boost": 2.5,
        "avg_stay_hours": 4,      # Mix of commuters and visitors
    },
    "Brooklyn": {
        "weekday_mult": 1.1,      # Moderate weekday activity
        "weekend_mult": 1.4,      # Busy weekends (brunch, shopping)
        "peak_entry_hours": [8, 9, 10, 11, 12],
        "peak_exit_hours": [17, 18, 19, 20, 21],
        "entry_boost": 1.8,
        "exit_boost": 1.6,
        "avg_stay_hours": 3,      # Shorter visits
    },
    "Queens": {
        "weekday_mult": 1.2,      # Commuter parking
        "weekend_mult": 1.0,      # Normal weekends
        "peak_entry_hours": [6, 7, 8, 9],
        "peak_exit_hours": [17, 18, 19, 20],
        "entry_boost": 2.0,
        "exit_boost": 2.0,
        "avg_stay_hours": 6,      # Commuter parking
    },
    "Bronx": {
        "weekday_mult": 1.0,      # Normal weekdays
        "weekend_mult": 1.3,      # Yankee Stadium, Bronx Zoo weekends
        "peak_entry_hours": [8, 9, 10, 17, 18],
        "peak_exit_hours": [17, 18, 21, 22, 23],
        "entry_boost": 1.8,
        "exit_boost": 1.8,
        "avg_stay_hours": 4,      # Events and shopping
    },
    "Staten_Island": {
        "weekday_mult": 1.1,      # Ferry commuters
        "weekend_mult": 0.7,      # Quieter weekends
        "peak_entry_hours": [6, 7, 8],
        "peak_exit_hours": [17, 18, 19],
        "entry_boost": 2.2,
        "exit_boost": 2.0,
        "avg_stay_hours": 8,      # Commuter parking at ferry
    },
    "Airport": {
        "weekday_mult": 1.4,      # Business travel
        "weekend_mult": 0.8,      # Less weekend travel
        "peak_entry_hours": [5, 6, 7, 8, 14, 15],
        "peak_exit_hours": [10, 11, 20, 21, 22],
        "entry_boost": 2.0,
        "exit_boost": 1.5,
        "avg_stay_hours": 72,     # Multi-day trips (JFK, LGA, Newark)
    },
}

# ============== Key Pair Authentication ==============

def load_private_key():
    """Load private key from file or base64 encoded string."""
    passphrase = PRIVATE_KEY_PASSPHRASE.encode() if PRIVATE_KEY_PASSPHRASE else None
    
    if PRIVATE_KEY_BASE64:
        try:
            key_bytes = base64.b64decode(PRIVATE_KEY_BASE64)
            private_key = serialization.load_pem_private_key(
                key_bytes, password=passphrase, backend=default_backend()
            )
            logger.info("Loaded private key from base64")
            return private_key
        except Exception as e:
            logger.error(f"Failed to load private key from base64: {e}")
    
    if PRIVATE_KEY_PATH and os.path.exists(PRIVATE_KEY_PATH):
        try:
            with open(PRIVATE_KEY_PATH, "rb") as key_file:
                private_key = serialization.load_pem_private_key(
                    key_file.read(), password=passphrase, backend=default_backend()
                )
            logger.info(f"Loaded private key from file: {PRIVATE_KEY_PATH}")
            return private_key
        except Exception as e:
            logger.error(f"Failed to load private key from file: {e}")
    
    return None

# ============== Snowflake Direct Connection ==============

def get_snowflake_connection():
    """Create a Snowflake connection using key-pair authentication."""
    try:
        import snowflake.connector
        
        private_key = load_private_key()
        if not private_key:
            return None
        
        private_key_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        conn = snowflake.connector.connect(
            account=SNOWFLAKE_CONFIG["account"],
            user=SNOWFLAKE_CONFIG["user"],
            private_key=private_key_bytes,
            role=SNOWFLAKE_CONFIG["role"],
            warehouse=SNOWFLAKE_CONFIG["warehouse"],
            database=SNOWFLAKE_CONFIG["database"],
            schema=SNOWFLAKE_CONFIG["schema"],
        )
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        return None

def populate_parking_facilities():
    """Populate PARKING_FACILITIES table in Snowflake."""
    if not ENABLE_SNOWPIPE:
        return False
    
    conn = get_snowflake_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM PARKING_FACILITIES")
        
        insert_sql = """
            INSERT INTO PARKING_FACILITIES 
            (facility_id, name, district, total_spots, rate_per_hour, created_at)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
        """
        
        facilities_data = []
        for config in FACILITY_CONFIGS:
            district = get_district_for_facility(config["id"])
            facilities_data.append((
                config["id"], config["name"], district, config["spots"], config["rate"]
            ))
        
        cursor.executemany(insert_sql, facilities_data)
        conn.commit()
        logger.info(f"Populated PARKING_FACILITIES with {len(facilities_data)} facilities")
        return True
    except Exception as e:
        logger.error(f"Failed to populate PARKING_FACILITIES: {e}")
        return False
    finally:
        conn.close()

# ============== Snowpipe Streaming ==============

def create_profile_json():
    """Create profile.json for snowpipe-streaming SDK authentication."""
    if not PRIVATE_KEY_PATH or not os.path.exists(PRIVATE_KEY_PATH):
        logger.error(f"Private key file not found: {PRIVATE_KEY_PATH}")
        return None
    
    account = SNOWFLAKE_CONFIG["account"]
    account_host = account.lower().replace("_", "-")
    host = f"{account_host}.snowflakecomputing.com"
    
    profile = {
        "account": account,
        "user": SNOWFLAKE_CONFIG["user"],
        "url": f"https://{host}:443",
        "host": host,
        "private_key_file": PRIVATE_KEY_PATH,
        "role": SNOWFLAKE_CONFIG["role"],
    }
    
    profile_path = os.path.join(os.path.dirname(__file__), "data", "profile.json")
    os.makedirs(os.path.dirname(profile_path), exist_ok=True)
    with open(profile_path, 'w') as f:
        json.dump(profile, f, indent=2)
    
    logger.info(f"Created profile.json at {profile_path}")
    return profile_path


class SnowpipeStreamer:
    """Real-time Snowpipe Streaming to Snowflake Iceberg tables."""
    
    def __init__(self):
        self.events_client = None
        self.sessions_client = None
        self.events_channel = None
        self.sessions_channel = None
        self.enabled = ENABLE_SNOWPIPE and SNOWFLAKE_CONFIG["account"] and SNOWFLAKE_CONFIG["user"]
        self.connected = False
        self.rows_inserted_events = 0
        self.rows_inserted_sessions = 0
        self.offset_id = 1
        
    def connect(self):
        """Initialize Snowpipe Streaming clients."""
        if not self.enabled:
            return False
        
        try:
            from snowflake.ingest.streaming import StreamingIngestClient
            
            profile_path = create_profile_json()
            if not profile_path:
                self.enabled = False
                return False
            
            database = SNOWFLAKE_CONFIG["database"]
            schema = SNOWFLAKE_CONFIG["schema"]
            
            self.events_client = StreamingIngestClient(
                client_name="PARKING_EVENTS_CLIENT",
                db_name=database,
                schema_name=schema,
                pipe_name="PARKING_EVENTS_PIPE",
                profile_json=profile_path,
            )
            
            self.sessions_client = StreamingIngestClient(
                client_name="PARKING_SESSIONS_CLIENT",
                db_name=database,
                schema_name=schema,
                pipe_name="PARKING_SESSIONS_PIPE",
                profile_json=profile_path,
            )
            
            self.events_channel, _ = self.events_client.open_channel("PARKING_EVENTS_CHANNEL")
            self.sessions_channel, _ = self.sessions_client.open_channel("PARKING_SESSIONS_CHANNEL")
            
            self.connected = True
            logger.info("Snowpipe Streaming connected")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Snowpipe Streaming: {e}")
            self.enabled = False
            return False
    
    def is_connected(self):
        return self.connected and self.enabled
    
    def _format_ts(self, dt):
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat(sep=" ", timespec="seconds")
    
    def stream_event(self, event_data):
        """Stream a CAR_IN or CAR_OUT event to Snowflake."""
        if not self.enabled or not self.connected or not self.events_channel:
            return
        
        try:
            now = datetime.now(timezone.utc)
            
            row = {
                "EVENT_ID": event_data.get('event_id', str(uuid.uuid4())),
                "EVENT_TYPE": event_data.get('event_type'),
                "SESSION_ID": event_data.get('session_id'),
                "FACILITY_ID": event_data.get('facility_id'),
                "FACILITY_NAME": event_data.get('facility_name'),
                "DISTRICT": event_data.get('district'),
                "LICENSE_PLATE": event_data.get('license_plate'),
                "LICENSE_PLATE_STATE": event_data.get('license_plate_state'),
                "EVENT_TIME": self._format_ts(event_data.get('event_time')),
                "AVAILABLE_SPOTS_AFTER": event_data.get('available_after'),
                "PARKING_DURATION_HOURS": event_data.get('parking_duration_hours'),
                "COST": event_data.get('cost'),
                "TRAFFIC_PATTERN": event_data.get('traffic_pattern'),
                "INGESTED_AT": self._format_ts(now),
                "ROW_TIMESTAMP": self._format_ts(now),
                "OFFSET_ID": self.offset_id,
            }
            self.offset_id += 1
            
            self.events_channel.append_row(row)
            self.rows_inserted_events += 1
            
        except Exception as e:
            logger.error(f"Error streaming event: {e}")
    
    def stream_session(self, session_data):
        """Stream session data to Snowflake."""
        if not self.enabled or not self.connected or not self.sessions_channel:
            return
        
        try:
            now = datetime.now(timezone.utc)
            
            row = {
                "SESSION_ID": session_data.get('session_id'),
                "LICENSE_PLATE": session_data.get('license_plate'),
                "LICENSE_PLATE_STATE": session_data.get('license_plate_state'),
                "FACILITY_ID": session_data.get('facility_id'),
                "FACILITY_NAME": session_data.get('facility_name'),
                "DISTRICT": session_data.get('district'),
                "IN_TIME": self._format_ts(session_data.get('in_time')),
                "OUT_TIME": self._format_ts(session_data.get('out_time')),
                "ACTUAL_DURATION_HOURS": session_data.get('actual_duration_hours'),
                "RATE_PER_HOUR": session_data.get('rate_per_hour'),
                "COST": session_data.get('cost'),
                "STATUS": session_data.get('status'),
                "INGESTED_AT": self._format_ts(now),
                "ROW_TIMESTAMP": self._format_ts(now),
                "OFFSET_ID": self.offset_id,
            }
            self.offset_id += 1
            
            self.sessions_channel.append_row(row)
            self.rows_inserted_sessions += 1
            
        except Exception as e:
            logger.error(f"Error streaming session: {e}")
    
    def flush(self):
        """Flush any buffered data - SDK auto-flushes, so this is a no-op."""
        # The snowpipe-streaming SDK auto-flushes data, no manual flush needed
        pass
    
    def get_stats(self):
        return {
            "events_streamed": self.rows_inserted_events,
            "sessions_streamed": self.rows_inserted_sessions,
        }

# Singleton pattern for Snowpipe streamer using Streamlit cache
@st.cache_resource
def get_snowpipe_streamer():
    """Get or create a singleton Snowpipe Streaming client."""
    streamer = SnowpipeStreamer()
    streamer.connect()
    return streamer

# ============== Helper Functions ==============

def get_district_for_facility(facility_id):
    for d_name, d_range in DISTRICTS.items():
        if facility_id in d_range:
            return d_name
    return "Unknown"

def get_facility_config(facility_id):
    for config in FACILITY_CONFIGS:
        if config["id"] == facility_id:
            return config
    return {"base_rate": 0.5, "peak_hours": [], "spots": 100, "rate": 5.0, "name": "Unknown"}

def is_weekend():
    return datetime.now().weekday() >= 5

def get_day_of_week_multiplier(district):
    pattern = DISTRICT_PATTERNS.get(district, {})
    if is_weekend():
        return pattern.get("weekend_mult", 1.0)
    return pattern.get("weekday_mult", 1.0)

# ============== State License Plate Generation ==============
# NYC-area traffic distribution: cars from surrounding states

STATE_PLATE_CONFIG = {
    # State: (format_function, weight percentage)
    "NY": 0.60,   # 60% New York plates
    "NJ": 0.15,   # 15% New Jersey plates
    "CT": 0.08,   # 8% Connecticut plates
    "PA": 0.07,   # 7% Pennsylvania plates
    "MA": 0.03,   # 3% Massachusetts plates
    "FL": 0.02,   # 2% Florida plates (snowbirds, visitors)
    "CA": 0.01,   # 1% California plates
    "TX": 0.01,   # 1% Texas plates
    "VA": 0.01,   # 1% Virginia plates
    "OTHER": 0.02,  # 2% Other states
}

def generate_license_plate():
    """Generate a license plate with state-specific format for NYC area traffic."""
    # Select state based on weighted distribution
    states = list(STATE_PLATE_CONFIG.keys())
    weights = list(STATE_PLATE_CONFIG.values())
    state = random.choices(states, weights=weights, k=1)[0]
    
    if state == "OTHER":
        # Random other state
        other_states = ["MD", "NC", "GA", "OH", "IL", "MI", "VT", "NH", "ME", "RI", "DE", "DC"]
        state = random.choice(other_states)
    
    plate = _generate_plate_for_state(state)
    return plate, state

def _generate_plate_for_state(state: str) -> str:
    """Generate a license plate in the format used by each state."""
    letters = string.ascii_uppercase.replace('I', '').replace('O', '').replace('Q', '')  # Exclude confusing letters
    
    if state == "NY":
        # New York: ABC-1234
        return f"{random.choices(letters, k=3)[0]}{random.choices(letters, k=1)[0]}{random.choices(letters, k=1)[0]}-{random.randint(1000, 9999)}"
    
    elif state == "NJ":
        # New Jersey: D12-ABC or A12-BCD
        if random.random() < 0.5:
            return f"{random.choice(letters)}{random.randint(10, 99)}-{random.choices(letters, k=3)[0]}{random.choices(letters, k=1)[0]}{random.choices(letters, k=1)[0]}"
        else:
            return f"{random.choice(letters)}{random.randint(10, 99)}-{random.choices(letters, k=3)[0]}{random.choices(letters, k=1)[0]}{random.choices(letters, k=1)[0]}"
    
    elif state == "CT":
        # Connecticut: AB-12345
        return f"{random.choices(letters, k=2)[0]}{random.choices(letters, k=1)[0]}-{random.randint(10000, 99999)}"
    
    elif state == "PA":
        # Pennsylvania: ABC-1234
        return f"{random.choices(letters, k=3)[0]}{random.choices(letters, k=1)[0]}{random.choices(letters, k=1)[0]}-{random.randint(1000, 9999)}"
    
    elif state == "MA":
        # Massachusetts: 1AB-C23 or 1ABC23
        return f"{random.randint(1, 9)}{random.choices(letters, k=2)[0]}{random.choices(letters, k=1)[0]}-{random.choice(letters)}{random.randint(10, 99)}"
    
    elif state == "FL":
        # Florida: ABC-1234 or AB1-2CD
        if random.random() < 0.5:
            return f"{random.choices(letters, k=3)[0]}{random.choices(letters, k=1)[0]}{random.choices(letters, k=1)[0]}-{random.randint(1000, 9999)}"
        else:
            return f"{random.choices(letters, k=2)[0]}{random.choices(letters, k=1)[0]}{random.randint(1, 9)}-{random.randint(1, 9)}{random.choices(letters, k=2)[0]}{random.choices(letters, k=1)[0]}"
    
    elif state == "CA":
        # California: 1ABC234
        return f"{random.randint(1, 9)}{random.choices(letters, k=3)[0]}{random.choices(letters, k=1)[0]}{random.choices(letters, k=1)[0]}{random.randint(100, 999)}"
    
    elif state == "TX":
        # Texas: ABC-1234
        return f"{random.choices(letters, k=3)[0]}{random.choices(letters, k=1)[0]}{random.choices(letters, k=1)[0]}-{random.randint(1000, 9999)}"
    
    elif state == "VA":
        # Virginia: ABC-1234
        return f"{random.choices(letters, k=3)[0]}{random.choices(letters, k=1)[0]}{random.choices(letters, k=1)[0]}-{random.randint(1000, 9999)}"
    
    elif state in ["VT", "NH", "ME"]:
        # New England: ABC-123
        return f"{random.choices(letters, k=3)[0]}{random.choices(letters, k=1)[0]}{random.choices(letters, k=1)[0]}-{random.randint(100, 999)}"
    
    elif state == "MD":
        # Maryland: 1AB-B123
        return f"{random.randint(1, 9)}{random.choices(letters, k=2)[0]}{random.choices(letters, k=1)[0]}-{random.choice(letters)}{random.randint(100, 999)}"
    
    elif state == "DC":
        # DC: AB-1234
        return f"{random.choices(letters, k=2)[0]}{random.choices(letters, k=1)[0]}-{random.randint(1000, 9999)}"
    
    elif state == "DE":
        # Delaware: 123456
        return f"{random.randint(10000, 999999)}"
    
    elif state == "RI":
        # Rhode Island: 123-456
        return f"{random.randint(100, 999)}-{random.randint(100, 999)}"
    
    else:
        # Generic format for other states
        return f"{random.choices(letters, k=3)[0]}{random.choices(letters, k=1)[0]}{random.choices(letters, k=1)[0]}-{random.randint(1000, 9999)}"

# ============== Real-Time Event Generation ==============

def initialize_facilities_state():
    """Initialize in-memory facility state."""
    facilities = {}
    for config in FACILITY_CONFIGS:
        facilities[config["id"]] = {
            "id": config["id"],
            "name": config["name"],
            "district": get_district_for_facility(config["id"]),
            "total_spots": config["spots"],
            "available": config["spots"],  # Start with all spots available
            "rate": config["rate"],
            "base_rate": config.get("base_rate", 0.5),
            "peak_hours": config.get("peak_hours", []),
        }
    return facilities

def reset_state():
    """Reset all session state."""
    st.session_state.facilities = initialize_facilities_state()
    st.session_state.active_sessions = {}
    st.session_state.recent_events = []
    st.session_state.total_car_in = 0
    st.session_state.total_car_out = 0
    st.session_state.events_per_second = 0
    st.session_state.last_event_count = 0
    st.session_state.last_count_time = time.time()

def get_traffic_multiplier(facility_id):
    """Get traffic multiplier based on district, day of week, and hour."""
    config = get_facility_config(facility_id)
    district = get_district_for_facility(facility_id)
    pattern = DISTRICT_PATTERNS.get(district, {})
    
    base_mult = get_day_of_week_multiplier(district)
    
    current_hour = datetime.now().hour
    peak_hours = config.get("peak_hours", [])
    
    if current_hour in peak_hours:
        base_mult *= pattern.get("entry_boost", 1.5)
    
    return base_mult

def get_traffic_pattern_tag(facility_id, event_type):
    """Generate traffic pattern tag for the event."""
    district = get_district_for_facility(facility_id)
    pattern = DISTRICT_PATTERNS.get(district, {})
    weekend = is_weekend()
    current_hour = datetime.now().hour
    
    day_type = "weekend" if weekend else "weekday"
    day_mult = pattern.get("weekend_mult", 1.0) if weekend else pattern.get("weekday_mult", 1.0)
    
    peak_entry = current_hour in pattern.get("peak_entry_hours", [])
    peak_exit = current_hour in pattern.get("peak_exit_hours", [])
    
    tags = []
    if day_mult > 1.0:
        tags.append(f"{day_type}_busy")
    elif day_mult < 0.5:
        tags.append(f"{day_type}_slow")
    else:
        tags.append(f"{day_type}_normal")
    
    if event_type == "CAR_IN" and peak_entry:
        tags.append("peak_entry_hour")
    elif event_type == "CAR_OUT" and peak_exit:
        tags.append("peak_exit_hour")
    
    return f"{district}|{'+'.join(tags)}|mult:{day_mult:.1f}x"

def get_exit_probability(parked_hours, district, current_hour):
    """Calculate probability of a car exiting based on duration and context."""
    # Minimum stay is 15 minutes
    if parked_hours < 0.25:
        return 0.0
    
    pattern = DISTRICT_PATTERNS.get(district, {})
    avg_stay = pattern.get("avg_stay_hours", 4)
    
    # Base probability increases with time parked
    if parked_hours < 0.5:
        base_prob = 0.05
    elif parked_hours < 1:
        base_prob = 0.1
    elif parked_hours < avg_stay * 0.5:
        base_prob = 0.15
    elif parked_hours < avg_stay:
        base_prob = 0.25
    elif parked_hours < avg_stay * 1.5:
        base_prob = 0.4
    elif parked_hours < avg_stay * 2:
        base_prob = 0.6
    else:
        base_prob = 0.8  # Very likely to leave after 2x average stay
    
    # Boost during peak exit hours
    if current_hour in pattern.get("peak_exit_hours", []):
        base_prob *= pattern.get("exit_boost", 1.5)
    
    return min(0.95, base_prob)

def process_car_entry(facility_id):
    """Process a single car entry event."""
    facility = st.session_state.facilities[facility_id]
    
    if facility["available"] <= 0:
        return None
    
    # Generate license plate with state
    license_plate, license_plate_state = generate_license_plate()
    session_id = str(uuid.uuid4())
    event_time = datetime.now()
    
    # Update facility state
    facility["available"] -= 1
    
    # Create session
    session = {
        "session_id": session_id,
        "license_plate": license_plate,
        "license_plate_state": license_plate_state,
        "facility_id": facility_id,
        "facility_name": facility["name"],
        "district": facility["district"],
        "in_time": event_time,
        "rate": facility["rate"],
    }
    st.session_state.active_sessions[session_id] = session
    
    # Create event
    traffic_pattern = get_traffic_pattern_tag(facility_id, "CAR_IN")
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "CAR_IN",
        "session_id": session_id,
        "facility_id": facility_id,
        "facility_name": facility["name"],
        "district": facility["district"],
        "license_plate": license_plate,
        "license_plate_state": license_plate_state,
        "event_time": event_time,
        "available_after": facility["available"],
        "parking_duration_hours": None,
        "cost": None,
        "traffic_pattern": traffic_pattern,
    }
    
    # Stream to Snowflake
    snowpipe_streamer = get_snowpipe_streamer()
    snowpipe_streamer.stream_event(event)
    
    st.session_state.total_car_in += 1
    
    return event

def process_car_exit(session_id):
    """Process a car exit event."""
    if session_id not in st.session_state.active_sessions:
        return None
    
    session = st.session_state.active_sessions.pop(session_id)
    facility_id = session["facility_id"]
    facility = st.session_state.facilities[facility_id]
    
    event_time = datetime.now()
    parked_duration = (event_time - session["in_time"]).total_seconds() / 3600
    
    # Calculate cost (minimum 1 hour billing)
    billable_hours = max(1, math.ceil(parked_duration))
    cost = billable_hours * session["rate"]
    
    # Update facility state
    facility["available"] = min(facility["total_spots"], facility["available"] + 1)
    
    # Create event
    traffic_pattern = get_traffic_pattern_tag(facility_id, "CAR_OUT")
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "CAR_OUT",
        "session_id": session_id,
        "facility_id": facility_id,
        "facility_name": facility["name"],
        "district": facility["district"],
        "license_plate": session["license_plate"],
        "license_plate_state": session.get("license_plate_state", "NY"),
        "event_time": event_time,
        "available_after": facility["available"],
        "parking_duration_hours": round(parked_duration, 2),
        "cost": round(cost, 2),
        "traffic_pattern": traffic_pattern,
    }
    
    # Create completed session data
    completed_session = {
        "session_id": session_id,
        "license_plate": session["license_plate"],
        "license_plate_state": session.get("license_plate_state", "NY"),
        "facility_id": facility_id,
        "facility_name": facility["name"],
        "district": facility["district"],
        "in_time": session["in_time"],
        "out_time": event_time,
        "actual_duration_hours": round(parked_duration, 2),
        "rate_per_hour": session["rate"],
        "cost": round(cost, 2),
        "status": "completed",
    }
    
    # Stream to Snowflake
    snowpipe_streamer = get_snowpipe_streamer()
    snowpipe_streamer.stream_event(event)
    snowpipe_streamer.stream_session(completed_session)
    
    st.session_state.total_car_out += 1
    
    return event

def process_potential_exits():
    """Check all active sessions and process exits based on probability."""
    exit_events = []
    current_time = datetime.now()
    current_hour = current_time.hour
    
    sessions_to_check = list(st.session_state.active_sessions.items())
    
    for session_id, session in sessions_to_check:
        parked_hours = (current_time - session["in_time"]).total_seconds() / 3600
        exit_prob = get_exit_probability(parked_hours, session["district"], current_hour)
        
        if random.random() < exit_prob:
            event = process_car_exit(session_id)
            if event:
                exit_events.append(event)
    
    return exit_events

def generate_facility_events(facility_id):
    """Generate events for a specific facility."""
    facility = st.session_state.facilities[facility_id]
    district = facility["district"]
    events = []
    
    # Determine how many entries based on traffic
    traffic_mult = get_traffic_multiplier(facility_id)
    current_hour = datetime.now().hour
    
    # Dynamic entry rate based on time
    config = get_facility_config(facility_id)
    is_peak = current_hour in config.get("peak_hours", [])
    
    # Entry rate biased by time of day and traffic
    if is_peak:
        entry_bias = 1.5 * traffic_mult
    else:
        entry_bias = 0.8 * traffic_mult
    
    # Random number of entries
    if 2 <= current_hour < 6:
        base_entry_count = random.randint(0, 2)
    elif 6 <= current_hour < 10 or 16 <= current_hour < 20:
        base_entry_count = random.randint(3, 6)
    else:
        base_entry_count = random.randint(5, 8)
    
    entry_count = int(base_entry_count * entry_bias)
    
    # Reduce entries if facility is nearly full
    occupancy = 1 - (facility["available"] / facility["total_spots"])
    if occupancy > 0.9:
        entry_count = max(0, entry_count - 3)
    elif occupancy > 0.7:
        entry_count = max(0, entry_count - 1)
    
    # Process entries
    for _ in range(entry_count):
        event = process_car_entry(facility_id)
        if event:
            events.append(event)
    
    return events

# ============== Historical Data Generator ==============

PROGRESS_FILE = os.path.join(os.path.dirname(__file__), "data", "historical_progress.json")

def save_progress(progress_data):
    """Save progress to a JSON file."""
    os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress_data, f)

def load_progress():
    """Load progress from JSON file."""
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r') as f:
                return json.load(f)
        except:
            return None
    return None

def clear_progress():
    """Clear progress file."""
    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)

def run_historical_generator(start_date, end_date, batch_size):
    """Run the historical data generator as a subprocess."""
    script_path = os.path.join(os.path.dirname(__file__), "generate_historical_data.py")
    
    # Initialize progress
    save_progress({
        "status": "starting",
        "start_date": start_date,
        "end_date": end_date,
        "current_date": start_date,
        "days_completed": 0,
        "total_days": 0,
        "total_events": 0,
        "total_sessions": 0,
        "last_update": datetime.now().isoformat(),
        "output_lines": [],
        "error": None
    })
    
    cmd = [
        "python", "-u", script_path,
        "--start-date", start_date,
        "--end-date", end_date,
        "--batch-size", str(batch_size)
    ]
    
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=os.path.dirname(__file__)
        )
        
        output_lines = []
        total_days = 0
        days_completed = 0
        total_events = 0
        total_sessions = 0
        current_date = start_date
        
        for line in iter(process.stdout.readline, ''):
            line = line.strip()
            if not line:
                continue
                
            output_lines.append(line)
            if len(output_lines) > 50:
                output_lines = output_lines[-50:]
            
            # Parse progress from output
            if "Total Days:" in line:
                try:
                    total_days = int(line.split("Total Days:")[1].strip())
                except:
                    pass
            
            # Parse day progress lines like: "2025-04-19 (Sat): Events=3,117, Pending=567, Total Events=3,117, Sessions=1,275"
            if "):  Events=" in line or "): Events=" in line:
                try:
                    # Extract date
                    date_part = line.split(" (")[0].strip()
                    current_date = date_part
                    days_completed += 1
                    
                    # Extract total events and sessions
                    if "Total Events=" in line:
                        events_part = line.split("Total Events=")[1].split(",")[0].replace(",", "")
                        total_events = int(events_part)
                    if "Sessions=" in line:
                        sessions_part = line.split("Sessions=")[1].split(",")[0].split()[0].replace(",", "")
                        total_sessions = int(sessions_part)
                except Exception as e:
                    pass
            
            # Update progress file
            save_progress({
                "status": "running",
                "start_date": start_date,
                "end_date": end_date,
                "current_date": current_date,
                "days_completed": days_completed,
                "total_days": total_days,
                "total_events": total_events,
                "total_sessions": total_sessions,
                "last_update": datetime.now().isoformat(),
                "output_lines": output_lines,
                "error": None
            })
        
        process.wait()
        
        # Final update
        final_status = "completed" if process.returncode == 0 else "failed"
        save_progress({
            "status": final_status,
            "start_date": start_date,
            "end_date": end_date,
            "current_date": end_date if final_status == "completed" else current_date,
            "days_completed": days_completed,
            "total_days": total_days,
            "total_events": total_events,
            "total_sessions": total_sessions,
            "last_update": datetime.now().isoformat(),
            "output_lines": output_lines,
            "error": None if process.returncode == 0 else f"Process exited with code {process.returncode}"
        })
        
    except Exception as e:
        save_progress({
            "status": "failed",
            "start_date": start_date,
            "end_date": end_date,
            "current_date": current_date,
            "days_completed": 0,
            "total_days": 0,
            "total_events": 0,
            "total_sessions": 0,
            "last_update": datetime.now().isoformat(),
            "output_lines": [],
            "error": str(e)
        })

# ============== Streamlit UI ==============

st.set_page_config(page_title="Parking IoT - Snowflake", page_icon="🅿️", layout="wide")

# Initialize state
if 'facilities' not in st.session_state:
    st.session_state.facilities = initialize_facilities_state()
if 'active_sessions' not in st.session_state:
    st.session_state.active_sessions = {}
if 'recent_events' not in st.session_state:
    st.session_state.recent_events = []
if 'generator_running' not in st.session_state:
    st.session_state.generator_running = False
if 'total_car_in' not in st.session_state:
    st.session_state.total_car_in = 0
if 'total_car_out' not in st.session_state:
    st.session_state.total_car_out = 0
if 'events_per_second' not in st.session_state:
    st.session_state.events_per_second = 0
if 'last_event_count' not in st.session_state:
    st.session_state.last_event_count = 0
if 'last_count_time' not in st.session_state:
    st.session_state.last_count_time = time.time()
if 'facility_timers' not in st.session_state:
    st.session_state.facility_timers = {fid: time.time() + random.uniform(0, 10) for fid in range(1, 51)}
if 'historical_thread' not in st.session_state:
    st.session_state.historical_thread = None

# Get singleton Snowpipe streamer (cached across reruns)
snowpipe_streamer = get_snowpipe_streamer()

# Populate facilities on first load
if 'facilities_populated' not in st.session_state:
    populate_parking_facilities()
    st.session_state.facilities_populated = True

st.title("🅿️ Smart Parking IoT Simulator")

# Connection status
sf_connected = snowpipe_streamer.is_connected()
if sf_connected:
    sf_status = "🟢 Connected"
elif not ENABLE_SNOWPIPE:
    sf_status = "🟡 Disabled"
else:
    sf_status = "🔴 Not Connected"

st.caption(f"Snowflake: {sf_status} | Architecture: In-Memory → Snowflake Streaming | Exit Logic: Probability-based (min 15 min)")

# Main Tabs - Add Historical Generator
main_tab1, main_tab2 = st.tabs(["🎮 Real-Time Simulator", "📅 Historical Data Generator"])

with main_tab1:
    # Control panel
    col_ctrl1, col_ctrl2, col_ctrl3, col_ctrl4 = st.columns([1, 1, 1, 2])
    
    with col_ctrl1:
        if st.button("▶️ Start" if not st.session_state.generator_running else "⏹️ Stop", 
                     type="primary", use_container_width=True, key="realtime_start"):
            st.session_state.generator_running = not st.session_state.generator_running
            if st.session_state.generator_running:
                for fid in range(1, 51):
                    st.session_state.facility_timers[fid] = time.time() + random.uniform(0, 5)
            st.rerun()
    
    with col_ctrl2:
        if st.button("🎲 Burst Events", use_container_width=True, key="burst"):
            num_facilities = random.randint(10, 20)
            selected = random.sample(range(1, 51), num_facilities)
            for fid in selected:
                events = generate_facility_events(fid)
                st.session_state.recent_events = events + st.session_state.recent_events
            # Also process some exits
            exit_events = process_potential_exits()
            st.session_state.recent_events = exit_events + st.session_state.recent_events
            st.session_state.recent_events = st.session_state.recent_events[:100]
            snowpipe_streamer.flush()
            st.rerun()
    
    with col_ctrl3:
        if st.button("🔄 Restart", use_container_width=True, help="Clear all state and start fresh", key="restart"):
            st.session_state.generator_running = False
            reset_state()
            populate_parking_facilities()
            st.success("System restarted!")
            st.rerun()
    
    with col_ctrl4:
        status_color = "🟢" if st.session_state.generator_running else "🔴"
        total_events = st.session_state.total_car_in + st.session_state.total_car_out
        active_cars = len(st.session_state.active_sessions)
        st.markdown(f"**{status_color} {'RUNNING' if st.session_state.generator_running else 'STOPPED'}** | ~{st.session_state.events_per_second:.1f}/sec | Active: {active_cars:,} | Total: {total_events:,}")
    
    # Metrics
    st.markdown("---")
    
    total_spots = sum(f["total_spots"] for f in st.session_state.facilities.values())
    total_available = sum(f["available"] for f in st.session_state.facilities.values())
    total_occupied = total_spots - total_available
    
    m1, m2, m3, m4, m5, m6 = st.columns(6)
    with m1:
        st.metric("🏢 Facilities", "50")
    with m2:
        st.metric("🅿️ Total Spots", f"{total_spots:,}")
    with m3:
        st.metric("✅ Available", f"{total_available:,}")
    with m4:
        st.metric("🚗 Occupied", f"{total_occupied:,}")
    with m5:
        st.metric("🚗 Total In", f"{st.session_state.total_car_in:,}")
    with m6:
        st.metric("🚶 Total Out", f"{st.session_state.total_car_out:,}")
    
    # Tabs
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "🏢 Facilities", "📜 Event Stream", "🚗 Active Sessions"])
    
    with tab1:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("District Occupancy")
            district_data = []
            for district_name, facility_ids in DISTRICTS.items():
                total = sum(st.session_state.facilities[fid]["total_spots"] for fid in facility_ids)
                available = sum(st.session_state.facilities[fid]["available"] for fid in facility_ids)
                occupied = total - available
                pattern = DISTRICT_PATTERNS.get(district_name, {})
                day_mult = pattern.get("weekend_mult" if is_weekend() else "weekday_mult", 1.0)
                district_data.append({
                    "District": district_name,
                    "Total": total,
                    "Occupied": occupied,
                    "Available": available,
                    "Occupancy": f"{(occupied/total)*100:.1f}%" if total > 0 else "0%",
                    "Day Pattern": f"{day_mult:.1f}x {'(Weekend)' if is_weekend() else '(Weekday)'}",
                })
            
            df = pd.DataFrame(district_data)
            st.dataframe(df, use_container_width=True, hide_index=True)
        
        with col2:
            st.subheader("Traffic Pattern Info")
            current_hour = datetime.now().hour
            st.info(f"**Current Hour:** {current_hour}:00 | **Day:** {'Weekend' if is_weekend() else 'Weekday'}")
            
            st.markdown("**District Activity Levels:**")
            for district_name, pattern in DISTRICT_PATTERNS.items():
                day_mult = pattern.get("weekend_mult" if is_weekend() else "weekday_mult", 1.0)
                is_entry_peak = current_hour in pattern.get("peak_entry_hours", [])
                is_exit_peak = current_hour in pattern.get("peak_exit_hours", [])
                
                status = "🟢" if day_mult > 1.0 else ("🔴" if day_mult < 0.5 else "🟡")
                entry_icon = "⬆️" if is_entry_peak else ""
                exit_icon = "⬇️" if is_exit_peak else ""
                
                st.markdown(f"{status} **{district_name}**: {day_mult:.1f}x {entry_icon}{exit_icon}")
    
    with tab2:
        st.subheader("All Facilities")
        for district_name, facility_ids in DISTRICTS.items():
            st.markdown(f"### {district_name}")
            cols = st.columns(5)
            for idx, fid in enumerate(facility_ids):
                fac = st.session_state.facilities[fid]
                with cols[idx % 5]:
                    occupied = fac["total_spots"] - fac["available"]
                    occ_pct = (occupied / fac["total_spots"]) * 100 if fac["total_spots"] > 0 else 0
                    status = "🔴" if occ_pct >= 90 else ("🟡" if occ_pct >= 70 else "🟢")
                    st.markdown(f"**{status} {fac['name'][:18]}**")
                    st.caption(f"{fac['available']}/{fac['total_spots']} | ${fac['rate']:.0f}/hr")
                    st.progress(occ_pct / 100)
    
    with tab3:
        st.subheader("Live Event Stream")
        
        if st.session_state.recent_events:
            events_data = []
            for e in st.session_state.recent_events[:100]:
                events_data.append({
                    'Type': "🚗 IN" if e.get('event_type') == 'CAR_IN' else "🚶 OUT",
                    'Plate': e.get('license_plate', ''),
                    'Facility': e.get('facility_name', ''),
                    'District': e.get('district', ''),
                    'Time': e.get('event_time').strftime('%H:%M:%S') if e.get('event_time') else '',
                    'Pattern': e.get('traffic_pattern', ''),
                    'Duration': f"{e.get('parking_duration_hours', 0):.2f}h" if e.get('parking_duration_hours') else "-",
                    'Cost': f"${e.get('cost', 0):.0f}" if e.get('cost') and e.get('cost') > 0 else ("-" if not e.get('cost') else "FREE"),
                })
            events_df = pd.DataFrame(events_data)
            st.dataframe(events_df, use_container_width=True, hide_index=True, height=500)
        else:
            st.info("No events yet. Click Start to begin generating events!")
    
    with tab4:
        st.subheader("Active Parking Sessions")
        st.caption("💡 Cars exit based on probability (increases with duration). Min parking: 15 minutes.")
        
        if st.session_state.active_sessions:
            sessions_data = []
            current_time = datetime.now()
            for session_id, session in list(st.session_state.active_sessions.items())[:100]:
                parked_hours = (current_time - session["in_time"]).total_seconds() / 3600
                exit_prob = get_exit_probability(parked_hours, session["district"], current_time.hour)
                sessions_data.append({
                    'Plate': session['license_plate'],
                    'Facility': session['facility_name'],
                    'District': session['district'],
                    'In Time': session['in_time'].strftime('%H:%M:%S'),
                    'Parked': f"{parked_hours:.2f}h",
                    'Exit Prob': f"{exit_prob*100:.1f}%",
                    'Rate': f"${session['rate']:.0f}/hr" if session['rate'] > 0 else "FREE",
                })
            sessions_df = pd.DataFrame(sessions_data)
            st.dataframe(sessions_df, use_container_width=True, hide_index=True, height=500)
        else:
            st.info("No active sessions. Start the generator to park some cars!")

with main_tab2:
    st.subheader("📅 Historical Data Generator")
    st.markdown("""
    Generate synthetic historical parking data for a specified date range. 
    The data will be inserted directly into Snowflake Iceberg tables.
    """)
    
    st.markdown("---")
    
    # Configuration Section
    col1, col2, col3 = st.columns(3)
    
    with col1:
        default_start = datetime(2025, 1, 1).date()
        start_date = st.date_input(
            "Start Date",
            value=default_start,
            min_value=datetime(2020, 1, 1).date(),
            max_value=datetime.now().date() - timedelta(days=1),
            key="hist_start_date"
        )
    
    with col2:
        default_end = (datetime.now() - timedelta(days=1)).date()
        end_date = st.date_input(
            "End Date",
            value=default_end,
            min_value=datetime(2020, 1, 1).date(),
            max_value=datetime.now().date() - timedelta(days=1),
            key="hist_end_date"
        )
    
    with col3:
        batch_size = st.number_input(
            "Batch Size",
            value=1000,
            min_value=100,
            max_value=10000,
            step=100,
            help="Number of records per batch insert",
            key="hist_batch_size"
        )
    
    # Calculate and show stats
    if start_date and end_date:
        total_days = (end_date - start_date).days + 1
        if total_days > 0:
            st.info(f"📊 **{total_days} days** selected | Estimated: ~{total_days * 2860:,} events, ~{total_days * 1415:,} sessions")
        else:
            st.error("End date must be after start date!")
    
    st.markdown("---")
    
    # Control buttons
    btn_col1, btn_col2, btn_col3 = st.columns([1, 1, 2])
    
    with btn_col1:
        start_clicked = st.button(
            "🚀 Start Generation",
            type="primary",
            use_container_width=True,
            disabled=(start_date >= end_date) if start_date and end_date else True,
            key="hist_start"
        )
    
    with btn_col2:
        clear_clicked = st.button(
            "🗑️ Clear Progress",
            use_container_width=True,
            key="hist_clear"
        )
    
    # Handle button clicks
    if clear_clicked:
        clear_progress()
        st.success("Progress cleared!")
        st.rerun()
    
    if start_clicked:
        # Start the generator in a background thread
        thread = threading.Thread(
            target=run_historical_generator,
            args=(str(start_date), str(end_date), batch_size),
            daemon=True
        )
        thread.start()
        st.session_state.historical_thread = thread
        st.rerun()
    
    st.markdown("---")
    
    # Progress Section
    st.subheader("📈 Progress")
    
    progress = load_progress()
    
    if progress:
        status = progress.get("status", "unknown")
        
        # Status indicator
        if status == "completed":
            st.success("✅ Generation Complete!")
        elif status == "failed":
            st.error(f"❌ Generation Failed: {progress.get('error', 'Unknown error')}")
        elif status == "running":
            st.info("🔄 Generation in progress...")
        elif status == "starting":
            st.info("⏳ Starting generator...")
        
        # Progress metrics
        days_completed = progress.get("days_completed", 0)
        total_days = progress.get("total_days", 0)
        total_events = progress.get("total_events", 0)
        total_sessions = progress.get("total_sessions", 0)
        current_date = progress.get("current_date", "")
        
        # Progress bar
        if total_days > 0:
            progress_pct = days_completed / total_days
            st.progress(progress_pct, text=f"{days_completed}/{total_days} days ({progress_pct*100:.1f}%)")
        
        # Metrics
        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.metric("📅 Days Completed", f"{days_completed:,}")
        with m2:
            st.metric("📅 Total Days", f"{total_days:,}")
        with m3:
            st.metric("📊 Total Events", f"{total_events:,}")
        with m4:
            st.metric("🎫 Total Sessions", f"{total_sessions:,}")
        
        # Current date being processed
        if current_date and status == "running":
            st.markdown(f"**Currently processing:** `{current_date}`")
        
        # Output log
        with st.expander("📜 Output Log", expanded=(status == "running")):
            output_lines = progress.get("output_lines", [])
            if output_lines:
                # Show last 30 lines
                log_text = "\n".join(output_lines[-30:])
                st.code(log_text, language="text")
            else:
                st.text("No output yet...")
        
        # Auto-refresh while running
        if status in ["running", "starting"]:
            time.sleep(2)
            st.rerun()
    else:
        st.info("No generation in progress. Configure the date range above and click 'Start Generation'.")
        
        # Show tips
        with st.expander("💡 Tips"):
            st.markdown("""
            - **Start Date**: Choose when you want historical data to begin
            - **End Date**: Usually yesterday (data up to present)
            - **Batch Size**: Larger batches are faster but use more memory (default 1000 is recommended)
            - **Duration**: Generating 1 year of data takes approximately 10-15 minutes
            - **Data Volume**: ~2,860 events and ~1,415 sessions per day
            """)

# Event generation loop for real-time simulator
if st.session_state.generator_running:
    current_time = time.time()
    events_generated = []
    
    # Generate entry events for facilities whose timer has elapsed
    for fid in range(1, 51):
        if current_time >= st.session_state.facility_timers.get(fid, 0):
            events = generate_facility_events(fid)
            events_generated.extend(events)
            
            # Set next event time based on traffic multiplier
            traffic_mult = get_traffic_multiplier(fid)
            min_interval = max(3, int(5 / max(traffic_mult, 0.3)))
            max_interval = max(10, int(30 / max(traffic_mult, 0.3)))
            st.session_state.facility_timers[fid] = current_time + random.randint(min_interval, max_interval)
    
    # Process potential exits (probability-based)
    exit_events = process_potential_exits()
    events_generated.extend(exit_events)
    
    # Update recent events
    if events_generated:
        st.session_state.recent_events = events_generated + st.session_state.recent_events
        st.session_state.recent_events = st.session_state.recent_events[:100]
    
    # Calculate events per second
    total_events = st.session_state.total_car_in + st.session_state.total_car_out
    time_diff = current_time - st.session_state.last_count_time
    if time_diff >= 1.0:
        events_diff = total_events - st.session_state.last_event_count
        st.session_state.events_per_second = events_diff / time_diff
        st.session_state.last_event_count = total_events
        st.session_state.last_count_time = current_time
    
    # Flush to Snowflake periodically
    snowpipe_streamer.flush()
    
    # Rerun to update UI
    time.sleep(0.5)
    st.rerun()
