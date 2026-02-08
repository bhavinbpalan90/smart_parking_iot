#!/usr/bin/env python3
"""
Historical Data Generator for Smart Parking IoT
Generates synthetic parking data from a start date to yesterday using realistic traffic patterns.

Usage:
    python generate_historical_data.py --start-date 2025-01-01 --batch-size 10000

Requirements:
    - Snowflake connection configured via environment variables or .env file
    - Same traffic patterns as the real-time simulator
"""

import os
import sys
import uuid
import random
import string
import argparse
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# Load environment variables
load_dotenv()

# ============== Configuration ==============

SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT", "SFPSCOGS-CAPITALONE_AWS_1"),
    "user": os.getenv("SNOWFLAKE_USER", "BPALAN"),
    "role": os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "PARKING_IOT"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "RAW_DATA"),
}

PRIVATE_KEY_PATH = os.getenv("PRIVATE_KEY_PATH", "/app/keys/rsa_key.p8")

# If running locally, try local path
if not os.path.exists(PRIVATE_KEY_PATH):
    local_key_path = os.path.join(os.path.dirname(__file__), "keys", "rsa_key.p8")
    if os.path.exists(local_key_path):
        PRIVATE_KEY_PATH = local_key_path

# ============== NYC Borough Traffic Patterns ==============

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

# NYC Boroughs as districts
DISTRICTS = {
    "Manhattan": range(1, 16),
    "Brooklyn": range(16, 26),
    "Queens": range(26, 36),
    "Bronx": range(36, 41),
    "Staten_Island": range(41, 46),
    "Airport": range(46, 51),
}

# ============== NYC Parking Facilities ==============
# 50 Parking Facilities across NYC's 5 boroughs + airports

FACILITY_CONFIGS = [
    # Manhattan (ID 1-15) - Highest rates in the city
    {"id": 1, "name": "Times Square 44th St", "spots": 200, "rate": 35.00},
    {"id": 2, "name": "Penn Station 33rd St", "spots": 300, "rate": 28.00},
    {"id": 3, "name": "Grand Central 42nd St", "spots": 250, "rate": 32.00},
    {"id": 4, "name": "Financial District Wall St", "spots": 180, "rate": 40.00},
    {"id": 5, "name": "Midtown 5th Ave", "spots": 150, "rate": 38.00},
    {"id": 6, "name": "Chelsea Market 15th St", "spots": 120, "rate": 25.00},
    {"id": 7, "name": "Upper East Side 86th St", "spots": 100, "rate": 30.00},
    {"id": 8, "name": "Upper West Side 72nd St", "spots": 100, "rate": 28.00},
    {"id": 9, "name": "SoHo Broadway", "spots": 80, "rate": 35.00},
    {"id": 10, "name": "Tribeca Greenwich St", "spots": 90, "rate": 32.00},
    {"id": 11, "name": "East Village 2nd Ave", "spots": 70, "rate": 22.00},
    {"id": 12, "name": "West Village 7th Ave", "spots": 60, "rate": 25.00},
    {"id": 13, "name": "Harlem 125th St", "spots": 150, "rate": 15.00},
    {"id": 14, "name": "Lincoln Center 65th St", "spots": 180, "rate": 30.00},
    {"id": 15, "name": "Columbus Circle", "spots": 140, "rate": 35.00},
    # Brooklyn (ID 16-25)
    {"id": 16, "name": "Downtown Brooklyn Borough Hall", "spots": 205, "rate": 9.00},
    {"id": 17, "name": "DUMBO Water St", "spots": 150, "rate": 18.00},
    {"id": 18, "name": "Williamsburg Bedford Ave", "spots": 120, "rate": 15.00},
    {"id": 19, "name": "Bay Ridge 5th Ave", "spots": 205, "rate": 4.00},
    {"id": 20, "name": "Park Slope 7th Ave", "spots": 100, "rate": 12.00},
    {"id": 21, "name": "Brooklyn Heights Montague St", "spots": 90, "rate": 14.00},
    {"id": 22, "name": "Coney Island Boardwalk", "spots": 300, "rate": 8.00},
    {"id": 23, "name": "Bensonhurst 86th St", "spots": 120, "rate": 5.00},
    {"id": 24, "name": "Flatbush Junction", "spots": 180, "rate": 6.00},
    {"id": 25, "name": "Brighton Beach", "spots": 150, "rate": 7.00},
    # Queens (ID 26-35)
    {"id": 26, "name": "Long Island City Court Square", "spots": 476, "rate": 9.00},
    {"id": 27, "name": "Flushing Main St", "spots": 200, "rate": 8.00},
    {"id": 28, "name": "Jamaica Station", "spots": 250, "rate": 6.00},
    {"id": 29, "name": "Astoria Steinway St", "spots": 46, "rate": 7.00},
    {"id": 30, "name": "Forest Hills 71st Ave", "spots": 150, "rate": 8.00},
    {"id": 31, "name": "Bayside Bell Blvd", "spots": 120, "rate": 6.00},
    {"id": 32, "name": "Queens Center Mall", "spots": 400, "rate": 5.00},
    {"id": 33, "name": "Rego Park 63rd Dr", "spots": 180, "rate": 5.00},
    {"id": 34, "name": "Jackson Heights 37th Ave", "spots": 100, "rate": 6.00},
    {"id": 35, "name": "Queens Family Court", "spots": 100, "rate": 5.00},
    # Bronx (ID 36-40)
    {"id": 36, "name": "Jerome-190th St Garage", "spots": 416, "rate": 7.00},
    {"id": 37, "name": "Yankee Stadium Lot A", "spots": 600, "rate": 25.00},
    {"id": 38, "name": "Fordham Road Plaza", "spots": 150, "rate": 6.00},
    {"id": 39, "name": "Bronx Zoo Southern Blvd", "spots": 300, "rate": 18.00},
    {"id": 40, "name": "Bronxdale Municipal", "spots": 100, "rate": 5.00},
    # Staten Island (ID 41-45)
    {"id": 41, "name": "St George Ferry Terminal", "spots": 200, "rate": 5.00},
    {"id": 42, "name": "Staten Island Mall", "spots": 400, "rate": 0.00},
    {"id": 43, "name": "Staten Island Courthouse", "spots": 150, "rate": 5.00},
    {"id": 44, "name": "Great Kills Municipal", "spots": 100, "rate": 3.00},
    {"id": 45, "name": "New Dorp Municipal", "spots": 80, "rate": 3.00},
    # Airport District (ID 46-50)
    {"id": 46, "name": "JFK Terminal 1 Garage", "spots": 500, "rate": 18.00},
    {"id": 47, "name": "JFK Long-Term Lot", "spots": 1000, "rate": 8.00},
    {"id": 48, "name": "LaGuardia Terminal B", "spots": 400, "rate": 18.00},
    {"id": 49, "name": "LaGuardia Economy Lot", "spots": 600, "rate": 6.00},
    {"id": 50, "name": "Newark EWR Daily Lot", "spots": 800, "rate": 12.00},
]

# ============== Helper Functions ==============

def get_district_for_facility(facility_id: int) -> str:
    for d_name, d_range in DISTRICTS.items():
        if facility_id in d_range:
            return d_name
    return "Unknown"

def get_facility_config(facility_id: int) -> dict:
    for config in FACILITY_CONFIGS:
        if config["id"] == facility_id:
            return config
    return {"id": facility_id, "name": "Unknown", "spots": 100, "rate": 5.0}

def is_weekend(date: datetime) -> bool:
    return date.weekday() >= 5

# ============== State License Plate Generation ==============
# NYC-area traffic distribution: cars from surrounding states

STATE_PLATE_CONFIG = {
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

def generate_license_plate() -> Tuple[str, str]:
    """Generate a license plate with state-specific format for NYC area traffic."""
    states = list(STATE_PLATE_CONFIG.keys())
    weights = list(STATE_PLATE_CONFIG.values())
    state = random.choices(states, weights=weights, k=1)[0]
    
    if state == "OTHER":
        other_states = ["MD", "NC", "GA", "OH", "IL", "MI", "VT", "NH", "ME", "RI", "DE", "DC"]
        state = random.choice(other_states)
    
    plate = _generate_plate_for_state(state)
    return plate, state

def _generate_plate_for_state(state: str) -> str:
    """Generate a license plate in the format used by each state."""
    letters = string.ascii_uppercase.replace('I', '').replace('O', '').replace('Q', '')
    
    if state == "NY":
        return f"{''.join(random.choices(letters, k=3))}-{random.randint(1000, 9999)}"
    elif state == "NJ":
        return f"{random.choice(letters)}{random.randint(10, 99)}-{''.join(random.choices(letters, k=3))}"
    elif state == "CT":
        return f"{''.join(random.choices(letters, k=2))}-{random.randint(10000, 99999)}"
    elif state == "PA":
        return f"{''.join(random.choices(letters, k=3))}-{random.randint(1000, 9999)}"
    elif state == "MA":
        return f"{random.randint(1, 9)}{''.join(random.choices(letters, k=2))}-{random.choice(letters)}{random.randint(10, 99)}"
    elif state == "FL":
        return f"{''.join(random.choices(letters, k=3))}-{random.randint(1000, 9999)}"
    elif state == "CA":
        return f"{random.randint(1, 9)}{''.join(random.choices(letters, k=3))}{random.randint(100, 999)}"
    elif state == "TX":
        return f"{''.join(random.choices(letters, k=3))}-{random.randint(1000, 9999)}"
    elif state == "VA":
        return f"{''.join(random.choices(letters, k=3))}-{random.randint(1000, 9999)}"
    elif state in ["VT", "NH", "ME"]:
        return f"{''.join(random.choices(letters, k=3))}-{random.randint(100, 999)}"
    elif state == "MD":
        return f"{random.randint(1, 9)}{''.join(random.choices(letters, k=2))}-{random.choice(letters)}{random.randint(100, 999)}"
    elif state == "DC":
        return f"{''.join(random.choices(letters, k=2))}-{random.randint(1000, 9999)}"
    elif state == "DE":
        return f"{random.randint(10000, 999999)}"
    elif state == "RI":
        return f"{random.randint(100, 999)}-{random.randint(100, 999)}"
    else:
        return f"{''.join(random.choices(letters, k=3))}-{random.randint(1000, 9999)}"

def get_traffic_multiplier(facility_id: int, date: datetime) -> float:
    district = get_district_for_facility(facility_id)
    pattern = DISTRICT_PATTERNS.get(district, {})
    
    if is_weekend(date):
        return pattern.get("weekend_mult", 1.0)
    return pattern.get("weekday_mult", 1.0)

def get_entry_probability(hour: int, district: str) -> float:
    """Get probability of a car entering at this hour."""
    pattern = DISTRICT_PATTERNS.get(district, {})
    peak_hours = pattern.get("peak_entry_hours", [])
    entry_boost = pattern.get("entry_boost", 1.5)
    
    # Base probability varies by hour (lower at night)
    if 0 <= hour < 6:
        base = 0.1
    elif 6 <= hour < 9:
        base = 0.5
    elif 9 <= hour < 12:
        base = 0.7
    elif 12 <= hour < 14:
        base = 0.6
    elif 14 <= hour < 18:
        base = 0.5
    elif 18 <= hour < 22:
        base = 0.4
    else:
        base = 0.2
    
    if hour in peak_hours:
        base *= entry_boost
    
    return min(1.0, base)

def generate_stay_duration(district: str, entry_hour: int) -> float:
    """Generate realistic stay duration based on district and entry time."""
    pattern = DISTRICT_PATTERNS.get(district, {})
    avg_stay = pattern.get("avg_stay_hours", 4)
    
    # Add some randomness around the average
    # Use log-normal distribution for realistic parking durations
    import math
    
    # Minimum stay is 15 minutes
    min_stay = 0.25
    
    if district == "Airport":
        # Airport: can be short (pickup) or very long (travel)
        if random.random() < 0.3:
            # Short stay - pickup/dropoff
            duration = random.uniform(0.25, 1.0)
        else:
            # Long stay - travel
            duration = random.uniform(24, 168)  # 1-7 days
    elif district == "Shopping":
        # Shopping: typically 1-4 hours
        duration = random.gauss(avg_stay, avg_stay * 0.4)
        duration = max(0.5, min(6, duration))
    elif district == "Entertainment":
        # Entertainment: typically 2-5 hours
        duration = random.gauss(avg_stay, avg_stay * 0.3)
        duration = max(1, min(8, duration))
    elif district == "Downtown":
        # Downtown: work hours or meetings
        if entry_hour < 10:
            # Morning arrival - likely full workday
            duration = random.gauss(8, 1.5)
        else:
            # Later arrival - meetings
            duration = random.gauss(3, 1)
        duration = max(0.5, min(12, duration))
    elif district == "Medical":
        # Medical: appointments or visits
        duration = random.gauss(avg_stay, avg_stay * 0.5)
        duration = max(0.5, min(8, duration))
    else:
        # Default
        duration = random.gauss(avg_stay, avg_stay * 0.4)
        duration = max(min_stay, duration)
    
    return max(min_stay, duration)

def get_traffic_pattern_tag(facility_id: int, event_type: str, event_time: datetime) -> str:
    """Generate traffic pattern tag for the event."""
    hour = event_time.hour
    district = get_district_for_facility(facility_id)
    pattern = DISTRICT_PATTERNS.get(district, {})
    weekend = is_weekend(event_time)
    
    day_type = "weekend" if weekend else "weekday"
    day_mult = pattern.get("weekend_mult", 1.0) if weekend else pattern.get("weekday_mult", 1.0)
    
    peak_entry = hour in pattern.get("peak_entry_hours", [])
    peak_exit = hour in pattern.get("peak_exit_hours", [])
    
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

# ============== Snowflake Connection ==============

def load_private_key():
    """Load RSA private key for Snowflake authentication."""
    if not PRIVATE_KEY_PATH or not os.path.exists(PRIVATE_KEY_PATH):
        print(f"Error: Private key file not found: {PRIVATE_KEY_PATH}")
        return None
    
    try:
        with open(PRIVATE_KEY_PATH, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,
                backend=default_backend()
            )
        print(f"Loaded private key from: {PRIVATE_KEY_PATH}")
        return private_key
    except Exception as e:
        print(f"Error loading private key: {e}")
        return None

def get_snowflake_connection():
    """Create Snowflake connection using key-pair authentication."""
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
        print(f"Connected to Snowflake: {SNOWFLAKE_CONFIG['account']}")
        return conn
    except Exception as e:
        print(f"Failed to connect to Snowflake: {e}")
        return None

# ============== Data Generation ==============

def generate_day_events(date: datetime, facility_states: Dict[int, dict]) -> Tuple[List[dict], List[dict]]:
    """
    Generate all parking events for a single day.
    Returns (events, sessions) lists.
    """
    events = []
    sessions = []
    pending_exits = []  # (exit_time, session_data)
    
    # Process each hour of the day
    for hour in range(24):
        current_time = date.replace(hour=hour, minute=0, second=0)
        
        # Process scheduled exits that should happen this hour
        exits_to_process = [(t, s) for t, s in pending_exits if t.hour == hour and t.date() == date.date()]
        pending_exits = [(t, s) for t, s in pending_exits if not (t.hour == hour and t.date() == date.date())]
        
        for exit_time, session in exits_to_process:
            facility_id = session["facility_id"]
            facility = facility_states.get(facility_id, {})
            
            # Calculate actual duration and cost
            actual_duration = (exit_time - session["in_time"]).total_seconds() / 3600
            rate = session["rate"]
            cost = actual_duration * rate if rate > 0 else 0
            
            # Update facility availability
            facility_states[facility_id]["available"] = min(
                facility["total_spots"],
                facility["available"] + 1
            )
            
            # Create CAR_OUT event
            traffic_pattern = get_traffic_pattern_tag(facility_id, "CAR_OUT", exit_time)
            
            event = {
                "event_id": str(uuid.uuid4()),
                "event_type": "CAR_OUT",
                "session_id": session["session_id"],
                "facility_id": facility_id,
                "facility_name": session["facility_name"],
                "district": session["district"],
                "license_plate": session["license_plate"],
                "license_plate_state": session.get("license_plate_state", "NY"),
                "event_time": exit_time,
                "available_spots_after": facility_states[facility_id]["available"],
                "parking_duration_hours": round(actual_duration, 2),
                "cost": round(cost, 2),
                "traffic_pattern": traffic_pattern,
            }
            events.append(event)
            
            # Complete the session
            session_record = {
                "session_id": session["session_id"],
                "license_plate": session["license_plate"],
                "license_plate_state": session.get("license_plate_state", "NY"),
                "facility_id": facility_id,
                "facility_name": session["facility_name"],
                "district": session["district"],
                "in_time": session["in_time"],
                "out_time": exit_time,
                "actual_duration_hours": round(actual_duration, 2),
                "rate_per_hour": rate,
                "cost": round(cost, 2),
                "status": "completed",
            }
            sessions.append(session_record)
        
        # Generate new entries for this hour
        for facility_id in range(1, 51):
            facility = facility_states.get(facility_id, {})
            if facility["available"] <= 0:
                continue
            
            district = facility["district"]
            traffic_mult = get_traffic_multiplier(facility_id, current_time)
            entry_prob = get_entry_probability(hour, district)
            
            # Determine number of entries this hour
            # Scale by facility size and traffic multiplier
            base_entries = int(facility["total_spots"] * 0.02 * traffic_mult * entry_prob)
            entries_this_hour = max(0, int(random.gauss(base_entries, base_entries * 0.3)))
            entries_this_hour = min(entries_this_hour, facility["available"])
            
            for _ in range(entries_this_hour):
                if facility_states[facility_id]["available"] <= 0:
                    break
                
                # Random minute within the hour
                entry_minute = random.randint(0, 59)
                entry_second = random.randint(0, 59)
                entry_time = current_time.replace(minute=entry_minute, second=entry_second)
                
                session_id = str(uuid.uuid4())
                license_plate, license_plate_state = generate_license_plate()
                
                # Calculate exit time based on stay duration
                stay_hours = generate_stay_duration(district, hour)
                exit_time = entry_time + timedelta(hours=stay_hours)
                
                # Update facility state
                facility_states[facility_id]["available"] -= 1
                
                # Create CAR_IN event
                traffic_pattern = get_traffic_pattern_tag(facility_id, "CAR_IN", entry_time)
                
                event = {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "CAR_IN",
                    "session_id": session_id,
                    "facility_id": facility_id,
                    "facility_name": facility["name"],
                    "district": district,
                    "license_plate": license_plate,
                    "license_plate_state": license_plate_state,
                    "event_time": entry_time,
                    "available_spots_after": facility_states[facility_id]["available"],
                    "parking_duration_hours": None,
                    "cost": None,
                    "traffic_pattern": traffic_pattern,
                }
                events.append(event)
                
                # Schedule exit
                session_data = {
                    "session_id": session_id,
                    "license_plate": license_plate,
                    "license_plate_state": license_plate_state,
                    "facility_id": facility_id,
                    "facility_name": facility["name"],
                    "district": district,
                    "in_time": entry_time,
                    "rate": facility["rate"],
                }
                
                # If exit is today, add to pending_exits; otherwise create session immediately
                if exit_time.date() == date.date():
                    pending_exits.append((exit_time, session_data))
                else:
                    # Exit is on a future date - we'll handle these separately
                    pending_exits.append((exit_time, session_data))
    
    # Return pending exits that extend to future days
    return events, sessions, pending_exits

def insert_events_batch(conn, events: List[dict], batch_num: int):
    """Insert a batch of events into Snowflake."""
    if not events:
        return 0
    
    cursor = conn.cursor()
    
    insert_sql = """
        INSERT INTO PARKING_EVENTS 
        (event_id, event_type, session_id, facility_id, facility_name, district,
         license_plate, license_plate_state, event_time, available_spots_after, parking_duration_hours,
         cost, traffic_pattern, ingested_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
    """
    
    data = [
        (
            e["event_id"],
            e["event_type"],
            e["session_id"],
            e["facility_id"],
            e["facility_name"],
            e["district"],
            e["license_plate"],
            e.get("license_plate_state", "NY"),
            e["event_time"],
            e["available_spots_after"],
            e["parking_duration_hours"],
            e["cost"],
            e["traffic_pattern"],
        )
        for e in events
    ]
    
    cursor.executemany(insert_sql, data)
    conn.commit()
    cursor.close()
    
    return len(data)

def insert_sessions_batch(conn, sessions: List[dict], batch_num: int):
    """Insert a batch of sessions into Snowflake."""
    if not sessions:
        return 0
    
    cursor = conn.cursor()
    
    insert_sql = """
        INSERT INTO PARKING_SESSIONS 
        (session_id, license_plate, license_plate_state, facility_id, facility_name, district,
         in_time, out_time, actual_duration_hours, rate_per_hour, cost, status, ingested_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
    """
    
    data = [
        (
            s["session_id"],
            s["license_plate"],
            s.get("license_plate_state", "NY"),
            s["facility_id"],
            s["facility_name"],
            s["district"],
            s["in_time"],
            s["out_time"],
            s["actual_duration_hours"],
            s["rate_per_hour"],
            s["cost"],
            s["status"],
        )
        for s in sessions
    ]
    
    cursor.executemany(insert_sql, data)
    conn.commit()
    cursor.close()
    
    return len(data)

def initialize_facility_states() -> Dict[int, dict]:
    """Initialize facility states with full availability."""
    states = {}
    for config in FACILITY_CONFIGS:
        district = get_district_for_facility(config["id"])
        states[config["id"]] = {
            "name": config["name"],
            "total_spots": config["spots"],
            "available": config["spots"],
            "rate": config["rate"],
            "district": district,
        }
    return states

def main():
    parser = argparse.ArgumentParser(description="Generate historical parking data")
    parser.add_argument("--start-date", type=str, default="2025-01-01",
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default=None,
                        help="End date (YYYY-MM-DD), defaults to yesterday")
    parser.add_argument("--batch-size", type=int, default=5000,
                        help="Batch size for inserts")
    parser.add_argument("--dry-run", action="store_true",
                        help="Generate data without inserting to Snowflake")
    
    args = parser.parse_args()
    
    # Parse dates
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    else:
        end_date = datetime.now() - timedelta(days=1)
    
    end_date = end_date.replace(hour=23, minute=59, second=59)
    
    print(f"\n{'='*60}")
    print(f"Historical Data Generator for Smart Parking IoT")
    print(f"{'='*60}")
    print(f"Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"Total Days: {(end_date - start_date).days + 1}")
    print(f"Batch Size: {args.batch_size}")
    print(f"Dry Run: {args.dry_run}")
    print(f"{'='*60}\n")
    
    # Connect to Snowflake
    conn = None
    if not args.dry_run:
        conn = get_snowflake_connection()
        if not conn:
            print("Failed to connect to Snowflake. Exiting.")
            sys.exit(1)
    
    # Initialize facility states
    facility_states = initialize_facility_states()
    
    # Track pending exits (exits that span multiple days)
    all_pending_exits = []
    
    # Statistics
    total_events = 0
    total_sessions = 0
    events_buffer = []
    sessions_buffer = []
    batch_num = 0
    
    # Process each day
    current_date = start_date
    while current_date <= end_date:
        day_str = current_date.strftime("%Y-%m-%d")
        day_of_week = current_date.strftime("%A")
        
        # Add any pending exits from previous days that should exit today
        todays_pending = [(t, s) for t, s in all_pending_exits if t.date() == current_date.date()]
        all_pending_exits = [(t, s) for t, s in all_pending_exits if t.date() != current_date.date()]
        
        # Generate events for this day
        events, sessions, new_pending = generate_day_events(current_date, facility_states)
        
        # Process today's pending exits from previous days
        for exit_time, session in todays_pending:
            facility_id = session["facility_id"]
            facility = facility_states.get(facility_id, {})
            
            actual_duration = (exit_time - session["in_time"]).total_seconds() / 3600
            rate = session["rate"]
            cost = actual_duration * rate if rate > 0 else 0
            
            facility_states[facility_id]["available"] = min(
                facility["total_spots"],
                facility["available"] + 1
            )
            
            traffic_pattern = get_traffic_pattern_tag(facility_id, "CAR_OUT", exit_time)
            
            event = {
                "event_id": str(uuid.uuid4()),
                "event_type": "CAR_OUT",
                "session_id": session["session_id"],
                "facility_id": facility_id,
                "facility_name": session["facility_name"],
                "district": session["district"],
                "license_plate": session["license_plate"],
                "license_plate_state": session.get("license_plate_state", "NY"),
                "event_time": exit_time,
                "available_spots_after": facility_states[facility_id]["available"],
                "parking_duration_hours": round(actual_duration, 2),
                "cost": round(cost, 2),
                "traffic_pattern": traffic_pattern,
            }
            events.append(event)
            
            session_record = {
                "session_id": session["session_id"],
                "license_plate": session["license_plate"],
                "license_plate_state": session.get("license_plate_state", "NY"),
                "facility_id": facility_id,
                "facility_name": session["facility_name"],
                "district": session["district"],
                "in_time": session["in_time"],
                "out_time": exit_time,
                "actual_duration_hours": round(actual_duration, 2),
                "rate_per_hour": rate,
                "cost": round(cost, 2),
                "status": "completed",
            }
            sessions.append(session_record)
        
        # Add new pending exits to the global list
        all_pending_exits.extend(new_pending)
        
        # Buffer events and sessions
        events_buffer.extend(events)
        sessions_buffer.extend(sessions)
        
        # Insert when buffer is full
        if len(events_buffer) >= args.batch_size:
            if not args.dry_run:
                inserted = insert_events_batch(conn, events_buffer, batch_num)
                total_events += inserted
            else:
                total_events += len(events_buffer)
            events_buffer = []
            batch_num += 1
        
        if len(sessions_buffer) >= args.batch_size:
            if not args.dry_run:
                inserted = insert_sessions_batch(conn, sessions_buffer, batch_num)
                total_sessions += inserted
            else:
                total_sessions += len(sessions_buffer)
            sessions_buffer = []
        
        # Progress update (use newline and flush for better visibility)
        print(f"{day_str} ({day_of_week[:3]}): Events={len(events):,}, Pending={len(all_pending_exits):,}, Total Events={total_events:,}, Sessions={total_sessions:,}", flush=True)
        
        current_date += timedelta(days=1)
    
    # Insert remaining buffered data
    if events_buffer:
        if not args.dry_run:
            inserted = insert_events_batch(conn, events_buffer, batch_num)
            total_events += inserted
        else:
            total_events += len(events_buffer)
    
    if sessions_buffer:
        if not args.dry_run:
            inserted = insert_sessions_batch(conn, sessions_buffer, batch_num)
            total_sessions += inserted
        else:
            total_sessions += len(sessions_buffer)
    
    print(f"\n\n{'='*60}")
    print(f"Generation Complete!")
    print(f"{'='*60}")
    print(f"Total Events Generated: {total_events:,}")
    print(f"Total Sessions Generated: {total_sessions:,}")
    print(f"Pending Exits (future dates): {len(all_pending_exits):,}")
    print(f"{'='*60}\n")
    
    if conn:
        conn.close()

if __name__ == "__main__":
    main()
