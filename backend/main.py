from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import datetime
import os
# --- BIG DATA PHASE 2 CHANGES ---
from kafka import KafkaProducer # NEW: Import Kafka Producer
from kafka.errors import NoBrokersAvailable # NEW: Import specific Kafka error for retry logic
import json
import logging
import psycopg2 # ADDED: Import for PostgreSQL connection
from psycopg2 import extras # ADDED: For dictionary-like row fetching
import threading # NEW: For background initialization thread
import time # NEW: For sleep in retry
# ---------------------------------

# --- COMMENTED OUT PHASE 1 IMPORTS ---
# import sqlite3
# import pandas as pd
# import os
# import pyarrow
# -------------------------------------

# --- Configure Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Kafka Configuration (NEW) ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092") 
KAFKA_TOPIC = "raw_activity_logs" 

# --- Global Producer State (UPDATED: Mutable container) ---
# We use a list to hold the producer object so we can modify it inside functions.
kafka_producer_container = [None] 


def try_init_kafka_producer():
    """Attempts to initialize the Kafka Producer, retrying if necessary."""
    logger.info("Attempting to initialize Kafka Producer in background thread...")
    max_retries = 10
    retry_delay_seconds = 5
    
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            # Success! Store the initialized producer globally.
            kafka_producer_container[0] = producer
            logger.info("Kafka Producer initialized successfully after attempts.")
            return
        except NoBrokersAvailable:
            logger.warning(f"Kafka not yet available. Retrying in {retry_delay_seconds}s... (Attempt {attempt + 1}/{max_retries})")
            time.sleep(retry_delay_seconds)
        except Exception as e:
            logger.error(f"Failed to initialize Kafka Producer: {e}")
            break # Exit on unexpected error

    logger.error("Failed to initialize Kafka Producer after all retries. The /log_activity endpoint will return 503.")


# --- PostgreSQL Configuration (NEW) ---
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost") 
POSTGRES_DB = os.getenv("POSTGRES_DB", "proact_log_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "proact_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "proact_password")
POSTGRES_TABLE = "processed_activity_metrics"


class ActivityLog(BaseModel):
    application_name: str
    window_title: str

app = FastAPI()


# --- STARTUP EVENT HOOK (NEW) ---
@app.on_event("startup")
async def startup_event():
    """Starts the Kafka producer initialization in a background thread."""
    # We do NOT want to block the FastAPI startup, so we run this in a separate thread.
    threading.Thread(target=try_init_kafka_producer, daemon=True).start()


# --- NEW POSTGRES CONNECTION FUNCTION ---
def get_db_connection():
    """Returns a connection to the PostgreSQL reporting database."""
    # ... (function remains the same)
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        return conn
    except Exception as e:
        logger.error(f"PostgreSQL connection failed: {e}")
        raise HTTPException(status_code=503, detail="Database service unavailable.")


@app.post("/log_activity")
async def log_activity_endpoint(activity: ActivityLog):
    """
    Kafka Producer Endpoint. Now checks the global state of the producer.
    """
    producer = kafka_producer_container[0] # Get current producer instance
    
    if producer is None:
        # This will happen if Kafka is still starting up or failed to initialize
        raise HTTPException(status_code=503, detail="Kafka Producer is unavailable. Topic ingestion stalled.")

    timestamp = datetime.datetime.now().isoformat()
    
    payload = {
        "timestamp": timestamp,
        "app_name": activity.application_name,
        "window_title": activity.window_title,
    }
    
    try:
        # Send the payload to the raw_activity_logs topic
        producer.send(KAFKA_TOPIC, value=payload)
        logger.info(f"Successfully sent log to Kafka: {activity.application_name}")
        return {"status": "success", "message": "Log published to Kafka"}
    except Exception as e:
        logger.error(f"Failed to send message to Kafka: {e}")
        raise HTTPException(status_code=500, detail=f"Kafka publishing failed: {e}")


# --- PHASE 2 REPORTING ENDPOINTS (Reading from PostgreSQL) ---
# ... (All reporting endpoints remain unchanged)


@app.get("/activities")
async def get_activities():
    """
    UPDATED FOR PHASE 2: Reads the 20 most recent processed activity logs from PostgreSQL.
    """
    conn = None
    try:
        conn = get_db_connection()
        # Use DictCursor to return results as dictionaries (easier JSON serialization)
        cur = conn.cursor(cursor_factory=extras.RealDictCursor) 
        cur.execute(f"SELECT timestamp, app_name, window_title, category FROM {POSTGRES_TABLE} ORDER BY timestamp DESC LIMIT 20")
        activities = cur.fetchall()
        cur.close()
        return activities
    except Exception as e:
        logger.error(f"PostgreSQL Error on /activities: {e}")
        raise HTTPException(status_code=500, detail="Database query failed.")
    finally:
        if conn:
            conn.close()


@app.get("/productivity-summary")
async def get_productivity_summary():
    """
    UPDATED FOR PHASE 2: Counts category occurrences for the current day from PostgreSQL.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # SQL logic uses PostgreSQL date functions (CURRENT_DATE)
        cur.execute(f"""
            SELECT category, COUNT(*) as count
            FROM {POSTGRES_TABLE}
            WHERE DATE(timestamp) = CURRENT_DATE
            GROUP BY category
        """)
        summary_data = cur.fetchall()
        cur.close()
        # Convert list of tuples to dictionary {category: count}
        summary_dict = {row[0]: row[1] for row in summary_data}
        return summary_dict
    except Exception as e:
        logger.error(f"PostgreSQL Error on /productivity-summary: {e}")
        raise HTTPException(status_code=500, detail="Database query failed.")
    finally:
        if conn:
            conn.close()


@app.get("/activity-count-by-app")
async def activity_count_by_app():
    """
    UPDATED FOR PHASE 2: Counts activity occurrences by app for the current day from PostgreSQL.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # SQL logic uses PostgreSQL date functions (CURRENT_DATE)
        cur.execute(f"""
            SELECT app_name, COUNT(*) as count
            FROM {POSTGRES_TABLE}
            WHERE DATE(timestamp) = CURRENT_DATE
            GROUP BY app_name
            ORDER BY count DESC
            LIMIT 10
        """)
        data = cur.fetchall()
        cur.close()
        return [{"app_name": row[0], "count": row[1]} for row in data]
    except Exception as e:
        logger.error(f"PostgreSQL Error on /activity-count-by-app: {e}")
        raise HTTPException(status_code=500, detail="Database query failed.")
    finally:
        if conn:
            conn.close()


@app.get("/activity-trend-by-hour")
async def activity_trend_by_hour():
    """
    UPDATED FOR PHASE 2: Counts activity occurrences by hour for the current day from PostgreSQL.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # SQL logic uses PostgreSQL date functions (EXTRACT for hour)
        cur.execute(f"""
            SELECT EXTRACT(HOUR FROM timestamp) as hour, COUNT(*) as count
            FROM {POSTGRES_TABLE}
            WHERE DATE(timestamp) = CURRENT_DATE
            GROUP BY hour
            ORDER BY hour
        """)
        data = cur.fetchall()
        cur.close()
        
        # Prepare data for frontend visualization (filling in zero counts for missing hours)
        counts_by_hour = {int(row[0]): row[1] for row in data}

        result = []
        for hr in range(24):
            count = counts_by_hour.get(hr, 0)
            suffix = "AM" if hr < 12 else "PM"
            # 12-hour format calculation
            hr_12 = hr if 1 <= hr <= 12 else (hr - 12 if hr > 12 else 12)
            hour_label = f"{hr_12:02d} {suffix}"
            result.append({"hour": hour_label, "count": count})

        return result
    except Exception as e:
        logger.error(f"PostgreSQL Error on /activity-trend-by-hour: {e}")
        raise HTTPException(status_code=500, detail="Database query failed.")
    finally:
        if conn:
            conn.close()