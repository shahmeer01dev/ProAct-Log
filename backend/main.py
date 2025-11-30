from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import datetime
import os
# --- BIG DATA PHASE 2 CHANGES ---
from kafka import KafkaProducer # NEW: Import Kafka Producer
import json
import logging
import psycopg2 # ADDED: Import for PostgreSQL connection
from psycopg2 import extras # ADDED: For dictionary-like row fetching
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
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092") # UPDATED: Read from ENV
KAFKA_TOPIC = "raw_activity_logs" # The topic name for raw data

# Initialize Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logger.info("Kafka Producer initialized successfully.")
except Exception as e:
    logger.error(f"Failed to initialize Kafka Producer: {e}")
    producer = None


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

# --- COMMENTED OUT PHASE 1 DB FUNCTIONS ---

# def init_db():
#     # OLD: Function to initialize SQLite/PostgreSQL DB table
#     try:
#         conn = get_db_connection()
#         cur = conn.cursor()
#         cur.execute("""
#             CREATE TABLE IF NOT EXISTS activity_logs (
#                 id INTEGER PRIMARY KEY AUTOINCREMENT,
#                 timestamp DATETIME NOT NULL,
#                 app_name TEXT NOT NULL,
#                 window_title TEXT,
#                 category TEXT
#             )
#         """)
#         conn.commit()
#         cur.close()
#         conn.close()
#         print("Database initialized. 'activity_logs' table is ready.")
#     except Exception as e:
#         print(f"Error initializing database: {e}")

# @app.on_event("startup")
# async def startup_event():
#     # OLD: Call init_db on startup
#     init_db()

# def get_db_connection():
#     # OLD: Returns the database connection object (SQLite/PostgreSQL)
#     conn = sqlite3.connect("activity_logs.db")
#     return conn
    
# def write_parquet_row(timestamp, app_name, window_title, category):
#     # OLD: Function to write raw data to a Parquet file for potential future Spark consumption
#     date_str = timestamp.strftime("%Y-%m-%d")
#     folder = "bigdata"
#     os.makedirs(folder, exist_ok=True)
#     path = os.path.join(folder, f"{date_str}.parquet")
#     df = pd.DataFrame([{
#         "timestamp": timestamp,
#         "app_name": app_name,
#         "window_title": window_title,
#         "category": category
#     }])
#     if os.path.exists(path):
#         old = pd.read_parquet(path, engine='pyarrow')
#         df = pd.concat([old, df], ignore_index=True)
#     df.to_parquet(path, engine='pyarrow')
    
# -----------------------------------------------------

# NOTE: The categorize_activity function is now moved to the Spark processor (Step 3) 
# as part of the Big Data processing layer. We keep it here commented out 
# for reference to its old logic.
# def categorize_activity(app_name: str, window_title: str) -> str:
#     app_lower = app_name.lower()
#     title_lower = window_title.lower()
#     if "code" in app_lower or "visual studio" in app_lower or "terminal" in app_lower or "iterm" in app_lower:
#         return "Development"
#     if "slack" in app_lower or "discord" in app_lower or "zoom" in app_lower or "teams" in app_lower:
#         return "Communication"
#     if "chrome" in app_lower or "firefox" in app_lower or "safari" in app_lower:
#         if "youtube" in title_lower:
#             return "Browsing (YouTube)"
#         if "github" in title_lower:
#             return "Development"
#         return "Browsing"
#     return "Uncategorized"


origins = [
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- NEW POSTGRES CONNECTION FUNCTION ---
def get_db_connection():
    """Returns a connection to the PostgreSQL reporting database."""
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
        # Raising HTTPException here is necessary for the FastAPI response
        raise HTTPException(status_code=503, detail="Database service unavailable.")


@app.post("/log_activity")
async def log_activity_endpoint(activity: ActivityLog):
    """
    CHANGED FOR PHASE 2: This endpoint now acts as a Kafka Producer, 
    publishing the raw activity log to a Kafka topic instead of writing 
    directly to a relational database.
    """
    if producer is None:
        raise HTTPException(status_code=503, detail="Kafka Producer is unavailable. Check Kafka Broker status.")

    timestamp = datetime.datetime.now().isoformat()
    # OLD: category = categorize_activity(activity.application_name, activity.window_title)

    # OLD: Database/Parquet Write Logic
    # try:
    #     conn = get_db_connection()
    #     cur = conn.cursor()
    #     cur.execute(
    #         "INSERT INTO activity_logs (timestamp, app_name, window_title, category) VALUES (?, ?, ?, ?)",
    #         (timestamp, activity.application_name, activity.window_title, category)
    #     )
    #     conn.commit()
    #     cur.close()
    #     conn.close()
    #     write_parquet_row(timestamp, activity.application_name, activity.window_title, category)
    #     return {"status": "success", "message": "Log saved"}
    # except Exception as e:
    #     raise HTTPException(status_code=500, detail=f"Database connection failed: {e}")

    # NEW: Kafka Publish Logic
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