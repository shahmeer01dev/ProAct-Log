from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import datetime
import sqlite3
import pandas as pd
import os
import pyarrow

class ActivityLog(BaseModel):
    application_name: str
    window_title: str

app = FastAPI()

def init_db():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS activity_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                app_name TEXT NOT NULL,
                window_title TEXT,
                category TEXT
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        print("Database initialized. 'activity_logs' table is ready.")
    except Exception as e:
        print(f"Error initializing database: {e}")

@app.on_event("startup")
async def startup_event():
    init_db()

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

def get_db_connection():
    conn = sqlite3.connect("activity_logs.db")
    return conn

def categorize_activity(app_name: str, window_title: str) -> str:
    app_lower = app_name.lower()
    title_lower = window_title.lower()
    if "code" in app_lower or "visual studio" in app_lower or "terminal" in app_lower or "iterm" in app_lower:
        return "Development"
    if "slack" in app_lower or "discord" in app_lower or "zoom" in app_lower or "teams" in app_lower:
        return "Communication"
    if "chrome" in app_lower or "firefox" in app_lower or "safari" in app_lower:
        if "youtube" in title_lower:
            return "Browsing (YouTube)"
        if "github" in title_lower:
            return "Development"
        return "Browsing"
    return "Uncategorized"

def write_parquet_row(timestamp, app_name, window_title, category):
    date_str = timestamp.strftime("%Y-%m-%d")
    folder = "bigdata"
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"{date_str}.parquet")
    df = pd.DataFrame([{
        "timestamp": timestamp,
        "app_name": app_name,
        "window_title": window_title,
        "category": category
    }])
    if os.path.exists(path):
        old = pd.read_parquet(path, engine='pyarrow')
        df = pd.concat([old, df], ignore_index=True)
    df.to_parquet(path, engine='pyarrow')

@app.post("/log_activity")
async def log_activity_endpoint(activity: ActivityLog):
    timestamp = datetime.datetime.now()
    category = categorize_activity(activity.application_name, activity.window_title)
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO activity_logs (timestamp, app_name, window_title, category) VALUES (?, ?, ?, ?)",
            (timestamp, activity.application_name, activity.window_title, category)
        )
        conn.commit()
        cur.close()
        conn.close()
        write_parquet_row(timestamp, activity.application_name, activity.window_title, category)
        return {"status": "success", "message": "Log saved"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {e}")

@app.get("/activities")
async def get_activities():
    try:
        conn = get_db_connection()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT timestamp, app_name, window_title, category FROM activity_logs ORDER BY timestamp DESC LIMIT 20")
        activities = cur.fetchall()
        cur.close()
        conn.close()
        return activities
    except Exception as e:
        print(f"Database Error: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

@app.get("/productivity-summary")
async def get_productivity_summary():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT category, COUNT(*) as count
            FROM activity_logs
            WHERE DATE(timestamp) = DATE('now')
            GROUP BY category
        """)
        summary_data = cur.fetchall()
        cur.close()
        conn.close()
        summary_dict = {row[0]: row[1] for row in summary_data}
        return summary_dict
    except Exception as e:
        print(f"Database Error on /productivity-summary: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

@app.get("/activity-count-by-app")
async def activity_count_by_app():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT app_name, COUNT(*) as count
            FROM activity_logs
            WHERE DATE(timestamp) = DATE('now')
            GROUP BY app_name
            ORDER BY count DESC
            LIMIT 10
        """)
        data = cur.fetchall()
        cur.close()
        conn.close()
        return [{"app_name": row[0], "count": row[1]} for row in data]
    except Exception as e:
        print(f"Database Error on /activity-count-by-app: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

@app.get("/activity-trend-by-hour")
async def activity_trend_by_hour():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT STRFTIME('%H', timestamp) as hour, COUNT(*) as count
            FROM activity_logs
            WHERE DATE(timestamp) = DATE('now')
            GROUP BY hour
            ORDER BY hour
        """)
        data = cur.fetchall()
        cur.close()
        conn.close()

        counts_by_hour = {int(row[0]): row[1] for row in data}

        result = []
        for hr in range(24):
            count = counts_by_hour.get(hr, 0)
            suffix = "AM" if hr < 12 else "PM"
            hr_12 = hr if 1 <= hr <= 12 else (hr - 12 if hr > 12 else 12)
            hour_label = f"{hr_12:02d} {suffix}"
            result.append({"hour": hour_label, "count": count})

        return result
    except Exception as e:
        print(f"Database Error on /activity-trend-by-hour: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")