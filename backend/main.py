from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import datetime
import sqlite3
import pandas as pd
import os
import pyarrow
 
# --- 1. Define the data structure our API expects ---
class ActivityLog(BaseModel):
    application_name: str
    window_title: str
 
# --- 2. Create the main FastAPI application instance ---
app = FastAPI()
 
# --- NEW: Database Initialization Function ---
def init_db():
    """Initializes the database and creates the table if it doesn't exist."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # Create the table if it's not already there
        # We add 'id' as a primary key (good practice) and the 'category' column
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
 
# --- NEW: Run the DB initialization on startup ---
@app.on_event("startup")
async def startup_event():
    init_db()
 
# --- 3. Set up CORS (Cross-Origin Resource Sharing) ---
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
 
# --- 4. Database Helper Function ---
def get_db_connection():
    """Establishes a connection to the SQLite database."""
    conn = sqlite3.connect("activity_logs.db")
    return conn
 
# --- NEW: Simple Categorization Logic ---
def categorize_activity(app_name: str, window_title: str) -> str:
    """Assigns a category based on the application name."""
    app_lower = app_name.lower()
    title_lower = window_title.lower()
    
    # Development
    if "code" in app_lower or "visual studio" in app_lower or "terminal" in app_lower or "iterm" in app_lower:
        return "Development"
    # Communication
    if "slack" in app_lower or "discord" in app_lower or "zoom" in app_lower or "teams" in app_lower:
        return "Communication"
    # Browsing
    if "chrome" in app_lower or "firefox" in app_lower or "safari" in app_lower:
        if "youtube" in title_lower:
            return "Browsing (YouTube)"
        if "github" in title_lower:
            return "Development"
        return "Browsing"
    # Other
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
 
 
# --- 5. API Endpoint to RECEIVE and SAVE data (UPDATED) ---
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
 
 
# --- 6. API Endpoint to SEND data to the frontend ---
@app.get("/activities")
async def get_activities():
    """Retrieves the last 20 activity logs from the database for the dashboard."""
    try:
        conn = get_db_connection()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        # Updated query to select the new category column as well
        cur.execute("SELECT timestamp, app_name, window_title, category FROM activity_logs ORDER BY timestamp DESC LIMIT 20")
        activities = cur.fetchall()
        cur.close()
        conn.close()
        return activities
    except Exception as e:
        print(f"Database Error: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")
 
# --- 7. API Endpoint for Productivity Summary (Corrected) ---
@app.get("/productivity-summary")
async def get_productivity_summary():
    """Calculates the count of activities per category for today."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # This query should now work correctly
        cur.execute("""
            SELECT category, COUNT(*) as count
            FROM activity_logs
            WHERE DATE(timestamp) = DATE('now')
            GROUP BY category
        """)
        summary_data = cur.fetchall()
        cur.close()
        conn.close()
        
        # Convert list of tuples [('Category', 5)] to a dictionary {'Category': 5}
        summary_dict = {row[0]: row[1] for row in summary_data}
        return summary_dict
    except Exception as e:
        print(f"Database Error on /productivity-summary: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")