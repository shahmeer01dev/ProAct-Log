# backend/main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import datetime
import sqlite3

# --- 1. Define the data structure our API expects ---
class ActivityLog(BaseModel):
    application_name: str
    window_title: str

# --- 2. Create the main FastAPI application instance ---
app = FastAPI()

# --- 3. Set up CORS (Cross-Origin Resource Sharing) ---
# This allows your frontend (at localhost:3000) to communicate with your backend.
origins = [
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)

# --- 4. Database Helper Function ---
def get_db_connection():
    """Establishes a connection to the SQLite database."""
    conn = sqlite3.connect("activity_logs.db")
    return conn

# --- 5. API Endpoint to RECEIVE and SAVE data ---
@app.post("/log_activity")
async def log_activity_endpoint(activity: ActivityLog):
    """Receives an activity log from the agent and saves it to the database."""
    timestamp = datetime.datetime.now()
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # SQL command to insert the data into the 'activity_logs' table
        cur.execute(
            "INSERT INTO activity_logs (timestamp, app_name, window_title) VALUES (?, ?, ?)",
            (timestamp, activity.application_name, activity.window_title)
        )
        conn.commit()  # Save the changes
        cur.close()
        conn.close()
        print(f"[{timestamp.strftime('%H:%M:%S')}] Log SAVED to DB: App='{activity.application_name}'")
        return {"status": "success", "message": "Log saved to database"}
    except Exception as e:
        print(f"Database Error: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

# --- 6. API Endpoint to SEND data to the frontend ---
@app.get("/activities")
async def get_activities():
    """Retrieves the last 20 activity logs from the database for the dashboard."""
    try:
        conn = get_db_connection()
        # This makes the results act like dictionaries, which is easier to work with
        conn.row_factory = sqlite3.Row 
        cur = conn.cursor()
        # SQL command to get the 20 most recent entries
        cur.execute("SELECT timestamp, app_name, window_title FROM activity_logs ORDER BY timestamp DESC LIMIT 20")
        activities = cur.fetchall()
        cur.close()
        conn.close()
        return activities
    except Exception as e:
        print(f"Database Error: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

@app.get("/productivity-summary")
async def get_productivity_summary():
    """Calculates the count of activities per category for today."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # --- CORRECTED SQL QUERY ---
        # This version is more compatible with SQLite
        cur.execute("""
            SELECT category, COUNT(*) as count
            FROM activity_logs
            WHERE DATE(timestamp) = DATE('now')
            GROUP BY category
        """)
        summary_data = cur.fetchall()
        cur.close()
        conn.close()
        return summary_data
    except Exception as e:
        print(f"Database Error on /productivity-summary: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")