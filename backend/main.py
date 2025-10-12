# backend/main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import datetime
import sqlite3

# This class defines the structure of the data our API expects.
class ActivityLog(BaseModel):
    application_name: str
    window_title: str
# This creates the main application instance.
app = FastAPI()

# Function to get a connection to the SQLite database
def get_db_connection():
    conn = sqlite3.connect("activity_logs.db")
    return conn
app = FastAPI()

# This defines an endpoint that listens for POST requests at "/log_activity".
@app.post("/log_activity")
async def log_activity_endpoint(activity: ActivityLog):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # For now, we'll just print the data to the server's terminal
    # to confirm that we've received it.
    print(f"[{timestamp}] LOG RECEIVED: App='{activity.application_name}', Title='{activity.window_title}'")

    # Send a success message back.
    return {"status": "success", "message": "Log received"}

# Add this to the bottom of backend/main.py

@app.get("/activities")
async def get_activities():
    """This endpoint retrieves the last 20 activity logs from the database."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # Get the 20 most recent entries
        cur.execute("SELECT timestamp, app_name, window_title FROM activity_logs ORDER BY timestamp DESC LIMIT 20")
        activities = cur.fetchall()
        cur.close()
        conn.close()
        # Format the data nicely for the frontend
        return [
            {"timestamp": row[0], "app_name": row[1], "window_title": row[2]}
            for row in activities
        ]
    except Exception as e:
        print(f"Database Error: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")