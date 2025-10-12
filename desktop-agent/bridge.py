# In desktop-agent/bridge.py

import requests
import time

# --- Configuration ---
AW_BUCKET_ID = "aw-watcher-window_SN" # Make sure this is still correct!
AW_API_URL = f"http://localhost:5600/api/0/buckets/{AW_BUCKET_ID}/events"
BACKEND_API_URL = "http://127.0.0.1:8000/log_activity" # The URL of your new backend

print("Starting bridge to ActivityWatch... Press Ctrl+C to stop.")

def get_latest_activity():
    """Fetches the most recent activity log from the ActivityWatch API."""
    try:
        response = requests.get(AW_API_URL, params={"limit": 1})
        response.raise_for_status()
        events = response.json()
        if events:
            latest_event = events[0]
            app = latest_event['data']['app']
            title = latest_event['data']['title']
            return app, title
        return None, None
    except requests.exceptions.RequestException:
        print("Error: Could not connect to ActivityWatch API. Is it running?")
        return None, None

# --- NEW FUNCTION ---
def send_activity_to_backend(app_name, window_title):
    """Sends the activity log to our FastAPI backend."""
    try:
        # Create the JSON payload that our backend expects
        payload = {
            "application_name": app_name,
            "window_title": window_title
        }
        # Send the data using an HTTP POST request
        response = requests.post(BACKEND_API_URL, json=payload)
        response.raise_for_status()
        print(f"Log sent successfully: {app_name}")
    except requests.exceptions.RequestException:
        print("Error: Could not send log to backend. Is the backend server running?")

# --- Main Loop (Updated) ---
last_title = ""
while True:
    app_name, window_title = get_latest_activity()

    if window_title and window_title != last_title:
        # Instead of printing, we now send the data to the backend
        send_activity_to_backend(app_name, window_title)
        last_title = window_title

    time.sleep(10)