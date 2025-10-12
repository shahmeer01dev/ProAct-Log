# In ProAct-Log/desktop-agent/bridge.py

import requests
import time

# --- IMPORTANT: UPDATE THIS VARIABLE ---
# Find your specific bucket ID from the ActivityWatch API browser.
# It looks like: aw-watcher-window_YOUR-PC-NAME
AW_BUCKET_ID = "aw-watcher-window_SN"
AW_API_URL = f"http://localhost:5600/api/0/buckets/{AW_BUCKET_ID}/events"

print("Starting bridge to ActivityWatch... Press Ctrl+C to stop.")

def get_latest_activity():
    """Fetches the most recent activity log from the ActivityWatch API."""
    try:
        # We only need the single most recent event, so we use ?limit=1
        response = requests.get(AW_API_URL, params={"limit": 1})
        response.raise_for_status()  # This will raise an error if the request fails

        events = response.json()
        if events:
            latest_event = events[0]
            app = latest_event['data']['app']
            title = latest_event['data']['title']
            return app, title
        return None, None

    except requests.exceptions.RequestException:
        print(f"Error: Could not connect to ActivityWatch API.")
        print("Please make sure ActivityWatch is running.")
        return None, None

# --- Main Loop ---
last_title = ""
while True:
    app_name, window_title = get_latest_activity()

    # Only print the activity if it is new and valid
    if window_title and window_title != last_title:
        print(f"New Activity -> App: {app_name}, Title: {window_title}")
        last_title = window_title

    # Wait for 10 seconds before checking again
    time.sleep(10)