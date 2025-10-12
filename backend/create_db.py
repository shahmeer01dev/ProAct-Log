# In backend/create_db.py
import sqlite3

conn = sqlite3.connect("activity_logs.db")
cur = conn.cursor()

# Create the table if it doesn't already exist
cur.execute("""
CREATE TABLE IF NOT EXISTS activity_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    app_name TEXT,
    window_title TEXT
)
""")

conn.commit()
conn.close()

print("Database and table created successfully.")