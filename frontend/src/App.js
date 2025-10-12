// In frontend/src/App.js

import React, { useState, useEffect } from 'react';
import './App.css';

function App() {
  // 'useState' creates variables to store the list of activities and any errors
  const [activities, setActivities] = useState([]);
  const [error, setError] = useState('');

  // This function fetches data from your backend API
  const fetchActivities = async () => {
    try {
      const response = await fetch('http://127.0.0.1:8000/activities');
      if (!response.ok) {
        throw new Error('Backend server is not responding correctly.');
      }
      const data = await response.json();
      setActivities(data); // Update the state with the new data
      setError(''); // Clear any previous errors
    } catch (error) {
      console.error("Failed to fetch activities:", error);
      setError('Could not connect to the backend. Is the server running?');
    }
  };

  // 'useEffect' runs code when the app loads
  useEffect(() => {
    fetchActivities(); // Fetch data immediately

    // Set up an interval to automatically refresh the data every 10 seconds
    const interval = setInterval(fetchActivities, 10000);

    // This is a cleanup function that stops the interval when the app closes
    return () => clearInterval(interval);
  }, []); // The empty array [] means this runs only once on mount

  return (
    <div className="App">
      <header className="App-header">
        <h1>ProAct Log Dashboard</h1>
        <p>A real-time view of your recent device activity.</p>
      </header>
      <main className="activity-container">
        {/* If there's an error, display it. Otherwise, show the activity list. */}
        {error ? (
          <p className="error-message">{error}</p>
        ) : (
          <div className="activity-list">
            {/* Loop through the 'activities' array and create a card for each one */}
            {activities.map((activity, index) => (
              <div key={index} className="activity-card">
                <p className="app-name">{activity.app_name}</p>
                <p className="window-title">{activity.window_title}</p>
                <p className="timestamp">
                  {new Date(activity.timestamp).toLocaleString()}
                </p>
              </div>
            ))}
          </div>
        )}
      </main>
    </div>
  );
}

export default App;