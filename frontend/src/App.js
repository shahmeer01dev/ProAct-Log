import React, { useState, useEffect } from 'react';
import './App.css';
import ProductivityChart from './ProductivityChart';
import BarChart from './BarChart';
import LineChart from './LineChart';

// Base URL constant for easier maintenance
const API_BASE_URL = 'http://localhost:8000'; // <--- CHANGED FROM 127.0.0.1

function App() {
  const [activities, setActivities] = useState([]);
  const [error, setError] = useState('');
  const [summaryData, setSummaryData] = useState(null);
  const [activityByHourData, setActivityByHourData] = useState(null);
  const [activityByDayData, setActivityByDayData] = useState(null);

  const [selectedCategories, setSelectedCategories] = useState([
    'Development',
    'Communication',
    'Browsing',
    'Browsing (YouTube)',
    'Uncategorized',
  ]);

  const fetchActivities = async () => {
    try {
      // Fetches from http://localhost:8000/activities
      const response = await fetch(`${API_BASE_URL}/activities`);
      if (!response.ok) throw new Error('Backend server error.');
      const data = await response.json();
      setActivities(data);
      setError('');
    } catch {
      setError('Could not connect to backend.');
    }
  };

  const fetchSummary = async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/productivity-summary`);
      const data = await response.json();
      setSummaryData(data);
    } catch {}
  };

const fetchActivityByHour = async () => {
    try {
      // CHANGE: Renamed endpoint to match the backend's /activity-trend-by-hour
      const response = await fetch(`${API_BASE_URL}/activity-trend-by-hour`); 
      const data = await response.json();
      setActivityByHourData(data);
    } catch {}
  };

  const fetchActivityByDay = async () => {
    try {
      // CHANGE: Renamed endpoint to match the backend's /activity-count-by-app 
      // (assuming this is the intended dashboard endpoint based on structure)
      const response = await fetch(`${API_BASE_URL}/activity-count-by-app`); 
      const data = await response.json();
      setActivityByDayData(data);
    } catch {}
  };

  useEffect(() => {
    fetchActivities();
    fetchSummary();
    fetchActivityByHour();
    fetchActivityByDay();

    const interval = setInterval(() => {
      fetchActivities();
      fetchSummary();
      fetchActivityByHour();
      fetchActivityByDay();
    }, 10000);

    return () => clearInterval(interval);
  }, []);

  // ... (rest of the component logic remains the same)

  function toggleCategory(cat) {
    setSelectedCategories(prev =>
      prev.includes(cat)
        ? prev.filter(c => c !== cat)
        : [...prev, cat]
    );
  }

  const filteredSummaryData = {};
  if (summaryData) {
    Object.entries(summaryData).forEach(([cat, count]) => {
      if (selectedCategories.includes(cat)) filteredSummaryData[cat] = count;
    });
  }

  return (
    <div className="App">
      <header className="App-header">
        <h1>ProAct Log Dashboard</h1>
        <p>A real-time view of your recent device activity.</p>
      </header>

      <main className="dashboard-container">

        <section className="left-panel">
          <div className="filter-container">
            <h3>Filter Categories:</h3>
            {['Development', 'Communication', 'Browsing', 'Browsing (YouTube)', 'Uncategorized'].map(cat => (
              <label key={cat} className="checkbox-label">
                <input
                  type="checkbox"
                  checked={selectedCategories.includes(cat)}
                  onChange={() => toggleCategory(cat)}
                />
                {cat}
              </label>
            ))}
          </div>

          <div className="charts-container">
            <ProductivityChart data={filteredSummaryData} />
            <BarChart data={activityByHourData} />
            <LineChart data={activityByDayData} />
          </div>
        </section>

        <section className="right-panel">
          <h2 className="activity-heading">Recent Activity Log</h2>
          {error ? (
            <p className="error-message">{error}</p>
          ) : (
            <div className="activity-list">
              {activities.map((activity, index) => (
                <div
                  key={index}
                  className={`activity-card ${activity.category.replace(/[\s()]/g, '-')}`}
                >
                  <div className="card-header">
                    <p className="app-name">{activity.app_name}</p>
                    <span
                      className={`category-badge ${activity.category.replace(/[\s()]/g, '-')}`}
                    >
                      {activity.category}
                    </span>
                  </div>
                  <p className="window-title">{activity.window_title}</p>
                  <p className="timestamp">
                    {new Date(activity.timestamp).toLocaleString()}
                  </p>
                </div>
              ))}
            </div>
          )}
        </section>

      </main>
    </div>
  );
}

export default App;