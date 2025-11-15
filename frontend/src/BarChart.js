import React, { useState, useEffect } from 'react';
import { Bar } from 'react-chartjs-2';
import { Chart as ChartJS, BarElement, CategoryScale, LinearScale, Tooltip, Legend } from 'chart.js';

ChartJS.register(BarElement, CategoryScale, LinearScale, Tooltip, Legend);

function BarChart() {
  const [chartData, setChartData] = useState(null);

  useEffect(() => {
    async function fetchData() {
      try {
        const res = await fetch('http://127.0.0.1:8000/activities');
        const activities = await res.json();

        const countsPerHour = Array.from({ length: 24 }, () => ({}));

        activities.forEach(({ timestamp, category }) => {
          const date = new Date(timestamp);
          const hour = date.getHours();
          if (!countsPerHour[hour][category]) countsPerHour[hour][category] = 0;
          countsPerHour[hour][category]++;
        });

        const categories = [...new Set(activities.map(a => a.category))];

        const datasets = categories.map((cat, idx) => ({
          label: cat,
          data: countsPerHour.map(hourObj => hourObj[cat] || 0),
          backgroundColor: `hsl(${(idx * 60) % 360}, 70%, 50%)`
        }));

        setChartData({
          labels: Array.from({ length: 24 }, (_, i) => `${i}:00`),
          datasets
        });
      } catch (error) {
        console.error("BarChart fetch error:", error);
      }
    }
    fetchData();
  }, []);

  if (!chartData) return <p>Loading Bar Chart...</p>;

  return (
    <div style={{ width: '600px', marginBottom: '30px' }}>
      <h3>Activity Count Per Hour</h3>
      <Bar data={chartData} options={{ responsive: true, plugins: { legend: { position: 'top' } } }} />
    </div>
  );
}

export default BarChart;