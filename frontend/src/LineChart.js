import React, { useState, useEffect } from 'react';
import { Line } from 'react-chartjs-2';
import { Chart as ChartJS, LineElement, CategoryScale, LinearScale, PointElement, Tooltip, Legend } from 'chart.js';

ChartJS.register(LineElement, CategoryScale, LinearScale, PointElement, Tooltip, Legend);

function LineChart() {
  const [chartData, setChartData] = useState(null);

  useEffect(() => {
    async function fetchData() {
      try {
        const res = await fetch('http://127.0.0.1:8000/activities');
        const activities = await res.json();

        const countsPerHour = new Array(24).fill(0);
        activities.forEach(({ timestamp }) => {
          const hour = new Date(timestamp).getHours();
          countsPerHour[hour]++;
        });

        const cumulativeCounts = countsPerHour.reduce((acc, val, idx) => {
          if (idx === 0) acc.push(val);
          else acc.push(acc[idx - 1] + val);
          return acc;
        }, []);

        setChartData({
          labels: Array.from({ length: 24 }, (_, i) => `${i}:00`),
          datasets: [{
            label: 'Cumulative Activity Count',
            data: cumulativeCounts,
            borderColor: 'rgba(75,192,192,1)',
            fill: false,
            tension: 0.3
          }]
        });
      } catch (error) {
        console.error("LineChart fetch error:", error);
      }
    }
    fetchData();
  }, []);

  if (!chartData) return <p>Loading Line Chart...</p>;

  return (
    <div style={{ width: '600px', marginBottom: '30px' }}>
      <h3>Cumulative Activity Over 24 Hours</h3>
      <Line data={chartData} options={{ responsive: true, plugins: { legend: { position: 'top' } } }} />
    </div>
  );
}

export default LineChart;