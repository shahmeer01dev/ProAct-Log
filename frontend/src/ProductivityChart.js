// In frontend/src/ProductivityChart.js
import React, { useState, useEffect } from 'react';
import { Pie } from 'react-chartjs-2';
import { Chart as ChartJS, ArcElement, Tooltip, Legend } from 'chart.js';

ChartJS.register(ArcElement, Tooltip, Legend);

function ProductivityChart() {
  const [chartData, setChartData] = useState(null);

  useEffect(() => {
    const fetchSummary = async () => {
      try {
        const response = await fetch('http://localhost:8000/productivity-summary');
        const data = await response.json();

        // Format the data for the pie chart
        const labels = data.map(item => item.category);
        const counts = data.map(item => item.count);

        setChartData({
          labels: labels,
          datasets: [{
            label: 'Activity Count',
            data: counts,
            backgroundColor: [
              'rgba(40, 167, 69, 0.7)',  // Productive (Green)
              'rgba(220, 53, 69, 0.7)',   // Distracting (Red)
              'rgba(108, 117, 125, 0.7)', // Neutral (Gray)
            ],
            borderColor: [
              '#28a745',
              '#dc3545',
              '#6c757d',
            ],
            borderWidth: 1,
          }]
        });
      } catch (error) {
        console.error("Failed to fetch summary:", error);
      }
    };

    fetchSummary();
  }, []);

  if (!chartData) {
    return <p>Loading summary...</p>;
  }

  return (
    <div className="chart-container">
      <h2>Today's Summary</h2>
      <Pie data={chartData} />
    </div>
  );
}

export default ProductivityChart;