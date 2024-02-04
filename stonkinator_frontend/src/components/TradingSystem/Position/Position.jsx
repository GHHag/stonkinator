import { useEffect, useRef } from 'react';
import { Bar } from 'react-chartjs-2';
import Chart from 'chart.js/auto';

import './Position.css';

function Position({ position }) {
  const chartRef = useRef(null);

  useEffect(() => {
    // Create the chart when the component mounts
    if (position && position.returns_list && position.returns_list.length > 0) {
      const data = {
        labels: position.returns_list.map((_, index) => index + 1),
        datasets: [
          {
            label: 'Return',
            data: position.returns_list,
            backgroundColor: '#007bff',
          },
        ],
      };

      const options = {
        scales: {
          y: {
            type: 'linear',
            beginAtZero: true,
          },
        },
      };

      const ctx = chartRef.current.getContext('2d');
      const chart = new Chart(ctx, {
        type: 'bar',
        data,
        options,
      });

      return () => {
        // Destroy the chart when the component unmounts
        chart.destroy();
      };
    }
  }, [position]);

  return (
    <div className="latest-position-wrapper">
      {position && (
        <>
          <h5>Position</h5>
          {position.returns_list && position.returns_list.length > 0 && (
            <div style={{ marginTop: '20px' }}>
              <canvas ref={chartRef} />
            </div>
          )}
        </>
      )}
    </div>
  );
}

export default Position;
