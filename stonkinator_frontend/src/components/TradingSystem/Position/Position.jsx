import { useEffect, useRef } from 'react';
import Chart from 'chart.js/auto';

import './Position.css';

function Position({ position }) {
  const chartRef = useRef(null);

  useEffect(() => {
    if (position && position.returns_list && position.returns_list.length > 0) {
      const datasetColor = position.returns_list.map(value => (value >= 0 ? '#00428a' : '#a8324b'));
      const datasetBorderColor = position.returns_list.map(value => (value >= 0 ? '#007bff' : '#ff6384'));

      const data = {
        labels: position.returns_list.map((_, index) => index + 1),
        datasets: [
          {
            label: 'Return (%)',
            data: position.returns_list,
            backgroundColor: datasetColor,
            borderColor: datasetBorderColor,
            borderWidth: 2
          },
        ],
      };

      const options = {
        scales: {
          x: {
            type: 'linear',
            position: 'bottom',
            title: {
              display: true,
              text: 'Period',
              color: '#007bff'
            },
            ticks: {
              color: '#b8b8b8'
            }
          },
          y: {
            title: {
              display: true,
              text: 'Return (%)',
              color: '#007bff'
            },
            ticks: {
              color: '#b8b8b8'
            },
            type: 'linear',
            beginAtZero: true,
          }
        },
        plugins: {
          legend: {
            display: false
          },
          title: {
            display: true,
            text: 'Return/Period',
            font: {
              size: 16,
            },
            color: '#b8b8b8'
          }
        }
      };

      const ctx = chartRef.current.getContext('2d');
      const chart = new Chart(ctx, {
        type: 'bar',
        data,
        options,
      });

      return () => {
        chart.destroy();
      };
    }
  }, [position]);

  return (
    <div className="latest-position-wrapper">
      {position && (
        <>
          <h4>Position</h4>
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
