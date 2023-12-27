import React, { useEffect, useRef } from 'react';
import Card from 'react-bootstrap/Card';
import Chart from 'chart.js/auto';

const Position = ({ position }) => {
  const chartRef = useRef(null);

  useEffect(() => {
    const datasetColor = position.returns_list.map(value => (value >= 0 ? '#00428a' : '#a8324b'));
    const datasetBorderColor = position.returns_list.map(value => (value >= 0 ? '#007bff' : '#ff6384'));

    if (position && position.returns_list && position.returns_list.length > 0) {
      const data = {
        labels: position.returns_list.map((_, index) => index + 1),
        datasets: [
          {
            data: position.returns_list,
            backgroundColor: datasetColor,
            borderColor: datasetBorderColor,
            borderWidth: 2
          }
        ]
      };

      const options = {
        scales: {
          x: {
            type: 'linear',
            position: 'bottom',
            title: {
              display: true,
              text: 'Period',
              color: '#b8b8b8'
            },
            ticks: {
              color: '#b8b8b8'
            }
          },
          y: {
            title: {
              display: true,
              text: 'Return (%)',
              color: '#b8b8b8'
            },
            ticks: {
              color: '#b8b8b8'
            }
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
        options
      });

      return () => {
        chart.destroy();
      };
    }
  }, [position]);

  return (
    <Card style={{ color: '#b8b8b8', backgroundColor: '#1a1c1f', textAlign: 'center', border: 'none' }}>
      {
        position &&
        <>
          <h5>Position</h5>
          {
            position.returns_list && position.returns_list.length > 0 && (
              <div>
                <canvas ref={chartRef} />
              </div>
            )
          }
        </>
      }
    </Card>
  );
};

export default Position;
