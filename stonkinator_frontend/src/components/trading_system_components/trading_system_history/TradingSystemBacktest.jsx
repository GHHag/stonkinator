import React, { useEffect, useRef } from 'react';
import { Chart } from 'chart.js/auto';

const TradingSystemBacktest = ({ tsReturnsHistory }) => {
  const chartRef = useRef(null);

  useEffect(() => {
    if (chartRef.current) {
      const cumulativeReturns = tsReturnsHistory.reduce((acc, value) => {
        const cumulativeReturn = acc.length === 0 ? 100 + value : acc[acc.length - 1] * (1 + value / 100);
        acc.push(cumulativeReturn);
        return acc;
      }, []);

      const data = {
        labels: Array.from({ length: tsReturnsHistory.length }, (_, i) => i + 1),
        datasets: [
          {
            label: 'Return (%)',
            data: cumulativeReturns,
            fill: true,
            backgroundColor: '#00428a',
            borderColor: '#007bff'
          }
        ]
      }

      const options = {
        scales: {
          x: {
            type: 'linear',
            position: 'bottom',
            ticks: {
              color: '#b8b8b8'
            },
            title: {
              display: true,
              text: 'Period',
              color: '#007bff'
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
            }
          }
        },
        plugins: {
          tooltip: {
            intersect: false
          },
          title: {
            display: true,
            text: 'Trading System Return',
            font: {
              size: 16,
            },
            color: '#b8b8b8'
          },
          legend: {
            labels: {
              color: '#b8b8b8',
            }
          }
        }
      }

      const ctx = chartRef.current.getContext('2d');
      const chart = new Chart(ctx, {
        type: 'line',
        data,
        options
      })

      return () => {
        chart.destroy();
      }
    }
  }, [tsReturnsHistory]);

  return (
    <canvas ref={chartRef} />
  );
};

export default TradingSystemBacktest;
