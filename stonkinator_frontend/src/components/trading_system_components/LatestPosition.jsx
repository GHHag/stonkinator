// import React, { useEffect, useState } from 'react';
// import { Card, Col, Row } from 'react-bootstrap';
// import { Bar } from 'react-chartjs-2';

// const LatestPosition = ({ position }) => {
//   console.log('LatestPosition', position);

//   const chartData = {
//     labels: position?.returns_list.map((_, index) => `Trade ${index + 1}`),
//     datasets: [
//       {
//         label: 'Returns',
//         backgroundColor: 'rgba(75,192,192,0.4)',
//         borderColor: 'rgba(75,192,192,1)',
//         borderWidth: 1,
//         hoverBackgroundColor: 'rgba(75,192,192,0.6)',
//         hoverBorderColor: 'rgba(75,192,192,1)',
//         data: position?.returns_list,
//       },
//     ],
//   };

//   return (
//     <Card className="latest-position-container" style={{ color: '#b8b8b8', maxHeight: '100%', backgroundColor: '#1a1c1f', textAlign: 'center', border: 'none' }}>
//       <h5>Position</h5>
//       <div>
//         {
//           position &&
//           <div>
//             <Row>
//               <Col>Entry Date: {new Date(position.entry_dt).toISOString().split('T')[0]}</Col>
//             </Row>
//             <Row>
//               <Col>Exit Date: {new Date(position.exit_signal_dt).toISOString().split('T')[0]}</Col>
//             </Row>
//             <Row>
//               <Col>Gross Result: {position.gross_result}</Col>
//             </Row>
//             <Row>
//               <Col>Net Result: {position.net_result}</Col>
//             </Row>
//           </div>

//         }
//         {
//           position.returns_list && position.returns_list.length > 0 && (
//             <div style={{ marginTop: '20px' }}>
//               <Bar
//                 data={chartData}
//                 options={{
//                   scales: {
//                     y: {
//                       beginAtZero: true,
//                     },
//                   },
//                 }}
//               />
//             </div>
//           )
//         }
//       </div>
//     </Card>
//   );
// }

// export default LatestPosition;

// import React from 'react';
// import Card from 'react-bootstrap/Card';
// import { Bar } from 'react-chartjs-2';

// const LatestPosition = ({ position }) => {
//   console.log('LatestPosition', position);

//   const chartData = {
//     labels: position?.returns_list.map((_, index) => `Trade ${index + 1}`),
//     datasets: [
//       {
//         label: 'Returns',
//         backgroundColor: 'rgba(75,192,192,0.4)',
//         borderColor: 'rgba(75,192,192,1)',
//         borderWidth: 1,
//         hoverBackgroundColor: 'rgba(75,192,192,0.6)',
//         hoverBorderColor: 'rgba(75,192,192,1)',
//         data: position?.returns_list,
//       },
//     ],
//   };

//   const chartOptions = {
//     scales: {
//       y: {
//         type: 'linear', // explicitly set the scale type
//         beginAtZero: true,
//       },
//     },
//   };

//   return (
//     <Card className="latest-position-container" style={{ color: '#b8b8b8', maxHeight: '100%', backgroundColor: '#1a1c1f', textAlign: 'center', border: 'none' }}>
//       {position && (
//         <>
//           <h5>Position</h5>
//           <p>Entry Date: {new Date(position.entry_dt).toLocaleDateString()}</p>
//           <p>Exit Date: {new Date(position.exit_signal_dt).toLocaleDateString()}</p>
//           <p>Gross Result: {position.gross_result}</p>
//           <p>Net Result: {position.net_result}</p>

//           {position.returns_list && position.returns_list.length > 0 && (
//             <div style={{ marginTop: '20px' }}>
//               <Bar
//                 data={chartData}
//                 options={chartOptions}
//               />
//             </div>
//           )}
//         </>
//       )}
//     </Card>
//   );
// };

// export default LatestPosition;

import React, { useEffect, useRef } from 'react';
import Card from 'react-bootstrap/Card';
import { Bar } from 'react-chartjs-2';
import Chart from 'chart.js/auto';

const LatestPosition = ({ position }) => {
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
    <Card className="latest-position-container" style={{ color: '#b8b8b8', maxHeight: '100%', backgroundColor: '#1a1c1f', textAlign: 'center', border: 'none' }}>
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
    </Card>
  );
};

export default LatestPosition;
