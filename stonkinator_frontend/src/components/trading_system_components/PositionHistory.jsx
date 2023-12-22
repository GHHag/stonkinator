import React, { useEffect, useState } from 'react';
import { Card, Col, Row } from 'react-bootstrap';

const PositionHistory = ({ positions, positionSelected }) => {
  const [formattedPositions, setFormattedPositions] = useState(null);
  const [expandedYears, setExpandedYears] = useState([]);

  useEffect(() => {
    let formattedPositions = {};

    positions.map((position) => {
      let entry_dt = new Date(position.entry_dt)

      if (!formattedPositions.hasOwnProperty(entry_dt.getFullYear())) {
        formattedPositions[entry_dt.getFullYear()] = [];
      }

      formattedPositions[entry_dt.getFullYear()].push(position);
    });

    const sortedYears = Object.keys(formattedPositions).sort((a, b) => b - a);
    const sortedFormattedPositions = new Map();
    sortedYears.forEach((year) => {
      sortedFormattedPositions.set(year, formattedPositions[year]);
    });

    setFormattedPositions(sortedFormattedPositions);
  }, [positions]);

  const handleYearClick = (year) => {
    setExpandedYears((prevYears) =>
      prevYears.includes(year) ? prevYears.filter((x) => x !== year) : [...prevYears, year]
    );
  };

  return (
    <Card style={{ color: '#b8b8b8', maxHeight: '100%', backgroundColor: '#1a1c1f', textAlign: 'center', border: 'none' }}>
      <h5>Position History</h5>
      <div className='position-history-year-list'>
        {
          formattedPositions &&
          <Row style={{ padding: 10 }}>
            <Col style={{ fontWeight: '600', color: '#007bff' }}>Year</Col>
            {
              expandedYears.length > 0 ? <Col style={{ fontWeight: 600, color: '#007bff' }}>Date</Col> : <Col></Col>
            }
            {
              expandedYears.length > 0 ? <Col style={{ fontWeight: 600, color: '#007bff' }}>Return</Col> : <Col></Col>
            }
          </Row>
        }
        {
          formattedPositions &&
          Array.from(formattedPositions.keys()).map((year, index) => (
            <div key={year} className={index % 2 == 0 ? 'position-history-year-wrapper-even' : 'position-history-year-wrapper-uneven'}>
              <Row style={{ padding: 5 }}>
                <Col onClick={() => handleYearClick(year)} style={{ cursor: 'pointer', fontWeight: 500 }}>{year}</Col>
                <Col></Col>
                <Col></Col>
              </Row>

              {
                expandedYears.includes(year) && (
                  <div>
                    {
                      formattedPositions.get(year).map((position, index) => (
                        <Row key={index} onClick={() => positionSelected(position)}>
                          <Col></Col>
                          <Col>{new Date(position.entry_dt).toISOString().split('T')[0]}</Col>
                          <Col>{position.position_return}%</Col>
                        </Row>
                      ))
                    }
                  </div>
                )
              }
            </div>
          ))
        }
      </div>
    </Card>
  );
}

export default PositionHistory;