import React, { useEffect, useState } from 'react';
import { Card, Col, Row } from 'react-bootstrap';

const LatestPosition = ({ position }) => {
  console.log('LatestPosition', position);

  return (
    <Card className="latest-position-container" style={{ color: '#b8b8b8', maxHeight: '100%', backgroundColor: '#1a1c1f', textAlign: 'center', border: 'none' }}>
      <h5>Position</h5>
      <div>
        {
          position &&
          <div>
            <Row>
              <Col>Entry Date: {new Date(position.entry_dt).toISOString().split('T')[0]}</Col>
            </Row>
            <Row>
              <Col>Exit Date: {new Date(position.exit_signal_dt).toISOString().split('T')[0]}</Col>
            </Row>
            <Row>
              <Col>Gross Result: {position.gross_result}</Col>
            </Row>
            <Row>
              <Col>Net Result: {position.net_result}</Col>
            </Row>
          </div>
        }
      </div>
    </Card>
  );
}

export default LatestPosition;