import React, { useEffect, useState } from 'react';
import Card from 'react-bootstrap/Card';

const TradingSystemHistory = ({ tradingSystemName, positions, marketState }) => {
  console.log('TradingSystemHistory', positions);
  console.log('TradingSystemHistory', marketState);

  return (
    <Card className="trading-system-history-container" style={{ backgroundColor: '#1a1c1f' }}>
      {
        <h5>Trading System History - {tradingSystemName}</h5>
      }
    </Card>
  );
}

export default TradingSystemHistory;