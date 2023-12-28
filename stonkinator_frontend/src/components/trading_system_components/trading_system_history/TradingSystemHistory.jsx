import React, { useEffect, useState } from 'react';
import Card from 'react-bootstrap/Card';
import TradingSystemBacktest from './TradingSystemBacktest';
import TradingSystemMetrics from './TradingSystemMetrics';
import TradingSystemState from './TradingSystemState';

const TradingSystemHistory = ({ tradingSystemName, positions, marketState }) => {
  const [returnHistory, setReturnHistory] = useState([]);

  useEffect(() => {
    let posReturns = [];
    for (const pos of positions) {
      posReturns = posReturns.concat(pos.mtm_returns_list)
    }

    setReturnHistory(posReturns);
  }, [positions]);

  return (
    <Card style={{ color: '#b8b8b8', backgroundColor: '#1a1c1f' }}>
      {
        <>
          <h5>Trading System History - {tradingSystemName}</h5>
          {
            returnHistory && returnHistory.length > 0 &&
            <TradingSystemBacktest tsReturnsHistory={returnHistory} />
          }
          <TradingSystemMetrics />
          <TradingSystemState marketState={marketState} />
        </>
      }
    </Card>
  );
}

export default TradingSystemHistory;