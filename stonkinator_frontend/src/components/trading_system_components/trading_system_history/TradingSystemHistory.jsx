import { useEffect, useState } from 'react';
import TradingSystemBacktest from './TradingSystemBacktest';
import TradingSystemMetrics from './TradingSystemMetrics';
import TradingSystemState from './TradingSystemState';

const TradingSystemHistory = ({ tradingSystemName, positions, marketState }) => {
  const [returnHistory, setReturnHistory] = useState([]);

  useEffect(() => {
    const posReturns = positions.flatMap((pos) => pos.mtm_returns_list);

    setReturnHistory(posReturns);
  }, [positions]);

  return (
    <div>
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
    </div>
  );
}

export default TradingSystemHistory;