import { useEffect, useState } from 'react';
import TradingSystemHistory from '../components/trading_system_components/trading_system_history/TradingSystemHistory';
import PageNavigation from '../components/PageNavigation/PageNavigation';
import Select from '../components/Select/Select';
import PositionHistory from '../components/TradingSystem/PositionHistory/PositionHistory';
import Position from '../components/TradingSystem/Position/Position';

const url = 'http://localhost:3000/api';

const TradingSystems = () => {
  const [systems, setSystems] = useState([]);
  const [selectedSystem, setSelectedSystem] = useState('');
  // const [selectedSystemName, setSelectedSystemName] = useState('');
  const [marketLists, setMarketLists] = useState([]);
  const [selectedMarketList, setSelectedMarketList] = useState('');
  const [instruments, setInstruments] = useState([]);
  const [selectedInstrument, setSelectedInstrument] = useState('')
  const [marketState, setMarketState] = useState(null);
  // const [marketStates, setMarketStates] = useState([]);
  const [positions, setPositions] = useState([]);
  const [selectedPosition, setSelectedPosition] = useState(null);

  useEffect(() => {
    getSystemsData();
    getMarketListsData();
  }, []);

  const getSystemsData = async () => {
    await fetch(
      `${url}/systems`,
      { method: 'GET' },
    )
      .then((res) => res.json())
      .then((data) => setSystems(data))
      .catch((err) => console.log(err.message));
  }

  const getMarketListsData = async () => {
    await fetch(
      `${url}/market-lists`,
      { method: 'GET' },
    )
      .then((res) => res.json())
      .then((data) => setMarketLists(data))
      .catch((err) => console.log(err.message));
  }

  const getInstrumentsData = async (marketListId) => {
    await fetch(
      `${url}/instruments?id=${marketListId}`,
      { method: 'GET' },
    )
      .then((res) => res.json())
      .then((data) => setInstruments(data['market_list_instruments']))
      .catch((err) => console.log(err.message));
  }

  const getMarketStateData = async (systemId, symbol) => {
    await fetch(
      `${url}/systems/market-state?id=${systemId}&symbol=${symbol}`,
      { method: 'GET' },
    )
      .then((res) => res.json())
      .then((data) => setMarketState(data))
      .catch((err) => console.log(err.message));
  }

  // const getMarketStatesData = async (systemId) => {
  //   await fetch(
  //     `${url}/systems/market-states?id=${systemId}`,
  //     { method: 'GET' },
  //   )
  //     .then((res) => res.json())
  //     .then((data) => setMarketStates(data))
  //     .catch((err) => console.log(err.message));
  // }

  const getPositionsData = async (systemId, symbol) => {
    await fetch(
      `${url}/systems/positions?id=${systemId}&symbol=${symbol}`,
      { method: 'GET' },
    )
      .then((res) => res.json())
      .then((data) => setPositions(data['position_list_json']))
      .catch((err) => console.log(err.message));

  }

  const systemSelected = async (systemId) => {
    setSelectedSystem(systemId);
    // setSelectedSystemName(systemName);
    setSelectedMarketList('');
    setInstruments([]);
    setSelectedInstrument('');
    setMarketState(null);
    setPositions([]);
    setSelectedPosition(null);
    // await getMarketStatesData(systemId);
  }

  const handleMarketListSelection = async (value) => {
    setSelectedMarketList(value);
    await getInstrumentsData(value);
  }

  const handleInstrumentSelection = async (value) => {
    setSelectedPosition(null);
    setSelectedInstrument(value);
    await getMarketStateData(selectedSystem, value);
    await getPositionsData(selectedSystem, value);
  }

  return (
    <main className="trading-systems-container">
      {
        systems && <PageNavigation items={systems} selectedItemCallback={systemSelected} />
      }
      {
        selectedSystem &&
        <>
          <div className="trading-systems-wrapper">
            <div className="trading-systems-selector-wrapper">
              <Select
                id="market-list-select"
                name="market-list-select"
                label="Select Market List"
                value={selectedMarketList}
                valueKey="_id"
                onChange={handleMarketListSelection}
                options={marketLists}
                textKey="market_list"
              />
              <Select
                disabled={!selectedMarketList}
                id="instrument-select"
                name="instrument-select"
                label="Select Instrument"
                value={selectedInstrument}
                valueKey="symbol"
                onChange={handleInstrumentSelection}
                options={instruments}
                textKey="instrument"
              />
            </div>
            <div className="trading-systems-history-wrapper">
              {
                positions && positions.length > 0 &&
                <TradingSystemHistory tradingSystemName={selectedSystem} positions={positions} marketState={marketState} />
              }
            </div>
          </div>

          <div className="trading-systems-lower-container">
            {
              positions && positions.length > 0 &&
              <PositionHistory positions={positions} positionSelected={setSelectedPosition} />
            }
            {
              <Position position={selectedPosition} />
            }
          </div>
        </>
      }
    </main>
  );
}

export default TradingSystems;