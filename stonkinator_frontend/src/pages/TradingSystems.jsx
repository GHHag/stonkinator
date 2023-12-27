import React, { useEffect, useState } from 'react';
import Card from 'react-bootstrap/Card';
import SideBar from '../components/SideBar';
import TradingSystemHistory from '../components/trading_system_components/trading_system_history/TradingSystemHistory';
import PositionHistory from '../components/trading_system_components/PositionHistory';
import Position from '../components/trading_system_components/Position';

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

  const handleSelectMarketList = async (event) => {
    setSelectedMarketList(event.target.value);
    await getInstrumentsData(event.target.value);
  }

  const handleSelectInstrument = async (event) => {
    setSelectedPosition(null);
    setSelectedInstrument(event.target.value);
    await getMarketStateData(selectedSystem, event.target.value);
    await getPositionsData(selectedSystem, event.target.value);//.then(() => {
    //   // Fix setting state to update correctly
    //   setSelectedPosition(positions[positions.length - 1]);
    //   if (positions.length > 0) {
    //     setSelectedPosition(positions[positions.length - 1]);
    //   }
    //   else {
    //     setSelectedPosition(null);
    //   }
    // });
  }

  return (
    <main className="trading-systems-container">
      {
        systems &&
        <SideBar sideBarContent={systems} itemKey={'name'} selectedItemCallback={systemSelected} />
      }
      {
        selectedSystem &&
        <Card className="trading-systems-card" style={{ backgroundColor: '#1a1c1f' }}>
          <Card.Body>
            <div className="trading-systems-upper-container">
              <div className="trading-system-history-wrapper">
                {
                  selectedSystem &&
                  <div className="custom-select-wrapper">
                    <div>
                      <select
                        value={selectedMarketList}
                        onChange={handleSelectMarketList}
                        className={`custom-select ${selectedMarketList ? 'custom-select-selected' : ''}`}
                      >
                        <option value="" disabled>Select Market List</option>
                        {
                          marketLists &&
                          marketLists.map((item, index) => (
                            <option key={index} value={item._id}>
                              {item.market_list.replace(/_/g, ' ').toUpperCase()}
                            </option>
                          ))
                        }
                      </select>
                    </div>
                    <div>
                      <select
                        value={selectedInstrument}
                        disabled={!selectedMarketList}
                        onChange={handleSelectInstrument}
                        className={`custom-select ${selectedInstrument ? 'custom-select-selected' : ''}`}
                      >
                        <option value="" disabled>Select Instrument</option>
                        {
                          instruments &&
                          instruments.map((item, index) => (
                            <option key={index} value={item.symbol}>
                              {item.symbol.replace(/_/g, ' ').toUpperCase()}
                            </option>
                          ))
                        }
                      </select>
                    </div>
                  </div>
                }
                {
                  positions && positions.length > 0 &&
                  <TradingSystemHistory tradingSystemName={selectedSystem} positions={positions} marketState={marketState} />
                }
              </div>
            </div>
            <div className="trading-systems-lower-container">
              <div className="position-history-wrapper">
                {
                  positions && positions.length > 0 &&
                  <PositionHistory positions={positions} positionSelected={setSelectedPosition} />
                }
              </div>
              <div className="position-wrapper">
                {
                  selectedPosition &&
                  <Position position={selectedPosition} />
                }
              </div>
            </div>
          </Card.Body>
        </Card>
      }
    </main>
  );
}

export default TradingSystems;