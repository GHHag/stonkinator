import React, { useEffect, useState } from 'react';
import Card from 'react-bootstrap/Card';
import SideBar from '../components/SideBar';
import TradingSystemHistory from '../components/trading_system_components/TradingSystemHistory';
import PositionHistory from '../components/trading_system_components/PositionHistory';

const url = 'http://localhost:3000/api';

const TradingSystems = () => {
  const [systems, setSystems] = useState([]);
  const [selectedSystem, setSelectedSystem] = useState('');
  const [marketLists, setMarketLists] = useState([]);
  const [selectedMarketList, setSelectedMarketList] = useState('');
  const [instruments, setInstruments] = useState([]);
  const [selectedInstrument, setSelectedInstrument] = useState('')
  const [marketState, setMarketState] = useState(null);
  const [marketStates, setMarketStates] = useState([]);
  const [positions, setPositions] = useState([]);

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

  const getMarketStatesData = async (systemId) => {
    await fetch(
      `${url}/systems/market-states?id=${systemId}`,
      { method: 'GET' },
    )
      .then((res) => res.json())
      .then((data) => setMarketStates(data))
      .catch((err) => console.log(err.message));
  }

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
    await getMarketStatesData(systemId);
  }

  const handleSelectMarketList = async (event) => {
    setSelectedMarketList(event.target.value);
    await getInstrumentsData(event.target.value);
  }

  const handleSelectInstrument = async (event) => {
    setSelectedInstrument(event.target.value);
    await getMarketStateData(selectedSystem, event.target.value);
    await getPositionsData(selectedSystem, event.target.value);
  }

  return (
    <main className="trading-systems-container">
      {
        systems &&
        <SideBar sideBarContent={systems} itemKey={'name'} selectedItemCallback={systemSelected} />
      }
      <Card className="trading-systems-card">
        <Card.Body>
          {
            selectedSystem &&
            <div className="custom-select-wrapper">
              <div>
                <select value={selectedMarketList} onChange={handleSelectMarketList} className="custom-select">
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
                <select value={selectedInstrument} disabled={!selectedMarketList} onChange={handleSelectInstrument} className="custom-select">
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
            positions &&
            <TradingSystemHistory positions={positions} />
          }

          {
            positions &&
            <PositionHistory positions={positions} />
          }
        </Card.Body>
      </Card>
    </main>
  );
}

export default TradingSystems;