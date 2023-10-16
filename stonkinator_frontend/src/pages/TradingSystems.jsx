import React, { useEffect, useState } from 'react';
import Card from 'react-bootstrap/Card';
import SideBar from '../components/SideBar';
import TradingSystemHistory from '../components/TradingSystemHistory';
import PositionHistory from '../components/PositionHistory';

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
  const [position, setPosition] = useState([]);
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
      .then((data) => setSystems(data.result))
      .catch((err) => console.log(err.message));
  }

  const getMarketListsData = async () => {
    await fetch(
      `${url}/market-lists`,
      { method: 'GET' },
    )
      .then((res) => res.json())
      .then((data) => setMarketLists(data.result))
      .catch((err) => console.log(err.message));
  }

  const getInstrumentsData = async (marketListId) => {
    await fetch(
      `${url}/instruments/${marketListId}`,
      { method: 'GET' },
    )
      .then((res) => res.json())
      .then((data) => setInstruments(data.result[0])) // FIX RETURN TO NOT BE ARRAY AND REMOVE 0 index when setInstruments
      .catch((err) => console.log(err.message));
  }

  const getMarketStateData = async (systemId, symbol) => {
    await fetch(
      `${url}/systems/market-state?systemId=${systemId}&symbol=${symbol}`,
      { method: 'GET' },
    )
      .then((res) => res.json())
      .then((data) => setMarketState(data.result))
      .catch((err) => console.log(err.message));
  }

  const getMarketStatesData = async (systemId) => {
    await fetch(
      `${url}/systems/market-states/${systemId}`,
      { method: 'GET' },
    )
      .then((res) => res.json())
      .then((data) => setMarketStates(data.result))
      .catch((err) => console.log(err.message));
  }

  const getPositionsData = async (systemId, symbol) => {
    await fetch(
      `${url}/systems/positions?systemId=${systemId}&symbol=${symbol}`,
      { method: 'GET' },
    )
      .then((res) => res.json())
      .then((data) => setPositions(data.result))
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
    <>
      {
        systems && 
        <SideBar sideBarContent={systems} itemKey={'name'} selectedItemCallback={systemSelected}/>
      }

      {
        selectedSystem &&
        <Card>
          <div>
            <select value={selectedMarketList} onChange={handleSelectMarketList}>
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
            <select value={selectedInstrument} onChange={handleSelectInstrument}>
              <option value="" disabled>Select Instrument</option>
              {
                instruments.market_list_instruments &&
                instruments.market_list_instruments.map((item, index) => (
                  <option key={index} value={item.symbol}>
                    {item.symbol.replace(/_/g, ' ').toUpperCase()}
                  </option>
                ))
              }
            </select>
          </div>
        </Card>
      }

      {
        marketState &&
        <Card>
            <div>
              {/* {console.log(marketState)} */}
            </div>
        </Card>
      }

      {
        positions.position_list_json &&
        <PositionHistory positions={positions.position_list_json}/>
      }

      {
        positions.position_list_json &&
        <TradingSystemHistory positions={positions.position_list_json}/>
      }
    </>
  );
}

export default TradingSystems;