import React, { useEffect, useState } from 'react';
import Card from 'react-bootstrap/Card';

const url = 'http://localhost:3000/api';

const TradingSystems = () => {
  const [systems, setSystems] = useState([]);
  const [marketLists, setMarketLists] = useState([]);
  const [instruments, setInstruments] = useState([]);
  const [marketState, seteMarketState] = useState(null);
  const [marketStates, setMarketStates] = useState([]);
  const [position, setPosition] = useState([]);
  const [positions, setPositions] = useState([]);

  useEffect(() => {
    getSystemsData();
    // getMarketListsData();
  }, []);

  const getSystemsData = async () => {
    await fetch(
      `${url}/systems`,
      {
        method: 'GET'
      },
    )
      .then((res) => res.json())
      .then((data) => setSystems(data.result))
      .catch((err) => console.log(err.message));
  }

  const getMarketListsData = async () => {
    const response = await fetch(`${url}/market-lists`);
    setMarketLists(response.data.result);
  }

  const getInstrumentsData = async (marketListId) => {
    const response = await fetch(`${url}/instruments/${marketListId}`);
    setInstruments(response.data.result);
  }

  const getMarketStateData = async (systemId, symbol) => {
    const response = await fetch(`${url}/systems/market-state?systemId=${systemId}&symbol=${symbol}`);
    console.log(response.data.result);
    setMarketState(response.data.result);
  }

  const getMarketStatesData = async (systemId) => {
    const response = await fetch(`${url}/systems/market-states/${systemId}`);
    setMarketStates(response.data.result);
  }

  const getPositionsData = async (systemId, symbol) => {
    const response = await fetch(`${url}/systems/positions?systemId=${systemId}&symbol=${symbol}`);
    setPositions(response.data.result.position_list_json);
  }

  return (
    <>
    </>
  );
}

export default TradingSystems;