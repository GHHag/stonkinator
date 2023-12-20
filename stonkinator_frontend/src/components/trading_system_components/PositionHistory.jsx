import React, { useEffect, useState } from 'react';
import Card from 'react-bootstrap/Card';

const PositionHistory = ({positions}) => {
  const [formattedPositions, setFormattedPositions] = useState({});
  console.log(formattedPositions);

  const formatPositions = (positions) => {
    let formattedPositions = {};

    positions.map((position) => {
      let entry_dt = new Date(position.entry_dt)
    
      if (!formattedPositions.hasOwnProperty(entry_dt.getFullYear())) {
        formattedPositions[entry_dt.getFullYear()] = [];
      }

      formattedPositions[entry_dt.getFullYear()].push(position);
    });

    setFormattedPositions(formattedPositions);
  }

  useEffect(() => {
    formatPositions(positions);
  }, []);

  return (
    <Card>

    </Card>
  );
}

export default PositionHistory;