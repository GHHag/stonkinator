import React, { useEffect, useState } from 'react';
import Card from 'react-bootstrap/Card';

const LatestPosition = ({marketState}) => {
  console.log(marketState);

  return (
    <Card className="latest-position-container">
      <h5>Latest Position</h5>
    </Card>
  );
}

export default LatestPosition;