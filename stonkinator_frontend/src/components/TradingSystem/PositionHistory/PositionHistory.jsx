import { useEffect, useState } from 'react';

import './PositionHistory.css';

function PositionHistory({ positions, positionSelected }) {
  const [formattedPositions, setFormattedPositions] = useState(null);
  const [selectedYear, setSelectedYear] = useState('');

  useEffect(() => {
    let formattedPositions = {};

    positions.map((position) => {
      let entry_dt = new Date(position.entry_dt)

      if (!formattedPositions.hasOwnProperty(entry_dt.getFullYear())) {
        formattedPositions[entry_dt.getFullYear()] = [];
      }

      formattedPositions[entry_dt.getFullYear()].push(position);
    });

    const sortedYears = Object.keys(formattedPositions).sort((a, b) => b - a);
    const sortedFormattedPositions = new Map();
    sortedYears.forEach((year) => {
      sortedFormattedPositions.set(year, formattedPositions[year]);
    });

    setFormattedPositions(sortedFormattedPositions);
  }, [positions]);

  const handleYearClick = (year) => {
    if (year === selectedYear) {
      setSelectedYear('');
      return;
    }

    setSelectedYear(year);
  }

  return (
    <div className="position-history-wrapper">
      <h5 className="position-history-title">Position History</h5>
      <div className="position-history-list-wrapper">
        {
          formattedPositions && Array.from(formattedPositions.keys()).map((key) => (
            <div key={key} className="position-history-list-item" onClick={() => handleYearClick(key)}>
              <div className={`position-history-list-item-title ${selectedYear === key && 'position-history-list-item-title--selected'}`}>{key}</div>
              <div className={`position-history-list-item-content ${selectedYear === key && 'position-history-list-item-content--expanded'}`}>
                {
                  formattedPositions.get(key).map((position, index) => (
                    <div key={index} className="position-history-list-item-position" onClick={() => positionSelected(position)}>
                      <div>{new Date(position.entry_dt).toISOString().split('T')[0]}</div>
                      <div>{position.position_return}%</div>
                    </div>
                  ))
                }
              </div>
            </div>
          ))
        }
      </div>
    </div>
  );
}

export default PositionHistory;