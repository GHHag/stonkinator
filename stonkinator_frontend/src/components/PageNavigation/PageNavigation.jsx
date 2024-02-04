import { useState } from 'react';

import './PageNavigation.css';

function PageNavigation({ items, selectedItemCallback }) {
  const [selectedItem, setSelectedItem] = useState(null);

  const handleSelectedItem = (itemId) => {
    setSelectedItem(itemId);
    selectedItemCallback(itemId);
  };

  return (
    <div className="page-navigation">
      {
        items.length && items.map((item) => (
          <div 
            key={item._id}
            className={`page-navigation-item ${selectedItem === item._id ? 'page-navigation-item--selected' : ''}`}
            onClick={() => handleSelectedItem(item._id)}>
            { item['name'].replace(/_/g, ' ').toUpperCase() }
          </div>
        ))
      }
    </div>
  );
}

export default PageNavigation;