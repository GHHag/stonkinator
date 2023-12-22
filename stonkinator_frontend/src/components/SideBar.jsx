import React, { useState } from 'react';

const SideBar = ({ sideBarContent, itemKey, selectedItemCallback }) => {
  const [selectedItem, setSelectedItem] = useState(null);

  const handleSelectedItem = (itemId) => {
    setSelectedItem(itemId);
    selectedItemCallback(itemId);
  }

  return (
    <div className="side-bar">
      <ul>
        {
          sideBarContent.length > 0 &&
          sideBarContent.map((item, index) => (
            <li
              key={index}
              className={`side-bar-list-item ${selectedItem === item._id ? 'side-bar-list-item-selected' : ''}`}
              onClick={() => handleSelectedItem(item._id)}
            >
              {item[itemKey].replace(/_/g, ' ').toUpperCase()}
            </li>
          ))
        }
      </ul>
    </div>
  );
}

export default SideBar;