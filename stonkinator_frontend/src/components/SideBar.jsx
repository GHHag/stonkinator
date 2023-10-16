import React, { useState, useEffect } from 'react';

const SideBar = ({ sideBarContent, itemKey, selectedItemCallback }) => {

  return (
    <div className="side-bar">
      <ul>
        {
          sideBarContent.length > 0 &&
          sideBarContent.map((item, index) => (
            <li key={index} className='side-bar-list-item' onClick={() => selectedItemCallback(item._id)}>
              {item[itemKey].replace(/_/g, ' ').toUpperCase()}
            </li>
          ))
        }
      </ul>
    </div>
  );
}

export default SideBar;