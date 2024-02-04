import React from "react";

import './Select.css';

function Select({ id, label, name, options, value, valueKey, textKey, onChange, disabled }) {
  return (
    <div className="select-wrapper">
      <label htmlFor={id} className="select-label">{label}</label>
      <select id={id} name={name} className="select" value={value} onChange={(event) => onChange(event.target.value)} disabled={disabled}>
        {options.map((option) => (
          <option key={option._id} value={option[valueKey]} className="select-option">
            {option[textKey]}
          </option>
        ))}
      </select>
    </div>
  )
}

export default Select;