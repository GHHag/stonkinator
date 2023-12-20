import React from 'react';
import ReactDOM from 'react-dom/client';
import Router from './application/Router.jsx'
import 'bootstrap/dist/css/bootstrap.min.css';
import './App.css';

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <Router />
  </React.StrictMode>,
);