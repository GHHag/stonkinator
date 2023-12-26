import { useState } from 'react';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import Home from '../pages/Home'
import Header from '../components/Header';
import '../App.css';
import TradingSystems from '../pages/TradingSystems';

function App() {
  const [user, setUser] = useState(null);

  return (
    <BrowserRouter>
      <Header user={user} setUserCallback={setUser}/>
      <Routes>
        <Route path='/' element={<Home/>}/>
        <Route path='/login'/>
        <Route path='/register'/>
        <Route path='/trading-systems' element={<TradingSystems/>}/>
      </Routes>
    </BrowserRouter>
  );
}

export default App;