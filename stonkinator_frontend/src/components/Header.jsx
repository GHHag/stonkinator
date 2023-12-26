import React from 'react';
import { Link } from 'react-router-dom';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';

const Header = ({ user, setUserCallback }) => {

  return (
    <>
      <Row className='header p-2 text-center'>
        <Col>
          <Link className='header-link' to='/'><div className='header-link-div'>🏠 Home</div></Link>
        </Col>
        {
          !user &&
          <Col>
            <Link className='header-link' to='/login'><div className='header-link-div'>➡️ Log in</div></Link>
          </Col>
        }
        {
          user &&
          <Col>
            <Link className='header-link' to='/'>
              <div onClick={() => {
                fetch(`api/user/logout`, { method: 'DELETE' });
                setUserCallback(null);
              }}
                className='header-link-div'>🚫 Log out</div>
            </Link>
          </Col>
        }
        {
          user &&
          <Col>
            <div className='header-link-div'>
              ☑️ Logged in as: <strong className='font-weight-bold'>{user.username}</strong>
            </div>
          </Col>
        }
        {
          !user &&
          <Col>
            <Link className='header-link' to='/register'><div className='header-link-div'>Register</div></Link>
          </Col>
        }
        {
          <Col>
            <Link className='header-link' to='/trading-systems'><div className='header-link-div'>Trading Systems</div></Link>
          </Col>
        }
      </Row>
    </>
  );
}

export default Header;