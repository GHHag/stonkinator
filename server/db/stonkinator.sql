CREATE TABLE IF NOT EXISTS exchanges
(
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    exchange_name VARCHAR(50) UNIQUE NOT NULL,
    currency VARCHAR(50)
);


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS instruments
(
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    exchange_id UUID,
    instrument_name VARCHAR(100),
    symbol VARCHAR(20) UNIQUE NOT NULL,
    sector VARCHAR(100),
    CONSTRAINT exchange_id_fk FOREIGN KEY(exchange_id) REFERENCES exchanges(id)
);

CREATE INDEX idx_symbol ON instruments (UPPER(symbol));


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS market_lists
(
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    exchange_id UUID,
    market_list VARCHAR(100) UNIQUE NOT NULL,
    CONSTRAINT exchange_id_fk FOREIGN KEY(exchange_id) REFERENCES exchanges(id)
);


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS market_list_instruments
(
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    instrument_id UUID,
    market_list_id UUID,
    CONSTRAINT instrument_id_fk FOREIGN KEY(instrument_id) REFERENCES instruments(id),
    CONSTRAINT market_list_id_fk FOREIGN KEY(market_list_id) REFERENCES market_lists(id),
    UNIQUE(instrument_id, market_list_id)
);


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS price_data
(
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    instrument_id UUID,
    open_price REAL NOT NULL,
    high_price REAL NOT NULL,
    low_price REAL NOT NULL,
    close_price REAL NOT NUll,
    volume BIGINT NOT NULL,
    date_time TIMESTAMP NOT NULL,
    CONSTRAINT instrument_id_fk FOREIGN KEY(instrument_id) REFERENCES instruments(id)
);

CREATE INDEX idx_price_data ON price_data (instrument_id, date_time);


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS user_roles
(
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    role_name VARCHAR(100) UNIQUE NOT NULL
);

INSERT INTO user_roles(role_name)
VALUES('admin'), ('user');


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS users
(
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    user_role_id UUID,
    username VARCHAR(100) UNIQUE NOT NULL,
    user_password VARCHAR(100) NOT NULL,
    CONSTRAINT user_role_id_fk FOREIGN KEY(user_role_id) REFERENCES user_roles(id)
);


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS watchlists
(
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    user_id UUID,
    watchlist_name VARCHAR(100) NOT NULL,
    CONSTRAINT user_id_fk FOREIGN KEY(user_id) REFERENCES users(id),
    UNIQUE(user_id, watchlist_name)
);


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS watchlist_instruments
(
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    watchlist_id UUID,
    instrument_id UUID,
    CONSTRAINT watchlist_id_fk FOREIGN KEY(watchlist_id) REFERENCES watchlists(id) ON DELETE CASCADE,
    CONSTRAINT instrument_id_fk FOREIGN KEY(instrument_id) REFERENCES instruments(id) ON DELETE CASCADE,
    UNIQUE(watchlist_id, instrument_id)
);


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS trading_systems
(
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    trading_system_name VARCHAR(100) UNIQUE NOT NULL,
    current_date_time TIMESTAMP
);


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS trading_system_subscriptions
(
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    user_id UUID,
    trading_system_id UUID,
    CONSTRAINT user_id_fk FOREIGN KEY(user_id) REFERENCES users(id),
    CONSTRAINT trading_system_id_fk FOREIGN KEY(trading_system_id) REFERENCES trading_systems(id),
    UNIQUE(user_id, trading_system_id)
);


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS subscribed_trading_system_watchlists
(
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    trading_system_subscription_id UUID,
    watchlist_id UUID,
    CONSTRAINT trading_system_subscription_id_fk FOREIGN KEY(trading_system_subscription_id) REFERENCES trading_system_subscriptions(id) ON DELETE CASCADE,
    CONSTRAINT watchlist_id_fk FOREIGN KEY(watchlist_id) REFERENCES watchlists(id) ON DELETE CASCADE,
    UNIQUE(trading_system_subscription_id, watchlist_id)
);


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS trading_system_orders
(
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    instrument_id UUID,
    trading_system_id UUID,
    order_data_json JSONB NOT NULL,
    order_data_binary BYTEA NOT NULL, --store all data to be able to recreate order objects in separate colums instead of as binary data?
    CONSTRAINT instrument_id_fk FOREIGN KEY(instrument_id) REFERENCES instruments(id),
    CONSTRAINT trading_system_id_fk FOREIGN KEY(trading_system_id) REFERENCES trading_systems(id),
    UNIQUE(instrument_id, trading_system_id)
);


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS trading_system_positions
(
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    instrument_id UUID,
    trading_system_id UUID,
    date_time TIMESTAMP NOT NULL,
    position_data_json JSONB NOT NULL, --JSON
    position_data_binary BYTEA NOT NULL, --store all data to be able to recreate order objects in separate colums instead of as binary data?
    -- num_periods INTEGER, --Should this be defined here or can we derive this value from price data dates and existing position data?
    CONSTRAINT instrument_id_fk FOREIGN KEY(instrument_id) REFERENCES instruments(id),
    CONSTRAINT trading_system_id_fk FOREIGN KEY(trading_system_id) REFERENCES trading_systems(id)
);


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS trading_system_market_states
(
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    instrument_id UUID,
    trading_system_id UUID,
    metrics JSONB NOT NULL, --JSON
    CONSTRAINT instrument_id_fk FOREIGN KEY(instrument_id) REFERENCES instruments(id),
    CONSTRAINT trading_system_id_fk FOREIGN KEY(trading_system_id) REFERENCES trading_systems(id),
    UNIQUE(instrument_id, trading_system_id)
);


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS trading_system_models (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    trading_system_id UUID,
    instrument_id UUID REFERENCES instruments(id),
    serialized_model BYTEA NOT NULL,
    CONSTRAINT trading_system_id_fk FOREIGN KEY(trading_system_id) REFERENCES trading_systems(id),
    UNIQUE(trading_system_id, instrument_id) --will this work with models objects that are used for multiple instruments?
);