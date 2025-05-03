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
    instrument_name VARCHAR(100) DEFAULT '',
    symbol VARCHAR(20) UNIQUE NOT NULL,
    sector VARCHAR(100) DEFAULT '',
    CONSTRAINT exchange_id_fk FOREIGN KEY(exchange_id) REFERENCES exchanges(id)
);


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
    CONSTRAINT instrument_id_fk FOREIGN KEY(instrument_id) REFERENCES instruments(id),
    UNIQUE(instrument_id, date_time)
);

CREATE INDEX idx_price_data
ON price_data (instrument_id, date_time);


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
    current_date_time TIMESTAMP,
    metrics JSONB DEFAULT '{}'::jsonb
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


CREATE TABLE IF NOT EXISTS orders
(
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    instrument_id UUID,
    trading_system_id UUID,
    order_type VARCHAR(20) NOT NULL,
    order_action VARCHAR(20) NOT NULL,
    created_date_time TIMESTAMP NOT NULL,
    active BOOLEAN NOT NULL,
    direction_long BOOLEAN,
    price REAL,
    max_order_duration SMALLINT,
    duration SMALLINT,
    CONSTRAINT instrument_id_fk FOREIGN KEY(instrument_id) REFERENCES instruments(id),
    CONSTRAINT trading_system_id_fk FOREIGN KEY(trading_system_id) REFERENCES trading_systems(id),
    UNIQUE(instrument_id, trading_system_id)
);

CREATE INDEX idx_orders
ON orders (instrument_id, trading_system_id);


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS positions
(
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    instrument_id UUID,
    trading_system_id UUID,
    date_time TIMESTAMP NOT NULL,
    position_data JSONB NOT NULL,
    serialized_position BYTEA NOT NULL,
    CONSTRAINT instrument_id_fk FOREIGN KEY(instrument_id) REFERENCES instruments(id),
    CONSTRAINT trading_system_id_fk FOREIGN KEY(trading_system_id) REFERENCES trading_systems(id)
);

CREATE INDEX idx_positions
ON positions (instrument_id, trading_system_id, date_time DESC);

CREATE INDEX idx_positions_active_false
ON positions (instrument_id, trading_system_id, date_time DESC)
WHERE (position_data ->> 'active')::boolean = false;

CREATE INDEX idx_positions_by_ts_active_false
ON positions (trading_system_id)
WHERE (position_data ->> 'active')::boolean = false;


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS market_states
(
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    instrument_id UUID,
    trading_system_id UUID,
    signal_date_time TIMESTAMP NOT NULL,
    metrics JSONB NOT NULL,
    market_action VARCHAR(10) NOT NULL,
    CONSTRAINT instrument_id_fk FOREIGN KEY(instrument_id) REFERENCES instruments(id),
    CONSTRAINT trading_system_id_fk FOREIGN KEY(trading_system_id) REFERENCES trading_systems(id),
    UNIQUE(instrument_id, trading_system_id)
);

CREATE INDEX idx_market_states
ON market_states (trading_system_id, market_action);


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS trading_system_models (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    trading_system_id UUID,
    instrument_id UUID REFERENCES instruments(id),
    serialized_model BYTEA NOT NULL,
    CONSTRAINT trading_system_id_fk FOREIGN KEY(trading_system_id) REFERENCES trading_systems(id),
    UNIQUE(trading_system_id, instrument_id)
);

CREATE UNIQUE INDEX unique_ts_model_with_null_instrument_id
ON trading_system_models (trading_system_id)
WHERE instrument_id IS NULL;