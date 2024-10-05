CREATE TABLE IF NOT EXISTS exchanges
(
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    exchange_name VARCHAR(50) UNIQUE NOT NULL,
    currency VARCHAR(50)
);


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS instruments
(
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    exchange_id uuid,
    instrument_name VARCHAR(100) NOT NULL,
    symbol VARCHAR(20) UNIQUE NOT NULL,
    industry VARCHAR(100),
    CONSTRAINT exchange_id_fk FOREIGN KEY(exchange_id) REFERENCES exchanges(id)
);

CREATE INDEX idx_symbol ON instruments (UPPER(symbol));

---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS market_lists
(
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    exchange_id uuid,
    market_list VARCHAR(100) UNIQUE NOT NULL,
    CONSTRAINT exchange_id_fk FOREIGN KEY(exchange_id) REFERENCES exchanges(id)
);


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS market_list_instruments
(
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    instrument_id uuid,
    market_list_id uuid,
    CONSTRAINT instrument_id_fk FOREIGN KEY(instrument_id) REFERENCES instruments(id),
    CONSTRAINT market_list_id_fk FOREIGN KEY(market_list_id) REFERENCES market_lists(id)
);


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS price_data
(
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    instrument_id uuid,
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
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    role_name VARCHAR(100) UNIQUE NOT NULL
);

INSERT INTO public.user_roles(role_name)
VALUES('admin'), ('user');


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS users
(
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    user_role_id uuid,
    username VARCHAR(100) UNIQUE NOT NULL,
    user_password VARCHAR(100) NOT NULL,
    CONSTRAINT user_role_id_fk FOREIGN KEY(user_role_id) REFERENCES user_roles(id)
);


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS watch_lists
(
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    user_id uuid,
    watch_list_name VARCHAR(100) NOT NULL,
    CONSTRAINT user_id_fk FOREIGN KEY(user_id) REFERENCES users(id)
);


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS watch_list_instruments
(
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    instrument_id uuid,
    watch_list_id uuid,
    CONSTRAINT instrument_id_fk FOREIGN KEY(instrument_id) REFERENCES instruments(id),
    CONSTRAINT watch_list_id_fk FOREIGN KEY(watch_list_id) REFERENCES watch_lists(id)
);


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS trading_systems
(
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    trading_system_name VARCHAR(100) UNIQUE NOT NULL
);


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS trading_system_subscriptions
(
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    user_id uuid,
    trading_system_id uuid,
    CONSTRAINT user_id_fk FOREIGN KEY(user_id) REFERENCES users(id),
    CONSTRAINT trading_system_id_fk FOREIGN KEY(trading_system_id) REFERENCES trading_systems(id)
);


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS trading_system_positions
(
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    instrument_id uuid,
    trading_system_id uuid,
    date_time TIMESTAMP NOT NULL,
    --position_data_json json, what datatype to use for json format?
    --position_data_binary binary, what datatype to use for serialized format?
    CONSTRAINT instrument_id_fk FOREIGN KEY(instrument_id) REFERENCES instruments(id),
    CONSTRAINT trading_system_id_fk FOREIGN KEY(trading_system_id) REFERENCES trading_systems(id)
);


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS trading_system_market_states
(
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    instrument_id uuid,
    trading_system_id uuid,
    --metrics json, what datatype to use for json format?
    CONSTRAINT instrument_id_fk FOREIGN KEY(instrument_id) REFERENCES instruments(id),
    CONSTRAINT trading_system_id_fk FOREIGN KEY(trading_system_id) REFERENCES trading_systems(id)
);