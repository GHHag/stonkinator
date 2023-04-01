CREATE TABLESPACE securities_db_tablespace IF NOT EXISTS
OWNER postgres
LOCATION '%/securities_pg_db/securities_db';

---------------------------------------------------------------------------


-- Database: securities_db

--DROP DATABASE IF EXISTS "securities_db";

CREATE DATABASE "securities_db"
    WITH
    OWNER = postgres
    ENCODING = 'UTF-8'
    LC_COLLATE = 'Swedish_Sweden.1252'
    LC_CTYPE = 'Swedish_Sweden.1252'
    TABLESPACE = securities_db_tablespace
    CONNECTION LIMIT = -1;

\c securities_db


---------------------------------------------------------------------------


-- Table: public.exchanges

-- DROP TABLE IF EXISTS public.exchanges;

CREATE TABLE IF NOT EXISTS public.exchanges
(
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    exchange_name VARCHAR(50) UNIQUE NOT NULL,
    currency VARCHAR(50)
)

TABLESPACE securities_db_tablespace;

ALTER TABLE IF EXISTS public.exchanges
    OWNER to postgres;


---------------------------------------------------------------------------


-- Table: public.instruments

-- DROP TABLE IF EXISTS public.instruments;

CREATE TABLE IF NOT EXISTS public.instruments
(
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    exchange_id uuid,
    symbol VARCHAR(20) UNIQUE NOT NULL,
    CONSTRAINT exchange_id_fk FOREIGN KEY(exchange_id) REFERENCES exchanges(id)
)

TABLESPACE securities_db_tablespace;

ALTER TABLE IF EXISTS public.instruments
    OWNER to postgres;


---------------------------------------------------------------------------


-- Table: public.price_data

-- DROP TABLE IF EXISTS public.price_data;

CREATE TABLE IF NOT EXISTS public.price_data
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
)

TABLESPACE securities_db_tablespace;

ALTER TABLE IF EXISTS public.price_data
    OWNER to postgres;
