CREATE TABLE IF NOT EXISTS public.exchanges
(
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    exchange_name VARCHAR(50) UNIQUE NOT NULL,
    currency VARCHAR(50)
);

ALTER TABLE IF EXISTS public.exchanges
    OWNER to postgres;


---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS public.instruments
(
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    exchange_id uuid,
    symbol VARCHAR(20) UNIQUE NOT NULL,
    CONSTRAINT exchange_id_fk FOREIGN KEY(exchange_id) REFERENCES exchanges(id)
);

ALTER TABLE IF EXISTS public.instruments
    OWNER to postgres;


---------------------------------------------------------------------------


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
);

ALTER TABLE IF EXISTS public.price_data
    OWNER to postgres;