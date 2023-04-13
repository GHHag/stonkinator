const { Pool } = require('pg');

const pool = new Pool(
    {
        host: process.env.PG_DB_SERVICE,
        user: process.env.PG_DB_USER,
        port: process.env.PG_DB_PORT,
        password: process.env.PG_DB_PASSWORD,
        database: process.env.PG_DB_NAME
    }
);

module.exports = pool;
