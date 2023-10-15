const express = require('express');
const bodyParser = require('body-parser');
const mdb = require('../database/mdb');
const router = require('./routes');

const port = process.env.TET_API_PORT;
const api_url = process.env.API_URL;

mdb.mdbConnect((err) => {
    if (err) {
        console.log(err);
    }
    else {
        console.log('Connected to Mongo DB');
    }
});

const server = express();

server.use(bodyParser.urlencoded({ extended: true, limit: '50mb' }));
server.use(bodyParser.json());
server.use(api_url, router);

server.listen(port, () => {
    console.log(`Server live at ${port}`);
});
