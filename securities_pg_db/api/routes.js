const express = require('express');
const router = express.Router();
const controller = require('./controller');

router.post('/exchange', controller.insertExchange);

// get exchange, param :name = exchanges.exchange_name
router.get('/exchange/:name', controller.getExchange);

// post instrument, param :id = instruments.exchange_id_fk
router.post('/instrument/:id', controller.insertInstrument);

// get instrument, param :symbol = instruments.symbol
router.get('/instrument/:symbol', controller.getInstrument);

// post price data, param :id = price_data.instrument_id_fk
router.post('/price-data/:id', controller.insertPriceData);

// get price data, param :symbol = instruments.symbol
router.get('/price-data/:symbol', controller.getPriceData);

router.get('/date/', controller.getLastDate);

module.exports = router;