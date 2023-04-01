const express = require('express');
const router = express.Router();
const instrumentsController = require('./controllers/instruments_controller');
const tradingSystemsController = require('./controllers/trading_systems_controller');
const screenersController = require('./controllers/screeners_controller');

router.post('/market-list', instrumentsController.insertMarketList);
router.get('/market-list', instrumentsController.getMarketListId);
router.get('/market-lists', instrumentsController.getMarketLists);

router.post('/instruments', instrumentsController.insertInstruments);
router.get('/instruments/sector', instrumentsController.getSectorInstruments);
router.get('/instruments/sectors', instrumentsController.getSectors);
router.get('/instruments/:id', instrumentsController.getMarketListInstruments);
router.get('/instruments/symbols/:id', instrumentsController.getMarketListInstrumentSymbols);
router.get('/instruments/sector/market-lists', instrumentsController.getSectorInstrumentsForMarketLists);

router.get('/systems', tradingSystemsController.getSystems);
router.get('/systems/metrics/:id', tradingSystemsController.getSystemMetrics);
router.get('/systems/positions', tradingSystemsController.getSystemPositionsForSymbol);
router.get('/systems/positions/:systemId', tradingSystemsController.getSystemPositions);
router.get('/systems/market-states/:systemId', tradingSystemsController.getSystemMarketStates);
router.get('/systems/market-state', tradingSystemsController.getSystemMarketStateForSymbol);

router.get('/screeners/market-breadth');
router.get('/screeners/sector-breadth');

module.exports = router;