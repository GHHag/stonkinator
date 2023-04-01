const express = require('express');
const router = express.Router();
const controller = require('./controller');

router.post('/insert', controller.insertData);

module.exports = router;