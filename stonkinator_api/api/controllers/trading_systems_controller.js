const { ObjectId } = require('mongodb');
const mdb = require('../../database/mdb');

const TRADING_SYSTEMS_COLLECTION = 'systems';
const MARKET_STATES_COLLECTION = 'market_states';
const POSITIONS_COLLECTION = 'positions';
const SINGLE_SYMBOL_POS_COLLECTION = `single_symbol_${POSITIONS_COLLECTION}`;

const ID_FIELD = '_id';
const SYSTEM_ID_FIELD = 'system_id';
const SYMBOL_FIELD = 'symbol';
const SYSTEM_NAME_FIELD = 'name';
const METRICS_FIELD = 'metrics';
const POSITION_LIST_FIELD = 'position_list_json';

const getSystems = async (req, res) => {
    try {
        const query = await mdb.getMdb()
            .collection(TRADING_SYSTEMS_COLLECTION)
            .find()
            .toArray();

        res.status(200).json({ success: true, result: query });
    }
    catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
}

const getSystemMetrics = async (req, res) => {
    if (!req.params.id) {
        res.status(500).json({ success: false, error: 'Incorrect params' });
        return;
    }

    try {
        const query = await mdb.getMdb()
            .collection(TRADING_SYSTEMS_COLLECTION)
            .findOne(
                { [ID_FIELD]: ObjectId(req.params.id) },
                { projection: { [METRICS_FIELD]: 1, [SYSTEM_NAME_FIELD]: 1 } }
            );

        res.status(200).json({ success: true, result: query });
    }
    catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
}

const getSystemPositions = async (req, res) => {
    if (!req.params.systemId) {
        res.status(500).json({ success: false, error: 'Incorrect params' });
        return;
    }

    try {
        const query = await mdb.getMdb()
            .collection(POSITIONS_COLLECTION)
            .findOne(
                { [SYSTEM_ID_FIELD]: ObjectId(req.params.systemId) },
                { projection: { [POSITION_LIST_FIELD]: 1, [SYSTEM_NAME_FIELD]: 1 } }
            );

        res.status(200).json({ success: true, result: query });
    }
    catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
}

const getSystemPositionsForSymbol = async (req, res) => {
    if (!req.query.systemId || !req.query.symbol) {
        res.status(500).json({ success: false, error: 'Incorrect query params' });
        return;
    }

    try {
        const query = await mdb.getMdb()
            .collection(SINGLE_SYMBOL_POS_COLLECTION)
            .findOne(
                {
                    [SYSTEM_ID_FIELD]: ObjectId(req.query.systemId),
                    [SYMBOL_FIELD]: req.query.symbol
                },
                {
                    projection: {
                        [POSITION_LIST_FIELD]: 1,
                        [SYSTEM_NAME_FIELD]: 1,
                        [SYMBOL_FIELD]: 1
                    }
                }
            );

        res.status(200).json({ success: true, result: query });
    }
    catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
}

const getSystemMarketStates = async (req, res) => {
    if (!req.params.systemId) {
        res.status(500).json({ success: false, error: 'Incorrect params' });
        return;
    }

    try {
        const query = await mdb.getMdb()
            .collection(MARKET_STATES_COLLECTION)
            .find({ [SYSTEM_ID_FIELD]: ObjectId(req.params.systemId) })
            .toArray();

        res.status(200).json({ success: true, result: query });
    }
    catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
}

const getSystemMarketStateForSymbol = async (req, res) => {
    if (!req.query.systemId || !req.query.symbol) {
        res.status(500).json({ success: false, error: 'Incorrect query params' });
        return;
    }

    try {
        const query = await mdb.getMdb()
            .collection(MARKET_STATES_COLLECTION)
            .findOne(
                {
                    [SYSTEM_ID_FIELD]: ObjectId(req.query.systemId),
                    [SYMBOL_FIELD]: req.query.symbol
                }
            );

        res.status(200).json({ success: true, result: query });
    }
    catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
}

module.exports = {
    getSystems,
    getSystemMetrics,
    getSystemPositions,
    getSystemPositionsForSymbol,
    getSystemMarketStates,
    getSystemMarketStateForSymbol
}
