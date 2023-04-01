const { ObjectId } = require('mongodb');
const mdb = require('../../database/mdb');

const MARKET_LISTS_COLLECTION = 'market_lists';
const INSTRUMENTS_COLLECTION = 'instruments';
//const MARKET_LIST_FIELD = 'market_list';
//const MARKET_LIST_IDS_FIELD = 'market_list_ids'
//const SYMBOL_FIELD = 'symbol';

const insertMarketList = async (req, res) => {
    if (!req.body.marketList) {
        res.status(500).json({ success: false, error: 'Incorrect body' });
        return;
    }

    try {
        let result = await mdb.getMdb()
            .collection(MARKET_LISTS_COLLECTION)
            .findOne({ market_list: req.body.marketList }, { _id: 1 });

        if (!result) {
            result = await mdb.getMdb()
                .collection(MARKET_LISTS_COLLECTION)
                .insertOne({ market_list: req.body.marketList });
        }
        else {
            result = 'Market list already exists in the database.'
        }

        res.status(200).json({ success: true, acknowledged: result });
    }
    catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
}

const getMarketListId = async (req, res) => {
    if (!req.body.marketList) {
        res.status(500).json({ success: false, error: 'Incorrect body' });
        return;
    }

    try {
        const query = await mdb.getMdb()
            .collection(MARKET_LISTS_COLLECTION)
            .findOne(
                { market_list: req.body.marketList },
                { projection: { _id: 1, market_list: 0 } }
            );

        res.status(200).json({ success: true, result: query });
    }
    catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
}

const getMarketLists = async (req, res) => {
    try {
        const query = await mdb.getMdb()
            .collection(MARKET_LISTS_COLLECTION)
            .find()
            .toArray();

        res.status(200).json({ success: true, result: query });
    }
    catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
}

const insertInstruments = async (req, res) => {
    if (!req.body.marketListId || !req.body.instrumentDataObjects) {
        res.status(500).json({ success: false, error: 'Incorrect body' });
        return;
    }

    try {
        const collection = mdb.getMdb().collection(INSTRUMENTS_COLLECTION);
        req.body.instrumentDataObjects.map(async instrumentDataObject => {
            collection.updateOne(
                { symbol: instrumentDataObject.symbol },
                {
                    $set: { ...instrumentDataObject },
                    $addToSet: { market_list_ids: ObjectId(req.body.marketListId) },
                },
                { upsert: true }
            );
        });

        res.status(200).json({ success: true });
    }
    catch (err) {
        res.status(500).json({ success: false, error: err.message })
    }
}

const getMarketListInstruments = async (req, res) => {
    if (!req.params.id) {
        res.status(500).json({ success: false, error: 'Incorrect body' });
        return;
    }

    try {
        const query = await mdb.getMdb()
            .collection(MARKET_LISTS_COLLECTION)
            .aggregate(
                [
                    {
                        $match: {
                            _id: new ObjectId(req.params.id)
                        }
                    },
                    {
                        $lookup: {
                            from: INSTRUMENTS_COLLECTION,
                            localField: '_id',
                            foreignField: 'market_list_ids',
                            as: 'market_list_instruments',
                        }
                    },
                ]
            )
            .toArray();

        res.status(200).json({ success: true, result: query });
    }
    catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
}

const getMarketListInstrumentSymbols = async (req, res) => {
    if (!req.params.id) {
        res.status(500).json({ success: false, error: 'Incorrect body' });
        return;
    }

    try {
        const query = await mdb.getMdb()
            .collection(MARKET_LISTS_COLLECTION)
            .aggregate(
                [
                    {
                        $match: {
                            _id: new ObjectId(req.params.id)
                        }
                    },
                    {
                        $lookup: {
                            from: INSTRUMENTS_COLLECTION,
                            localField: '_id',
                            foreignField: 'market_list_ids',
                            as: 'market_list_instruments',
                        }
                    },
                    {
                        $project: {
                            'market_list_instruments._id': 1,
                            'market_list_instruments.symbol': 1
                        }
                    }
                ]
            )
            .toArray();

        res.status(200).json({ success: true, result: query });
    }
    catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
}

const getSectors = async (req, res) => {
    try {
        const query = await mdb.getMdb()
            .collection(INSTRUMENTS_COLLECTION)
            .distinct('industry');

        res.status(200).json({ success: true, result: query });
    }
    catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
}

const getSectorInstruments = async (req, res) => {
    if (!req.query.sector) {
        res.status(500).json({ success: false, error: 'Incorrect query params' });
        return;
    }

    try {
        const query = await mdb.getMdb()
            .collection(INSTRUMENTS_COLLECTION)
            .find({ industry: req.query.sector })
            .toArray();

        res.status(200).json({ success: true, result: query });
    }
    catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
}

const getSectorInstrumentsForMarketLists = async (req, res) => {
    if (!req.query.sector || !req.body.marketListIds) {
        res.status(500).json({ success: false, error: 'Incorrect query params' });
        return;
    }

    try {
        const pipeline = [
            {
                $match: { industry: req.query.sector }
            },
            {
                $match: {
                    $nor: [
                        {
                            market_list_ids: {
                                $nin: req.body.marketListIds.map(x => ObjectId(x))
                            }
                        }
                    ]
                }
            }
        ];
        const query = await mdb.getMdb()
            .collection(INSTRUMENTS_COLLECTION)
            .aggregate(pipeline)
            .toArray();

        res.status(200).json({ success: true, result: query });
    }
    catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
}

module.exports = {
    insertMarketList,
    getMarketListId,
    getMarketLists,
    insertInstruments,
    getMarketListInstruments,
    getMarketListInstrumentSymbols,
    getSectors,
    getSectorInstruments,
    getSectorInstrumentsForMarketLists
}
