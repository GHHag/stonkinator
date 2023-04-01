require('dotenv').config({ path: `${process.cwd()}/../../.env` });
const mdb = require('../database/mdb');

const insertData = async (req, res) => {
    console.log(req.body.data);
    if (!req.body.data) {
        res.status(500).json({ success: false, error: 'Incorrect body' });
        return;
    }

    try {
        let result = await mdb.getMdb()
            .collection('scraped_data')
            .insertOne({ data: req.body.data });

        res.status(200).json({ success: true, acknowledged: result });
    }
    catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
}

module.exports = {
    insertData
}