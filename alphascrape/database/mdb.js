require('dotenv').config({ path: `${process.cwd()}/../.env` });
const { MongoClient } = require('mongodb');

let _db;
//const mdbUri = process.env.LOCALHOST_MONGO_DB_URL;
const mdbUri = process.env.ATLAS_MONGO_DB_URL;

module.exports = {
    mdbConnect: (callback) => {
        MongoClient.connect(mdbUri, (err, client) => {
            _db = client.db(process.env.DB);
            return callback(err);
        });
    },
    getMdb: () => {
        return _db;
    }
}
