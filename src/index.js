const { Schema } = require('./Schema.js');
const { Connection } = require('./Connection.js');
const { ObjectId } = require('bson');

const localgoose = {
  Schema,
  Connection,

  // Mongoose-compatible Types namespace
  Types: {
    ObjectId,
    ...Schema.Types
  },

  // Convenience: localgoose.ObjectId
  ObjectId,

  createConnection: (dbPath) => {
    const connection = new Connection(dbPath);
    return connection;
  },

  connect: (dbPath) => {
    const connection = new Connection(dbPath);
    return connection.connect();
  },

  flushDisk: async () => {
    const { flushDisk } = require('./utils.js');
    await flushDisk();
  }
};

module.exports = { localgoose };