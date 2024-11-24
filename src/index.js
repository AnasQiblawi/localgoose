const { Schema } = require('./Schema.js');
const { Connection } = require('./Connection.js');

const localgoose = {
  Schema,
  Connection,
  createConnection: (dbPath) => new Connection(dbPath),
  connect: async (dbPath) => {
    const connection = new Connection(dbPath);
    return connection.connect();
  }
};

module.exports = { localgoose };