// Import necessary modules
import { Schema } from './Schema.js';
import { Connection } from './Connection.js';

// Define the localgoose object
const localgoose = {
  Schema,
  Connection,
  createConnection: (dbPath) => new Connection(dbPath),
  connect: async (dbPath) => {
    const connection = new Connection(dbPath);
    return connection.connect();
  }
};

// export default localgoose;
export { localgoose };