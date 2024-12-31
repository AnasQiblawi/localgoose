const fs = require('fs-extra');
const path = require('path');
const { Model } = require('./Model.js');
const { EventEmitter } = require('events');

class Connection {
  // === Core Functionality ===
  constructor(dbPath = './db') {
    this.dbPath = dbPath;
    this.models = {};
    this.collections = {};
    this.config = new Map();
    this.plugins = new Set();
    this.events = new EventEmitter();
    this.readyState = 0; // 0: disconnected, 1: connected, 2: connecting, 3: disconnecting
    this.name = path.basename(dbPath);
    this.host = 'localhost';
    this.port = null;
    this.user = null;
    this.pass = null;
  }

  connect() {
    try {
      this.readyState = 2;
      fs.mkdirSync(this.dbPath, { recursive: true });
      this.readyState = 1;
      return this;
    } catch (error) {
      this.readyState = 0;
      throw new Error(`Failed to connect to database: ${error.message}`);
    }
  }

  disconnect() {
    this.models = {};
    this.collections = {};
    this.readyState = 0;
  }

  close() {
    this.readyState = 3;
    this.disconnect();
    this.readyState = 0;
  }

  dropDatabase() {
    try {
      fs.rmSync(this.dbPath, { recursive: true, force: true });
      this.collections = {};
      return true;
    } catch (error) {
      return false;
    }
  }

  removeDb() {
    this.dropDatabase();
  }

  destroy() {
    this.dropDatabase();
    this.close();
  }

  useDb(name) {
    const newDbPath = path.join(path.dirname(this.dbPath), name);
    const newConnection = new Connection(newDbPath);
    newConnection.connect();
    return newConnection;
  }

  collection(name) {
    if (!this.collections[name]) {
      this.collections[name] = {
        name,
        collectionPath: path.join(this.dbPath, `${name}.json`)
      };
    }
    return this.collections[name];
  }

  dropCollection(name) {
    const collectionPath = path.join(this.dbPath, `${name}.json`);
    try {
      fs.unlinkSync(collectionPath);
      delete this.collections[name];
      return true;
    } catch (error) {
      if (error.code === 'ENOENT') return false;
      throw error;
    }
  }

  listCollections() {
    try {
      const files = fs.readdirSync(this.dbPath);
      return files
        .filter(file => file.endsWith('.json'))
        .map(file => ({
          name: path.basename(file, '.json'),
          type: 'collection'
        }));
    } catch (error) {
      return [];
    }
  }

  // === Model Management ===
  model(name, schema) {
    if (schema) {
      this.models[name] = new Model(name, schema, this);
    }
    return this.models[name];
  }

  deleteModel(name) {
    delete this.models[name];
  }

  modelNames() {
    return Object.keys(this.models);
  }

  // === Configuration Methods ===
  get(key) {
    return this.config.get(key);
  }

  set(key, value) {
    this.config.set(key, value);
    return this;
  }

  plugin(fn, opts) {
    if (this.plugins.has(fn)) return this;
    fn(this, opts);
    this.plugins.add(fn);
    return this;
  }

  startSession() {
    throw new Error('Sessions are not supported in file-based storage');
  }

  transaction() {
    throw new Error('Transactions are not supported in file-based storage');
  }

  withSession() {
    throw new Error('Sessions are not supported in file-based storage');
  }

  // === Client Management ===
  get client() {
    return this;
  }

  getClient() {
    return this.client;
  }

  setClient(client) {
    // No-op for file-based system
    return this;
  }

  syncIndexes() {
    return [];
  }

  asPromise() {
    return this.connect();
  }

  watch() {
    throw new Error('Watch is not supported in file-based storage');
  }
}

module.exports = { Connection };