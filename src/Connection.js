const fs = require('fs/promises');
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

  async connect() {
    try {
      this.readyState = 2;
      await fs.mkdir(this.dbPath, { recursive: true });
      this.readyState = 1;
      return this;
    } catch (error) {
      this.readyState = 0;
      throw new Error(`Failed to connect to database: ${error.message}`);
    }
  }

  async disconnect() {
    this.models = {};
    this.collections = {};
    this.readyState = 0;
  }

  async close() {
    this.readyState = 3;
    await this.disconnect();
    this.readyState = 0;
  }

  // === Database Operations ===
  async dropDatabase() {
    try {
      await fs.rm(this.dbPath, { recursive: true, force: true });
      this.collections = {};
      return true;
    } catch (error) {
      return false;
    }
  }

  async removeDb() {
    await this.dropDatabase();
  }

  async destroy() {
    await this.dropDatabase();
    await this.close();
  }

  async useDb(name) {
    const newDbPath = path.join(path.dirname(this.dbPath), name);
    const newConnection = new Connection(newDbPath);
    await newConnection.connect();
    return newConnection;
  }

  // === Collection Management ===
  async collection(name) {
    if (!this.collections[name]) {
      this.collections[name] = {
        name,
        collectionPath: path.join(this.dbPath, `${name}.json`)
      };
    }
    return this.collections[name];
  }

  async dropCollection(name) {
    const collectionPath = path.join(this.dbPath, `${name}.json`);
    try {
      await fs.unlink(collectionPath);
      delete this.collections[name];
      return true;
    } catch (error) {
      if (error.code === 'ENOENT') return false;
      throw error;
    }
  }

  async listCollections() {
    try {
      const files = await fs.readdir(this.dbPath);
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

  // === Session and Transaction Handling ===
  async startSession() {
    throw new Error('Sessions are not supported in file-based storage');
  }

  async transaction(fn) {
    throw new Error('Transactions are not supported in file-based storage');
  }

  async withSession(fn) {
    throw new Error('Sessions are not supported in file-based storage');
  }

  // === Client Management ===
  get client() {
    return this;
  }

  async getClient() {
    return this.client;
  }

  setClient(client) {
    // No-op for file-based system
    return this;
  }

  // === Index Management ===
  async syncIndexes(options = {}) {
    // No-op for file-based system
    return [];
  }

  // === Promise Interface ===
  async asPromise() {
    return this.connect();
  }

  // === Watching and Monitoring ===
  async watch() {
    throw new Error('Watch is not supported in file-based storage');
  }
}

module.exports = { Connection };