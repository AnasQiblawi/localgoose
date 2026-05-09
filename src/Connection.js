const fs = require('fs-extra');
const path = require('path');
const { clearCache, flushDisk } = require('./utils.js');
const { Model } = require('./Model.js');
const { Document } = require('./Document.js');
const { EventEmitter } = require('events');

class Connection {
  constructor(dbPath = './db') {
    this.dbPath      = path.resolve(dbPath);
    this.models      = {};
    this.collections = {};
    this.config      = new Map();
    this.plugins     = new Set();
    this.events      = new EventEmitter();
    this.readyState  = 0; // 0: disconnected, 1: connected, 2: connecting, 3: disconnecting
    this.name        = path.basename(dbPath);
    this.host        = 'localhost';
    this.port        = null;
    this.user        = null;
    this.pass        = null;
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
    await flushDisk();
    this.readyState = 3;
    this.models      = {};
    this.collections = {};
    this.readyState  = 0;
    return Promise.resolve();
  }

  close() {
    return this.disconnect();
  }

  async dropDatabase() {
    try {
      await flushDisk();
      clearCache(this.dbPath);
      if (fs.existsSync(this.dbPath)) {
        fs.rmSync(this.dbPath, { recursive: true, force: true });
      }
      this.collections = {};
      return true;
    } catch (error) {
      return false;
    }
  }

  removeDb()  { return this.dropDatabase(); }
  destroy()   { return this.dropDatabase().then(() => this.close()); }

  useDb(name) {
    const newDbPath = path.join(path.dirname(this.dbPath), name);
    const conn = new Connection(newDbPath);
    conn.connect();
    return conn;
  }

  collection(name) {
    if (!this.collections[name]) {
      this.collections[name] = { name, collectionPath: path.join(this.dbPath, `${name}.json`) };
    }
    return this.collections[name];
  }

  dropCollection(name) {
    const collectionPath = path.join(this.dbPath, `${name}.json`);
    try {
      fs.unlinkSync(collectionPath);
      delete this.collections[name];
      return Promise.resolve(true);
    } catch (error) {
      if (error.code === 'ENOENT') return Promise.resolve(false);
      return Promise.reject(error);
    }
  }

  listCollections() {
    try {
      const files = fs.readdirSync(this.dbPath);
      return files
        .filter(f => f.endsWith('.json'))
        .map(f => ({ name: path.basename(f, '.json'), type: 'collection' }));
    } catch { return []; }
  }

  // === Model Management ===
  model(name, schema) {
    if (schema) {
      const modelInstance = new Model(name, schema, this);
      
      class ModelConstructor extends Document {
        constructor(obj) {
          super(obj, schema, modelInstance);
          this.isNew = true;
          this._isNew = true;
        }
      }
      
      this.models[name] = new Proxy(ModelConstructor, {
        get(target, prop) {
          if (prop in target) return Reflect.get(target, prop);
          const val = modelInstance[prop];
          if (typeof val === 'function') return val.bind(modelInstance);
          return val;
        },
        set(target, prop, value) {
          if (prop in target) return Reflect.set(target, prop, value);
          modelInstance[prop] = value;
          return true;
        }
      });
    }
    if (!this.models[name]) {
      throw new Error(`Model "${name}" has not been registered.`);
    }
    return this.models[name];
  }

  deleteModel(name) { delete this.models[name]; }
  modelNames()      { return Object.keys(this.models); }

  // === Configuration ===
  get(key)        { return this.config.get(key); }
  set(key, value) { this.config.set(key, value); return this; }

  plugin(fn, opts) {
    if (this.plugins.has(fn)) return this;
    fn(this, opts);
    this.plugins.add(fn);
    return this;
  }

  startSession()  { 
    return Promise.resolve({
      startTransaction() {},
      commitTransaction() { return Promise.resolve(); },
      abortTransaction() { return Promise.resolve(); },
      endSession() { return Promise.resolve(); }
    });
  }
  transaction()   { return Promise.resolve(); }
  withSession(fn) { return Promise.resolve(fn(this.startSession())); }

  get client()    { return this; }
  getClient()     { return this; }
  setClient()     { return this; }

  syncIndexes()   { return Promise.resolve([]); }
  asPromise()     { return Promise.resolve(this.connect()); }
  watch()         { throw new Error('Watch is not supported in file-based storage'); }
}

module.exports = { Connection };