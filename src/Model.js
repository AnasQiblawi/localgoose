const { readJSON, writeJSON } = require('./utils.js');
const { ObjectId } = require('bson');
const path = require('path');
const { Query } = require('./Query.js');
const { Aggregate } = require('./Aggregate.js');
const { Document } = require('./Document.js');
const { EventEmitter } = require('events');
const fs = require('fs-extra');

class Model {
  constructor(name, schema, connection) {
    this.name = name;
    this.schema = schema;
    this.connection = connection;
    this.collectionPath = path.join(connection.dbPath, `${name}.json`);
    this.collection = {
      name: this.name,
      collectionPath: this.collectionPath,
      async find(conditions = {}) {
        return readJSON(this.collectionPath);
      }
    };
    this.base = connection;
    this.db = connection;
    this.discriminators = null;
    this.events = new EventEmitter();
    this.modelName = name;
    this.baseModelName = null;
    this._indexes = new Map();
    this._searchIndexes = new Map();
    
    this._initializeCollection();
    
    Object.entries(schema.statics).forEach(([name, fn]) => {
      this[name] = fn.bind(this);
    });

    Object.entries(schema.methods).forEach(([name, fn]) => {
      this[name] = fn;
    });
  }

  async _createOne(data) {
    const defaultedData = { ...data };
    
    for (const [field, schema] of Object.entries(this.schema.definition)) {
      if (defaultedData[field] === undefined && schema.default !== undefined) {
        defaultedData[field] = typeof schema.default === 'function' ? 
          schema.default() : schema.default;
      }
      
      if (schema.type === Date && typeof defaultedData[field] === 'string') {
        defaultedData[field] = new Date(defaultedData[field]);
      }
    }

    const errors = this.schema.validate(defaultedData);
    if (errors.length > 0) {
      throw new Error(errors.join(', '));
    }

    if (this.schema.middleware.pre.save) {
      for (const middleware of this.schema.middleware.pre.save) {
        await middleware.call(defaultedData);
      }
    }

    const docs = await readJSON(this.collectionPath);
    const now = new Date();
    const newDoc = { 
      _id: new ObjectId().toString(), 
      ...defaultedData,
      createdAt: now,
      updatedAt: now
    };

    docs.push(newDoc);
    await writeJSON(this.collectionPath, docs);

    if (this.schema.middleware.post.save) {
      for (const middleware of this.schema.middleware.post.save) {
        await middleware.call(newDoc);
      }
    }

    return new Document(newDoc, this.schema, this);
  }

  async _find(conditions = {}) {
    const docs = await readJSON(this.collectionPath);
    return docs.filter(doc => this._matchQuery(doc, conditions));
  }

  _matchQuery(doc, query) {
    return Object.entries(query).every(([key, value]) => {
      if (value && typeof value === 'object') {
        return Object.entries(value).every(([operator, operand]) => {
          switch (operator) {
            case '$gt': return doc[key] > operand;
            case '$gte': return doc[key] >= operand;
            case '$lt': return doc[key] < operand;
            case '$lte': return doc[key] <= operand;
            case '$ne': return doc[key] !== operand;
            case '$in': 
              const docValue = Array.isArray(doc[key]) ? doc[key] : [doc[key]];
              return operand.some(item => docValue.includes(item));
            case '$nin':
              const docVal = Array.isArray(doc[key]) ? doc[key] : [doc[key]];
              return !operand.some(item => docVal.includes(item));
            case '$regex':
              const regex = new RegExp(operand, value.$options);
              return regex.test(doc[key]);
            default:
              return false;
          }
        });
      }
      return doc[key] === value;
    });
  }

  async _initializeCollection() {
    try {
      await readJSON(this.collectionPath);
    } catch (error) {
      if (error.code === 'ENOENT') {
        await writeJSON(this.collectionPath, []);
      }
    }
  }

  find(conditions = {}) {
    return new Query(this, conditions);
  }

  async create(data) {
    if (Array.isArray(data)) {
      return Promise.all(data.map(item => this._createOne(item)));
    }
    return this._createOne(data);
  }

  async findOne(conditions = {}) {
    const docs = await this._find(conditions);
    return docs[0] ? new Document(docs[0], this.schema, this) : null;
  }

  async updateOne(conditions, update) {
    const docs = await readJSON(this.collectionPath);
    const index = docs.findIndex(doc => this._matchQuery(doc, conditions));
    if (index !== -1) {
      docs[index] = { ...docs[index], ...update, updatedAt: new Date() };
      await writeJSON(this.collectionPath, docs);
      return { modifiedCount: 1, upsertedCount: 0 };
    }
    return { modifiedCount: 0, upsertedCount: 0 };
  }

  async deleteMany(conditions = {}) {
    const docs = await readJSON(this.collectionPath);
    const remaining = docs.filter(doc => !this._matchQuery(doc, conditions));
    await writeJSON(this.collectionPath, remaining);
    return { deletedCount: docs.length - remaining.length };
  }

  aggregate(pipeline = []) {
    return new Aggregate(this, pipeline);
  }


  static $where(condition) {
    return this.find({ $where: condition });
  }

  async applyDefaults(doc) {
    for (const [path, schemaType] of this.schema._paths.entries()) {
      if (doc[path] === undefined && schemaType.getDefault() !== undefined) {
        doc[path] = schemaType.getDefault();
      }
    }
    return doc;
  }

  applyTimestamps(doc) {
    const now = new Date();
    if (!doc.createdAt) {
      doc.createdAt = now;
    }
    doc.updatedAt = now;
    return doc;
  }

  applyVirtuals(doc) {
    const virtuals = {};
    for (const [path, virtual] of Object.entries(this.schema.virtuals)) {
      virtuals[path] = virtual.applyGetters(undefined, doc);
    }
    return { ...doc, ...virtuals };
  }

  async bulkSave(docs, options = {}) {
    const result = await this.bulkWrite(
      docs.map(doc => ({
        insertOne: { document: doc }
      })),
      options
    );
    return result;
  }

  async bulkWrite(operations, options = {}) {
    const docs = await readJSON(this.collectionPath);
    let nModified = 0;
    let nInserted = 0;
    let nUpserted = 0;
    let nRemoved = 0;

    for (const op of operations) {
      if (op.insertOne) {
        const doc = await this.applyDefaults(op.insertOne.document);
        this.applyTimestamps(doc);
        doc._id = new ObjectId().toString();
        docs.push(doc);
        nInserted++;
      } else if (op.updateOne) {
        const index = docs.findIndex(doc => 
          this._matchQuery(doc, op.updateOne.filter)
        );
        if (index !== -1) {
          Object.assign(docs[index], op.updateOne.update);
          this.applyTimestamps(docs[index]);
          nModified++;
        } else if (op.updateOne.upsert) {
          const doc = await this.applyDefaults({
            ...op.updateOne.filter,
            ...op.updateOne.update
          });
          this.applyTimestamps(doc);
          doc._id = new ObjectId().toString();
          docs.push(doc);
          nUpserted++;
        }
      } else if (op.deleteOne) {
        const index = docs.findIndex(doc => 
          this._matchQuery(doc, op.deleteOne.filter)
        );
        if (index !== -1) {
          docs.splice(index, 1);
          nRemoved++;
        }
      }
    }

    await writeJSON(this.collectionPath, docs);
    return { nModified, nInserted, nUpserted, nRemoved };
  }

  castObject(obj) {
    const castedObj = {};
    for (const [path, value] of Object.entries(obj)) {
      const schemaType = this.schema.path(path);
      if (schemaType) {
        castedObj[path] = schemaType.cast(value);
      } else {
        castedObj[path] = value;
      }
    }
    return castedObj;
  }

  async cleanIndexes() {
    this._indexes.clear();
    return true;
  }

  async countDocuments(conditions = {}) {
    const docs = await this._find(conditions);
    return docs.length;
  }

  async createCollection() {
    await this._initializeCollection();
    return this.collection;
  }

  async createIndexes(indexes = []) {
    for (const [fields, options] of indexes) {
      this._indexes.set(
        Object.keys(fields).sort().join('_'),
        { fields, options }
      );
    }
    return indexes.length;
  }

  async createSearchIndex(options = {}) {
    this._searchIndexes.set(options.name || 'default', options);
    return true;
  }

  async diffIndexes() {
    return {
      toDrop: [],
      toCreate: Array.from(this._indexes.values())
    };
  }

  discriminator(name, schema) {
    if (!this.discriminators) {
      this.discriminators = {};
    }
    this.discriminators[name] = new Model(name, schema, this.connection);
    return this.discriminators[name];
  }

  async distinct(field, conditions = {}) {
    const docs = await this._find(conditions);
    return [...new Set(docs.map(doc => doc[field]))];
  }

  async dropSearchIndex(name = 'default') {
    return this._searchIndexes.delete(name);
  }

  async ensureIndexes() {
    return this.createIndexes(Array.from(this._indexes.values()));
  }

  async estimatedDocumentCount() {
    const docs = await readJSON(this.collectionPath);
    return docs.length;
  }

  async exists(conditions) {
    const doc = await this.findOne(conditions);
    return doc !== null;
  }

  async findById(id) {
    return this.findOne({ _id: id });
  }

  async findByIdAndDelete(id) {
    return this.findOneAndDelete({ _id: id });
  }

  async findByIdAndUpdate(id, update, options = {}) {
    return this.findOneAndUpdate({ _id: id }, update, options);
  }

  async findOneAndDelete(conditions) {
    const doc = await this.findOne(conditions);
    if (doc) {
      await this.deleteOne(conditions);
    }
    return doc;
  }

  async findOneAndReplace(conditions, replacement, options = {}) {
    const doc = await this.findOne(conditions);
    if (doc) {
      Object.assign(doc, replacement);
      await doc.save();
    } else if (options.upsert) {
      return this.create(replacement);
    }
    return doc;
  }

  async findOneAndUpdate(conditions, update, options = {}) {
    const doc = await this.findOne(conditions);
    if (doc) {
      Object.assign(doc, update);
      await doc.save();
    } else if (options.upsert) {
      return this.create({ ...conditions, ...update });
    }
    return doc;
  }

  async deleteOne(conditions = {}) {
    const docs = await readJSON(this.collectionPath);
    const index = docs.findIndex(doc => this._matchQuery(doc, conditions));
  
    if (index !== -1) {
      docs.splice(index, 1);
      await writeJSON(this.collectionPath, docs);
      return { deletedCount: 1 };
    }
  
    return { deletedCount: 0 };
  }  

  hydrate(obj) {
    return new Document(obj, this.schema, this);
  }

  async init() {
    await this._initializeCollection();
    return this;
  }

  async insertMany(docs, options = {}) {
    return this.create(docs, options);
  }

  inspect() {
    return `Model { ${this.modelName} }`;
  }

  async listIndexes() {
    return Array.from(this._indexes.values());
  }

  async listSearchIndexes() {
    return Array.from(this._searchIndexes.values());
  }

  $model(name) {
    return this.db.model(name);
  }

  async recompileSchema() {
    this.schema._init();
    return this;
  }

  async replaceOne(conditions, doc, options = {}) {
    const result = await this.updateOne(
      conditions,
      doc,
      { ...options, overwrite: true }
    );
    return result;
  }

  startSession() {
    throw new Error('Sessions are not supported in file-based storage');
  }

  async syncIndexes() {
    await this.cleanIndexes();
    await this.ensureIndexes();
    return this._indexes.size;
  }

  translateAliases(raw) {
    const translated = { ...raw };
    for (const [alias, path] of Object.entries(this.schema.aliases || {})) {
      if (translated[alias] !== undefined) {
        translated[path] = translated[alias];
        delete translated[alias];
      }
    }
    return translated;
  }

  async updateSearchIndex(options = {}) {
    const name = options.name || 'default';
    if (this._searchIndexes.has(name)) {
      this._searchIndexes.set(name, {
        ...this._searchIndexes.get(name),
        ...options
      });
      return true;
    }
    return false;
  }

  async validate(obj) {
    return this.schema.validate(obj);
  }

  watch() {
    throw new Error('Watch is not supported in file-based storage');
  }

  where(path) {
    return new Query(this).where(path);
  }

  async backup(backupPath) {
    const defaultBackupPath = path.join(
      path.dirname(this.collectionPath), 
      `${this.name}_backup_${new Date().toISOString().replace(/:/g, '-')}.json`
    );
    
    const docs = await readJSON(this.collectionPath);
    await writeJSON(backupPath || defaultBackupPath, docs);
    return backupPath || defaultBackupPath;
  }
  
  async restore(backupPath) {
    if (!backupPath) {
      // Find the most recent backup file if no path is provided
      const backupDir = path.dirname(this.collectionPath);
      const backupFiles = await fs.readdir(backupDir);
      const modelBackupFiles = backupFiles.filter(file => 
        file.startsWith(`${this.name}_backup_`) && file.endsWith('.json')
      );
      
      if (modelBackupFiles.length === 0) {
        throw new Error(`No backup files found for model: ${this.name}`);
      }
      
      // Sort backup files and get the most recent one
      const mostRecentBackup = modelBackupFiles.sort().reverse()[0];
      backupPath = path.join(backupDir, mostRecentBackup);
    }
  
    const backupDocs = await readJSON(backupPath);
    await writeJSON(this.collectionPath, backupDocs);
    return backupPath;
  }
  
  async listBackups() {
    try {
      const backupDir = path.dirname(this.collectionPath);
      const backupFiles = await fs.readdir(backupDir);
      
      // Filter backup files for this specific model
      const modelBackups = backupFiles
        .filter(file => 
          file.startsWith(`${this.name}_backup_`) && 
          file.endsWith('.json')
        )
        .map(filename => {
          const fullPath = path.join(backupDir, filename);
          const stats = fs.statSync(fullPath);
          
          return {
            filename,
            path: fullPath,
            createdAt: stats.birthtime,
            size: stats.size // in bytes
          };
        })
        // Sort from most recent to oldest
        .sort((a, b) => b.createdAt - a.createdAt);
      
      return modelBackups;
    } catch (error) {
      console.error('Error listing backups:', error);
      return [];
    }
  }

  async cleanupBackups(backedupFileName = null) {
    const backups = await this.listBackups();
  
    if (backedupFileName) {
      // Find and delete specific backup
      const backupToDelete = backups.find(backup => backup.filename === backedupFileName);
      
      if (!backupToDelete) {
        throw new Error(`Backup file '${backedupFileName}' not found`);
      }
  
      await fs.unlink(backupToDelete.path);
      return [backupToDelete];
    }
  
    // Delete all backup files by default
    for (const backup of backups) {
      await fs.unlink(backup.path);
    }
  
    return [];
  }

}

module.exports = { Model };