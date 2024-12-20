const { readJSON, writeJSON } = require('./utils.js');
const { ObjectId } = require('bson');
const path = require('path');
const { Query } = require('./Query.js');
const { Aggregate } = require('./Aggregate.js');
const { Document } = require('./Document.js');
const { EventEmitter } = require('events');
const fs = require('fs-extra');

class Model {
  // === Core Functionality ===
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

  async _initializeCollection() {
    try {
      await readJSON(this.collectionPath);
    } catch (error) {
      if (error.code === 'ENOENT') {
        await writeJSON(this.collectionPath, []);
      }
    }
  }

  async init() {
    await this._initializeCollection();
    return this;
  }

  _getCollection(collectionName) {
    try {
      const collectionPath = path.join(this.connection.dbPath, `${collectionName}.json`);
      const data = fs.readFileSync(collectionPath, 'utf8');
      return JSON.parse(data);
    } catch (error) {
      return [];
    }
  }

  // === CRUD Operations ===
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

    await this._executeMiddleware('pre', 'validate', defaultedData);
    const errors = this.schema.validate(defaultedData);
    if (errors.length > 0) {
      throw new Error(errors.join(', '));
    }
    await this._executeMiddleware('post', 'validate', defaultedData);

    await this._executeMiddleware('pre', 'save', defaultedData);

    const docs = await readJSON(this.collectionPath);
    const now = new Date();
    const newDoc = {
      _id: new ObjectId().toString(),
      ...defaultedData,
      createdAt: now,
      updatedAt: now,
      __v: 0
    };

    docs.push(newDoc);
    await writeJSON(this.collectionPath, docs);

    await this._executeMiddleware('post', 'save', newDoc);

    return new Document(newDoc, this.schema, this);
  }

  async create(data) {
    if (Array.isArray(data)) {
      return Promise.all(data.map(item => this._createOne(item)));
    }
    return this._createOne(data);
  }

  async updateOne(conditions, update, options = {}) {
    const docs = await readJSON(this.collectionPath);
    const index = docs.findIndex(doc => this._matchQuery(doc, conditions));

    if (index !== -1) {
      const doc = this._applyUpdateOperators(docs[index], update, options);
      await writeJSON(this.collectionPath, docs);
      return { modifiedCount: 1, upsertedCount: 0 };
    }

    return { modifiedCount: 0, upsertedCount: 0 };
  }

  async updateMany(conditions, update, options = {}) {
    const docs = await readJSON(this.collectionPath);
    let modifiedCount = 0;

    for (const doc of docs) {
      if (this._matchQuery(doc, conditions)) {
        this._applyUpdateOperators(doc, update, options);
        modifiedCount++;
      }
    }

    await writeJSON(this.collectionPath, docs);
    return { modifiedCount, upsertedCount: 0 };
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

  async deleteMany(conditions = {}) {
    const docs = await readJSON(this.collectionPath);
    const remaining = docs.filter(doc => !this._matchQuery(doc, conditions));
    await writeJSON(this.collectionPath, remaining);
    return { deletedCount: docs.length - remaining.length };
  }

  async replaceOne(conditions, doc, options = {}) {
    const result = await this.updateOne(
      conditions,
      doc,
      { ...options, overwrite: true }
    );
    return result;
  }

  async save() {
    if (this._timestamps) {
      this._doc.updatedAt = new Date();
      if (this.isNew) {
        this._doc.createdAt = new Date();
      }
    }

    if (this._schema.middleware.pre.save) {
      for (const middleware of this._schema.middleware.pre.save) {
        await middleware.call(this);
      }
    }

    const errors = await this.$validate();
    if (errors.length > 0) {
      throw new Error(errors.join(', '));
    }

    const result = await this._model.updateOne(
      { _id: this._id },
      this._doc
    );

    if (this._schema.middleware.post.save) {
      for (const middleware of this._schema.middleware.post.save) {
        await middleware.call(this);
      }
    }

    this._isNew = false;
    return result;
  }

  // === Query Operations ===
  find(conditions = {}, options = {}) {
    const query = new Query(this, conditions);
    if (options.lean) {
      query.lean();
    }
    return query;
  }

  findOne(conditions = {}, options = {}) {
    const query = new Query(this, conditions);
    query._limit = 1;
    if (options.lean) {
      query.lean();
    }
    return query;
  }

  async findById(id) {
    const docs = await readJSON(this.collectionPath);
    const doc = docs.find(doc => doc._id === id);
    return doc ? new Document(doc, this.schema, this) : null;
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

  async findByIdAndDelete(id) {
    return this.findOneAndDelete({ _id: id });
  }

  async findByIdAndRemove(id) {
    return this.findOneAndDelete({ _id: id });
  }

  async findByIdAndUpdate(id, update, options = {}) {
    return this.findOneAndUpdate({ _id: id }, update, options);
  }

  // === Index Operations ===
  async createIndexes(indexes = []) {
    for (const [fields, options] of indexes) {
      this._indexes.set(
        Object.keys(fields).sort().join('_'),
        { fields, options }
      );
    }
    return indexes.length;
  }

  async cleanIndexes() {
    this._indexes.clear();
    return true;
  }

  async createSearchIndex(options = {}) {
    this._searchIndexes.set(options.name || 'default', options);
    return true;
  }

  async dropSearchIndex(name = 'default') {
    return this._searchIndexes.delete(name);
  }

  async ensureIndexes() {
    return this.createIndexes(Array.from(this._indexes.values()));
  }

  async diffIndexes() {
    return {
      toDrop: [],
      toCreate: Array.from(this._indexes.values())
    };
  }

  async listIndexes() {
    return Array.from(this._indexes.values());
  }

  async listSearchIndexes() {
    return Array.from(this._searchIndexes.values());
  }

  async syncIndexes() {
    await this.cleanIndexes();
    await this.ensureIndexes();
    return this._indexes.size;
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

  // === Document Operations ===
  async _find(conditions = {}) {
    const docs = await readJSON(this.collectionPath);
    return docs.filter(doc => this._matchQuery(doc, conditions));
  }

  _matchQuery(doc, query) {
    return Object.entries(query).every(([key, value]) => {
      if (key === '$and') {
        return value.every(condition => this._matchQuery(doc, condition));
      }
      
      if (key === '$or') {
        return value.some(condition => this._matchQuery(doc, condition));
      }
      
      if (key === '$nor') {
        return !value.some(condition => this._matchQuery(doc, condition));
      }
  
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
            case '$exists':
              return (operand && doc[key] !== undefined) || (!operand && doc[key] === undefined);
            case '$type':
              return typeof doc[key] === operand;
            case '$mod':
              return doc[key] % operand[0] === operand[1];
            case '$text':
              return typeof doc[key] === 'string' && 
                     doc[key].toLowerCase().includes(operand.toLowerCase());
            default:
              return false;
          }
        });
      }
      return doc[key] === value;
    });
  }

  _applyUpdateOperators(doc, update, options = {}) {
    // const now = new Date();
    
    for (const [key, value] of Object.entries(update)) {
      switch (key) {
        case '$set':
          Object.assign(doc, value);
          break;
        case '$unset':
          Object.keys(value).forEach(field => delete doc[field]);
          break;
        case '$inc':
          Object.entries(value).forEach(([field, amount]) => {
            doc[field] = (doc[field] || 0) + amount;
          });
          break;
        case '$mul':
          Object.entries(value).forEach(([field, factor]) => {
            doc[field] = (doc[field] || 0) * factor;
          });
          break;
        case '$min':
          Object.entries(value).forEach(([field, limit]) => {
            doc[field] = Math.min(doc[field] || Infinity, limit);
          });
          break;
        case '$max':
          Object.entries(value).forEach(([field, limit]) => {
            doc[field] = Math.max(doc[field] || -Infinity, limit);
          });
          break;
        case '$rename':
          Object.entries(value).forEach(([oldField, newField]) => {
            if (doc[oldField] !== undefined) {
              doc[newField] = doc[oldField];
              delete doc[oldField];
            }
          });
          break;
        case '$currentDate':
          Object.entries(value).forEach(([field, typeSpec]) => {
            doc[field] = typeSpec === true || typeSpec.$type === 'date' 
              ? new Date() 
              : Date.now();
          });
          break;
        case '$setOnInsert':
          if (options.upsert) {
            Object.assign(doc, value);
          }
          break;
        case '$push':
          Object.entries(value).forEach(([field, item]) => {
            if (!Array.isArray(doc[field])) doc[field] = [];
            doc[field].push(item);
          });
          break;
        case '$pull':
          Object.entries(value).forEach(([field, query]) => {
            if (Array.isArray(doc[field])) {
              doc[field] = doc[field].filter(item => 
                !this._matchQuery({ item }, { item: query })
              );
            }
          });
          break;
        case '$addToSet':
          Object.entries(value).forEach(([field, item]) => {
            if (!Array.isArray(doc[field])) doc[field] = [];
            if (!doc[field].includes(item)) {
              doc[field].push(item);
            }
          });
          break;
        case '$pop':
          Object.entries(value).forEach(([field, pos]) => {
            if (Array.isArray(doc[field])) {
              pos === -1 ? doc[field].shift() : doc[field].pop();
            }
          });
          break;
        case '$pullAll':
          Object.entries(value).forEach(([field, items]) => {
            if (Array.isArray(doc[field])) {
              doc[field] = doc[field].filter(item => !items.includes(item));
            }
          });
          break;
        case '$bit':
          Object.entries(value).forEach(([field, ops]) => {
            if (typeof doc[field] === 'number') {
              if (ops.and !== undefined) doc[field] &= ops.and;
              if (ops.or !== undefined) doc[field] |= ops.or;
              if (ops.xor !== undefined) doc[field] ^= ops.xor;
            }
          });
          break;
      }
    }
    
    // Increment version key
    if (this.schema.options.versionKey !== false) {
      doc.__v = (doc.__v || 0) + 1;
    }

    this.applyTimestamps(doc);
    // doc.updatedAt = now;
    return doc;
   }

  async _executeMiddleware(type, action, doc) {
    const middlewares = this.schema.middleware[type][action] || [];
    for (const middleware of middlewares) {
      await middleware.call(doc);
    }
  }

  async _populateDoc(doc, populateOptions) {
    const populatedDoc = new Document(doc._doc, this.schema, this);
    for (const populate of populateOptions) {
      const path = populate.path;
      const pathSchema = this.schema.path(path);
      if (pathSchema && pathSchema.options && pathSchema.options.ref) {
        const refModel = this.db.model(pathSchema.options.ref);
        if (!refModel) continue;
        const value = doc[path];
        if (!value) continue;
        try {
          const populatedValue = await refModel.findOne({ _id: value });
          if (populatedValue) {
            populatedDoc._populated.set(path, populatedValue);
            populatedDoc[path] = populatedValue;
          }
        } catch (error) {
          console.error(`Error populating ${path}:`, error);
        }
      }
    }
    return populatedDoc;
  }

  // === Backup Operations ===
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

  // === Utility Methods ===
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

  async countDocuments(conditions = {}) {
    const docs = await this._find(conditions);
    return docs.length;
  }

  async createCollection() {
    await this._initializeCollection();
    return this.collection;
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

  async estimatedDocumentCount() {
    const docs = await readJSON(this.collectionPath);
    return docs.length;
  }

  async exists(conditions) {
    const doc = await this.findOne(conditions);
    return doc !== null;
  }

  hydrate(obj) {
    return new Document(obj, this.schema, this);
  }

  async insertMany(docs, options = {}) {
    return this.create(docs, options);
  }

  inspect() {
    return `Model { ${this.modelName} }`;
  }

  $model(name) {
    return this.db.model(name);
  }

  async recompileSchema() {
    this.schema._init();
    return this;
  }

  async increment(conditions, field, amount = 1) {
    const docs = await readJSON(this.collectionPath);
    let modifiedCount = 0;

    for (const doc of docs) {
      if (this._matchQuery(doc, conditions)) {
        // Initialize field if it doesn't exist
        if (typeof doc[field] !== 'number') {
          doc[field] = 0;
        }
        doc[field] += amount;
        doc.updatedAt = new Date();
        modifiedCount++;
      }
    }

    if (modifiedCount > 0) {
      await writeJSON(this.collectionPath, docs);
    }

    return { modifiedCount };
  }

  async startSession() {
    throw new Error('Sessions are not supported in file-based storage');
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

  async validate(obj) {
    return this.schema.validate(obj);
  }

  watch() {
    throw new Error('Watch is not supported in file-based storage');
  }

  where(path) {
    return new Query(this).where(path);
  }
}

module.exports = { Model };