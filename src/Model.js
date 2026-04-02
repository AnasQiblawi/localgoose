const { readJSON, writeJSON, getNestedValue, setNestedValue } = require('./utils.js');
const { ObjectId } = require('bson');
const path = require('path');
const { Query } = require('./Query.js');
const { Aggregate } = require('./Aggregate.js');
const { Document } = require('./Document.js');
const { EventEmitter } = require('events');
const fs = require('fs-extra');

class Model {
  constructor(name, schema, connection) {
    this.name           = name;
    this.schema         = schema;
    this.connection     = connection;
    this.collectionPath = path.join(connection.dbPath, `${name}.json`);
    this.collection = {
      name: this.name,
      collectionPath: this.collectionPath,
      async find() { return readJSON(this.collectionPath); }
    };
    this.base      = connection;
    this.db        = connection;
    this.discriminators = null;
    this.events    = new EventEmitter();
    this.modelName = name;
    this.baseModelName = null;
    this._indexes  = new Map();
    this._searchIndexes = new Map();
    this._lock = Promise.resolve();

    this._initializeCollection();

    // Bind static methods from schema
    for (const [n, fn] of Object.entries(schema.statics)) {
      this[n] = fn.bind(this);
    }
  }

  async _initializeCollection() {
    try {
      await readJSON(this.collectionPath);
    } catch (error) {
      if (error.code === 'ENOENT') await writeJSON(this.collectionPath, []);
    }
  }

  async init() { await this._initializeCollection(); return this; }

  _getCollection(collectionName) {
    try {
      const collectionPath = path.join(this.connection.dbPath, `${collectionName}.json`);
      const data = fs.readFileSync(collectionPath, 'utf8');
      return JSON.parse(data);
    } catch (error) { return []; }
  }

  async _runLocked(fn) {
    const resultPromise = this._lock.then(() => fn(), () => fn());
    this._lock = resultPromise.catch(() => {});
    return resultPromise;
  }

  _checkUniqueConstraints(docs, targetDoc, excludeIndex = -1) {
    const uniquePaths = [];
    this.schema.eachPath((pathname, schemaType) => {
      if (schemaType.options && schemaType.options.unique) {
        uniquePaths.push(pathname);
      }
    });

    for (const path of uniquePaths) {
      const val = getNestedValue(targetDoc, path);
      if (val === undefined || val === null) continue; // Sparse-equivalent behavior for simplicity
      
      for (let i = 0; i < docs.length; i++) {
        if (i === excludeIndex) continue;
        const otherVal = getNestedValue(docs[i], path);
        if (this._deepEqual(val, otherVal)) {
          const err = new Error(`E11000 duplicate key error collection: ${this.name} index: ${path}_1 dup key: { ${path}: ${JSON.stringify(val)} }`);
          err.name = 'MongoServerError';
          err.code = 11000;
          throw err;
        }
      }
    }
  }

  // ─── CRUD ─────────────────────────────────────────────────────────────────
  async _createOne(data) { return this._runLocked(() => this._createOneImpl(data)); }
  async _createOneImpl(data) {
    const defaultedData = { ...data };

    // Apply schema defaults
    for (const [field, schemaDef] of Object.entries(this.schema.definition)) {
      if (defaultedData[field] === undefined && schemaDef.default !== undefined) {
        defaultedData[field] = typeof schemaDef.default === 'function'
          ? schemaDef.default()
          : JSON.parse(JSON.stringify(schemaDef.default));
      }
      if (schemaDef.type === Date && defaultedData[field] && typeof defaultedData[field] === 'string') {
        defaultedData[field] = new Date(defaultedData[field]);
      }
    }

    // Run pre-validate middleware
    await this._executeMiddleware('pre', 'validate', defaultedData);
    const errors = this.schema.validate(defaultedData);
    if (errors.length > 0) throw new Error(errors.join(', '));
    await this._executeMiddleware('post', 'validate', defaultedData);

    // Run pre-save middleware (bind to a doc-like object for `this` context)
    const docProxy = this._makeDocProxy(defaultedData);
    await this._executeMiddleware('pre', 'save', docProxy);
    Object.assign(defaultedData, docProxy._data);

    const docs = await readJSON(this.collectionPath);
    const now = new Date();

    const vKey = this.schema.options.versionKey !== false
      ? (typeof this.schema.options.versionKey === 'string' ? this.schema.options.versionKey : '__v')
      : null;

    const newDoc = {
      _id: new ObjectId().toString(),
      ...defaultedData,
      createdAt: defaultedData.createdAt || now,
      updatedAt: now,
      ...(vKey ? { [vKey]: defaultedData[vKey] !== undefined ? defaultedData[vKey] : 0 } : {})
    };

    this._checkUniqueConstraints(docs, newDoc);

    docs.push(newDoc);
    await writeJSON(this.collectionPath, docs);

    const document = new Document(newDoc, this.schema, this);
    document._isNew = false;
    document.isNew  = false;

    await this._executeMiddleware('post', 'save', document);
    return document;
  }

  // Minimal proxy so pre-save `this.field = x` mutations are captured
  _makeDocProxy(data) {
    const proxy = { _data: { ...data } };
    const handler = {
      get(t, k) { if (k === '_data') return t._data; return t._data[k]; },
      set(t, k, v) { if (k === '_data') { t._data = v; return true; } t._data[k] = v; return true; }
    };
    return new Proxy(proxy, handler);
  }

  async create(data) {
    if (Array.isArray(data)) {
      // Run pre-insertMany middleware
      await this._executeMiddleware('pre', 'insertMany', data);
      const results = await Promise.all(data.map(item => this._createOne(item)));
      await this._executeMiddleware('post', 'insertMany', results);
      return results;
    }
    return this._createOne(data);
  }

  async updateOne(conditions, update, options = {}) { return this._runLocked(() => this._updateOneImpl(conditions, update, options)); }
  async _updateOneImpl(conditions, update, options = {}) {
    const docs = await readJSON(this.collectionPath);
    const index = docs.findIndex(doc => this._matchQuery(doc, conditions));
    if (index !== -1) {
      docs[index] = this._applyUpdateOperators(docs[index], update, options);
      this._checkUniqueConstraints(docs, docs[index], index);
      await writeJSON(this.collectionPath, docs);
      return { acknowledged: true, modifiedCount: 1, upsertedCount: 0, upsertedId: null };
    }
    if (options.upsert) {
      const newDoc = await this._createOneImpl({ ...this._flattenConditions(conditions), ...this._extractSetValues(update) });
      return { acknowledged: true, modifiedCount: 0, upsertedCount: 1, upsertedId: newDoc._id };
    }
    return { acknowledged: true, modifiedCount: 0, upsertedCount: 0, upsertedId: null };
  }

  async updateMany(conditions, update, options = {}) { return this._runLocked(() => this._updateManyImpl(conditions, update, options)); }
  async _updateManyImpl(conditions, update, options = {}) {
    const docs = await readJSON(this.collectionPath);
    let modifiedCount = 0;
    for (let i = 0; i < docs.length; i++) {
      if (this._matchQuery(docs[i], conditions)) {
        docs[i] = this._applyUpdateOperators(docs[i], update, options);
        this._checkUniqueConstraints(docs, docs[i], i);
        modifiedCount++;
      }
    }
    await writeJSON(this.collectionPath, docs);
    return { acknowledged: true, modifiedCount, upsertedCount: 0 };
  }

  async deleteOne(conditions = {}) { return this._runLocked(() => this._deleteOneImpl(conditions)); }
  async _deleteOneImpl(conditions = {}) {
    const docs = await readJSON(this.collectionPath);
    const index = docs.findIndex(doc => this._matchQuery(doc, conditions));
    if (index !== -1) {
      docs.splice(index, 1);
      await writeJSON(this.collectionPath, docs);
      return { acknowledged: true, deletedCount: 1 };
    }
    return { acknowledged: true, deletedCount: 0 };
  }

  async deleteMany(conditions = {}) { return this._runLocked(() => this._deleteManyImpl(conditions)); }
  async _deleteManyImpl(conditions = {}) {
    const docs = await readJSON(this.collectionPath);
    const remaining = docs.filter(doc => !this._matchQuery(doc, conditions));
    await writeJSON(this.collectionPath, remaining);
    return { acknowledged: true, deletedCount: docs.length - remaining.length };
  }

  async replaceOne(conditions, doc, options = {}) {
    return this.updateOne(conditions, doc, { ...options, overwrite: true });
  }

  // ─── Query Operations ─────────────────────────────────────────────────────
  find(conditions = {}, projection, options = {}) {
    if (typeof projection === 'object' && !Array.isArray(projection) && projection !== null && !options) {
      options = projection; projection = null;
    }
    const query = new Query(this, conditions);
    if (projection) query.select(projection);
    if (options && options.lean) query.lean();
    return query;
  }

  findOne(conditions = {}, projection, options = {}) {
    const query = this.find(conditions, projection, options);
    query._limit = 1;
    return query;
  }

  // findById returns a Query for chaining: .findById(id).populate('author').exec()
  findById(id, projection, options) {
    if (!id) return this.findOne({ _id: null }, projection, options);
    return this.findOne({ _id: typeof id === 'string' ? id : id.toString() }, projection, options);
  }

  async findOneAndDelete(conditions, options = {}) {
    const doc = await this.findOne(conditions).exec();
    if (doc) await this.deleteOne({ _id: doc._id });
    return doc;
  }

  async findOneAndReplace(conditions, replacement, options = {}) {
    const doc = await this.findOne(conditions).exec();
    if (doc) {
      Object.assign(doc._doc, replacement);
      await doc.save();
    } else if (options.upsert) {
      return this.create(replacement);
    }
    return doc;
  }

  async findOneAndUpdate(conditions, update, options = {}) { return this._runLocked(() => this._findOneAndUpdateImpl(conditions, update, options)); }
  async _findOneAndUpdateImpl(conditions, update, options = {}) {
    const docs = await readJSON(this.collectionPath);
    const index = docs.findIndex(doc => this._matchQuery(doc, conditions));
    if (index !== -1) {
      const before = options.new === false ? { ...docs[index] } : null;
      docs[index] = this._applyUpdateOperators(docs[index], update, options);
      this._checkUniqueConstraints(docs, docs[index], index);
      await writeJSON(this.collectionPath, docs);
      const resultDoc = options.new === false ? before : docs[index];
      return new Document(resultDoc, this.schema, this);
    } else if (options.upsert) {
      return this._createOneImpl({ ...this._flattenConditions(conditions), ...this._extractSetValues(update) });
    }
    return null;
  }

  async findByIdAndDelete(id)              { return this.findOneAndDelete({ _id: id }); }
  async findByIdAndRemove(id)              { return this.findOneAndDelete({ _id: id }); }
  async findByIdAndUpdate(id, update, opts){ return this.findOneAndUpdate({ _id: id }, update, opts); }

  // ─── Internal Find ────────────────────────────────────────────────────────
  async _find(conditions = {}) {
    const docs = await readJSON(this.collectionPath);
    if (!conditions || Object.keys(conditions).length === 0) return docs;
    return docs.filter(doc => this._matchQuery(doc, conditions));
  }

  // ─── Query Matching ───────────────────────────────────────────────────────
  _matchQuery(doc, query) {
    if (!query || typeof query !== 'object') return true;
    return Object.entries(query).every(([key, value]) => {
      if (key === '$and') return value.every(c => this._matchQuery(doc, c));
      if (key === '$or')  return value.some(c  => this._matchQuery(doc, c));
      if (key === '$nor') return !value.some(c => this._matchQuery(doc, c));
      if (key === '$where') {
        if (typeof value === 'function') return value.call(doc);
        return true;
      }
      if (key === '$text') return true; // handled per-field below

      // Support dot-notation field access
      const docValue = getNestedValue(doc, key);

      if (value !== null && typeof value === 'object' &&
          !Array.isArray(value) && !(value instanceof Date) && !(value instanceof RegExp)) {
        return Object.entries(value).every(([op, operand]) => {
          switch (op) {
            case '$eq':  return this._deepEqual(docValue, operand);
            case '$gt':  return docValue > operand;
            case '$gte': return docValue >= operand;
            case '$lt':  return docValue < operand;
            case '$lte': return docValue <= operand;
            case '$ne':  return !this._deepEqual(docValue, operand);
            case '$in': {
              const arr = Array.isArray(docValue) ? docValue : [docValue];
              return operand.some(item => arr.some(a => this._deepEqual(a, item)));
            }
            case '$nin': {
              const arr = Array.isArray(docValue) ? docValue : [docValue];
              return !operand.some(item => arr.some(a => this._deepEqual(a, item)));
            }
            case '$regex': {
              const rgx = operand instanceof RegExp ? operand : new RegExp(operand, value.$options);
              return typeof docValue === 'string' && rgx.test(docValue);
            }
            case '$options': return true; // consumed alongside $regex
            case '$exists': return operand ? docValue !== undefined : docValue === undefined;
            case '$type':   return typeof docValue === operand;
            case '$mod':    return typeof docValue === 'number' && docValue % operand[0] === operand[1];
            case '$size':   return Array.isArray(docValue) && docValue.length === operand;
            case '$all':    return Array.isArray(docValue) && operand.every(item => docValue.some(d => this._deepEqual(d, item)));
            case '$elemMatch': return Array.isArray(docValue) && docValue.some(item => {
              // If item is a primitive and operand uses $ operators, wrap item
              if (item === null || typeof item !== 'object') {
                return this._matchQuery({ _v: item }, Object.fromEntries(Object.entries(operand).map(([k, v]) => ['_v', { [k]: v }])));
              }
              return this._matchQuery(item, operand);
            });
            case '$not': {
              if (operand instanceof RegExp) return !operand.test(docValue);
              return !this._matchQuery({ [key]: docValue }, { [key]: operand });
            }
            case '$text': {
              const search = operand.$search || operand;
              return typeof docValue === 'string' && docValue.toLowerCase().includes(String(search).toLowerCase());
            }
            case '$near':        return true; // geospatial: not supported in file mode
            case '$maxDistance': return true;
            default: return false;
          }
        });
      }

      if (value instanceof RegExp) return typeof docValue === 'string' && value.test(docValue);
      if (value instanceof Date) {
        const dv = docValue instanceof Date ? docValue : new Date(docValue);
        return !isNaN(dv) && dv.getTime() === value.getTime();
      }
      return this._deepEqual(docValue, value);
    });
  }

  _deepEqual(a, b) {
    if (a === b) return true;
    if (a instanceof Date && b instanceof Date) return a.getTime() === b.getTime();
    // ObjectId / string comparison
    const as = (a !== null && a !== undefined && typeof a.toString === 'function') ? a.toString() : String(a);
    const bs = (b !== null && b !== undefined && typeof b.toString === 'function') ? b.toString() : String(b);
    return as === bs;
  }

  // ─── Update Operators ─────────────────────────────────────────────────────
  _applyUpdateOperators(doc, update, options = {}) {
    const hasOperators = update && Object.keys(update).some(k => k.startsWith('$'));

    if (!hasOperators) {
      // Direct replace (no $ operators): merge fields, keep _id
      const { _id, ...rest } = update || {};
      Object.assign(doc, rest);
    } else {
      for (const [op, value] of Object.entries(update)) {
        switch (op) {
          case '$set':     Object.entries(value).forEach(([f, v]) => setNestedValue(doc, f, v)); break;
          case '$unset':   Object.keys(value).forEach(f => {
            const parts = f.split('.');
            let curr = doc;
            for(let i=0; i<parts.length-1; i++) { if(!curr) return; curr = curr[parts[i]]; }
            if(curr) delete curr[parts[parts.length-1]];
          }); break;
          case '$inc':
            Object.entries(value).forEach(([f, amt]) => { const curr = getNestedValue(doc, f) || 0; setNestedValue(doc, f, curr + amt); });
            break;
          case '$mul':
            Object.entries(value).forEach(([f, fac]) => { const curr = getNestedValue(doc, f) || 0; setNestedValue(doc, f, curr * fac); });
            break;
          case '$min':
            Object.entries(value).forEach(([f, lim]) => { const curr = getNestedValue(doc, f); setNestedValue(doc, f, Math.min(curr !== undefined ? curr : Infinity, lim)); });
            break;
          case '$max':
            Object.entries(value).forEach(([f, lim]) => { const curr = getNestedValue(doc, f); setNestedValue(doc, f, Math.max(curr !== undefined ? curr : -Infinity, lim)); });
            break;
          case '$rename':
            Object.entries(value).forEach(([old, nw]) => {
              const oldV = getNestedValue(doc, old);
              if (oldV !== undefined) { 
                setNestedValue(doc, nw, oldV); 
                const parts = old.split('.');
                let curr = doc;
                for(let i=0; i<parts.length-1; i++) { if(!curr) return; curr = curr[parts[i]]; }
                if(curr) delete curr[parts[parts.length-1]];
              }
            });
            break;
          case '$currentDate':
            Object.entries(value).forEach(([f, spec]) => {
              setNestedValue(doc, f, (spec === true || (spec && spec.$type === 'date')) ? new Date() : Date.now());
            });
            break;
          case '$setOnInsert':
            if (options.upsert) Object.entries(value).forEach(([f, v]) => setNestedValue(doc, f, v));
            break;
          case '$push':
            Object.entries(value).forEach(([f, item]) => {
              let arr = getNestedValue(doc, f);
              if (!Array.isArray(arr)) { arr = []; setNestedValue(doc, f, arr); }
              if (item && typeof item === 'object' && item.$each) {
                const each  = item.$each || [];
                const slice = item.$slice;
                const sort  = item.$sort;
                const pos   = item.$position;
                if (pos !== undefined) doc[f].splice(pos, 0, ...each);
                else doc[f].push(...each);
                if (sort) {
                  if (typeof sort === 'number') doc[f].sort((a, b) => sort * (a > b ? 1 : a < b ? -1 : 0));
                  else doc[f].sort((a, b) => { for (const [sk, sv] of Object.entries(sort)) { if (a[sk] < b[sk]) return -sv; if (a[sk] > b[sk]) return sv; } return 0; });
                }
                if (slice !== undefined) doc[f] = slice >= 0 ? doc[f].slice(0, slice) : doc[f].slice(slice);
              } else {
                doc[f].push(item);
              }
            });
            break;
          case '$pull':
            Object.entries(value).forEach(([f, query]) => {
              let arr = getNestedValue(doc, f);
              if (Array.isArray(arr)) {
                arr = arr.filter(item => {
                  if (query !== null && typeof query === 'object') return !this._matchQuery({ _item: item }, { _item: query });
                  return !this._deepEqual(item, query);
                });
                setNestedValue(doc, f, arr);
              }
            });
            break;
          case '$pullAll':
            Object.entries(value).forEach(([f, items]) => {
              let arr = getNestedValue(doc, f);
              if (Array.isArray(arr)) setNestedValue(doc, f, arr.filter(item => !items.some(i => this._deepEqual(item, i))));
            });
            break;
          case '$addToSet':
            Object.entries(value).forEach(([f, item]) => {
              let arr = getNestedValue(doc, f);
              if (!Array.isArray(arr)) { arr = []; setNestedValue(doc, f, arr); }
              const items = item && item.$each ? item.$each : [item];
              items.forEach(i => { if (!arr.some(e => this._deepEqual(e, i))) arr.push(i); });
            });
            break;
          case '$pop':
            Object.entries(value).forEach(([f, pos]) => {
              let arr = getNestedValue(doc, f);
              if (Array.isArray(arr)) pos === -1 ? arr.shift() : arr.pop();
            });
            break;
          case '$bit':
            Object.entries(value).forEach(([f, ops]) => {
              let val = getNestedValue(doc, f);
              if (typeof val === 'number') {
                if (ops.and !== undefined) val &= ops.and;
                if (ops.or  !== undefined) val |= ops.or;
                if (ops.xor !== undefined) val ^= ops.xor;
                setNestedValue(doc, f, val);
              }
            });
            break;
        }
      }
    }

    // Increment version key
    const vKey = this.schema.options.versionKey !== false
      ? (typeof this.schema.options.versionKey === 'string' ? this.schema.options.versionKey : '__v')
      : null;
    if (vKey) doc[vKey] = (doc[vKey] || 0) + 1;

    this._applyTimestamps(doc);
    return doc;
  }

  _applyTimestamps(doc) {
    const now = new Date();
    if (!doc.createdAt) doc.createdAt = now;
    doc.updatedAt = now;
    return doc;
  }

  _flattenConditions(conditions) {
    const flat = {};
    for (const [k, v] of Object.entries(conditions)) {
      if (!k.startsWith('$')) flat[k] = v;
    }
    return flat;
  }

  _extractSetValues(update) {
    const result = {};
    if (!update) return result;
    for (const [op, value] of Object.entries(update)) {
      if (op === '$set' || op === '$setOnInsert') Object.assign(result, value);
      else if (!op.startsWith('$')) result[op] = value;
    }
    return result;
  }

  async _executeMiddleware(type, action, doc) {
    const middlewares = this.schema.middleware[type][action] || [];
    for (const mw of middlewares) await mw.call(doc);
  }

  // ─── Population ───────────────────────────────────────────────────────────
  async _populateDoc(doc, populateOptions) {
    const populatedDoc = new Document(doc._doc || doc, this.schema, this);
    for (const populate of populateOptions) {
      const p = populate.path;
      const pathSchema = this.schema.path(p);
      if (!pathSchema || !pathSchema.options || !pathSchema.options.ref) continue;
      const refModel = this.db.model(pathSchema.options.ref);
      if (!refModel) continue;
      const value = doc[p] || (doc._doc && doc._doc[p]);
      if (!value) continue;
      try {
        let q = refModel.findOne({ _id: value });
        if (populate.select) q = q.select(populate.select);
        const pv = await q.exec();
        if (pv) { populatedDoc._populated.set(p, pv); populatedDoc[p] = pv; }
      } catch (err) { console.error(`Error populating ${p}:`, err); }
    }
    return populatedDoc;
  }

  // ─── Aggregate ────────────────────────────────────────────────────────────
  aggregate(pipeline = []) {
    const agg = new Aggregate(this, pipeline);
    // Run pre-aggregate middleware
    const preHooks = this.schema.middleware.pre.aggregate || [];
    agg._preHooks = preHooks;
    return agg;
  }

  // ─── Backup / Restore ─────────────────────────────────────────────────────
  async backup(backupPath) {
    const def = path.join(path.dirname(this.collectionPath),
      `${this.name}_backup_${new Date().toISOString().replace(/:/g, '-')}.json`);
    const docs = await readJSON(this.collectionPath);
    await writeJSON(backupPath || def, docs);
    return backupPath || def;
  }

  async restore(backupPath) {
    if (!backupPath) {
      const dir = path.dirname(this.collectionPath);
      const files = await fs.readdir(dir);
      const found = files.filter(f => f.startsWith(`${this.name}_backup_`) && f.endsWith('.json')).sort().reverse();
      if (!found.length) throw new Error(`No backup files found for model: ${this.name}`);
      backupPath = path.join(dir, found[0]);
    }
    const backupDocs = await readJSON(backupPath);
    await writeJSON(this.collectionPath, backupDocs);
    return backupPath;
  }

  async listBackups() {
    try {
      const dir = path.dirname(this.collectionPath);
      const files = await fs.readdir(dir);
      return files
        .filter(f => f.startsWith(`${this.name}_backup_`) && f.endsWith('.json'))
        .map(filename => {
          const full = path.join(dir, filename);
          const stats = fs.statSync(full);
          return { filename, path: full, createdAt: stats.birthtime, size: stats.size };
        })
        .sort((a, b) => b.createdAt - a.createdAt);
    } catch { return []; }
  }

  async cleanupBackups(fileName = null) {
    const backups = await this.listBackups();
    if (fileName) {
      const target = backups.find(b => b.filename === fileName);
      if (!target) throw new Error(`Backup file '${fileName}' not found`);
      await fs.unlink(target.path);
      return [target];
    }
    for (const b of backups) await fs.unlink(b.path);
    return [];
  }

  // ─── Index Operations ─────────────────────────────────────────────────────
  async createIndexes(indexes = []) {
    for (const [fields, options] of indexes) {
      this._indexes.set(Object.keys(fields).sort().join('_'), { fields, options });
    }
    return indexes.length;
  }

  async cleanIndexes()  { this._indexes.clear(); return true; }
  async ensureIndexes() { return this.createIndexes(Array.from(this._indexes.values())); }
  async syncIndexes()   { await this.cleanIndexes(); await this.ensureIndexes(); return this._indexes.size; }
  async listIndexes()   { return Array.from(this._indexes.values()); }
  async diffIndexes()   { return { toDrop: [], toCreate: Array.from(this._indexes.values()) }; }

  async createSearchIndex(opts = {}) { this._searchIndexes.set(opts.name || 'default', opts); return true; }
  async dropSearchIndex(name = 'default') { return this._searchIndexes.delete(name); }
  async listSearchIndexes() { return Array.from(this._searchIndexes.values()); }
  async updateSearchIndex(opts = {}) {
    const name = opts.name || 'default';
    if (this._searchIndexes.has(name)) { this._searchIndexes.set(name, { ...this._searchIndexes.get(name), ...opts }); return true; }
    return false;
  }

  // ─── Utility Methods ──────────────────────────────────────────────────────
  async applyDefaults(doc) {
    for (const [p, st] of this.schema._paths.entries()) {
      if (doc[p] === undefined && st.getDefault() !== undefined) doc[p] = st.getDefault();
    }
    return doc;
  }

  applyVirtuals(doc) {
    const virtuals = {};
    for (const [p, virtual] of Object.entries(this.schema.virtuals)) {
      try { virtuals[p] = virtual.applyGetters(undefined, doc); } catch (e) {}
    }
    return { ...doc, ...virtuals };
  }

  async bulkSave(docs, options = {}) {
    return this.bulkWrite(docs.map(doc => ({ insertOne: { document: doc } })), options);
  }

  async bulkWrite(operations, options = {}) { return this._runLocked(() => this._bulkWriteImpl(operations, options)); }
  async _bulkWriteImpl(operations, options = {}) {
    const docs = await readJSON(this.collectionPath);
    let nModified = 0, nInserted = 0, nUpserted = 0, nRemoved = 0;

    for (const op of operations) {
      if (op.insertOne) {
        const doc = await this.applyDefaults({ ...op.insertOne.document });
        this._applyTimestamps(doc);
        if (!doc._id) doc._id = new ObjectId().toString();
        this._checkUniqueConstraints(docs, doc);
        docs.push(doc);
        nInserted++;
      } else if (op.updateOne) {
        const idx = docs.findIndex(d => this._matchQuery(d, op.updateOne.filter));
        if (idx !== -1) {
          docs[idx] = this._applyUpdateOperators(docs[idx], op.updateOne.update, op.updateOne);
          this._checkUniqueConstraints(docs, docs[idx], idx);
          nModified++;
        } else if (op.updateOne.upsert) {
          const doc = await this.applyDefaults({ ...op.updateOne.filter, ...this._extractSetValues(op.updateOne.update) });
          this._applyTimestamps(doc);
          doc._id = new ObjectId().toString();
          this._checkUniqueConstraints(docs, doc);
          docs.push(doc);
          nUpserted++;
        }
      } else if (op.updateMany) {
        for (let i = 0; i < docs.length; i++) {
          if (this._matchQuery(docs[i], op.updateMany.filter)) {
            docs[i] = this._applyUpdateOperators(docs[i], op.updateMany.update, op.updateMany);
            this._checkUniqueConstraints(docs, docs[i], i);
            nModified++;
          }
        }
      } else if (op.deleteOne) {
        const idx = docs.findIndex(d => this._matchQuery(d, op.deleteOne.filter));
        if (idx !== -1) { docs.splice(idx, 1); nRemoved++; }
      } else if (op.deleteMany) {
        const before = docs.length;
        const remaining = docs.filter(d => !this._matchQuery(d, op.deleteMany.filter));
        nRemoved += before - remaining.length;
        docs.length = 0;
        docs.push(...remaining);
      } else if (op.replaceOne) {
        const idx = docs.findIndex(d => this._matchQuery(d, op.replaceOne.filter));
        if (idx !== -1) {
          const { _id } = docs[idx];
          docs[idx] = { _id, ...op.replaceOne.replacement };
          this._applyTimestamps(docs[idx]);
          nModified++;
        } else if (op.replaceOne.upsert) {
          const doc = { _id: new ObjectId().toString(), ...op.replaceOne.replacement };
          this._applyTimestamps(doc);
          docs.push(doc);
          nUpserted++;
        }
      }
    }

    await writeJSON(this.collectionPath, docs);
    return { acknowledged: true, insertedCount: nInserted, modifiedCount: nModified, upsertedCount: nUpserted, deletedCount: nRemoved };
  }

  castObject(obj) {
    const casted = {};
    for (const [p, value] of Object.entries(obj)) {
      const st = this.schema.path(p);
      casted[p] = st ? st.cast(value) : value;
    }
    return casted;
  }

  async countDocuments(conditions = {}) {
    const docs = await this._find(conditions);
    return docs.length;
  }

  async createCollection() { await this._initializeCollection(); return this.collection; }

  discriminator(name, schema) {
    if (!this.discriminators) this.discriminators = {};
    this.discriminators[name] = new Model(name, schema, this.connection);
    return this.discriminators[name];
  }

  async distinct(field, conditions = {}) {
    const docs = await this._find(conditions);
    const values = [];
    for (const doc of docs) {
      const v = getNestedValue(doc, field);
      if (Array.isArray(v)) v.forEach(item => { if (!values.some(x => this._deepEqual(x, item))) values.push(item); });
      else if (v !== undefined) { if (!values.some(x => this._deepEqual(x, v))) values.push(v); }
    }
    return values;
  }

  async estimatedDocumentCount() {
    const docs = await readJSON(this.collectionPath);
    return docs.length;
  }

  async exists(conditions) {
    const doc = await this.findOne(conditions).exec();
    return doc !== null ? { _id: doc._id } : null;
  }

  hydrate(obj) {
    const doc = new Document(obj, this.schema, this);
    doc._isNew = false;
    doc.isNew  = false;
    return doc;
  }

  async insertMany(docs, options = {}) {
    await this._executeMiddleware('pre', 'insertMany', docs);
    const results = await Promise.all(docs.map(d => this._createOne(d)));
    await this._executeMiddleware('post', 'insertMany', results);
    if (options.lean) return results.map(d => d.toObject());
    return results;
  }

  inspect() { return `Model { ${this.modelName} }`; }
  $model(name) { return this.db.model(name); }

  async recompileSchema() { this.schema._init(); return this; }

  async increment(conditions, field, amount = 1) {
    return this.updateMany(conditions, { $inc: { [field]: amount } });
  }

  async startSession() { throw new Error('Sessions are not supported in file-based storage'); }

  translateAliases(raw) {
    const translated = { ...raw };
    for (const [alias, p] of Object.entries(this.schema.aliases || {})) {
      if (translated[alias] !== undefined) { translated[p] = translated[alias]; delete translated[alias]; }
    }
    return translated;
  }

  async validate(obj) { return this.schema.validate(obj); }

  watch() { throw new Error('Watch is not supported in file-based storage'); }

  where(p) { return new Query(this).where(p); }

  namespace() { return `${this.db.name}.${this.collection.name}`; }

  // Deprecated shims
  static count(conditions) {
    console.warn('Model.count() is deprecated. Use Model.countDocuments() instead.');
    return this.countDocuments(conditions);
  }
  static remove(conditions) {
    console.warn('Model.remove() is deprecated. Use Model.deleteMany() instead.');
    return this.deleteMany(conditions);
  }
  static update(conditions, doc, options) {
    console.warn('Model.update() is deprecated. Use Model.updateOne() or updateMany() instead.');
    return options && options.multi ? this.updateMany(conditions, doc, options) : this.updateOne(conditions, doc, options);
  }

  static async populate(docs, options) {
    if (!docs) return docs;
    const isArray = Array.isArray(docs);
    const documents = isArray ? docs : [docs];
    const p = typeof options === 'string' ? options : options.path;
    const select = options.select || '';
    for (const doc of documents) {
      if (doc[p]) {
        const refModel = this.db?.model(this.schema?.path(p)?.options?.ref);
        if (refModel) {
          let q = refModel.findOne({ _id: doc[p] });
          if (select) q = q.select(select);
          doc[p] = await q.exec();
        }
      }
    }
    return isArray ? documents : documents[0];
  }

  static mapReduce() { throw new Error('mapReduce is not supported in this implementation'); }

  get $where() {
    return (condition) => this.find({ $where: condition });
  }

  async insertOne(doc) {
    return this.create(doc);
  }
}

module.exports = { Model };