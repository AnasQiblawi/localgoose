const { EventEmitter } = require('events');

class Document {
  constructor(obj, schema, model) {
    this._doc      = { ...obj };
    this._schema   = schema;
    this._model    = model;
    this._modifiedPaths = new Set();
    this._populated     = new Map();
    this._parent  = null;
    this._isNew   = true;
    this._snapshot = null;
    this._session  = null;
    this._locals   = {};
    this._op       = null;
    this._where    = {};
    this._deleted  = false;
    this._errors   = {};
    this._selected = new Set(Object.keys(obj));
    this._init     = new Set();

    this.isNew  = true;
    this.errors = {};
    this._id    = obj._id;
    this.id     = obj._id ? obj._id.toString() : undefined;

    // Set up virtuals
    for (const [vpath, virtual] of Object.entries(schema.virtuals)) {
      Object.defineProperty(this, vpath, {
        get: function () { return virtual.applyGetters(undefined, this); },
        set: function (value) { return virtual.applySetters(value, this); },
        configurable: true,
        enumerable: false
      });
    }

    // Set up instance methods
    for (const [name, method] of Object.entries(schema.methods)) {
      this[name] = method.bind(this);
    }

    // Set up direct property access for each doc field
    for (const key of Object.keys(this._doc)) {
      if (!(key in this)) {
        Object.defineProperty(this, key, {
          get: function () { return this._doc[key]; },
          set: function (value) {
            this._doc[key] = value;
            this._modifiedPaths.add(key);
          },
          configurable: true,
          enumerable: true
        });
      }
    }
  }

  // === Document State ===
  init(obj) { return this.$init(obj); }
  $init(obj) {
    Object.assign(this._doc, obj);
    this._modifiedPaths.clear();
    this._isNew = false;
    this.isNew  = false;
    Object.keys(obj).forEach(key => this._init.add(key));
    return this;
  }

  $clone() { return new Document({ ...this._doc }, this._schema, this._model); }
  $isNew()      { return this._isNew; }
  $isDeleted()  { return this._deleted; }

  // === Data Access ===
  get(path) { return this._doc[path]; }

  set(path, val) {
    if (typeof path === 'string') {
      this._doc[path] = val;
      this._modifiedPaths.add(path);
    } else if (typeof path === 'object') {
      for (const [k, v] of Object.entries(path)) {
        this._doc[k] = v;
        this._modifiedPaths.add(k);
      }
    }
    return this;
  }

  $set(path, val) { return this.set(path, val); }

  $inc(path, val = 1) {
    const cur = this.get(path) || 0;
    return this.set(path, cur + val);
  }

  overwrite(obj) {
    this._doc = { _id: this._id, ...obj };
    this._modifiedPaths = new Set(Object.keys(obj));
    return this;
  }

  // === Modification Tracking ===
  isModified(path) { return this.$isModified(path); }
  $isModified(path) {
    return path ? this._modifiedPaths.has(path) : this._modifiedPaths.size > 0;
  }
  isDirectModified(path)  { return this._modifiedPaths.has(path); }
  markModified(path)      { this._modifiedPaths.add(path); return this; }
  unmarkModified(path)    { this._modifiedPaths.delete(path); return this; }
  modifiedPaths()         { return Array.from(this._modifiedPaths); }
  directModifiedPaths()   { return Array.from(this._modifiedPaths); }

  getChanges() {
    const changes = {};
    for (const path of this._modifiedPaths) changes[path] = this.get(path);
    return changes;
  }

  // === Population ===
  async populate(path, select) {
    if (typeof path === 'string') {
      const st = this._schema.path(path);
      if (st && st.options.ref) {
        const refModel = this._model.db.models[st.options.ref];
        if (!refModel) return this;
        const value = this.get(path);
        if (!value) return this;
        try {
          let query = refModel.findOne({ _id: value });
          if (select) query = query.select(select);
          const populatedDoc = await query.exec();
          if (populatedDoc) {
            this._populated.set(path, populatedDoc);
            // Reassign so property access returns the populated doc
            Object.defineProperty(this, path, {
              get: function () { return this._populated.get(path); },
              configurable: true,
              enumerable: true
            });
          }
        } catch (error) {
          console.error(`Error populating ${path}:`, error);
        }
      }
    } else if (typeof path === 'object') {
      // populate({ path, select, model })
      const opts = path;
      await this.populate(opts.path, opts.select);
    }
    return this;
  }

  populated(path) { return this.$populated(path); }
  $populated(path) { return this._populated.get(path); }
  $assertPopulated(path, values) {
    if (!this._populated.has(path)) throw new Error(`Path '${path}' is not populated`);
    if (values) this.$set(values);
    return this;
  }

  depopulate(path) {
    if (path) this._populated.delete(path);
    else this._populated.clear();
    return this;
  }

  $getPopulatedDocs() {
    return Array.from(this._populated.entries()).map(([path, doc]) => ({ path, doc }));
  }

  // === Validation ===
  async validate(pathsToValidate) { return this.$validate(pathsToValidate); }

  validateSync(pathsToValidate) {
    const paths = pathsToValidate || Array.from(this._modifiedPaths);
    return paths.map(p => this._validatePath(p)).filter(Boolean);
  }

  async $validate(pathsToValidate) {
    const paths = pathsToValidate || Array.from(this._modifiedPaths);
    const errors = await Promise.all(paths.map(p => this._validatePath(p)));
    return errors.filter(Boolean);
  }

  async _validatePath(path) {
    const st = this._schema.path(path);
    if (!st) return null;
    const value = this.get(path);
    return new Promise(resolve => {
      st.doValidate(value, (error) => {
        if (error) { this._errors[path] = error; resolve(error); }
        else resolve(null);
      }, this);
    });
  }

  invalidate(path, err) { this._errors[path] = err; return this; }
  $markValid(path) { delete this._errors[path]; return this; }

  // === Persistence ===
  async save(options = {}) {
    // Apply timestamps
    const now = new Date();
    this._doc.updatedAt = now;
    if (this._isNew || this.isNew) this._doc.createdAt = now;

    // Run pre-save middleware
    if (this._schema.middleware.pre.save) {
      for (const mw of this._schema.middleware.pre.save) {
        await mw.call(this);
      }
    }

    // Validate
    const allPaths = Array.from(this._schema._paths.keys());
    const errors = await Promise.all(allPaths.map(p => {
      const error = this._schema._validatePath(p, this._doc[p]);
      return error;
    }));
    const validationErrors = errors.filter(Boolean);
    if (validationErrors.length > 0) {
      throw new Error(validationErrors.join(', '));
    }

    if (this._isNew || this.isNew) {
      // New document: insert
      const { readJSON, writeJSON } = require('./utils.js');
      const docs = await readJSON(this._model.collectionPath);
      if (!this._doc._id) {
        const { ObjectId } = require('bson');
        this._doc._id = new ObjectId().toString();
        this._id = this._doc._id;
        this.id = this._doc._id;
      }
      // Initialize versionKey
      const vKey = this._schema.options.versionKey !== false
        ? (typeof this._schema.options.versionKey === 'string' ? this._schema.options.versionKey : '__v')
        : null;
      if (vKey && this._doc[vKey] === undefined) this._doc[vKey] = 0;

      docs.push(this._doc);
      await writeJSON(this._model.collectionPath, docs);
    } else {
      // Existing document: update using $set to avoid double __v increment
      await this._model.updateOne({ _id: this._id }, { $set: this._doc });
    }

    // Run post-save middleware
    if (this._schema.middleware.post.save) {
      for (const mw of this._schema.middleware.post.save) {
        await mw.call(this);
      }
    }

    this._isNew = false;
    this.isNew  = false;
    this._modifiedPaths.clear();
    return this;
  }

  async updateOne(update, options = {}) {
    return this._model.updateOne({ _id: this._id }, update, options);
  }

  async replaceOne(replacement, options = {}) {
    return this._model.replaceOne({ _id: this._id }, replacement, options);
  }

  async delete() {
    await this._model.deleteOne({ _id: this._id });
    this._deleted = true;
    return this;
  }

  async remove() { return this.delete(); }

  // === State Checks ===
  $isEmpty(path) {
    const val = this.get(path);
    if (val === null || val === undefined) return true;
    if (Array.isArray(val)) return val.length === 0;
    if (typeof val === 'object') return Object.keys(val).length === 0;
    if (typeof val === 'string') return val.trim().length === 0;
    return false;
  }

  $isDefault(path) {
    const st = this._schema.path(path);
    return st ? this.get(path) === st.getDefault() : false;
  }

  isInit(path)             { return this._init.has(path); }
  isDirectSelected(path)   { return this._selected.has(path); }
  isSelected(path)         { return this._selected.has(path); }
  $isSelected(path)        { return !this._selected || this._selected.has(path); }

  equals(doc) {
    return doc instanceof Document && this._id.toString() === doc._id.toString();
  }

  // === Snapshot ===
  $createModifiedPathsSnapshot()  { this._snapshot = new Set(this._modifiedPaths); return this; }
  $restoreModifiedPathsSnapshot() { if (this._snapshot) this._modifiedPaths = new Set(this._snapshot); return this; }
  $clearModifiedPaths()           { this._modifiedPaths.clear(); return this; }
  $ignore(path)                   { this._modifiedPaths.delete(path); return this; }

  // === Subdocuments ===
  $getAllSubdocs() {
    const subdocs = [];
    const addSubdocs = (obj, path = '') => {
      for (const [key, value] of Object.entries(obj)) {
        const fullPath = path ? `${path}.${key}` : key;
        if (value instanceof Document) subdocs.push({ doc: value, path: fullPath });
        else if (value && typeof value === 'object' && !Array.isArray(value)) addSubdocs(value, fullPath);
      }
    };
    addSubdocs(this._doc);
    return subdocs;
  }

  $parent() { return this._parent; }
  parent()  { return this._parent; }

  // === Session ===
  $session(session = null) {
    if (arguments.length === 0) return this._session;
    this._session = session;
    return this;
  }

  $timestamps(value = true) { return this; }

  // === Serialization ===
  toObject(options = {}) {
    const obj = { ...this._doc };

    // Merge populated documents
    for (const [path, populatedDoc] of this._populated.entries()) {
      obj[path] = populatedDoc && typeof populatedDoc.toObject === 'function'
        ? populatedDoc.toObject(options)
        : populatedDoc;
    }

    if (options.getters || options.virtuals) {
      for (const [vpath, virtual] of Object.entries(this._schema.virtuals)) {
        if (options.aliases === false && virtual.isAlias) continue;
        try { obj[vpath] = virtual.applyGetters(undefined, this); } catch (e) {}
      }
    }

    if (options.minimize) {
      for (const key in obj) {
        if (obj[key] === undefined || (typeof obj[key] === 'object' && obj[key] !== null && !Array.isArray(obj[key]) && Object.keys(obj[key]).length === 0)) {
          delete obj[key];
        }
      }
    }

    if (options.versionKey === false) {
      const vKey = this._schema.options.versionKey;
      if (vKey) delete obj[vKey];
    }

    if (options.transform) return options.transform(this, obj, options);
    return obj;
  }

  toJSON(options = {}) {
    const schemaToJSON = this._schema.options.toJSON || {};
    return this.toObject({ flattenMaps: true, ...schemaToJSON, ...options });
  }

  toString()  { return `Document { _id: ${this._id} }`; }
  inspect()   { return this.toObject(); }

  // === Getters ===
  get schema()   { return this._schema; }
  get $errors()  { return this._errors; }
  get $locals()  { return this._locals; }
  get $op()      { return this._op; }
  get $where()   { return this._where; }

  // === Versioning ===
  $versioningInit() {
    const vKey = this._schema.options.versionKey;
    if (vKey) { this._doc[vKey] = this._doc[vKey] || 0; this._lastVersion = this._doc[vKey]; }
  }

  $incVersion() {
    const vKey = this._schema.options.versionKey;
    if (vKey) { this._doc[vKey] = (this._doc[vKey] || 0) + 1; this._lastVersion = this._doc[vKey]; }
  }

  $checkVersion() {
    const vKey = this._schema.options.versionKey;
    if (vKey && this._lastVersion !== this._doc[vKey]) {
      throw new Error('VersionError: Document has been modified since retrieval');
    }
  }
}

module.exports = { Document };