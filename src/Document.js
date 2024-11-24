const { EventEmitter } = require('events');

class Document {
  constructor(obj, schema, model) {
    this._doc = { ...obj };
    this._schema = schema;
    this._model = model;
    this._modifiedPaths = new Set();
    this._populated = new Map();
    this._parent = null;
    this._isNew = true;
    this._snapshot = null;
    this._session = null;
    this._locals = {};
    this._op = null;
    this._where = {};
    this._timestamps = true;
    this._deleted = false;
    this._errors = {};
    this._validationError = null;
    this._selected = new Set(Object.keys(obj));
    this._init = new Set();
    
    this.isNew = true;
    this.errors = {};
    this.id = obj._id;
    this._id = obj._id;
    
    // Set up virtuals
    Object.entries(schema.virtuals).forEach(([path, virtual]) => {
      Object.defineProperty(this, path, {
        get: function() {
          return virtual.applyGetters(undefined, this);
        },
        set: function(value) {
          return virtual.applySetters(value, this);
        },
        configurable: true
      });
    });

    // Set up methods
    Object.entries(schema.methods).forEach(([name, method]) => {
      this[name] = method.bind(this);
    });

    // Set up direct property access
    Object.keys(this._doc).forEach(key => {
      if (!(key in this)) {
        Object.defineProperty(this, key, {
          get: function() { return this._doc[key]; },
          set: function(value) { 
            this._doc[key] = value;
            this._modifiedPaths.add(key);
          },
          configurable: true,
          enumerable: true
        });
      }
    });
  }

  $assertPopulated(path, values) {
    if (!this._populated.has(path)) {
      throw new Error(`Path '${path}' is not populated`);
    }
    return this;
  }

  $clearModifiedPaths() {
    this._modifiedPaths.clear();
    return this;
  }

  $clone() {
    return new Document({ ...this._doc }, this._schema, this._model);
  }

  $createModifiedPathsSnapshot() {
    this._snapshot = new Set(this._modifiedPaths);
    return this;
  }

  get $errors() {
    return this._errors;
  }

  $getAllSubdocs() {
    const subdocs = [];
    const addSubdocs = (obj, path = '') => {
      for (const [key, value] of Object.entries(obj)) {
        const fullPath = path ? `${path}.${key}` : key;
        if (value instanceof Document) {
          subdocs.push({ doc: value, path: fullPath });
        } else if (value && typeof value === 'object') {
          addSubdocs(value, fullPath);
        }
      }
    };
    
    addSubdocs(this._doc);
    return subdocs;
  }

  $getPopulatedDocs() {
    return Array.from(this._populated.entries()).map(([path, doc]) => ({
      path,
      doc
    }));
  }

  $ignore(path) {
    this._modifiedPaths.delete(path);
    return this;
  }

  $inc(path, val = 1) {
    const curVal = this.get(path) || 0;
    return this.set(path, curVal + val);
  }

  $init(obj) {
    Object.assign(this._doc, obj);
    this._modifiedPaths.clear();
    this._isNew = false;
    Object.keys(obj).forEach(key => this._init.add(key));
    return this;
  }

  $isDefault(path) {
    const schemaType = this._schema.path(path);
    return schemaType ? this.get(path) === schemaType.getDefault() : false;
  }

  $isDeleted() {
    return this._deleted;
  }

  $isEmpty(path) {
    const val = this.get(path);
    return val == null || val === '' || 
           (Array.isArray(val) && val.length === 0) ||
           (typeof val === 'object' && Object.keys(val).length === 0);
  }

  $isModified(path) {
    return path ? this._modifiedPaths.has(path) : this._modifiedPaths.size > 0;
  }

  get $isNew() {
    return this._isNew;
  }

  get $locals() {
    return this._locals;
  }

  $markValid(path) {
    delete this._errors[path];
    return this;
  }

  get $op() {
    return this._op;
  }

  $parent() {
    return this._parent;
  }

  $populated(path) {
    return this._populated.get(path);
  }

  $restoreModifiedPathsSnapshot() {
    if (this._snapshot) {
      this._modifiedPaths = new Set(this._snapshot);
    }
    return this;
  }

  $session(session = null) {
    if (arguments.length === 0) return this._session;
    this._session = session;
    return this;
  }

  $set(path, val) {
    return this.set(path, val);
  }

  $timestamps(value = true) {
    this._timestamps = value;
    return this;
  }

  async $validate(pathsToValidate) {
    const paths = pathsToValidate || Array.from(this._modifiedPaths);
    const errors = await Promise.all(
      paths.map(path => this._validatePath(path))
    );
    return errors.filter(Boolean);
  }

  get $where() {
    return this._where;
  }

  depopulate(path) {
    if (path) {
      this._populated.delete(path);
    } else {
      this._populated.clear();
    }
    return this;
  }

  directModifiedPaths() {
    return Array.from(this._modifiedPaths);
  }

  equals(doc) {
    return doc instanceof Document && 
           this._id.toString() === doc._id.toString();
  }

  get(path) {
    return this._doc[path];
  }

  getChanges() {
    const changes = {};
    for (const path of this._modifiedPaths) {
      changes[path] = this.get(path);
    }
    return changes;
  }

  init(obj) {
    return this.$init(obj);
  }

  inspect() {
    return this.toObject();
  }

  invalidate(path, err) {
    this._errors[path] = err;
    return this;
  }

  isDirectModified(path) {
    return this._modifiedPaths.has(path);
  }

  isDirectSelected(path) {
    return this._selected.has(path);
  }

  isInit(path) {
    return this._init.has(path);
  }

  isModified(path) {
    return this.$isModified(path);
  }

  isSelected(path) {
    return this._selected.has(path);
  }

  markModified(path) {
    this._modifiedPaths.add(path);
    return this;
  }

  modifiedPaths() {
    return Array.from(this._modifiedPaths);
  }

  overwrite(obj) {
    this._doc = { _id: this._id, ...obj };
    this._modifiedPaths = new Set(Object.keys(obj));
    return this;
  }

  parent() {
    return this._parent;
  }

  async populate(path, select) {
    if (typeof path === 'string') {
      const schemaType = this._schema.path(path);
      if (schemaType && schemaType.options.ref) {
        const refModel = this._model.db.models[schemaType.options.ref];
        if (!refModel) return this;

        const value = this.get(path);
        if (!value) return this;

        try {
          const populatedDoc = await refModel.findOne({ _id: value });
          if (populatedDoc) {
            this._populated.set(path, populatedDoc);
          }
        } catch (error) {
          console.error(`Error populating ${path}:`, error);
        }
      }
    }
    return this;
  }

  populated(path) {
    return this.$populated(path);
  }

  replaceOne(replacement, options = {}) {
    return this._model.replaceOne({ _id: this._id }, replacement, options);
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

  get schema() {
    return this._schema;
  }

  set(path, val) {
    if (typeof path === 'string') {
      this._doc[path] = val;
      this._modifiedPaths.add(path);
    } else if (typeof path === 'object') {
      Object.entries(path).forEach(([key, value]) => {
        this._doc[key] = value;
        this._modifiedPaths.add(key);
      });
    }
    return this;
  }

  toJSON() {
    return this.toObject();
  }

  toObject(options = {}) {
    const obj = { ...this._doc };
    
    // Handle populated fields
    for (const [path, value] of this._populated.entries()) {
      if (value instanceof Document) {
        obj[path] = value.toObject(options);
      } else {
        obj[path] = value;
      }
    }

    return obj;
  }

  toString() {
    return `Document { _id: ${this._id} }`;
  }

  unmarkModified(path) {
    this._modifiedPaths.delete(path);
    return this;
  }

  updateOne(update, options = {}) {
    return this._model.updateOne({ _id: this._id }, update, options);
  }

  async validate(pathsToValidate) {
    return this.$validate(pathsToValidate);
  }

  validateSync(pathsToValidate) {
    const paths = pathsToValidate || Array.from(this._modifiedPaths);
    return paths.map(path => this._validatePath(path))
               .filter(Boolean);
  }

  async _validatePath(path) {
    const schemaType = this._schema.path(path);
    if (!schemaType) return null;

    const value = this.get(path);
    return new Promise((resolve) => {
      schemaType.doValidate(value, (error) => {
        if (error) {
          this._errors[path] = error;
          resolve(error);
        } else {
          resolve(null);
        }
      }, this);
    });
  }
}

module.exports = { Document };