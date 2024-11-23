import { EventEmitter } from 'events';

export class Document {
  constructor(obj, schema, model) {
    this._doc = { ...obj };
    this._schema = schema;
    this._model = model;
    this._modifiedPaths = new Set();
    this._populated = new Map();
    this._parent = null;
    this._isNew = true;
    this._snapshot = null;
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

  get(path) {
    return this._doc[path];
  }

  set(path, value) {
    this._doc[path] = value;
    this._modifiedPaths.add(path);
    return this;
  }

  async save() {
    if (this._schema.middleware.pre.save) {
      for (const middleware of this._schema.middleware.pre.save) {
        await middleware.call(this);
      }
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

    return result;
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

  toJSON() {
    return this.toObject();
  }

  markModified(path) {
    this._modifiedPaths.add(path);
    return this;
  }

  $assertPopulated(path, values) {
    if (!this._populated.has(path)) {
      throw new Error(`Path '${path}' is not populated`);
    }
    return this;
  }

  $createModifiedPathsSnapshot() {
    this._snapshot = new Set(this._modifiedPaths);
    return this;
  }

  $restoreModifiedPathsSnapshot() {
    if (this._snapshot) {
      this._modifiedPaths = new Set(this._snapshot);
    }
    return this;
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
    return Array.from(this._populated.entries()).map(([path, options]) => ({
      path,
      options
    }));
  }

  $init(obj) {
    Object.assign(this._doc, obj);
    this._modifiedPaths.clear();
    this._isNew = false;
    return this;
  }

  $isDefault(path) {
    const schemaType = this._schema.path(path);
    if (!schemaType) return false;
    
    const value = this.get(path);
    return value === schemaType.getDefault();
  }

  getChanges() {
    const changes = {};
    for (const path of this._modifiedPaths) {
      changes[path] = this.get(path);
    }
    return changes;
  }

  isDirectSelected(path) {
    return path in this._doc;
  }

  isSelected(path) {
    if (this._schema.options.selectAll) return true;
    return this.isDirectSelected(path);
  }

  parent() {
    return this._parent;
  }

  replaceOne(replacement, options = {}) {
    return this._model.replaceOne({ _id: this._id }, replacement, options);
  }
}