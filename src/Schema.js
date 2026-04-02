const { SchemaType } = require('./SchemaType.js');
const { VirtualType } = require('./VirtualType.js');
const { ObjectId, Decimal128 } = require('bson');

// All valid Mongoose-compatible middleware hooks
const VALID_HOOKS = [
  'init', 'validate', 'save', 'remove',
  'deleteOne', 'deleteMany',
  'find', 'findOne', 'findOneAndUpdate', 'findOneAndRemove', 'findOneAndDelete',
  'updateOne', 'updateMany',
  'count', 'countDocuments', 'estimatedDocumentCount',
  'aggregate', 'insertMany'
];

class Schema {
  constructor(definition, options = {}) {
    if (definition !== undefined && definition !== null && typeof definition !== 'object') {
      throw new TypeError('Schema definition must be an object or null');
    }
    if (Array.isArray(definition)) {
      throw new TypeError('Schema definition cannot be an array');
    }

    this.obj = { ...(definition || {}) };
    this.definition = this._parseDefinition(definition || {});
    this.options = options;
    this.virtuals = {};
    this.methods = {};
    this.statics = {};
    this.middleware = { pre: {}, post: {} };
    this._indexes = [];
    this._paths = new Map();
    this._requiredPaths = new Set();
    this._plugins = new Set();
    this.childSchemas = [];
    this.discriminatorMapping = null;
    this._searchIndexes = new Map();
    this._doc = {};
    this._queue = new Map();
    this.reserved = Schema.reserved;

    if (options.timestamps) {
      this.definition.createdAt = { type: Date, default: Date.now };
      this.definition.updatedAt = { type: Date, default: Date.now };
    }
    if (options.versionKey !== false) {
      const vKey = typeof options.versionKey === 'string' ? options.versionKey : '__v';
      this.definition[vKey] = { type: Number, default: 0 };
    }

    this.strict = options.strict !== undefined ? options.strict : true;
    this.minimize = options.minimize !== undefined ? options.minimize : true;
    this._init();
  }

  _init() {
    for (const [p, opts] of Object.entries(this.definition)) {
      try {
        this._paths.set(p, this._createSchemaType(p, opts));
        if (opts && opts.required) this._requiredPaths.add(p);
        if (opts && opts.index) this._indexes.push({ fields: { [p]: 1 }, options: {} });
      } catch (e) { /* skip paths that cant create SchemaType */ }
    }
  }

  _parseDefinition(definition) {
    if (!definition || typeof definition !== 'object') return {};
    const parsed = {};
    for (const [key, value] of Object.entries(definition)) {
      if (value === null || value === undefined) {
        parsed[key] = { type: Object };
      } else if (value instanceof Schema) {
        parsed[key] = { type: Object, schema: value };
      } else if (typeof value === 'function') {
        // Shorthand: field: String
        parsed[key] = { type: value };
      } else if (Array.isArray(value)) {
        parsed[key] = { type: Array, schema: value[0] ? new Schema(value[0]) : null };
      } else if (typeof value === 'object') {
        if (value.type instanceof Schema) {
          parsed[key] = { ...value, schema: value.type, type: Object };
        } else if (value.type && Array.isArray(value.type)) {
          parsed[key] = { type: Array, schema: value.type[0] ? new Schema(value.type[0]) : null };
        } else if (value.type) {
          parsed[key] = {
            ...value,
            isReference: value.type === Schema.Types.ObjectId && !!value.ref,
            validate: value.validate || null
          };
        } else {
          // Nested object without type: treat as Mixed/Object
          parsed[key] = { type: Object, _nestedDef: value };
        }
      } else {
        parsed[key] = { type: value };
      }
    }
    return parsed;
  }

  _createSchemaType(p, options) {
    if (!options) return new SchemaType(p, {}, Object);
    const type = options.type !== undefined ? options.type : options;
    if (type === undefined) return new SchemaType(p, {}, Object);
    // Allow any type including custom classes — do NOT throw for unknown types
    const opts = typeof options === 'object' ? options : {};
    const st = new SchemaType(p, opts, type);
    if (opts.default !== undefined) st.default(opts.default);
    return st;
  }

  static get reserved() {
    return {
      _id: true, __v: true, createdAt: true, updatedAt: true,
      collection: true, emit: true, errors: true, get: true,
      init: true, isModified: true, isNew: true, listeners: true,
      modelName: true, on: true, once: true, populated: true,
      remove: true, removeListener: true, save: true, schema: true,
      set: true, toObject: true, validate: true
    };
  }

  static get Types() {
    return {
      String, Number, Boolean, Array, Date, Object,
      ObjectId, Mixed: Object, Decimal128, Map, Buffer,
      UUID: String, BigInt, Subdocument: Object, Embedded: Object
    };
  }

  static get indexTypes() {
    return ['2d', '2dsphere', 'hashed', 'text', 'unique', 'sparse', 'compound'];
  }

  // === Schema Modification ===
  add(fieldOrSchema, singleValue) {
    if (fieldOrSchema instanceof Schema) {
      const other = fieldOrSchema;
      for (const [p, def] of Object.entries(other.obj)) {
        this.obj[p] = def;
        const parsed = this._parseDefinition({ [p]: def });
        this.definition[p] = parsed[p];
        try {
          this._paths.set(p, this._createSchemaType(p, parsed[p]));
          if (parsed[p] && parsed[p].required) this._requiredPaths.add(p);
        } catch (e) {}
      }
      Object.assign(this.methods, other.methods);
      Object.assign(this.statics, other.statics);
      Object.assign(this.virtuals, other.virtuals);
    } else {
      const obj = fieldOrSchema;
      for (const [p, def] of Object.entries(obj)) {
        const defValue = singleValue !== undefined ? singleValue : def;
        this.obj[p] = defValue;
        const parsed = this._parseDefinition({ [p]: defValue });
        this.definition[p] = parsed[p];
        try {
          this._paths.set(p, this._createSchemaType(p, parsed[p]));
          if (parsed[p] && parsed[p].required) this._requiredPaths.add(p);
        } catch (e) {}
      }
    }
    return this;
  }

  remove(p) {
    delete this.definition[p];
    delete this.obj[p];
    this._paths.delete(p);
    this._requiredPaths.delete(p);
    return this;
  }

  alias(from, to) {
    this.virtual(from).get(function () { return this[to]; });
    return this;
  }

  index(fields, options = {}) {
    if (typeof fields !== 'object' || Array.isArray(fields)) throw new TypeError('Index fields must be an object');
    this._indexes.push({ fields, options });
    return this;
  }

  path(p) { return this._paths.get(p); }

  pathType(p) {
    if (this._paths.has(p)) return 'real';
    if (this.virtuals[p]) return 'virtual';
    if (this.reserved[p]) return 'reserved';
    return 'adhoc';
  }

  clone() {
    const clone = new Schema(this.obj, { ...this.options });
    clone.virtuals = { ...this.virtuals };
    clone.methods = { ...this.methods };
    clone.statics = { ...this.statics };
    clone.middleware = {
      pre: Object.fromEntries(Object.entries(this.middleware.pre).map(([k, v]) => [k, [...v]])),
      post: Object.fromEntries(Object.entries(this.middleware.post).map(([k, v]) => [k, [...v]]))
    };
    clone._indexes = [...this._indexes];
    clone._plugins = new Set([...this._plugins]);
    clone.childSchemas = [...this.childSchemas];
    clone._searchIndexes = new Map(this._searchIndexes);
    return clone;
  }

  discriminator(name, schema) {
    if (!this.discriminatorMapping) {
      this.discriminatorMapping = { key: '_type', value: this.options.name || 'Base' };
    }
    schema.discriminatorMapping = { key: this.discriminatorMapping.key, value: name };
    this.childSchemas.push({ name, schema });
    return schema;
  }

  pre(action, fn) {
    if (!VALID_HOOKS.includes(action)) {
      throw new Error(`Invalid hook: ${action}. Valid hooks are: ${VALID_HOOKS.join(', ')}`);
    }
    if (!this.middleware.pre[action]) this.middleware.pre[action] = [];
    this.middleware.pre[action].push(fn);
    return this;
  }

  post(action, fn) {
    if (!VALID_HOOKS.includes(action)) {
      throw new Error(`Invalid hook: ${action}. Valid hooks are: ${VALID_HOOKS.join(', ')}`);
    }
    if (!this.middleware.post[action]) this.middleware.post[action] = [];
    this.middleware.post[action].push(fn);
    return this;
  }

  plugin(fn, opts) {
    fn(this, opts || {});
    this._plugins.add(fn);
    return this;
  }

  virtual(name) {
    if (!this.virtuals[name]) this.virtuals[name] = new VirtualType({ path: name });
    return this.virtuals[name];
  }

  virtualpath(name) { return this.virtuals[name]; }

  method(name, fn) {
    if (typeof name === 'object') Object.assign(this.methods, name);
    else this.methods[name] = fn;
    return this;
  }

  static(name, fn) {
    if (typeof name === 'object') Object.assign(this.statics, name);
    else this.statics[name] = fn;
    return this;
  }

  loadClass(model) {
    Object.getOwnPropertyNames(model.prototype)
      .filter(n => n !== 'constructor')
      .forEach(n => this.method(n, model.prototype[n]));
    Object.getOwnPropertyNames(model)
      .filter(n => typeof model[n] === 'function')
      .forEach(n => this.static(n, model[n]));
    return this;
  }

  eachPath(fn) { this._paths.forEach((st, p) => fn(p, st)); }

  requiredPaths(invalidate = false) {
    if (invalidate) {
      this._requiredPaths.clear();
      this.eachPath((p, st) => { if (st.isRequired) this._requiredPaths.add(p); });
    }
    return Array.from(this._requiredPaths);
  }

  indexes() { return [...this._indexes]; }
  clearIndexes() { this._indexes = []; return this; }

  searchIndex(options = {}) {
    if (!options.name) throw new Error('Search index must have a name');
    if (!options.definition) throw new Error('Search index must have a definition');
    this._searchIndexes.set(options.name, {
      weights: options.weights || {},
      name: options.name,
      definition: options.definition,
      default_language: options.default_language || 'english',
      language_override: options.language_override || 'language'
    });
    return this;
  }

  removeIndex(index) {
    if (typeof index === 'string') {
      this._indexes = this._indexes.filter(idx => idx.options.name !== index);
    } else if (typeof index === 'object' && !Array.isArray(index)) {
      this._indexes = this._indexes.filter(idx => JSON.stringify(idx.fields) !== JSON.stringify(index));
    } else {
      throw new TypeError('Index parameter must be a string name or an object specification');
    }
    return this;
  }

  removeVirtual(paths) {
    (Array.isArray(paths) ? paths : [paths]).forEach(p => delete this.virtuals[p]);
    return this;
  }

  toJSONSchema(options = {}) {
    const useBsonType = options.useBsonType || false;
    const typeKey = useBsonType ? 'bsonType' : 'type';
    const jsonSchema = { type: 'object', required: ['_id', ...this.requiredPaths()], properties: {} };
    this.eachPath((p, st) => {
      let property = {};
      if (st.instance === ObjectId) {
        property[typeKey] = useBsonType ? 'objectId' : 'string';
      } else {
        property[typeKey] = st.instance?.name?.toLowerCase() || 'mixed';
      }
      if (st.enumValues && st.enumValues.length) property.enum = st.enumValues;
      jsonSchema.properties[p] = property;
    });
    return jsonSchema;
  }

  get(key) { return this.options[key]; }
  set(key, value) { this.options[key] = value; return this; }

  // === Validation ===
  _validatePath(p, value) {
    const st = this._paths.get(p);
    if (!st) return null;

    if (st.options.required) {
      const isReq = Array.isArray(st.options.required) ? st.options.required[0] : st.options.required;
      const msg = Array.isArray(st.options.required) ? st.options.required[1] : `Path \`${p}\` is required.`;
      if (isReq && (value == null || value === '')) return msg;
    }
    if (value == null) return null; // skip other validations for null/undefined

    if (st.options.min != null) {
      const min = Array.isArray(st.options.min) ? st.options.min[0] : st.options.min;
      const msg = Array.isArray(st.options.min) ? st.options.min[1] : `${p} should be at least ${min}`;
      if (value < min) return msg;
    }
    if (st.options.max != null) {
      const max = Array.isArray(st.options.max) ? st.options.max[0] : st.options.max;
      const msg = Array.isArray(st.options.max) ? st.options.max[1] : `${p} should be at most ${max}`;
      if (value > max) return msg;
    }
    if (st.options.minlength != null && typeof value === 'string' && value.length < st.options.minlength) {
      return `${p} must be at least ${st.options.minlength} characters`;
    }
    if (st.options.maxlength != null && typeof value === 'string' && value.length > st.options.maxlength) {
      return `${p} must be at most ${st.options.maxlength} characters`;
    }
    if (st.options.match) {
      const regex = Array.isArray(st.options.match) ? st.options.match[0] : st.options.match;
      const msg = Array.isArray(st.options.match) ? st.options.match[1] : `${p} does not match required pattern`;
      if (typeof value === 'string' && !regex.test(value)) return msg;
    }
    if (st.options.enum) {
      if (!st.options.enum.includes(value)) return `${p} must be one of: ${st.options.enum.join(', ')}`;
    }
    if (st.options.validate) {
      const validator = st.options.validate;
      if (typeof validator === 'function') {
        const result = validator(value);
        if (result !== true) return result || `${p} validation failed`;
      } else if (typeof validator === 'object' && validator.validator) {
        if (!validator.validator(value)) {
          const message = typeof validator.message === 'function'
            ? validator.message({ value, path: p })
            : validator.message || `${p} validation failed`;
          return message;
        }
      }
    }
    return null;
  }

  validate(data) {
    const errors = [];
    for (const [p] of this._paths.entries()) {
      const error = this._validatePath(p, data[p]);
      if (error) errors.push(error);
    }
    return errors;
  }

  queue(name, args) {
    if (!this._queue.has(name)) this._queue.set(name, []);
    this._queue.get(name).push(args);
    return this;
  }

  omit(paths) {
    const clone = this.clone();
    (Array.isArray(paths) ? paths : [paths]).forEach(p => clone.remove(p));
    return clone;
  }

  pick(paths) {
    const newSchema = new Schema({});
    (Array.isArray(paths) ? paths : [paths]).forEach(p => {
      if (this.obj[p]) newSchema.add({ [p]: this.obj[p] });
    });
    return newSchema;
  }

  get paths() { return Object.fromEntries(this._paths); }

  nested(p) {
    const parts = p.split('.');
    let current = this.definition;
    let nested = false;
    for (const part of parts) {
      if (current[part] && typeof current[part] === 'object') { nested = true; current = current[part]; }
      else break;
    }
    return nested;
  }

  isArray(p) {
    const st = this._paths.get(p);
    return st && (Array.isArray(st.instance) || st.instance === Array);
  }

  extend(schema) {
    if (!(schema instanceof Schema)) throw new Error('extend() argument must be a Schema');
    return this.add(schema);
  }
}

module.exports = { Schema };