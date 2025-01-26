const { SchemaType } = require('./SchemaType.js');
const { VirtualType } = require('./VirtualType.js');
const { ObjectId, Decimal128 } = require('bson'); // Added Decimal128

class Schema {
  // === Core Functionality ===
  constructor(definition, options = {}) {
    this.definition = this._parseDefinition(definition);
    this.options = options;
    this.virtuals = {};
    this.methods = {};
    this.statics = {};
    this.middleware = {
      pre: {},
      post: {}
    };
    this._indexes = [];
    this._paths = new Map();
    this._requiredPaths = new Set();
    this._plugins = new Set();
    this.childSchemas = [];
    this.discriminatorMapping = null;
    this.obj = { ...definition };
    this._searchIndexes = new Map();
    this._doc = {};
    this._queue = new Map();

    this.reserved = Schema.reserved;

    // Handle schema options
    if (options.timestamps) {
      this.definition.createdAt = { type: Date, default: Date.now };
      this.definition.updatedAt = { type: Date, default: Date.now };
    }
    if (options.versionKey !== false) {
      this.definition.__v = { type: Number, default: 0 };
    }

    this.strict = options.strict !== undefined ? options.strict : true;
    this.minimize = options.minimize !== undefined ? options.minimize : true;

    this._init();
  }

  _init() {
    for (const [path, options] of Object.entries(this.definition)) {
      this._paths.set(path, this._createSchemaType(path, options));
      if (options.required) {
        this._requiredPaths.add(path);
      }
      if (options.index) {
        this.index({ [path]: 1 });
      }
    }
  }

  _parseDefinition(definition) {
    const parsed = {};
    for (const [key, value] of Object.entries(definition)) {
      if (typeof value === 'object' && !Array.isArray(value) && value !== null) {
        if (value instanceof Schema) {
          // Handle nested schema
          parsed[key] = {
            type: Object,
            schema: value
          };
        } else if (value.type && value.type instanceof Schema) {
          // Handle array of nested schemas
          parsed[key] = {
            ...value,
            schema: value.type
          };
        } else if (value.type && Array.isArray(value.type)) {
          // Handle array of subdocuments
          parsed[key] = {
            type: Array,
            schema: new Schema(value.type[0])
          };
        } else if (value.type) {
          parsed[key] = {
            ...value,
            isReference: value.type === Schema.Types.ObjectId && value.ref,
            validate: value.validate || null // Add support for custom validators
          };
        } else {
          parsed[key] = this._parseDefinition(value);
        }
      } else {
        parsed[key] = { type: value };
      }
    }
    return parsed;
  }

  _createSchemaType(path, options) {
    const type = options.type || options;
    const schemaTypeOptions = typeof options === 'object' ? options : {};
    const schemaType = new SchemaType(path, schemaTypeOptions, type);

    // Handle default values
    if (options.default !== undefined) {
      schemaType.default(options.default);
    }

    return schemaType;
  }

  // === Static Properties ===
  static get reserved() {
    return {
      _id: true,
      __v: true,
      createdAt: true,
      updatedAt: true,
      collection: true,
      emit: true,
      errors: true,
      get: true,
      init: true,
      isModified: true,
      isNew: true,
      listeners: true,
      modelName: true,
      on: true,
      once: true,
      populated: true,
      remove: true,
      removeListener: true,
      save: true,
      schema: true,
      set: true,
      toObject: true,
      validate: true
    };
  }

  static get Types() {
    return {
      String: String,
      Number: Number,
      Boolean: Boolean,
      Array: Array,
      Date: Date,
      Object: Object,
      ObjectId: ObjectId,
      Mixed: Object,
      Decimal128: Decimal128,
      Map: Map,
      Buffer: Buffer,
      UUID: String,
      BigInt: BigInt,
      Subdocument: Object,
      Embedded: Object
    };
  }

  static get indexTypes() {
    return ['2d', '2dsphere', 'hashed', 'text', 'unique', 'sparse', 'compound'];
  }

  // === Schema Modification Methods ===
  add(obj) {
    for (const [path, options] of Object.entries(obj)) {
      this.definition[path] = options;
      this._paths.set(path, this._createSchemaType(path, options));
      if (options.required) {
        this._requiredPaths.add(path);
      }
    }
    return this;
  }

  remove(path) {
    delete this.definition[path];
    this._paths.delete(path);
    this._requiredPaths.delete(path);
    return this;
  }

  // === Schema Configuration ===
  alias(from, to) {
    this.virtual(from).get(function() {
      return this[to];
    });
    return this;
  }

  index(fields, options = {}) {
    const index = { fields, options };
    this._indexes.push(index);
    return this;
  }

  path(path) {
    return this._paths.get(path);
  }

  pathType(path) {
    if (this._paths.has(path)) return 'real';
    if (this.virtuals[path]) return 'virtual';
    if (this.reserved[path]) return 'reserved';
    return 'adhoc';
  }

  // === Schema Operations ===
  clone() {
    const clone = new Schema(this.definition, { ...this.options });
    clone.virtuals = { ...this.virtuals };
    clone.methods = { ...this.methods };
    clone.statics = { ...this.statics };
    clone.middleware = {
      pre: { ...this.middleware.pre },
      post: { ...this.middleware.post }
    };
    clone._indexes = [...this._indexes];
    clone._plugins = new Set([...this._plugins]);
    clone.childSchemas = [...this.childSchemas];
    clone._searchIndexes = new Map(this._searchIndexes);
    return clone;
  }

  discriminator(name, schema) {
    if (!this.discriminatorMapping) {
      this.discriminatorMapping = {
        key: '_type',
        value: this.options.name || 'Base'
      };
    }
    schema.discriminatorMapping = {
      key: this.discriminatorMapping.key,
      value: name
    };
    this.childSchemas.push({ name, schema });
    return schema;
  }

  // === Middleware and Plugins ===
  pre(action, fn) {
    const validHooks = ['init', 'validate', 'save', 'remove', 'deleteOne', 'deleteMany'];
    if (!validHooks.includes(action)) {
      throw new Error(`Invalid hook: ${action}. Valid hooks are: ${validHooks.join(', ')}`);
    }
    if (!this.middleware.pre[action]) {
      this.middleware.pre[action] = [];
    }
    this.middleware.pre[action].push(fn);
    return this;
  }

  post(action, fn) {
    const validHooks = ['init', 'validate', 'save', 'remove', 'deleteOne', 'deleteMany'];
    if (!validHooks.includes(action)) {
      throw new Error(`Invalid hook: ${action}. Valid hooks are: ${validHooks.join(', ')}`);
    }
    if (!this.middleware.post[action]) {
      this.middleware.post[action] = [];
    }
    this.middleware.post[action].push(fn);
    return this;
  }

  plugin(fn, opts) {
    fn(this, opts || {});
    this._plugins.add(fn);
    return this;
  }

  // === Virtual Fields ===
  virtual(name) {
    if (!this.virtuals[name]) {
      this.virtuals[name] = new VirtualType({ path: name });
    }
    return this.virtuals[name];
  }

  virtualpath(name) {
    return this.virtuals[name];
  }

  // === Methods and Statics ===
  method(name, fn) {
    this.methods[name] = fn;
    return this;
  }

  static(name, fn) {
    this.statics[name] = fn;
    return this;
  }

  loadClass(model) {
    const methods = Object.getOwnPropertyNames(model.prototype)
      .filter(name => name !== 'constructor');
    
    methods.forEach(method => {
      this.method(method, model.prototype[method]);
    });
    
    const statics = Object.getOwnPropertyNames(model)
      .filter(name => typeof model[name] === 'function');
    
    statics.forEach(staticMethod => {
      this.static(staticMethod, model[staticMethod]);
    });
    
    return this;
  }

  // === Schema Traversal ===
  eachPath(fn) {
    this._paths.forEach((schemaType, path) => {
      fn(path, schemaType);
    });
  }

  requiredPaths(invalidate = false) {
    if (invalidate) {
      this._requiredPaths.clear();
      this.eachPath((path, schemaType) => {
        if (schemaType.required()) {
          this._requiredPaths.add(path);
        }
      });
    }
    return Array.from(this._requiredPaths);
  }

  // === Index Management ===
  indexes() {
    return [...this._indexes];
  }

  clearIndexes() {
    this._indexes = [];
    return this;
  }

  searchIndex(options = {}) {
    const index = {
      weights: options.weights || {},
      name: options.name,
      default_language: options.default_language || 'english',
      language_override: options.language_override || 'language'
    };
    this._searchIndexes.set(options.name || 'default', index);
    return this;
  }

  // === Schema Options ===
  get(key) {
    return this.options[key];
  }

  set(key, value) {
    this.options[key] = value;
    return this;
  }

  // === Validation ===
  _validatePath(path, value) {
    const schemaType = this._paths.get(path);
    if (!schemaType) return null;

    // Built-in validators
    if (schemaType.options.required && value == null) {
      return `${path} is required`;
    }
    if (schemaType.options.min != null && value < schemaType.options.min) {
      return `${path} should be at least ${schemaType.options.min}`;
    }
    if (schemaType.options.max != null && value > schemaType.options.max) {
      return `${path} should be at most ${schemaType.options.max}`;
    }

    // Custom validators
    if (schemaType.options.validate) {
      const validator = schemaType.options.validate;
      if (typeof validator === 'function') {
        const result = validator(value);
        if (result !== true) {
          return result || `${path} validation failed`;
        }
      } else if (typeof validator === 'object') {
        if (!validator.validator(value)) {
          return validator.message || `${path} validation failed`;
        }
      }
    }

    return null;
  }

  validate(data) {
    const errors = [];
    for (const [path, schemaType] of this._paths.entries()) {
      const value = data[path];
      const error = this._validatePath(path, value);
      if (error) {
        errors.push(error);
      }
    }
    return errors;
  }

  // === Additional Methods ===
  queue(name, args) {
    if (!this._queue) this._queue = new Map();
    if (!this._queue.has(name)) this._queue.set(name, []);
    this._queue.get(name).push(args);
    return this;
  }

  omit(paths) {
    const newSchema = this.clone();
    paths = Array.isArray(paths) ? paths : [paths];
    paths.forEach(path => {
      newSchema.remove(path);
    });
    return newSchema;
  }

  pick(paths) {
    const newSchema = new Schema({});
    paths = Array.isArray(paths) ? paths : [paths];
    paths.forEach(path => {
      if (this._paths.has(path)) {
        newSchema.add({ [path]: this.definition[path] });
      }
    });
    return newSchema;
  }

  version(condition, versionKey = '__v') {
    if (!this.options.versionKey) {
      this.options.versionKey = versionKey;
    }

    this.pre('save', async function() {
      const shouldVersion = typeof condition === 'function' 
        ? await condition.call(this)
        : true;
      
      if (shouldVersion) {
        this[versionKey] = (this[versionKey] || 0) + 1;
      }
    });

    return this;
  }

  get paths() {
    return Object.fromEntries(this._paths);
  }

  toObject(options = {}) {
    const obj = { ...this._doc };
    if (this.minimize) {
      for (const key in obj) {
        if (obj[key] === undefined) {
          delete obj[key];
        }
      }
    }
    return obj;
  }

  // Method to handle nested paths
  nested(path) {
    const parts = path.split('.');
    let nested = false;
    let current = this.definition;

    for (let i = 0; i < parts.length; i++) {
      if (current[parts[i]] && typeof current[parts[i]] === 'object') {
        nested = true;
        current = current[parts[i]];
      } else {
        break;
      }
    }
    return nested;
  }

  // Method to handle array paths
  isArray(path) {
    const schemaType = this._paths.get(path);
    return schemaType && (
      Array.isArray(schemaType.instance) || 
      schemaType.instance === Array
    );
  }

  // Method for schema inheritance
  extend(schema) {
    if (!(schema instanceof Schema)) {
      throw new Error('extend() argument must be a Schema');
    }

    this.add(schema.definition);
    this.methods = { ...this.methods, ...schema.methods };
    this.statics = { ...this.statics, ...schema.statics };
    this.virtuals = { ...this.virtuals, ...schema.virtuals };

    return this;
  }
}

module.exports = { Schema };