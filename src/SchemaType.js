class SchemaType {
  constructor(path, options = {}, instance) {
    this.path = path;
    this.instance = instance;
    this.validators = [];
    this.setters = [];
    this.getters = [];
    this.options = options;
    this._index = null;
    this.selected = true;
    this._default = undefined;
    this._ref = null;
    this._sparse = false;
    this._text = false;
    this._unique = false;
    this._immutable = false;
    this._embedded = null;

    if (options.required) {
      this.required(options.required);
    }

    if (options.default != null) {
      this.default(options.default);
    }

    if (options.select != null) {
      this.select(options.select);
    }

    if (options.validate != null) {
      this.validate(options.validate);
    }

    if (options.get) {
      this.get(options.get);
    }

    if (options.set) {
      this.set(options.set);
    }

    if (options.transform) {
      this.transform(options.transform);
    }

    if (options.ref) {
      this.ref(options.ref);
    }

    if (options.immutable) {
      this.immutable(options.immutable);
    }

    if (options.sparse) {
      this.sparse(options.sparse);
    }

    if (options.unique) {
      this.unique(options.unique);
    }

    if (options.text) {
      this.text(options.text);
    }

    if (options.index) {
      this.index(options.index);
    }
  }

  static cast(val) {
    return val;
  }

  static checkRequired(val) {
    return val != null;
  }

  static get(fn) {
    if (!this._getters) this._getters = [];
    this._getters.push(fn);
    return this;
  }

  static set(fn) {
    if (!this._setters) this._setters = [];
    this._setters.push(fn);
    return this;
  }

  cast(val) {
    if (val == null) {
      return val;
    }

    let value = val;
    
    // Apply static casts first
    if (this.constructor._setters) {
      for (const setter of this.constructor._setters) {
        value = setter(value);
      }
    }

    // Apply instance casts
    for (const setter of this.setters) {
      value = setter(value);
    }

    return value;
  }

  castFunction() {
    return (val) => this.cast(val);
  }

  default(val) {
    if (arguments.length === 0) {
      return this._default;
    }

    if (val === null) {
      this._default = null;
      return this;
    }

    this._default = val;
    return this;
  }

  async doValidate(value, fn, context) {
    let err = null;
    const validatorCount = this.validators.length;

    if (validatorCount === 0) {
      return fn(null);
    }

    let validatorsCompleted = 0;
    
    const handleValidationResult = (ok) => {
      validatorsCompleted++;
      if (ok === false && !err) {
        err = new Error(`Validation failed for path \`${this.path}\``);
      }
      if (validatorsCompleted === validatorCount) {
        fn(err);
      }
    };

    for (const validator of this.validators) {
      try {
        const result = validator.validator.call(context, value);
        if (result && typeof result.then === 'function') {
          await result.then(
            ok => handleValidationResult(ok),
            error => handleValidationResult(false)
          );
        } else {
          handleValidationResult(result);
        }
      } catch (error) {
        handleValidationResult(false);
      }
    }
  }

  get(fn) {
    this.getters.push(fn);
    return this;
  }

  getDefault() {
    if (typeof this._default === 'function') {
      return this._default();
    }
    return this._default;
  }

  getEmbeddedSchemaType() {
    return this._embedded;
  }

  immutable(value = true) {
    this._immutable = value;
    return this;
  }

  index(val) {
    this._index = val;
    return this;
  }

  get isRequired() {
    return this.validators.some(v => v.isRequired);
  }

  ref(ref) {
    this._ref = ref;
    return this;
  }

  required(required = true, message) {
    if (arguments.length === 0) {
      return this.validators.some(v => v.isRequired);
    }

    if (required) {
      const validator = {
        validator: v => v != null,
        message: message || `Path \`${this.path}\` is required.`,
        type: 'required',
        isRequired: true
      };

      // Remove any existing required validators
      this.validators = this.validators.filter(v => !v.isRequired);
      // Add new required validator at the beginning
      this.validators.unshift(validator);
    } else {
      // Remove all required validators
      this.validators = this.validators.filter(v => !v.isRequired);
    }

    return this;
  }

  select(val) {
    this.selected = val;
    return this;
  }

  set(fn) {
    this.setters.push(fn);
    return this;
  }

  sparse(val = true) {
    this._sparse = val;
    return this;
  }

  text(val = true) {
    this._text = val;
    return this;
  }

  transform(fn) {
    this._transform = fn;
    return this;
  }

  unique(val = true) {
    this._unique = val;
    return this;
  }

  validate(obj, message) {
    if (obj == null) {
      return this;
    }

    const validator = this._createValidator(obj, message);
    this.validators.push(validator);
    return this;
  }

  async validateAll() {
    const results = await Promise.all(
      this.validators.map(validator => {
        try {
          const result = validator.validator();
          return Promise.resolve(result);
        } catch (error) {
          return Promise.reject(error);
        }
      })
    );
    return results.every(result => result === true);
  }

  get validators() {
    return this._validators || [];
  }

  set validators(v) {
    this._validators = v;
  }

  _createValidator(obj, message) {
    if (typeof obj === 'function') {
      return {
        validator: obj,
        message: message || `Validation failed for path \`${this.path}\``,
        type: 'user defined'
      };
    }

    if (obj.validator) {
      return {
        validator: obj.validator,
        message: obj.message || message || `Validation failed for path \`${this.path}\``,
        type: obj.type || 'user defined'
      };
    }

    throw new Error('Invalid validator. Validator must be a function or an object with a validator function');
  }
}

module.exports = { SchemaType };