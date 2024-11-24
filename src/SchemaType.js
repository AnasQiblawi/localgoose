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
  }

  static cast(val) {
    return val;
  }

  static checkRequired(val) {
    return val != null;
  }

  static get(fn) {
    this._defaultGetters = this._defaultGetters || [];
    this._defaultGetters.push(fn);
    return this;
  }

  static set(fn) {
    this._defaultSetters = this._defaultSetters || [];
    this._defaultSetters.push(fn);
    return this;
  }

  cast(val) {
    if (val == null) {
      return val;
    }

    let value = val;
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

  doValidate(value, fn, context) {
    let err = null;
    const validatorCount = this.validators.length;

    if (validatorCount === 0) {
      return fn(null);
    }

    let validatorsCompleted = 0;
    for (const validator of this.validators) {
      const validatorWrapper = (ok) => {
        validatorsCompleted++;
        if (ok === false && !err) {
          err = new Error(validator.message || `Validation failed for path \`${this.path}\``);
        }
        if (validatorsCompleted === validatorCount) {
          fn(err);
        }
      };

      try {
        const result = validator.validator.call(context, value);
        if (result && typeof result.then === 'function') {
          result.then(
            ok => validatorWrapper(ok),
            error => validatorWrapper(false)
          );
        } else {
          validatorWrapper(result);
        }
      } catch (error) {
        validatorWrapper(false);
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
    return null;
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

  required(required) {
    if (arguments.length === 0) {
      return this.validators.some(v => v.isRequired);
    }

    if (required) {
      this.validators = [{
        validator: v => v != null,
        message: `Path \`${this.path}\` is required.`,
        type: 'required',
        isRequired: true
      }, ...this.validators];
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

  validate(obj) {
    if (obj == null) {
      return this;
    }

    if (typeof obj === 'function' || obj.validator) {
      this.validators.push(this._createValidator(obj));
    }

    return this;
  }

  validateAll() {
    return Promise.all(this.validators.map(v => v.validator()));
  }

  get validators() {
    return this._validators || [];
  }

  set validators(v) {
    this._validators = v;
  }

  _createValidator(obj) {
    if (typeof obj === 'function') {
      return {
        validator: obj,
        message: `Validation failed for path \`${this.path}\``
      };
    }

    return {
      validator: obj.validator,
      message: obj.message || `Validation failed for path \`${this.path}\``,
      type: obj.type
    };
  }
}

module.exports = { SchemaType };