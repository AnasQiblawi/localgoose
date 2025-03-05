class SchemaType {
  // === Core Functionality ===
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

    this._enum = null;
    this._min = null;
    this._max = null;
    this._trim = false;
    this._lowercase = false;
    this._uppercase = false;
    this._match = null;
    this._validate = null;

    this._isArray = false;
    this._arrayType = null;

    if (Array.isArray(instance)) {
      this._isArray = true;
      this._arrayType = instance[0];
    }

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

    if (options.enum) {
      this.enum(options.enum);
    }

    if (options.min != null) {
      this.min(options.min);
    }

    if (options.max != null) {
      this.max(options.max);
    }

    if (options.trim) {
      this.trim(options.trim);
    }

    if (options.lowercase) {
      this.lowercase(options.lowercase);
    }

    if (options.uppercase) {
      this.uppercase(options.uppercase);
    }

    if (options.match) {
      this.match(options.match);
    }
  }

  // === Static Methods ===
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

  // === Type Validation and Casting ===
  cast(val) {
    if (val == null) {
      return val;
    }

    if (this._isArray) {
      if (!Array.isArray(val)) {
        throw new Error(`${this.path} must be an array`);
      }
      return val.map(item => this._castArrayItem(item));
    }

    // Type-specific casting
    switch (this.instance) {
      case String:
        val = String(val);
        if (this._trim) val = val.trim();
        if (this._lowercase) val = val.toLowerCase();
        if (this._uppercase) val = val.toUpperCase();
        break;
      case Number:
        val = Number(val);
        break;
      case Date:
        val = val instanceof Date ? val : new Date(val);
        break;
      case Boolean:
        val = Boolean(val);
        break;
    }

    // Enum validation
    if (this._enum && !this._enum.includes(val)) {
      throw new Error(`${val} is not a valid enum value for ${this.path}`);
    }

    // Min/Max validation
    if (this._min != null && val < this._min) {
      throw new Error(`${val} is less than the minimum value ${this._min} for ${this.path}`);
    }
    if (this._max != null && val > this._max) {
      throw new Error(`${val} is greater than the maximum value ${this._max} for ${this.path}`);
    }

    // Regex matching
    if (this._match && typeof val === 'string' && !this._match.test(val)) {
      throw new Error(`${val} does not match the required pattern for ${this.path}`);
    }

    // Apply static and instance setters
    let value = val;
    if (this.constructor._setters) {
      for (const setter of this.constructor._setters) {
        value = setter(value);
      }
    }

    for (const setter of this.setters) {
      value = setter(value);
    }

    return value;
  }

  _castArrayItem(item) {
    if (this._arrayType instanceof SchemaType) {
      return this._arrayType.cast(item);
    }
    return item;
  }

  castFunction() {
    return (val) => this.cast(val);
  }

  // === Validation Methods ===
  validate(obj, message) {
    if (obj == null) {
      return this;
    }

    const validator = this._createValidator(obj, message);
    
    // Ensure this is the last validator added
    const existingValidatorIndex = this.validators.findIndex(v => v.type === validator.type);
    if (existingValidatorIndex !== -1) {
      this.validators.splice(existingValidatorIndex, 1);
    }

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

  _createValidator(obj, message) {
    try {
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

      throw new Error('Invalid validator');
    } catch (error) {
      console.error(`Validator creation error for ${this.path}:`, error);
      throw error;
    }
  }

  // === Field Constraints ===
  enum(values) {
    if (Array.isArray(values)) {
      this._enum = values;
      this.validate({
        validator: (val) => values.includes(val),
        message: `{PATH} must be one of: ${values.join(', ')}`
      });
    }
    return this;
  }

  min(value, message) {
    this._min = value;
    this.validate({
      validator: (val) => {
        if (typeof val === 'string' || typeof val === 'number') {
          return val >= value;
        }
        return true;
      },
      message: message || `{PATH} must be at least ${value}`
    });
    return this;
  }

  max(value, message) {
    this._max = value;
    this.validate({
      validator: (val) => {
        if (typeof val === 'string' || typeof val === 'number') {
          return val <= value;
        }
        return true;
      },
      message: message || `{PATH} must be no more than ${value}`
    });
    return this;
  }

  match(regex, message) {
    this._match = regex;
    this.validate({
      validator: (val) => {
        if (typeof val === 'string') {
          return regex.test(val);
        }
        return true;
      },
      message: message || `{PATH} does not match the required pattern`
    });
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

  // === String Modifiers ===
  trim(value = true) {
    this._trim = value;
    if (value) {
      this.set(val => typeof val === 'string' ? val.trim() : val);
    }
    return this;
  }

  lowercase(value = true) {
    this._lowercase = value;
    if (value) {
      this.set(val => typeof val === 'string' ? val.toLowerCase() : val);
    }
    return this;
  }

  uppercase(value = true) {
    this._uppercase = value;
    if (value) {
      this.set(val => typeof val === 'string' ? val.toUpperCase() : val);
    }
    return this;
  }

  // === Schema Options ===
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

  sparse(val = true) {
    this._sparse = val;
    return this;
  }

  unique(val = true) {
    this._unique = val;
    return this;
  }

  text(val = true) {
    this._text = val;
    return this;
  }

  index(val) {
    this._index = val;
    return this;
  }

  immutable(value = true) {
    this._immutable = value;
    return this;
  }

  ref(ref) {
    this._ref = ref;
    return this;
  }

  transform(fn) {
    this._transform = fn;
    return this;
  }

  // === Getters and Setters ===
  get(fn) {
    this.getters.push(fn);
    return this;
  }

  set(fn) {
    this.setters.push(fn);
    return this;
  }

  select(val) {
    this.selected = val;
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

  // === Property Accessors ===
  get validators() {
    return this._validators || [];
  }

  set validators(v) {
    this._validators = v;
  }

  get isRequired() {
    return this.validators.some(v => v.isRequired);
  }

  toJSONSchema(options = {}) {
    const jsonSchema = {};
    
    // Handle basic type mapping
    switch (this.instance) {
      case String:
        jsonSchema.type = options.useBsonType ? 'string' : 'string';
        break;
      case Number:
        jsonSchema.type = options.useBsonType ? 'number' : 'number';
        break;
      case Date:
        jsonSchema.type = options.useBsonType ? 'date' : 'string';
        jsonSchema.format = 'date-time';
        break;
      case Boolean:
        jsonSchema.type = options.useBsonType ? 'bool' : 'boolean';
        break;
      default:
        jsonSchema.type = 'object';
    }

    // Handle array types
    if (this._isArray) {
      jsonSchema.type = 'array';
      if (this._arrayType && typeof this._arrayType.toJSONSchema === 'function') {
        jsonSchema.items = this._arrayType.toJSONSchema(options);
      }
    }

    // Add validation constraints
    if (this._enum) {
      jsonSchema.enum = this._enum;
    }

    if (this._min != null) {
      jsonSchema.minimum = this._min;
    }

    if (this._max != null) {
      jsonSchema.maximum = this._max;
    }

    if (this._match) {
      jsonSchema.pattern = this._match.source;
    }

    if (this.isRequired) {
      jsonSchema.required = true;
    }

    return jsonSchema;
  }
}

module.exports = { SchemaType };