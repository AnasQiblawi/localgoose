class VirtualType {
  // === Core Functionality ===
  constructor(options = {}) {
    this.path = options.path;
    this.getters = [];
    this.setters = [];
    this.options = options;
    this._ref = null;
    this._localField = null;
    this._foreignField = null;
    this._justOne = false;
    this._count = false;
    this._match = null;
    this._defaultValue = undefined;
  }

  clone() {
    const clone = new VirtualType(this.options);
    clone.getters = [...this.getters];
    clone.setters = [...this.setters];
    clone._ref = this._ref;
    clone._localField = this._localField;
    clone._foreignField = this._foreignField;
    clone._justOne = this._justOne;
    clone._count = this._count;
    clone._match = this._match;
    clone._defaultValue = this._defaultValue;
    return clone;
  }

  // === Getter and Setter Methods ===
  get(fn) {
    if (typeof fn !== 'function') {
      throw new Error('Getter must be a function');
    }
    this.getters.push(fn);
    return this;
  }

  set(fn) {
    if (typeof fn !== 'function') {
      throw new Error('Setter must be a function');
    }
    this.setters.push(fn);
    return this;
  }

  applyGetters(value, doc) {
    if (!doc) {
      throw new Error('Document is required to apply getters');
    }

    let val = value;
    
    // Apply each getter in sequence
    for (const getter of this.getters) {
      try {
        val = getter.call(doc, val, this);
      } catch (error) {
        throw new Error(`Error applying getter for path "${this.path}": ${error.message}`);
      }
    }

    return val;
  }

  applySetters(value, doc) {
    if (!doc) {
      throw new Error('Document is required to apply setters');
    }

    let val = value;
    
    // Apply each setter in sequence
    for (const setter of this.setters) {
      try {
        val = setter.call(doc, val, this);
      } catch (error) {
        throw new Error(`Error applying setter for path "${this.path}": ${error.message}`);
      }
    }

    return val;
  }

  // === Reference Configuration ===
  ref(model) {
    this._ref = model;
    return this;
  }

  localField(field) {
    this._localField = field;
    return this;
  }

  foreignField(field) {
    this._foreignField = field;
    return this;
  }

  // === Virtual Configuration ===
  justOne(val = true) {
    this._justOne = val;
    return this;
  }

  count(val = true) {
    this._count = val;
    return this;
  }

  match(val) {
    this._match = val;
    return this;
  }

  default(val) {
    this._defaultValue = val;
    return this;
  }
}

module.exports = { VirtualType };