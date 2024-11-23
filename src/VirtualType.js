export class VirtualType {
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
  }

  applyGetters(value, doc) {
    let val = value;
    for (const getter of this.getters) {
      val = getter.call(doc, val);
    }
    return val;
  }

  applySetters(value, doc) {
    let val = value;
    for (const setter of this.setters) {
      val = setter.call(doc, val);
    }
    return val;
  }

  get(fn) {
    this.getters.push(fn);
    return this;
  }

  set(fn) {
    this.setters.push(fn);
    return this;
  }

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
}