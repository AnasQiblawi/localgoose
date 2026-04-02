class QueryBuilder {
  constructor(query, path) {
    this.query = query;
    this.path = path;
  }

  // Merge a new operator into existing conditions for this path (preserves $gt + $lt etc.)
  _merge(op, val) {
    const existing = this.query.conditions[this.path];
    if (existing !== null && existing !== undefined &&
        typeof existing === 'object' && !Array.isArray(existing) &&
        !(existing instanceof RegExp) && !(existing instanceof Date)) {
      this.query.conditions[this.path] = { ...existing, [op]: val };
    } else {
      this.query.conditions[this.path] = { [op]: val };
    }
    return this.query;
  }

  validate() {
    if (!this.path) throw new Error('Path must be specified before building query conditions');
    return this;
  }

  // === Comparison Operators ===
  equals(val) {
    this.query.conditions[this.path] = val;
    return this.query;
  }

  ne(val)  { return this._merge('$ne', val); }
  gt(val)  { return this._merge('$gt', val); }
  gte(val) { return this._merge('$gte', val); }
  lt(val)  { return this._merge('$lt', val); }
  lte(val) { return this._merge('$lte', val); }

  // === Array Operators ===
  in(arr)  { return this._merge('$in', Array.isArray(arr) ? arr : [arr]); }
  nin(arr) { return this._merge('$nin', Array.isArray(arr) ? arr : [arr]); }
  size(val) { return this._merge('$size', val); }
  all(vals) { return this._merge('$all', vals); }
  elemMatch(criteria) { return this._merge('$elemMatch', criteria); }

  // === Element Operators ===
  exists(val = true) { return this._merge('$exists', val); }
  type(val) { return this._merge('$type', val); }

  // === Evaluation Operators ===
  regex(pattern, options = 'i') {
    const rgx = pattern instanceof RegExp ? pattern : new RegExp(pattern, options);
    return this._merge('$regex', rgx);
  }

  mod(divisor, remainder) { return this._merge('$mod', [divisor, remainder]); }

  not(criteria) { return this._merge('$not', criteria); }

  // === Geospatial Operators ===
  near(coords, maxDistance) {
    const val = { $near: Array.isArray(coords) ? coords : [coords.lng || coords.longitude, coords.lat || coords.latitude] };
    if (maxDistance) val.$maxDistance = maxDistance;
    this.query.conditions[this.path] = val;
    return this.query;
  }

  within(shape) { return this._merge('$geoWithin', shape); }

  // === Logical Operators (path-level, sets at root) ===
  or(conditions)  { this.query.conditions.$or  = conditions; return this.query; }
  nor(conditions) { this.query.conditions.$nor = conditions; return this.query; }
  and(conditions) { this.query.conditions.$and = conditions; return this.query; }
}

module.exports = { QueryBuilder };