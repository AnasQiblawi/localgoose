class QueryBuilder {
  // === Core Functionality ===
  constructor(query, path) {
    this.query = query;
    this.path = path;
  }

  validate() {
    if (!this.path) {
      throw new Error('Path must be specified before building query conditions');
    }
    return this;
  }

  // === Comparison Operators ===
  equals(val) {
    this.query.conditions[this.path] = val;
    return this.query;
  }

  ne(val) {
    this.query.conditions[this.path] = { $ne: val };
    return this.query;
  }

  gt(val) {
    this.query.conditions[this.path] = { $gt: val };
    return this.query;
  }

  gte(val) {
    this.query.conditions[this.path] = { $gte: val };
    return this.query;
  }

  lt(val) {
    this.query.conditions[this.path] = { $lt: val };
    return this.query;
  }

  lte(val) {
    this.query.conditions[this.path] = { $lte: val };
    return this.query;
  }

  // === Array Operators ===
  in(arr) {
    this.query.conditions[this.path] = { 
      $in: Array.isArray(arr) ? arr : [arr] 
    };
    return this.query;
  }

  nin(arr) {
    this.query.conditions[this.path] = { 
      $nin: Array.isArray(arr) ? arr : [arr] 
    };
    return this.query;
  }

  size(val) {
    this.query.conditions[this.path] = { $size: val };
    return this.query;
  }

  // === Element Operators ===
  exists(val = true) {
    this.query.conditions[this.path] = { $exists: val };
    return this.query;
  }

  type(val) {
    this.query.conditions[this.path] = { $type: val };
    return this.query;
  }

  // === Evaluation Operators ===
  regex(pattern, options = 'i') {
    if (pattern instanceof RegExp) {
      this.query.conditions[this.path] = { $regex: pattern };
    } else if (typeof pattern === 'string') {
      this.query.conditions[this.path] = { $regex: new RegExp(pattern, options) };
    } else {
      throw new Error('Pattern must be a string or RegExp object');
    }
    return this.query;
  }

  mod(divisor, remainder) {
    this.query.conditions[this.path] = { $mod: [divisor, remainder] };
    return this.query;
  }

  // === Geospatial Operators ===
  near(coords, maxDistance) {
    this.query.conditions[this.path] = { 
      $near: coords, 
      ...(maxDistance && { $maxDistance: maxDistance }) 
    };
    return this.query;
  }

  // === Logical Operators ===
  or(conditions) {
    this.query.conditions.$or = conditions;
    return this.query;
  }

  nor(conditions) {
    this.query.conditions.$nor = conditions;
    return this.query;
  }

  and(conditions) {
    this.query.conditions.$and = conditions;
    return this.query;
  }
}

module.exports = { QueryBuilder };