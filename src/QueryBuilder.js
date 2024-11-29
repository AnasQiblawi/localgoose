class QueryBuilder {
  constructor(query, path) {
    this.query = query;
    this.path = path;
  }

  equals(val) {
    this.query.conditions[this.path] = val;
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

  exists(val = true) {
    this.query.conditions[this.path] = { $exists: val };
    return this.query;
  }

  regex(pattern, options = 'i') {
    if (pattern instanceof RegExp) {
      // Use the regular expression directly
      this.query.conditions[this.path] = { $regex: pattern };
    } else if (typeof pattern === 'string') {
      // Convert the string to a RegExp with the provided options
      this.query.conditions[this.path] = { $regex: new RegExp(pattern, options) };
    } else {
      throw new Error('Pattern must be a string or RegExp object');
    }
    return this.query;
  } 

  ne(val) {
    this.query.conditions[this.path] = { $ne: val };
    return this.query;
  }

  mod(divisor, remainder) {
    this.query.conditions[this.path] = { $mod: [divisor, remainder] };
    return this.query;
  }

  size(val) {
    this.query.conditions[this.path] = { $size: val };
    return this.query;
  }

  type(val) {
    this.query.conditions[this.path] = { $type: val };
    return this.query;
  }

  // Geospatial query methods
  near(coords, maxDistance) {
    this.query.conditions[this.path] = { 
      $near: coords, 
      ...(maxDistance && { $maxDistance: maxDistance }) 
    };
    return this.query;
  }

  // Validation method to ensure query building is type-safe
  validate() {
    if (!this.path) {
      throw new Error('Path must be specified before building query conditions');
    }
    return this;
  }
}

module.exports = { QueryBuilder };