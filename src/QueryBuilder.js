export class QueryBuilder {
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
    this.query.conditions[this.path] = { $in: Array.isArray(arr) ? arr : [arr] };
    return this.query;
  }

  nin(arr) {
    this.query.conditions[this.path] = { $nin: Array.isArray(arr) ? arr : [arr] };
    return this.query;
  }

  exists(val = true) {
    this.query.conditions[this.path] = { $exists: val };
    return this.query;
  }

  regex(pattern, options) {
    this.query.conditions[this.path] = { $regex: pattern, $options: options };
    return this.query;
  }
}