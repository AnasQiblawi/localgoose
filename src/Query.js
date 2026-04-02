const { QueryBuilder } = require('./QueryBuilder.js');
const { Document } = require('./Document.js');
const { getNestedValue } = require('./utils.js');

class Query {
  constructor(model, conditions = {}) {
    this.model       = model;
    this.conditions  = { ...conditions };
    this._conditions = this.conditions; // alias
    this._fields     = {};
    this._sort       = {};
    this._limit      = null;
    this._skip       = null;
    this._populate   = [];
    this._lean       = false;
    this._batchSize  = null;
    this._readPreference = null;
    this._hint       = null;
    this._comment    = null;
    this._maxTimeMS  = null;
    this._tailable   = false;
    this._session    = null;
    this._options    = {};
    this._update     = null;
    this._distinct   = null;
    this._error      = null;
    this._explain    = false;
    this._mongooseOptions = {};
    this._geoComparison   = null;
    this._middleware = { pre: [], post: [] };
    this._geometry   = null;
    this._writeConcern    = {};
    this._readConcern     = null;
    this._transform  = null;
    this._currentPath = null;
  }

  // ─── Execution ────────────────────────────────────────────────────────────
  async exec() {
    if (this._error) throw this._error;

    // Run pre-find / pre-findOne middleware
    const hookName = this._limit === 1 ? 'findOne' : 'find';
    await this._runSchemaMiddleware('pre', hookName);

    let docs = await this.model._find(this.conditions);

    // Sort
    if (Object.keys(this._sort).length > 0) {
      docs.sort((a, b) => {
        for (const [field, order] of Object.entries(this._sort)) {
          const av = getNestedValue(a, field);
          const bv = getNestedValue(b, field);
          if (av < bv) return -1 * order;
          if (av > bv) return order;
        }
        return 0;
      });
    }

    if (this._skip)  docs = docs.slice(this._skip);
    if (this._limit) docs = docs.slice(0, this._limit);

    // Field projection
    if (Object.keys(this._fields).length > 0) {
      docs = docs.map(doc => this._applyProjection(doc, this._fields));
    }

    // Lean mode
    if (this._lean) {
      const result = this._limit === 1 ? (docs[0] || null) : docs;
      await this._runSchemaMiddleware('post', hookName, result);
      return result;
    }

    const documents = docs.map(doc => {
      const d = new Document(doc, this.model.schema, this.model);
      d._isNew = false;
      d.isNew  = false;
      return d;
    });

    // Population
    if (this._populate.length > 0) {
      const populated = await Promise.all(documents.map(d => this._populateDoc(d)));
      const result = this._limit === 1 ? (populated[0] || null) : populated;
      await this._runSchemaMiddleware('post', hookName, result);
      if (this._transform) {
        return Array.isArray(result) ? result.map(this._transform) : (result ? this._transform(result) : null);
      }
      return result;
    }

    if (this._transform) {
      const transformed = documents.map(this._transform);
      return this._limit === 1 ? (transformed[0] || null) : transformed;
    }

    const result = this._limit === 1 ? (documents[0] || null) : documents;
    await this._runSchemaMiddleware('post', hookName, result);
    return result;
  }

  // Run schema-level pre/post hooks (find, findOne, etc.)
  async _runSchemaMiddleware(type, action, doc) {
    const hooks = this.model.schema.middleware[type][action] || [];
    for (const fn of hooks) await fn.call(this, doc);
  }

  // Projection helper
  _applyProjection(doc, fields) {
    const hasInclusions = Object.values(fields).some(v => v === 1);
    if (hasInclusions) {
      const result = {};
      if (fields._id !== 0) result._id = doc._id;
      for (const [f, v] of Object.entries(fields)) {
        if (v === 1) result[f] = doc[f];
      }
      return result;
    }
    // Exclusive projection
    const result = { ...doc };
    for (const [f, v] of Object.entries(fields)) {
      if (v === 0) delete result[f];
    }
    return result;
  }

  // Population helper
  async _populateDoc(doc) {
    const populatedDoc = new Document({ ...doc._doc }, this.model.schema, this.model);
    populatedDoc._isNew = false;
    populatedDoc.isNew  = false;
    for (const populate of this._populate) {
      const { path, select, match, options: popOptions } = typeof populate === 'string'
        ? { path: populate } : populate;

      const pathSegments = path.split('.');
      let currentDoc = populatedDoc;
      let currentPath = '';

      for (const segment of pathSegments) {
        currentPath = currentPath ? `${currentPath}.${segment}` : segment;
        const pathSchema = this.model.schema._paths.get(currentPath);
        if (!pathSchema || !pathSchema.options || !pathSchema.options.ref) {
          currentDoc = currentDoc ? currentDoc[segment] : undefined;
          continue;
        }

        const refModel = this.model.db.models[pathSchema.options.ref];
        if (!refModel) { currentDoc = currentDoc ? currentDoc[segment] : undefined; continue; }

        const value = currentDoc ? currentDoc._doc ? currentDoc._doc[segment] : currentDoc[segment] : undefined;
        if (!value) { currentDoc = undefined; continue; }

        try {
          if (Array.isArray(value)) {
            let populatedValues = await Promise.all(value.map(id => {
              let q = refModel.findOne({ _id: id });
              if (select) q = q.select(select);
              if (match) Object.assign(q.conditions, match);
              return q.exec();
            }));
            populatedValues = populatedValues.filter(Boolean);
            if (currentDoc._doc) currentDoc._doc[segment] = populatedValues;
            currentDoc[segment] = populatedValues;
            currentDoc._populated.set(segment, populatedValues);
          } else {
            let q = refModel.findOne({ _id: value });
            if (select) q = q.select(select);
            if (match) Object.assign(q.conditions, match);
            const pv = await q.exec();
            if (pv) {
              if (currentDoc._doc) currentDoc._doc[segment] = pv;
              currentDoc[segment] = pv;
              currentDoc._populated.set(segment, pv);
            }
          }
        } catch (err) {
          console.error(`Error populating ${currentPath}:`, err);
        }
        currentDoc = currentDoc ? currentDoc[segment] : undefined;
      }
    }
    return populatedDoc;
  }

  clone() {
    const c = new Query(this.model, { ...this.conditions });
    c._fields  = { ...this._fields };
    c._sort    = { ...this._sort };
    c._limit   = this._limit;
    c._skip    = this._skip;
    c._populate = [...this._populate];
    c._lean    = this._lean;
    c._options = { ...this._options };
    c._batchSize = this._batchSize;
    c._readPreference = this._readPreference;
    c._hint    = this._hint;
    c._comment = this._comment;
    c._maxTimeMS = this._maxTimeMS;
    c._tailable  = this._tailable;
    c._session   = this._session;
    c._update    = this._update ? { ...this._update } : null;
    c._distinct  = this._distinct;
    c._error     = this._error;
    c._explain   = this._explain;
    c._mongooseOptions = { ...this._mongooseOptions };
    c._geoComparison   = this._geoComparison;
    c._middleware = { pre: [...this._middleware.pre], post: [...this._middleware.post] };
    c._geometry  = this._geometry;
    c._writeConcern = { ...this._writeConcern };
    c._readConcern  = this._readConcern;
    c._transform    = this._transform;
    return c;
  }

  // Thenable — so Query works with await without .exec()
  then(resolve, reject) { return this.exec().then(resolve, reject); }
  catch(fn)             { return this.exec().catch(fn); }
  finally(fn)           { return this.exec().finally(fn); }

  // ─── CRUD convenience methods ──────────────────────────────────────────────
  async findOne(conditions = {}) {
    Object.assign(this.conditions, conditions);
    this._limit = 1;
    return this.exec();
  }

  async findById(id) {
    this.conditions._id = typeof id === 'string' ? id : id.toString();
    this._limit = 1;
    return this.exec();
  }

  async findOneAndDelete(conditions = {}) {
    Object.assign(this.conditions, conditions);
    const doc = await this.exec();
    if (doc) await this.model.deleteOne({ _id: doc._id });
    return doc;
  }

  async findOneAndReplace(conditions, replacement, options = {}) {
    Object.assign(this.conditions, conditions);
    const doc = await this.exec();
    if (doc) { Object.assign(doc._doc, replacement); await doc.save(); }
    else if (options.upsert) return this.model.create(replacement);
    return doc;
  }

  async findOneAndUpdate(conditions, update, options = {}) {
    if (conditions) Object.assign(this.conditions, conditions);
    const doc = await this.model.findOneAndUpdate(this.conditions, update, options);
    return doc;
  }

  async findByIdAndUpdate(id, update, options = {}) {
    return this.model.findByIdAndUpdate(id, update, options);
  }

  async findByIdAndDelete(id) { return this.model.findByIdAndDelete(id); }

  async deleteMany(conditions = {})           { return this.model.deleteMany({ ...this.conditions, ...conditions }); }
  async deleteOne(conditions = {})            { return this.model.deleteOne({ ...this.conditions, ...conditions }); }
  async updateMany(conditions, update, opts)  { return this.model.updateMany({ ...this.conditions, ...conditions }, update, opts); }
  async updateOne(conditions, update, opts)   { return this.model.updateOne({ ...this.conditions, ...conditions }, update, opts); }
  async replaceOne(conditions, doc, opts)     { return this.model.replaceOne({ ...this.conditions, ...conditions }, doc, opts); }

  // ─── Query Building ────────────────────────────────────────────────────────
  where(path) {
    if (typeof path === 'object') { Object.assign(this.conditions, path); return this; }
    this._currentPath = path;
    // Return QueryBuilder but also ensure Query methods honour _currentPath afterwards
    const qb = new QueryBuilder(this, path);
    return qb;
  }

  equals(val) {
    if (typeof val === 'object' && val !== null) this.conditions = val;
    else this.conditions._id = val;
    return this;
  }

  _setConditional(path, val, op) {
    const p = arguments.length === 2 ? this._currentPath : path;
    const v = arguments.length === 2 ? path : val;
    const existing = this.conditions[p];
    if (existing && typeof existing === 'object' && !Array.isArray(existing) && !(existing instanceof RegExp)) {
      this.conditions[p] = { ...existing, [op]: v };
    } else {
      this.conditions[p] = { [op]: v };
    }
    return this;
  }

  gt(path, val)  { if (arguments.length === 1) { this._setConditional(this._currentPath, path, '$gt');  return this; } return this._setConditional(path, val, '$gt');  }
  gte(path, val) { if (arguments.length === 1) { this._setConditional(this._currentPath, path, '$gte'); return this; } return this._setConditional(path, val, '$gte'); }
  lt(path, val)  { if (arguments.length === 1) { this._setConditional(this._currentPath, path, '$lt');  return this; } return this._setConditional(path, val, '$lt');  }
  lte(path, val) { if (arguments.length === 1) { this._setConditional(this._currentPath, path, '$lte'); return this; } return this._setConditional(path, val, '$lte'); }
  ne(path, val)  { if (arguments.length === 1) { this._setConditional(this._currentPath, path, '$ne');  return this; } return this._setConditional(path, val, '$ne');  }

  in(path, vals) {
    const p = arguments.length === 1 ? this._currentPath : path;
    const v = arguments.length === 1 ? path : vals;
    this.conditions[p] = { ...this.conditions[p], $in: Array.isArray(v) ? v : [v] };
    return this;
  }

  nin(path, vals) {
    const p = arguments.length === 1 ? this._currentPath : path;
    const v = arguments.length === 1 ? path : vals;
    this.conditions[p] = { ...this.conditions[p], $nin: Array.isArray(v) ? v : [v] };
    return this;
  }

  regex(path, val, options) {
    const p = (typeof path === 'string' && (val instanceof RegExp || typeof val === 'string')) ? path : this._currentPath;
    const v = (typeof path === 'string' && (val instanceof RegExp || typeof val === 'string')) ? val : path;
    const rgx = v instanceof RegExp ? v : new RegExp(v, options || 'i');
    this.conditions[p] = { ...this.conditions[p], $regex: rgx };
    return this;
  }

  mod(path, divisor, remainder) {
    if (arguments.length === 2) { remainder = divisor; divisor = path; path = this._currentPath; }
    this.conditions[path] = { $mod: [divisor, remainder] };
    return this;
  }

  size(path, val) {
    const p = arguments.length === 1 ? this._currentPath : path;
    const v = arguments.length === 1 ? path : val;
    this.conditions[p] = { $size: v };
    return this;
  }

  exists(path, val = true) {
    const p = arguments.length === 1 ? this._currentPath : path;
    const v = arguments.length === 1 ? path : val;
    this.conditions[p] = { $exists: v };
    return this;
  }

  elemMatch(path, criteria) { this.conditions[path] = { $elemMatch: criteria }; return this; }
  all(path, values)          { this.conditions[path] = { $all: values }; return this; }
  and(conditions) { if (!this.conditions.$and) this.conditions.$and = []; this.conditions.$and.push(...conditions); return this; }
  or(array)  { this.conditions.$or  = array; return this; }
  nor(array) { this.conditions.$nor = array; return this; }

  // ─── Result Modifiers ─────────────────────────────────────────────────────
  select(fields) {
    if (typeof fields === 'string') {
      fields.split(/\s+/).forEach(f => {
        if (!f) return;
        this._fields[f.replace(/^-/, '')] = f.startsWith('-') ? 0 : 1;
      });
    } else if (typeof fields === 'object') {
      Object.assign(this._fields, fields);
    }
    return this;
  }

  sort(fields) {
    if (typeof fields === 'string') {
      fields.split(/\s+/).forEach(f => {
        if (!f) return;
        this._sort[f.replace(/^-/, '')] = f.startsWith('-') ? -1 : 1;
      });
    } else if (typeof fields === 'object') {
      Object.assign(this._sort, fields);
    }
    return this;
  }

  limit(n)        { this._limit = n; return this; }
  skip(n)         { this._skip  = n; return this; }
  lean(value = true) { this._lean = value; return this; }
  populate(path, select) {
    if (typeof path === 'string') this._populate.push({ path, select });
    else if (Array.isArray(path)) path.forEach(p => this.populate(p));
    else if (typeof path === 'object') this._populate.push(path);
    return this;
  }

  paginate(page = 1, limit = 10) {
    this._skip  = (page - 1) * limit;
    this._limit = limit;
    return this;
  }

  transform(fn) { this._transform = fn; return this; }

  // ─── Query Options ─────────────────────────────────────────────────────────
  allowDiskUse(allow = true)      { this._options.allowDiskUse = allow; return this; }
  batchSize(size)                 { this._batchSize = size; return this; }
  collation(value)                { this._options.collation = value; return this; }
  comment(value)                  { this._comment = value; return this; }
  explain(value = true)           { this._explain = value; return this; }
  hint(value)                     { this._hint = value; return this; }
  maxTimeMS(value)                { this._maxTimeMS = value; return this; }
  mongooseOptions(opts)           { Object.assign(this._mongooseOptions, opts); return this; }
  read(pref)                      { this._readPreference = pref; return this; }
  readConcern(level)              { this._readConcern = level; return this; }
  session(session)                { this._session = session; return this; }
  setOptions(opts)                { Object.assign(this._options, opts); return this; }
  tailable(value = true)          { this._tailable = value; return this; }
  writeConcern(concern)           { this._writeConcern = concern; return this; }
  j(value = true)                 { this._writeConcern.j = value; return this; }
  w(val)                          { this._writeConcern.w = val; return this; }
  wtimeout(ms)                    { this._writeConcern.wtimeout = ms; return this; }

  // ─── Utility Methods ───────────────────────────────────────────────────────
  $where(js)              { this.conditions.$where = js; return this; }
  box(path, box)          { this._geometry = { type: 'box', path, coordinates: box }; return this; }
  center(path, c)         { this._geometry = { type: 'center', path, coordinates: c }; return this; }
  centerSphere(path, c)   { this._geometry = { type: 'centerSphere', path, coordinates: c }; return this; }
  circle(path, c)         { this._geometry = { type: 'circle', path, coordinates: c }; return this; }
  polygon(path, coords)   { this._geometry = { type: 'polygon', path, coordinates: coords }; return this; }
  geometry(path, geo)     { this._geometry = { type: 'geometry', path, coordinates: geo }; return this; }
  intersects(arg)         { this._geoComparison = { $geoIntersects: arg }; return this; }
  near(path, coords)      { this._geoComparison = { $near: coords }; return this; }
  nearSphere(path, coords){ this._geoComparison = { $nearSphere: coords }; return this; }
  maxDistance(value)      { if (this._geoComparison) this._geoComparison.$maxDistance = value; return this; }
  within()                { return this; }

  cursor() { throw new Error('Cursors are not supported in file-based storage'); }
  error(err) { this._error = err; return this; }
  orFail(err) { this._error = err || new Error('No document found'); return this; }

  get(path) { return path ? this._fields[path] : this.exec(); }
  getFilter()        { return { ...this.conditions }; }
  getOptions()       { return { ...this._options }; }
  getPopulatedPaths(){ return [...this._populate]; }
  getQuery()         { return { ...this.conditions }; }
  getUpdate()        { return this._update; }

  isPathSelectedInclusive(path) { return !!this._fields[path]; }
  merge(source) {
    Object.assign(this.conditions, source.conditions);
    Object.assign(this._fields, source._fields);
    Object.assign(this._sort, source._sort);
    this._limit = source._limit;
    this._skip  = source._skip;
    return this;
  }

  projection(fields) { this._fields = fields; return this; }
  rand()             { this._sort.$rand = 1; return this; }
  natural()          { this._sort.$natural = 1; return this; }

  sanitizeProjection(fields) {
    const s = {};
    for (const [k, v] of Object.entries(fields)) {
      if (typeof v === 'number' || typeof v === 'boolean') s[k] = v ? 1 : 0;
    }
    return s;
  }

  selected()            { return Object.keys(this._fields).length > 0; }
  selectedExclusively() { return Object.values(this._fields).some(v => v === 0); }
  selectedInclusively() { return Object.values(this._fields).some(v => v === 1); }

  set(path, val) {
    if (typeof path === 'object') Object.assign(this._update || (this._update = {}), path);
    else { this._update = this._update || {}; this._update[path] = val; }
    return this;
  }

  setQuery(conditions) { this.conditions = conditions; return this; }
  setUpdate(update)    { this._update = update; return this; }
  slice(path, val)     { this._fields[path] = { $slice: val }; return this; }
  cast(model)          { this.model = model; return this; }

  text(search) { this.conditions.$text = { $search: search }; return this; }
  expr(expression)    { this.conditions.$expr = expression; return this; }
  jsonSchema(schema)  { this.conditions.$jsonSchema = schema; return this; }
  meta(path)          { this._fields[path] = { $meta: 'textScore' }; return this; }

  toConstructor() {
    const Q = function (criteria, opts) { Query.call(this, this.model, criteria); this.setOptions(opts || {}); };
    Q.prototype = Object.create(Query.prototype);
    Q.prototype.constructor = Q;
    Q.prototype.model = this.model;
    return Q;
  }

  pre(method, fn)  {
    if (!this._middleware.pre[method])  this._middleware.pre[method]  = [];
    this._middleware.pre[method].push(fn);
    return this;
  }

  post(method, fn) {
    if (!this._middleware.post[method]) this._middleware.post[method] = [];
    this._middleware.post[method].push(fn);
    return this;
  }

  // ─── Async queries ─────────────────────────────────────────────────────────
  async countDocuments(conditions = {}) {
    const docs = await this.model._find({ ...this.conditions, ...conditions });
    return docs.length;
  }

  async distinct(field) {
    const docs = await this.model._find(this.conditions);
    return [...new Set(docs.map(d => d[field]))];
  }

  async estimatedDocumentCount() { return this.model.estimatedDocumentCount(); }

  // ─── Symbols ───────────────────────────────────────────────────────────────
  [Symbol.asyncIterator]() {
    let index = 0;
    let documents;
    return {
      next: async () => {
        if (!documents) documents = await this.exec();
        if (!Array.isArray(documents)) documents = documents ? [documents] : [];
        if (index < documents.length) return { value: documents[index++], done: false };
        return { done: true };
      }
    };
  }

  get [Symbol.toStringTag]() { return 'Query'; }
  static get use$geoWithin() { return true; }
}

module.exports = { Query };