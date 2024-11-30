const { validateType } = require('./utils.js');
const { QueryBuilder } = require('./QueryBuilder.js');
const { Document } = require('./Document.js');

class Query {
  constructor(model, conditions = {}) {
    this.model = model;
    this.conditions = conditions;
    this._fields = {};
    this._sort = {};
    this._limit = null;
    this._skip = null;
    this._populate = [];
    this._batchSize = null;
    this._readPreference = null;
    this._hint = null;
    this._comment = null;
    this._maxTimeMS = null;
    this._tailable = false;
    this._session = null;
    this._options = {};
    this._update = null;
    this._distinct = null;
    this._error = null;
    this._explain = false;
    this._mongooseOptions = {};
    this._geoComparison = null;
    this._middleware = { pre: [], post: [] };
    this._geometry = null;
    this._lean = false;
    this._writeConcern = {};
    this._readConcern = null;
    this._transform = null;
  }

  $where(js) {
    this.conditions.$where = js;
    return this;
  }

  all(path, values) {
    this.conditions[path] = { $all: values };
    return this;
  }

  allowDiskUse(allow = true) {
    this._options.allowDiskUse = allow;
    return this;
  }

  and(conditions) {
    if (!this.conditions.$and) {
      this.conditions.$and = [];
    }
    this.conditions.$and.push(...conditions);
    return this;
  }

  batchSize(size) {
    this._batchSize = size;
    return this;
  }

  box(path, box) {
    this._geometry = { type: 'box', path, coordinates: box };
    return this;
  }

  cast(model) {
    this.model = model;
    return this;
  }

  catch(fn) {
    return this.exec().catch(fn);
  }

  center(path, center) {
    this._geometry = { type: 'center', path, coordinates: center };
    return this;
  }

  centerSphere(path, centerSphere) {
    this._geometry = { type: 'centerSphere', path, coordinates: centerSphere };
    return this;
  }

  circle(path, circle) {
    this._geometry = { type: 'circle', path, coordinates: circle };
    return this;
  }

  clone() {
    const clone = new Query(this.model);
    clone.conditions = { ...this.conditions };
    clone._fields = { ...this._fields };
    clone._sort = { ...this._sort };
    clone._limit = this._limit;
    clone._skip = this._skip;
    clone._populate = [...this._populate];
    clone._options = { ...this._options };
    clone._batchSize = this._batchSize;
    clone._readPreference = this._readPreference;
    clone._hint = this._hint;
    clone._comment = this._comment;
    clone._maxTimeMS = this._maxTimeMS;
    clone._tailable = this._tailable;
    clone._session = this._session;
    clone._update = this._update ? { ...this._update } : null;
    clone._distinct = this._distinct;
    clone._error = this._error;
    clone._explain = this._explain;
    clone._mongooseOptions = { ...this._mongooseOptions };
    clone._geoComparison = this._geoComparison;
    clone._middleware = { 
      pre: [...this._middleware.pre], 
      post: [...this._middleware.post] 
    };
    clone._geometry = this._geometry;
    clone._lean = this._lean;
    clone._writeConcern = { ...this._writeConcern };
    clone._readConcern = this._readConcern;
    clone._transform = this._transform;
    return clone;
  }

  collation(value) {
    this._options.collation = value;
    return this;
  }

  comment(value) {
    this._comment = value;
    return this;
  }

  async countDocuments(conditions = {}) {
    const docs = await this.model._find({ ...this.conditions, ...conditions });
    return docs.length;
  }

  cursor() {
    throw new Error('Cursors are not supported in file-based storage');
  }

  async deleteMany(conditions = {}) {
    return this.model.deleteMany({ ...this.conditions, ...conditions });
  }

  async deleteOne(conditions = {}) {
    const docs = await this.model._find({ ...this.conditions, ...conditions });
    if (docs.length > 0) {
      await this.model.deleteMany({ _id: docs[0]._id });
      return { deletedCount: 1 };
    }
    return { deletedCount: 0 };
  }

  async distinct(field) {
    const docs = await this.model._find(this.conditions);
    return [...new Set(docs.map(doc => doc[field]))];
  }

  elemMatch(path, criteria) {
    this.conditions[path] = { $elemMatch: criteria };
    return this;
  }

  equals(val) {
    if (typeof val === 'object') {
      this.conditions = val;
    } else {
      this.conditions = { _id: val };
    }
    return this;
  }

  error(err) {
    this._error = err;
    return this;
  }

  async estimatedDocumentCount() {
    return this.model.estimatedDocumentCount();
  }

  async exec() {
    if (this._error) {
      throw this._error;
    }

    let docs = await this.model._find(this.conditions);
    
    if (Object.keys(this._sort).length > 0) {
      docs.sort((a, b) => {
        for (const [field, order] of Object.entries(this._sort)) {
          if (a[field] < b[field]) return -1 * order;
          if (a[field] > b[field]) return 1 * order;
        }
        return 0;
      });
    }

    if (this._skip) {
      docs = docs.slice(this._skip);
    }
    
    if (this._limit) {
      docs = docs.slice(0, this._limit);
    }

    if (this._lean) {
      return docs;
    }

    const documents = docs.map(doc => new Document(doc, this.model.schema, this.model));
    
    if (this._populate.length > 0) {
      return Promise.all(documents.map(doc => this._populateDoc(doc)));
    }

    if (this._transform) {
      return documents.map(this._transform);
    }

    return documents;
  }

  exists(path, val = true) {
    if (arguments.length === 1) {
      val = path;
      path = this._currentPath;
    }
    this.conditions[path] = { $exists: val };
    return this;
  }

  explain(value = true) {
    this._explain = value;
    return this;
  }

  finally(fn) {
    return this.exec().finally(fn);
  }

  find(conditions = {}) {
    Object.assign(this.conditions, conditions);
    return this;
  }

  async findOne(conditions = {}) {
    const docs = await this.model._find({ ...this.conditions, ...conditions });
    return docs[0] ? new Document(docs[0], this.model.schema, this.model) : null;
  }

  async findOneAndDelete(conditions = {}) {
    const doc = await this.findOne(conditions);
    if (doc) {
      await this.model.deleteOne({ _id: doc._id });
    }
    return doc;
  }

  async findOneAndReplace(conditions, replacement, options = {}) {
    const doc = await this.findOne(conditions);
    if (doc) {
      Object.assign(doc, replacement);
      await doc.save();
    } else if (options.upsert) {
      return this.model.create(replacement);
    }
    return doc;
  }

  async findOneAndUpdate(conditions, update, options = {}) {
    const doc = await this.findOne(conditions);
    if (doc) {
      Object.assign(doc, update);
      await doc.save();
    } else if (options.upsert) {
      return this.model.create({ ...conditions, ...update });
    }
    return doc;
  }

  geometry(path, geometry) {
    this._geometry = { type: 'geometry', path, coordinates: geometry };
    return this;
  }

  get(path) {
    return path ? this._fields[path] : this.exec();
  }

  getFilter() {
    return { ...this.conditions };
  }

  getOptions() {
    return { ...this._options };
  }

  getPopulatedPaths() {
    return [...this._populate];
  }

  getQuery() {
    return { ...this.conditions };
  }

  getUpdate() {
    return this._update;
  }

  gt(path, val) {
    if (arguments.length === 1) {
      val = path;
      path = this._currentPath;
    }
    this.conditions[path] = { $gt: val };
    return this;
  }

  gte(path, val) {
    if (arguments.length === 1) {
      val = path;
      path = this._currentPath;
    }
    this.conditions[path] = { $gte: val };
    return this;
  }

  hint(value) {
    this._hint = value;
    return this;
  }

  in(path, vals) {
    if (arguments.length === 1) {
      vals = path;
      path = this._currentPath;
    }
    this.conditions[path] = { $in: Array.isArray(vals) ? vals : [vals] };
    return this;
  }

  intersects(arg) {
    this._geoComparison = { $geoIntersects: arg };
    return this;
  }

  isPathSelectedInclusive(path) {
    return !!this._fields[path];
  }

  j(value = true) {
    this._writeConcern.j = value;
    return this;
  }

  lean(value = true) {
    this._lean = value;
    return this;
  }

  limit(n) {
    this._limit = n;
    return this;
  }

  lt(path, val) {
    if (arguments.length === 1) {
      val = path;
      path = this._currentPath;
    }
    this.conditions[path] = { $lt: val };
    return this;
  }

  lte(path, val) {
    if (arguments.length === 1) {
      val = path;
      path = this._currentPath;
    }
    this.conditions[path] = { $lte: val };
    return this;
  }

  maxDistance(value) {
    if (this._geoComparison) {
      this._geoComparison.$maxDistance = value;
    }
    return this;
  }

  maxTimeMS(value) {
    this._maxTimeMS = value;
    return this;
  }

  merge(source) {
    Object.assign(this.conditions, source.conditions);
    Object.assign(this._fields, source._fields);
    Object.assign(this._sort, source._sort);
    this._limit = source._limit;
    this._skip = source._skip;
    return this;
  }

  mod(path, divisor, remainder) {
    if (arguments.length === 2) {
      remainder = divisor;
      divisor = path;
      path = this._currentPath;
    }
    this.conditions[path] = { $mod: [divisor, remainder] };
    return this;
  }

  mongooseOptions(options) {
    Object.assign(this._mongooseOptions, options);
    return this;
  }

  ne(path, val) {
    if (arguments.length === 1) {
      val = path;
      path = this._currentPath;
    }
    this.conditions[path] = { $ne: val };
    return this;
  }

  near(path, coords) {
    this._geoComparison = { $near: coords };
    return this;
  }

  nearSphere(path, coords) {
    this._geoComparison = { $nearSphere: coords };
    return this;
  }

  nin(path, vals) {
    if (arguments.length === 1) {
      vals = path;
      path = this._currentPath;
    }
    this.conditions[path] = { $nin: Array.isArray(vals) ? vals : [vals] };
    return this;
  }

  nor(array) {
    this.conditions.$nor = array;
    return this;
  }

  or(array) {
    this.conditions.$or = array;
    return this;
  }

  orFail(err) {
    this._error = err || new Error('No document found');
    return this;
  }

  polygon(path, coords) {
    this._geometry = { type: 'polygon', path, coordinates: coords };
    return this;
  }

  populate(path, select) {
    if (typeof path === 'string') {
      this._populate.push({ path, select });
    } else if (typeof path === 'object') {
      this._populate.push(path);
    }
    return this;
  }

  post(method, fn) {
    if (!this._middleware.post[method]) {
      this._middleware.post[method] = [];
    }
    this._middleware.post[method].push(fn);
    return this;
  }

  pre(method, fn) {
    if (!this._middleware.pre[method]) {
      this._middleware.pre[method] = [];
    }
    this._middleware.pre[method].push(fn);
    return this;
  }

  projection(fields) {
    this._fields = fields;
    return this;
  }

  read(pref) {
    this._readPreference = pref;
    return this;
  }

  readConcern(level) {
    this._readConcern = level;
    return this;
  }

  regex(path, val, options = 'i') {
    if (arguments.length === 1 || typeof path === 'string' && arguments.length === 2) {
      val = path;
      options = arguments.length === 2 ? val : 'i';
      path = this._currentPath;
    }
    this.conditions[path] = { 
      $regex: val,
      $options: options 
    };
    return this;
  }

  async replaceOne(conditions, doc, options = {}) {
    return this.model.replaceOne({ ...this.conditions, ...conditions }, doc, options);
  }

  sanitizeProjection(fields) {
    const sanitized = {};
    for (const [key, value] of Object.entries(fields)) {
      if (typeof value === 'number' || typeof value === 'boolean') {
        sanitized[key] = value ? 1 : 0;
      }
    }
    return sanitized;
  }

  select(fields) {
    if (typeof fields === 'string') {
      fields.split(/\s+/).forEach(field => {
        this._fields[field.replace(/^-/, '')] = field.startsWith('-') ? 0 : 1;
      });
    } else {
      Object.assign(this._fields, fields);
    }
    return this;
  }

  selected() {
    return Object.keys(this._fields).length > 0;
  }

  selectedExclusively() {
    return Object.values(this._fields).some(v => v === 0);
  }

  selectedInclusively() {
    return Object.values(this._fields).some(v => v === 1);
  }

  session(session) {
    this._session = session;
    return this;
  }

  set(path, val) {
    if (typeof path === 'object') {
      Object.assign(this._update, path);
    } else {
      this._update = this._update || {};
      this._update[path] = val;
    }
    return this;
  }

  setOptions(options) {
    Object.assign(this._options, options);
    return this;
  }

  setQuery(conditions) {
    this.conditions = conditions;
    return this;
  }

  setUpdate(update) {
    this._update = update;
    return this;
  }

  size(path, val) {
    if (arguments.length === 1) {
      val = path;
      path = this._currentPath;
    }
    this.conditions[path] = { $size: val };
    return this;
  }

  skip(n) {
    this._skip = n;
    return this;
  }

  slice(path, val) {
    this._fields[path] = { $slice: val };
    return this;
  }

  sort(fields) {
    if (typeof fields === 'string') {
      fields.split(/\s+/).forEach(field => {
        this._sort[field.replace(/^-/, '')] = field.startsWith('-') ? -1 : 1;
      });
    } else {
      Object.assign(this._sort, fields);
    }
    return this;
  }

  tailable(value = true) {
    this._tailable = value;
    return this;
  }

  then(resolve, reject) {
    return this.exec().then(resolve, reject);
  }

  toConstructor() {
    const CustomQuery = function(criteria, options) {
      Query.call(this, this.model, criteria);
      this.setOptions(options);
    };
    CustomQuery.prototype = Object.create(Query.prototype);
    CustomQuery.prototype.constructor = CustomQuery;
    CustomQuery.prototype.model = this.model;
    return CustomQuery;
  }

  transform(fn) {
    this._transform = fn;
    return this;
  }

  async updateMany(conditions, update, options = {}) {
    return this.model.updateMany({ ...this.conditions, ...conditions }, update, options);
  }

  async updateOne(conditions, update, options = {}) {
    return this.model.updateOne({ ...this.conditions, ...conditions }, update, options);
  }

  w(val) {
    this._writeConcern.w = val;
    return this;
  }

  where(path) {
    this._currentPath = path;
    return new QueryBuilder(this, path);
  }

  within() {
    return this;
  }

  writeConcern(concern) {
    this._writeConcern = concern;
    return this;
  }

  wtimeout(ms) {
    this._writeConcern.wtimeout = ms;
    return this;
  }

  [Symbol.asyncIterator]() {
    let index = 0;
    let documents;

    return {
      next: async () => {
        if (!documents) {
          documents = await this.exec();
        }

        if (index < documents.length) {
          return { value: documents[index++], done: false };
        }

        return { done: true };
      }
    };
  }

  get [Symbol.toStringTag]() {
    return 'Query';
  }

  static get use$geoWithin() {
    return true;
  }

  async _populateDoc(doc) {
    const populatedDoc = new Document(doc._doc, this.model.schema, this.model);
    
    for (const populate of this._populate) {
      const path = populate.path;
      const pathSchema = this.model.schema._paths.get(path);
      
      if (pathSchema && pathSchema.options && pathSchema.options.ref) {
        const refModel = this.model.db.models[pathSchema.options.ref];
        if (!refModel) continue;

        const value = doc[path];
        if (!value) continue;

        try {
          const populatedValue = await refModel.findOne({ _id: value });
          if (populatedValue) {
            populatedDoc._populated.set(path, populatedValue);
            populatedDoc[path] = populatedValue;
          }
        } catch (error) {
          console.error(`Error populating ${path}:`, error);
        }
      }
    }
    
    return populatedDoc;
  }

  schemaLevelProjections() {
    const projections = {};
    this.model.schema.eachPath((path, schemaType) => {
      if (schemaType.selected) {
        projections[path] = 1;
      }
    });
    return projections;
  }
}

module.exports = { Query };