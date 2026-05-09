const { readJSON, writeJSON, getNestedValue, extractCoordinates } = require('./utils.js');
const path = require('path');
const geolib = require('geolib');

// All stages supported by exec()
const VALID_STAGES = [
  '$match', '$group', '$sort', '$limit', '$skip', '$unwind',
  '$project', '$lookup', '$addFields', '$densify',
  '$facet', '$graphLookup', '$unionWith', '$sortByCount',
  '$count', '$out', '$merge', '$replaceRoot', '$set', '$unset',
  '$bucket', '$bucketAuto', '$sample', '$fill', '$setWindowFields',
  '$changeStream', '$documents', '$redact', '$search', '$geoNear'
];

class Aggregate {
  constructor(model, pipeline = []) {
    if (!model) throw new Error('Model is required for aggregation');
    this.model = model;
    this._pipeline = Array.isArray(pipeline) ? [...pipeline] : [];
    this._explain  = false;
    this._options  = {};
    this._preHooks = [];
    this._validatePipeline();
  }

  _validatePipeline() {
    for (const stage of this._pipeline) {
      const op = Object.keys(stage)[0];
      if (!VALID_STAGES.includes(op)) throw new Error(`Invalid pipeline stage: ${op}`);
    }
  }

  // Returns a copy of the pipeline array (Mongoose API)
  pipeline() { return [...this._pipeline]; }

  async exec() {
    // Run schema-registered pre-aggregate hooks (the definitive source)
    const schemaHooks = this.model.schema.middleware.pre.aggregate || [];
    for (const fn of schemaHooks) await fn.call(this);
    // Run any hooks added directly to this Aggregate instance (not already in schema)
    for (const fn of (this._preHooks || [])) {
      if (!schemaHooks.includes(fn)) await fn.call(this);
    }

    if (!this.model._find) throw new Error('_find method is not implemented in the model');
    let docs = await this.model._find();

    for (const stage of this._pipeline) {
      const op        = Object.keys(stage)[0];
      const operation = stage[op];
      switch (op) {
        case '$match':
          docs = docs.filter(doc => this.model._matchQuery(doc, operation));
          break;
        case '$group':
          docs = this._group(docs, operation);
          break;
        case '$sort':
          docs = this._sortDocs(docs, operation);
          break;
        case '$limit':
          docs = docs.slice(0, operation);
          break;
        case '$skip':
          docs = docs.slice(operation);
          break;
        case '$unwind':
          docs = this._unwind(docs, operation);
          break;
        case '$project':
          docs = docs.map(doc => this._project(doc, operation));
          break;
        case '$lookup':
          docs = await this._lookup(docs, operation);
          break;
        case '$addFields': case '$set':
          docs = docs.map(doc => { const nd = { ...doc }; for (const [f, v] of Object.entries(operation)) nd[f] = this._evalExpr(v, doc); return nd; });
          break;
        case '$unset':
          docs = docs.map(doc => { const nd = { ...doc }; for (const f of (Array.isArray(operation) ? operation : [operation])) delete nd[f]; return nd; });
          break;
        case '$replaceRoot':
          docs = docs.map(doc => this._evalExpr(operation.newRoot, doc));
          break;
        case '$densify':
          docs = this._densify(docs, operation);
          break;
        case '$facet':
          docs = [await this._facet(docs, operation)];
          break;
        case '$graphLookup':
          docs = this._graphLookup(docs, operation);
          break;
        case '$unionWith':
          docs = await this._unionWith(docs, operation);
          break;
        case '$sortByCount':
          docs = this._sortByCount(docs, operation);
          break;
        case '$count':
          docs = [{ [operation]: docs.length }];
          break;
        case '$bucket':
          docs = this._bucket(docs, operation);
          break;
        case '$bucketAuto':
          docs = this._bucketAuto(docs, operation);
          break;
        case '$sample':
          docs = this._sample(docs, operation.size);
          break;
        case '$fill':
          docs = this._fill(docs, operation);
          break;
        case '$setWindowFields':
          docs = this._setWindowFields(docs, operation);
          break;
        case '$out':
          docs = await this._out(docs, operation);
          break;
        case '$merge':
          docs = await this._merge(docs, operation);
          break;
        case '$geoNear':
          docs = this._geoNear(docs, operation);
          break;
        case '$redact':
          docs = docs.map(doc => this._redact(doc, operation)).filter(Boolean);
          break;
        case '$search':
          docs = docs.filter(doc => this.model._matchQuery(doc, { $text: operation }));
          break;
        default:
          // Unknown stages pass through
          break;
      }
    }

    // Run post-aggregate hooks
    const postHooks = this.model.schema.middleware.post.aggregate || [];
    for (const fn of postHooks) await fn.call(this, docs);

    return docs;
  }

  // ─── Pipeline stage builders ───────────────────────────────────────────────
  match(criteria)         { this._pipeline.push({ $match: criteria });             return this; }
  group(grouping)         { this._pipeline.push({ $group: grouping });             return this; }
  sort(sorting)           { this._pipeline.push({ $sort: sorting });               return this; }
  limit(n)                { this._pipeline.push({ $limit: n });                    return this; }
  skip(n)                 { this._pipeline.push({ $skip: n });                     return this; }
  unwind(p)               { this._pipeline.push({ $unwind: p });                   return this; }
  project(projection)     { this._pipeline.push({ $project: projection });         return this; }
  lookup(opts)            { this._pipeline.push({ $lookup: opts });                return this; }
  addFields(fields)       { this._pipeline.push({ $addFields: fields });           return this; }
  densify(opts)           { this._pipeline.push({ $densify: opts });               return this; }
  facet(facets)           { this._pipeline.push({ $facet: facets });               return this; }
  graphLookup(opts)       { this._pipeline.push({ $graphLookup: opts });           return this; }
  unionWith(coll, pl = []){ this._pipeline.push({ $unionWith: { coll, pipeline: pl } }); return this; }
  sortByCount(field)      { this._pipeline.push({ $sortByCount: field });          return this; }
  count(fieldName = 'count') { this._pipeline.push({ $count: fieldName });         return this; }
  out(collection)         { this._pipeline.push({ $out: collection });             return this; }
  merge(opts)             { this._pipeline.push({ $merge: opts });                 return this; }
  replaceRoot(newRoot)    { this._pipeline.push({ $replaceRoot: { newRoot } });    return this; }
  set(fields)             { this._pipeline.push({ $set: fields });                 return this; }
  unset(fields)           { this._pipeline.push({ $unset: Array.isArray(fields) ? fields : [fields] }); return this; }
  bucket(opts)            { this._pipeline.push({ $bucket: opts });                return this; }
  bucketAuto(opts)        { this._pipeline.push({ $bucketAuto: opts });            return this; }
  sample(size)            { this._pipeline.push({ $sample: { size } });            return this; }
  fill(opts)              { this._pipeline.push({ $fill: opts });                  return this; }
  setWindowFields(opts)   { this._pipeline.push({ $setWindowFields: opts });       return this; }
  redact(expr, thenExpr, elseExpr) {
    let expression = expr;
    if (thenExpr && elseExpr) {
      expression = { $cond: { if: expr, then: thenExpr.startsWith('$$') ? thenExpr : `$$${thenExpr}`, else: elseExpr.startsWith('$$') ? elseExpr : `$$${elseExpr}` } };
    }
    this._pipeline.push({ $redact: expression });
    return this;
  }
  search($search)         { this._pipeline.push({ $search }); return this; }
  near(arg) {
    if (!arg || typeof arg !== 'object') throw new Error('$geoNear requires valid arguments');
    this._pipeline.unshift({ $geoNear: arg });
    return this;
  }

  append(...ops) {
    const items = Array.isArray(ops[0]) ? ops[0] : ops;
    this._pipeline.push(...items);
    return this;
  }

  allowDiskUse(value)     { this._options.allowDiskUse = value; return this; }
  collation(opts)         { this._options.collation = opts; return this; }
  cursor(opts = {})       { this._options.cursor = opts; return this; }
  explain(verbosity)      { this._explain = verbosity || true; return this; }
  hint(value)             { this._options.hint = value; return this; }
  option(opts = {})       { Object.assign(this._options, opts); return this; }
  read(pref)              { this._options.readPreference = pref; return this; }
  readConcern(level)      { this._options.readConcern = { level }; return this; }

  then(resolve, reject)   { return this.exec().then(resolve, reject); }
  catch(reject)           { return this.exec().catch(reject); }
  finally(onFinally)      { return this.exec().finally(onFinally); }

  [Symbol.asyncIterator]() {
    let cursor;
    return {
      next: async () => {
        if (!cursor) cursor = (await this.exec())[Symbol.iterator]();
        return cursor.next();
      }
    };
  }

  // ─── Stage implementations ─────────────────────────────────────────────────
  _group(docs, grouping) {
    const groups = new Map();

    for (const doc of docs) {
      let key;
      if (grouping._id === null) {
        key = '__null__';
      } else if (typeof grouping._id === 'object' && !Array.isArray(grouping._id)) {
        key = JSON.stringify(this._evalExpr(grouping._id, doc));
      } else {
        key = this._evalExpr(grouping._id, doc);
      }

      if (!groups.has(key)) {
        const init = {};
        for (const [field, acc] of Object.entries(grouping)) {
          if (field === '_id') continue;
          init[field] = this._initAccumulator(acc);
        }
        groups.set(key, init);
      }

      const group = groups.get(key);
      for (const [field, acc] of Object.entries(grouping)) {
        if (field === '_id') continue;
        this._updateAccumulator(group, field, acc, doc);
      }
    }

    return Array.from(groups.entries()).map(([key, value]) => {
      const result = { _id: key === '__null__' ? null : key };
      for (const [field, acc] of Object.entries(grouping)) {
        if (field === '_id') continue;
        const op = Object.keys(acc)[0];
        // Finalize avg accumulator
        if (op === '$avg') {
          const v = value[field];
          result[field] = v && v._count > 0 ? v._sum / v._count : 0;
        } else if (op === '$stdDevPop' || op === '$stdDevSamp') {
          const v = value[field];
          if (!v || v._count === 0) { result[field] = null; continue; }
          const mean = v._sum / v._count;
          const variance = v._sumSq / v._count - mean * mean;
          result[field] = op === '$stdDevPop' ? Math.sqrt(Math.max(0, variance)) : Math.sqrt(Math.max(0, variance * v._count / (v._count - 1)));
        } else {
          result[field] = value[field];
        }
      }
      return result;
    });
  }

  _initAccumulator(acc) {
    const op = Object.keys(acc)[0];
    switch (op) {
      case '$sum':        return 0;
      case '$avg':        return { _sum: 0, _count: 0 };
      case '$min':        return Infinity;
      case '$max':        return -Infinity;
      case '$push':       return [];
      case '$addToSet':   return [];
      case '$first':      return undefined;
      case '$last':       return undefined;
      case '$stdDevPop':  return { _sum: 0, _sumSq: 0, _count: 0 };
      case '$stdDevSamp': return { _sum: 0, _sumSq: 0, _count: 0 };
      case '$mergeObjects': return {};
      case '$count':      return 0;
      default: return null;
    }
  }

  _updateAccumulator(group, field, spec, doc) {
    const op = Object.keys(spec)[0];
    const fieldPath = spec[op];
    const value = this._evalExpr(fieldPath, doc);

    switch (op) {
      case '$sum':
        if (typeof value === 'number') group[field] += value;
        else if (typeof fieldPath === 'number') group[field] += fieldPath;
        break;
      case '$avg':
        if (typeof value === 'number') { group[field]._sum += value; group[field]._count++; }
        break;
      case '$min':
        if (value !== undefined && value !== null) group[field] = Math.min(group[field], value);
        break;
      case '$max':
        if (value !== undefined && value !== null) group[field] = Math.max(group[field], value);
        break;
      case '$push':
        group[field].push(value);
        break;
      case '$addToSet':
        if (!group[field].some(e => JSON.stringify(e) === JSON.stringify(value))) group[field].push(value);
        break;
      case '$first':
        if (group[field] === undefined) group[field] = value;
        break;
      case '$last':
        group[field] = value;
        break;
      case '$stdDevPop': case '$stdDevSamp':
        if (typeof value === 'number') { group[field]._sum += value; group[field]._sumSq += value * value; group[field]._count++; }
        break;
      case '$mergeObjects':
        if (value && typeof value === 'object') Object.assign(group[field], value);
        break;
      case '$count':
        group[field]++;
        break;
    }
  }

  _sortDocs(docs, sorting) {
    return [...docs].sort((a, b) => {
      for (const [field, order] of Object.entries(sorting)) {
        const av = getNestedValue(a, field);
        const bv = getNestedValue(b, field);
        if (av < bv) return -order;
        if (av > bv) return order;
      }
      return 0;
    });
  }

  _unwind(docs, p) {
    const result = [];
    const fieldPath = typeof p === 'object' ? p.path.replace(/^\$/, '') : p.replace(/^\$/, '');
    const preserveNull = typeof p === 'object' && p.preserveNullAndEmptyArrays;
    const includeIdx  = typeof p === 'object' && p.includeArrayIndex;

    for (const doc of docs) {
      const array = getNestedValue(doc, fieldPath);
      if (!Array.isArray(array) || array.length === 0) {
        if (preserveNull) result.push({ ...doc });
        continue;
      }
      array.forEach((item, idx) => {
        const nd = { ...doc };
        nd[fieldPath] = item;
        if (includeIdx) nd[includeIdx] = idx;
        result.push(nd);
      });
    }
    return result;
  }

  _project(doc, projection) {
    const hasInclusion = Object.values(projection).some(v => v === 1 || (typeof v === 'object' && v !== null));
    const result = {};
    if (hasInclusion) {
      if (projection._id !== 0) result._id = doc._id;
      for (const [f, spec] of Object.entries(projection)) {
        if (spec === 1) result[f] = getNestedValue(doc, f);
        else if (spec === 0) {} // exclusion handled below
        else if (typeof spec === 'object' && spec !== null) result[f] = this._evalExpr(spec, doc);
      }
    } else {
      Object.assign(result, doc);
      for (const [f, spec] of Object.entries(projection)) {
        if (spec === 0) delete result[f];
      }
    }
    return result;
  }

  async _lookup(docs, { from, localField, foreignField, as, pipeline: pl, let: letVars }) {
    let foreignDocs = this.model._getCollection(from);
    if (!Array.isArray(foreignDocs)) foreignDocs = [];
    return docs.map(doc => {
      const localValue = getNestedValue(doc, localField);
      const matches = foreignDocs.filter(fd => {
        const fv = getNestedValue(fd, foreignField);
        if (Array.isArray(localValue)) return localValue.some(lv => String(lv) === String(fv));
        return String(getNestedValue(fd, foreignField)) === String(localValue);
      });
      return { ...doc, [as]: matches };
    });
  }

  async _merge(docs, { into, on = '_id', whenMatched = 'merge', whenNotMatched = 'insert' }) {
    const targetPath = path.join(this.model.connection.dbPath, `${into}.json`);
    const targetDocs = await readJSON(targetPath);
    for (const doc of docs) {
      const idx = targetDocs.findIndex(td => String(getNestedValue(td, on)) === String(getNestedValue(doc, on)));
      if (idx !== -1) {
        if (whenMatched === 'replace') targetDocs[idx] = doc;
        else if (whenMatched === 'merge') Object.assign(targetDocs[idx], doc);
        // 'keepExisting' → do nothing
      } else if (whenNotMatched === 'insert') {
        targetDocs.push(doc);
      }
    }
    await writeJSON(targetPath, targetDocs);
    return docs;
  }

  async _out(docs, collection) {
    const targetPath = path.join(this.model.connection.dbPath, `${collection}.json`);
    await writeJSON(targetPath, docs);
    return docs;
  }

  async _facet(docs, facets) {
    const result = {};
    for (const [name, pl] of Object.entries(facets)) {
      const facetAgg = new Aggregate(this.model, pl);
      result[name] = await facetAgg._executePipeline(docs);
    }
    return result;
  }

  async _executePipeline(inputDocs) {
    let docs = [...inputDocs];
    for (const stage of this._pipeline) {
      const op = Object.keys(stage)[0];
      const operation = stage[op];
      switch (op) {
        case '$match': docs = docs.filter(d => this.model._matchQuery(d, operation)); break;
        case '$group': docs = this._group(docs, operation); break;
        case '$sort':  docs = this._sortDocs(docs, operation); break;
        case '$limit': docs = docs.slice(0, operation); break;
        case '$skip':  docs = docs.slice(operation); break;
        case '$unwind': docs = this._unwind(docs, operation); break;
        case '$project': docs = docs.map(d => this._project(d, operation)); break;
        case '$addFields': case '$set':
          docs = docs.map(d => { const nd = { ...d }; for (const [f, v] of Object.entries(operation)) nd[f] = this._evalExpr(v, d); return nd; }); break;
        case '$unset':
          docs = docs.map(d => { const nd = { ...d }; for (const f of (Array.isArray(operation) ? operation : [operation])) delete nd[f]; return nd; }); break;
        case '$lookup':  docs = await this._lookup(docs, operation); break;
        case '$count':   docs = [{ [operation]: docs.length }]; break;
        case '$sample':  docs = this._sample(docs, operation.size); break;
        case '$replaceRoot': docs = docs.map(d => this._evalExpr(operation.newRoot, d)); break;
        default: break;
      }
    }
    return docs;
  }

  _graphLookup(docs, { from, startWith, connectFromField, connectToField, as, maxDepth = Infinity, depthField }) {
    if (!from || !startWith || !connectFromField || !connectToField || !as) throw new Error('Missing required graph lookup parameters');
    const foreignDocs = this.model._getCollection(from) || [];
    const connectMap = new Map();
    foreignDocs.forEach(doc => {
      const key = String(getNestedValue(doc, connectFromField));
      if (!connectMap.has(key)) connectMap.set(key, []);
      connectMap.get(key).push(doc);
    });

    return docs.map(doc => {
      const visited = new Set();
      const build = (d, depth = 0) => {
        if (depth > maxDepth) return [];
        const startVal = String(this._evalExpr(startWith, d));
        const connected = (connectMap.get(startVal) || []).filter(cd => {
          const key = JSON.stringify(cd);
          if (visited.has(key)) return false;
          visited.add(key);
          return String(getNestedValue(cd, connectToField)) === startVal;
        });
        return connected.map(cd => {
          const r = { ...cd };
          if (depthField) r[depthField] = depth;
          r[as] = build(cd, depth + 1);
          return r;
        });
      };
      return { ...doc, [as]: build(doc) };
    });
  }

  async _unionWith(docs, { coll, pipeline: pl }) {
    const additionalDocs = this.model._getCollection(coll) || [];
    if (pl && pl.length) {
      const unionAgg = new Aggregate(this.model, pl);
      const unionDocs = await unionAgg._executePipeline(additionalDocs);
      return [...docs, ...unionDocs];
    }
    return [...docs, ...additionalDocs];
  }

  _sortByCount(docs, field) {
    const counts = docs.reduce((acc, doc) => {
      const key = this._evalExpr(field, doc);
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {});
    return Object.entries(counts).map(([k, c]) => ({ _id: k, count: c })).sort((a, b) => b.count - a.count);
  }

  _bucket(docs, { groupBy, boundaries, default: defaultBucket, output = {} }) {
    const buckets = {};
    for (const doc of docs) {
      const v = this._evalExpr(groupBy, doc);
      let bucketKey = boundaries.findIndex(b => v < b);
      if (bucketKey === -1) { if (defaultBucket !== undefined) bucketKey = 'default'; else continue; }
      else bucketKey = bucketKey > 0 ? boundaries[bucketKey - 1] : boundaries[0];
      if (!buckets[bucketKey]) {
        buckets[bucketKey] = { _id: bucketKey, count: 0 };
        for (const [f, acc] of Object.entries(output)) buckets[bucketKey][f] = this._initAccumulator(acc);
      }
      buckets[bucketKey].count++;
      for (const [f, acc] of Object.entries(output)) this._updateAccumulator(buckets[bucketKey], f, acc, doc);
    }
    return Object.values(buckets).map(b => this._finalizeGroup(b, output));
  }

  _bucketAuto(docs, { groupBy, buckets: numBuckets, output = {} }) {
    const values = docs.map((doc, i) => ({ val: this._evalExpr(groupBy, doc), doc, i })).sort((a, b) => a.val - b.val);
    const size = Math.ceil(values.length / numBuckets);
    return Array.from({ length: numBuckets }, (_, i) => {
      const slice = values.slice(i * size, (i + 1) * size);
      if (!slice.length) return null;
      const bucket = { _id: { min: slice[0].val, max: slice[slice.length - 1].val }, count: slice.length };
      for (const [f, acc] of Object.entries(output)) {
        bucket[f] = this._initAccumulator(acc);
        slice.forEach(({ doc }) => this._updateAccumulator(bucket, f, acc, doc));
      }
      return this._finalizeGroup(bucket, output);
    }).filter(Boolean);
  }

  _finalizeGroup(group, accumulators) {
    const result = { ...group };
    for (const [f, acc] of Object.entries(accumulators)) {
      const op = Object.keys(acc)[0];
      if (op === '$avg' && result[f] && result[f]._count !== undefined) {
        result[f] = result[f]._count > 0 ? result[f]._sum / result[f]._count : 0;
      }
    }
    return result;
  }

  _densify(docs, { field, range, partitionByFields = [] }) {
    if (!field || !range) throw new Error('$densify requires "field" and "range"');
    const { step, unit, bounds } = range;
    const increment = (v) => {
      const d = new Date(v);
      if (unit === 'day' || unit === 'days')    { d.setDate(d.getDate() + step); return d; }
      if (unit === 'hour' || unit === 'hours')  { d.setHours(d.getHours() + step); return d; }
      if (unit === 'minute' || unit === 'minutes') { d.setMinutes(d.getMinutes() + step); return d; }
      return v + step;
    };
    const groups = partitionByFields.length ? this._groupByPartition(docs, partitionByFields) : { __all__: docs };
    const result = [];
    for (const group of Object.values(groups)) {
      const vals = group.map(d => d[field]).sort((a, b) => a - b);
      const start = bounds && bounds[0] !== undefined ? new Date(bounds[0]) : new Date(Math.min(...vals));
      const end   = bounds && bounds[1] !== undefined ? new Date(bounds[1]) : new Date(Math.max(...vals));
      let cur = new Date(start);
      while (cur <= end) {
        const existing = group.find(d => new Date(d[field]).getTime() === cur.getTime());
        if (existing) { result.push(existing); }
        else {
          const nd = {};
          for (const pf of partitionByFields) nd[pf] = group[0][pf];
          nd[field] = new Date(cur);
          result.push(nd);
        }
        cur = increment(cur);
      }
    }
    return result;
  }

  _fill(docs, { sortBy, output }) {
    const sorted = sortBy ? this._sortDocs(docs, sortBy) : [...docs];
    return sorted.map((doc, idx) => {
      const filled = { ...doc };
      for (const [field, method] of Object.entries(output)) {
        if (filled[field] === null || filled[field] === undefined) {
          if (method === 'linear' || (method && method.method === 'linear')) {
            const prev = sorted.slice(0, idx).reverse().find(d => d[field] != null);
            const next = sorted.slice(idx + 1).find(d => d[field] != null);
            if (prev && next) filled[field] = (prev[field] + next[field]) / 2;
          } else if (method === 'locf' || (method && method.method === 'locf')) {
            const last = sorted.slice(0, idx).reverse().find(d => d[field] != null);
            if (last) filled[field] = last[field];
          } else if (method && method.value !== undefined) {
            filled[field] = method.value;
          }
        }
      }
      return filled;
    });
  }

  _sample(docs, size) {
    const shuffled = [...docs].sort(() => Math.random() - 0.5);
    return shuffled.slice(0, size);
  }

  _setWindowFields(docs, { partitionBy, sortBy, output }) {
    const partitions = partitionBy ? this._groupByPartition(docs, Array.isArray(partitionBy) ? partitionBy : [partitionBy]) : { __all__: docs };
    const result = [];
    for (const partition of Object.values(partitions)) {
      const sorted = sortBy ? this._sortDocs(partition, sortBy) : partition;
      sorted.forEach((doc, idx) => {
        const nd = { ...doc };
        for (const [field, spec] of Object.entries(output)) {
          const op = Object.keys(spec)[0];
          const fieldExpr = spec[op];
          const window = spec.window;
          let windowDocs = sorted;
          if (window && (window.documents || window.range)) {
            const range = window.documents || window.range;
            const from = range[0] === 'unbounded' ? 0 : idx + (range[0] || 0);
            const to   = range[1] === 'unbounded' ? sorted.length - 1 : idx + (range[1] || 0);
            windowDocs = sorted.slice(Math.max(0, from), to + 1);
          }
          switch (op) {
            case '$sum':  nd[field] = windowDocs.reduce((s, d) => s + (this._evalExpr(fieldExpr, d) || 0), 0); break;
            case '$avg':  { const vs = windowDocs.map(d => this._evalExpr(fieldExpr, d)).filter(v => v != null); nd[field] = vs.length ? vs.reduce((a, b) => a + b, 0) / vs.length : null; break; }
            case '$min':  nd[field] = Math.min(...windowDocs.map(d => this._evalExpr(fieldExpr, d)).filter(v => v != null)); break;
            case '$max':  nd[field] = Math.max(...windowDocs.map(d => this._evalExpr(fieldExpr, d)).filter(v => v != null)); break;
            case '$count': nd[field] = windowDocs.length; break;
            case '$rank':  nd[field] = idx + 1; break;
            case '$denseRank': nd[field] = idx + 1; break;
            case '$documentNumber': nd[field] = idx + 1; break;
            case '$first': nd[field] = windowDocs[0] ? this._evalExpr(fieldExpr, windowDocs[0]) : null; break;
            case '$last':  nd[field] = windowDocs[windowDocs.length - 1] ? this._evalExpr(fieldExpr, windowDocs[windowDocs.length - 1]) : null; break;
            case '$shift': { const offset = spec.by || 0; const target = sorted[idx + offset]; nd[field] = target ? this._evalExpr(fieldExpr, target) : (spec.default !== undefined ? spec.default : null); break; }
          }
        }
        result.push(nd);
      });
    }
    return result;
  }

  // ─── Expression evaluator ──────────────────────────────────────────────────
  _evalExpr(expr, doc) {
    if (expr === null || expr === undefined) return expr;
    if (typeof expr === 'string' && expr.startsWith('$')) {
      if (expr.startsWith('$$')) return expr;
      return getNestedValue(doc, expr.slice(1));
    }
    if (typeof expr !== 'object' || Array.isArray(expr)) return expr;

    // Arithmetic
    if (expr.$add)      return expr.$add.reduce((s, e) => s + (this._evalExpr(e, doc) || 0), 0);
    if (expr.$subtract) { const [a, b] = expr.$subtract.map(e => this._evalExpr(e, doc)); return a - b; }
    if (expr.$multiply) return expr.$multiply.reduce((p, e) => p * (this._evalExpr(e, doc) || 1), 1);
    if (expr.$divide)   { const [a, b] = expr.$divide.map(e => this._evalExpr(e, doc)); return a / b; }
    if (expr.$mod)      { const [a, b] = expr.$mod.map(e => this._evalExpr(e, doc)); return a % b; }
    if (expr.$abs)      return Math.abs(this._evalExpr(expr.$abs, doc));
    if (expr.$ceil)     return Math.ceil(this._evalExpr(expr.$ceil, doc));
    if (expr.$floor)    return Math.floor(this._evalExpr(expr.$floor, doc));
    if (expr.$round)    { const [v, place = 0] = Array.isArray(expr.$round) ? expr.$round.map(e => this._evalExpr(e, doc)) : [this._evalExpr(expr.$round, doc), 0]; return Math.round(v * Math.pow(10, place)) / Math.pow(10, place); }
    if (expr.$sqrt)     return Math.sqrt(this._evalExpr(expr.$sqrt, doc));
    if (expr.$pow)      { const [b, exp] = expr.$pow.map(e => this._evalExpr(e, doc)); return Math.pow(b, exp); }
    if (expr.$log)      { const [n, base] = expr.$log.map(e => this._evalExpr(e, doc)); return Math.log(n) / Math.log(base); }
    if (expr.$ln)       return Math.log(this._evalExpr(expr.$ln, doc));
    if (expr.$exp)      return Math.exp(this._evalExpr(expr.$exp, doc));
    if (expr.$trunc)    return Math.trunc(this._evalExpr(expr.$trunc, doc));

    // Arrays
    if (expr.$size)     { const arr = this._evalExpr(expr.$size, doc); return Array.isArray(arr) ? arr.length : 0; }
    if (expr.$sum)      { if (typeof expr.$sum === 'number') return expr.$sum; const arr = this._evalExpr(expr.$sum, doc); return Array.isArray(arr) ? arr.reduce((a, b) => a + (b || 0), 0) : (arr || 0); }
    if (expr.$avg)      { const arr = this._evalExpr(expr.$avg, doc); if (!Array.isArray(arr) || !arr.length) return null; return arr.reduce((a, b) => a + b, 0) / arr.length; }
    if (expr.$min)      { const arr = this._evalExpr(expr.$min, doc); return Array.isArray(arr) ? Math.min(...arr) : arr; }
    if (expr.$max)      { const arr = this._evalExpr(expr.$max, doc); return Array.isArray(arr) ? Math.max(...arr) : arr; }
    if (expr.$first)    { const arr = this._evalExpr(expr.$first, doc); return Array.isArray(arr) ? arr[0] : arr; }
    if (expr.$last)     { const arr = this._evalExpr(expr.$last, doc); return Array.isArray(arr) ? arr[arr.length - 1] : arr; }
    if (expr.$push)     return [this._evalExpr(expr.$push, doc)];
    if (expr.$concatArrays) return expr.$concatArrays.reduce((acc, e) => { const a = this._evalExpr(e, doc); return acc.concat(Array.isArray(a) ? a : []); }, []);
    if (expr.$filter)   { const { input, as: alias, cond } = expr.$filter; const arr = this._evalExpr(input, doc); if (!Array.isArray(arr)) return []; return arr.filter(item => this._evalExpr(cond, { ...doc, [`$${alias || 'this'}`]: item, [alias || 'this']: item })); }
    if (expr.$map)      { const { input, as: alias, in: inExpr } = expr.$map; const arr = this._evalExpr(input, doc); if (!Array.isArray(arr)) return []; return arr.map(item => this._evalExpr(inExpr, { ...doc, [alias || 'this']: item })); }
    if (expr.$reduce)   { const { input, initialValue, in: inExpr } = expr.$reduce; const arr = this._evalExpr(input, doc) || []; return arr.reduce((acc, el) => this._evalExpr(inExpr, { ...doc, value: acc, this: el }), this._evalExpr(initialValue, doc)); }
    if (expr.$arrayElemAt) { const [arr, idx] = expr.$arrayElemAt.map(e => this._evalExpr(e, doc)); return Array.isArray(arr) ? arr[idx < 0 ? arr.length + idx : idx] : null; }
    if (expr.$indexOfArray) { const [arr, item] = expr.$indexOfArray.map(e => this._evalExpr(e, doc)); return Array.isArray(arr) ? arr.findIndex(e => JSON.stringify(e) === JSON.stringify(item)) : -1; }
    if (expr.$in)       { const [item, arr] = expr.$in.map(e => this._evalExpr(e, doc)); return Array.isArray(arr) && arr.includes(item); }
    if (expr.$slice)    { const [arr, ...args] = expr.$slice.map(e => this._evalExpr(e, doc)); return Array.isArray(arr) ? (args.length === 2 ? arr.slice(args[0], args[0] + args[1]) : arr.slice(0, args[0])) : []; }
    if (expr.$range)    { const [start, end, step = 1] = expr.$range.map(e => this._evalExpr(e, doc)); const r = []; for (let i = start; step > 0 ? i < end : i > end; i += step) r.push(i); return r; }
    if (expr.$reverseArray) { const arr = this._evalExpr(expr.$reverseArray, doc); return Array.isArray(arr) ? [...arr].reverse() : []; }
    if (expr.$zip)      { const inputs = (expr.$zip.inputs || []).map(e => this._evalExpr(e, doc)); const len = Math.max(...inputs.map(a => a.length)); return Array.from({ length: len }, (_, i) => inputs.map(a => a[i])); }
    if (expr.$setUnion)      return [...new Set(expr.$setUnion.flatMap(e => this._evalExpr(e, doc) || []))];
    if (expr.$setIntersection) { const sets = expr.$setIntersection.map(e => this._evalExpr(e, doc) || []); return sets[0].filter(v => sets.every(s => s.includes(v))); }
    if (expr.$setDifference)   { const [a, b] = expr.$setDifference.map(e => this._evalExpr(e, doc) || []); return a.filter(v => !b.includes(v)); }
    if (expr.$setIsSubset)     { const [a, b] = expr.$setIsSubset.map(e => this._evalExpr(e, doc) || []); return a.every(v => b.includes(v)); }

    // Strings
    if (expr.$concat)      return expr.$concat.map(e => String(this._evalExpr(e, doc) ?? '')).join('');
    if (expr.$toUpper)     return String(this._evalExpr(expr.$toUpper, doc) ?? '').toUpperCase();
    if (expr.$toLower)     return String(this._evalExpr(expr.$toLower, doc) ?? '').toLowerCase();
    if (expr.$trim)        { const v = String(this._evalExpr(expr.$trim.input || expr.$trim, doc) ?? ''); return v.trim(); }
    if (expr.$ltrim)       return String(this._evalExpr(expr.$ltrim.input || expr.$ltrim, doc) ?? '').trimStart();
    if (expr.$rtrim)       return String(this._evalExpr(expr.$rtrim.input || expr.$rtrim, doc) ?? '').trimEnd();
    if (expr.$substr || expr.$substrBytes) { const [s, start, len] = (expr.$substr || expr.$substrBytes).map(e => this._evalExpr(e, doc)); return String(s ?? '').substring(start, start + len); }
    if (expr.$substrCP)    { const [s, start, len] = expr.$substrCP.map(e => this._evalExpr(e, doc)); return [...String(s ?? '')].slice(start, start + len).join(''); }
    if (expr.$strLenBytes) return Buffer.byteLength(String(this._evalExpr(expr.$strLenBytes, doc) ?? ''));
    if (expr.$strLenCP)    return [...String(this._evalExpr(expr.$strLenCP, doc) ?? '')].length;
    if (expr.$split)       { const [s, delim] = expr.$split.map(e => this._evalExpr(e, doc)); return String(s ?? '').split(delim); }
    if (expr.$indexOfBytes || expr.$indexOfCP) { const [s, search] = (expr.$indexOfBytes || expr.$indexOfCP).map(e => this._evalExpr(e, doc)); return String(s ?? '').indexOf(search); }
    if (expr.$regexFind)   { const { input, regex, options } = expr.$regexFind; const s = this._evalExpr(input, doc); const rgx = new RegExp(regex, options); const m = String(s ?? '').match(rgx); return m ? { match: m[0], idx: m.index, captures: m.slice(1) } : null; }
    if (expr.$regexFindAll) { const { input, regex, options } = expr.$regexFindAll; const s = String(this._evalExpr(input, doc) ?? ''); const rgx = new RegExp(regex, (options || '') + 'g'); return [...s.matchAll(rgx)].map(m => ({ match: m[0], idx: m.index, captures: m.slice(1) })); }
    if (expr.$regexMatch)   { const { input, regex, options } = expr.$regexMatch; const s = this._evalExpr(input, doc); return new RegExp(regex, options).test(String(s ?? '')); }
    if (expr.$replaceOne || expr.$replaceAll) {
      const { input, find, replacement } = expr.$replaceOne || expr.$replaceAll;
      const s = String(this._evalExpr(input, doc) ?? '');
      const f = String(this._evalExpr(find, doc) ?? '');
      const r = String(this._evalExpr(replacement, doc) ?? '');
      return expr.$replaceAll ? s.split(f).join(r) : s.replace(f, r);
    }

    // Dates
    if (expr.$year)        { const d = new Date(this._evalExpr(expr.$year, doc)); return d.getFullYear(); }
    if (expr.$month)       { const d = new Date(this._evalExpr(expr.$month, doc)); return d.getMonth() + 1; }
    if (expr.$dayOfMonth)  { const d = new Date(this._evalExpr(expr.$dayOfMonth, doc)); return d.getDate(); }
    if (expr.$dayOfWeek)   { const d = new Date(this._evalExpr(expr.$dayOfWeek, doc)); return d.getDay() + 1; }
    if (expr.$dayOfYear)   { const d = new Date(this._evalExpr(expr.$dayOfYear, doc)); return Math.ceil((d - new Date(d.getFullYear(), 0, 1)) / 86400000) + 1; }
    if (expr.$hour)        { const d = new Date(this._evalExpr(expr.$hour, doc)); return d.getHours(); }
    if (expr.$minute)      { const d = new Date(this._evalExpr(expr.$minute, doc)); return d.getMinutes(); }
    if (expr.$second)      { const d = new Date(this._evalExpr(expr.$second, doc)); return d.getSeconds(); }
    if (expr.$millisecond) { const d = new Date(this._evalExpr(expr.$millisecond, doc)); return d.getMilliseconds(); }
    if (expr.$dateToString) { const { format, date } = expr.$dateToString; const d = new Date(this._evalExpr(date, doc)); return d.toISOString(); }
    if (expr.$toDate)      return new Date(this._evalExpr(expr.$toDate, doc));
    if (expr.$dateAdd)     { const { startDate, unit, amount } = expr.$dateAdd; const d = new Date(this._evalExpr(startDate, doc)); const amt = this._evalExpr(amount, doc); if (unit === 'day') d.setDate(d.getDate() + amt); else if (unit === 'hour') d.setHours(d.getHours() + amt); else if (unit === 'minute') d.setMinutes(d.getMinutes() + amt); else if (unit === 'second') d.setSeconds(d.getSeconds() + amt); return d; }
    if (expr.$dateDiff)    { const { startDate, endDate, unit } = expr.$dateDiff; const start = new Date(this._evalExpr(startDate, doc)); const end = new Date(this._evalExpr(endDate, doc)); const diff = end - start; if (unit === 'day') return Math.floor(diff / 86400000); if (unit === 'hour') return Math.floor(diff / 3600000); if (unit === 'minute') return Math.floor(diff / 60000); return diff; }

    // Comparison
    if (expr.$eq)  { const [a, b] = expr.$eq.map(e => this._evalExpr(e, doc));  return a === b; }
    if (expr.$ne)  { const [a, b] = expr.$ne.map(e => this._evalExpr(e, doc));  return a !== b; }
    if (expr.$gt)  { const [a, b] = expr.$gt.map(e => this._evalExpr(e, doc));  return a > b; }
    if (expr.$gte) { const [a, b] = expr.$gte.map(e => this._evalExpr(e, doc)); return a >= b; }
    if (expr.$lt)  { const [a, b] = expr.$lt.map(e => this._evalExpr(e, doc));  return a < b; }
    if (expr.$lte) { const [a, b] = expr.$lte.map(e => this._evalExpr(e, doc)); return a <= b; }
    if (expr.$cmp) { const [a, b] = expr.$cmp.map(e => this._evalExpr(e, doc)); return a > b ? 1 : a < b ? -1 : 0; }

    // Logical
    if (expr.$and)  return expr.$and.every(e => this._evalExpr(e, doc));
    if (expr.$or)   return expr.$or.some(e => this._evalExpr(e, doc));
    if (expr.$not)  { const v = Array.isArray(expr.$not) ? expr.$not[0] : expr.$not; return !this._evalExpr(v, doc); }
    if (expr.$cond) {
      const cond = expr.$cond;
      const test = Array.isArray(cond) ? this._evalExpr(cond[0], doc) : this._evalExpr(cond.if, doc);
      return test ? this._evalExpr(Array.isArray(cond) ? cond[1] : cond.then, doc) : this._evalExpr(Array.isArray(cond) ? cond[2] : cond.else, doc);
    }
    if (expr.$switch) {
      for (const branch of expr.$switch.branches) {
        if (this._evalExpr(branch.case, doc)) return this._evalExpr(branch.then, doc);
      }
      return expr.$switch.default !== undefined ? this._evalExpr(expr.$switch.default, doc) : null;
    }
    if (expr.$ifNull) { for (const e of expr.$ifNull) { const v = this._evalExpr(e, doc); if (v !== null && v !== undefined) return v; } return null; }

    // Type conversion
    if (expr.$toString)  return String(this._evalExpr(expr.$toString, doc) ?? '');
    if (expr.$toInt)     return parseInt(this._evalExpr(expr.$toInt, doc));
    if (expr.$toLong)    return parseInt(this._evalExpr(expr.$toLong, doc));
    if (expr.$toDouble)  return parseFloat(this._evalExpr(expr.$toDouble, doc));
    if (expr.$toBool)    return Boolean(this._evalExpr(expr.$toBool, doc));
    if (expr.$toDecimal) return parseFloat(this._evalExpr(expr.$toDecimal, doc));
    if (expr.$type)      return typeof this._evalExpr(expr.$type, doc);
    if (expr.$convert)   { try { return this._evalExpr({ [`$to${expr.$convert.to.charAt(0).toUpperCase() + expr.$convert.to.slice(1)}`]: expr.$convert.input }, doc); } catch { return expr.$convert.onError || null; } }

    // Miscellaneous
    if (expr.$literal)   return expr.$literal;
    if (expr.$objectToArray) { const obj = this._evalExpr(expr.$objectToArray, doc); return obj ? Object.entries(obj).map(([k, v]) => ({ k, v })) : []; }
    if (expr.$arrayToObject) { const arr = this._evalExpr(expr.$arrayToObject, doc); if (!Array.isArray(arr)) return {}; return arr.reduce((o, item) => { if (Array.isArray(item)) o[item[0]] = item[1]; else if (item.k !== undefined) o[item.k] = item.v; return o; }, {}); }
    if (expr.$mergeObjects) { const objs = (Array.isArray(expr.$mergeObjects) ? expr.$mergeObjects : [expr.$mergeObjects]).map(e => this._evalExpr(e, doc)); return Object.assign({}, ...objs.filter(Boolean)); }
    if (expr.$getField)  { const { field: f, input } = expr.$getField; const obj = input ? this._evalExpr(input, doc) : doc; return obj ? obj[f] : undefined; }
    if (expr.$setField)  { const { field: f, input, value: v } = expr.$setField; const obj = { ...(this._evalExpr(input, doc) || {}) }; obj[f] = this._evalExpr(v, doc); return obj; }
    if (expr.$unsetField) { const { field: f, input } = expr.$unsetField; const obj = { ...(this._evalExpr(input, doc) || {}) }; delete obj[f]; return obj; }
    if (expr.$rand)      return Math.random();
    if (expr.$sampleRate) return Math.random() < expr.$sampleRate;
    if (expr.$let)       { const vars = Object.fromEntries(Object.entries(expr.$let.vars).map(([k, v]) => [k, this._evalExpr(v, doc)])); return this._evalExpr(expr.$let.in, { ...doc, ...vars }); }

    return expr;
  }

  // ─── Utilities ─────────────────────────────────────────────────────────────
  _groupByPartition(docs, fields) {
    const groups = {};
    for (const doc of docs) {
      const key = JSON.stringify(fields.map(f => doc[f]));
      if (!groups[key]) groups[key] = [];
      groups[key].push(doc);
    }
    return groups;
  }

  async _readCollectionData(collectionName) {
    try { return this.model._getCollection(collectionName) || []; }
    catch { return []; }
  }

  _redact(doc, expression) {
    const result = this._evalExpr(expression, doc);
    if (result === '$$PRUNE') return null;
    if (result === '$$KEEP' || result === '$$DESCEND') return doc;
    // Default to doc if result is unknown
    return doc;
  }

  _geoNear(docs, options) {
    const { near, distanceField, maxDistance, query, minDistance, key, includeLocs } = options;
    if (!distanceField) throw new Error('$geoNear requires "distanceField" option');
    
    const nearCoords = extractCoordinates(near.$geometry || near);
    if (!nearCoords) throw new Error('$geoNear requires valid "near" coordinates');

    let filtered = docs;
    if (query) {
      filtered = docs.filter(doc => this.model._matchQuery(doc, query));
    }

    const results = filtered.map(doc => {
      const fieldPath = key || this._findCoordinateField(doc);
      const docCoords = extractCoordinates(getNestedValue(doc, fieldPath));
      
      if (!docCoords) return null;

      const distance = geolib.getDistance(
        { latitude: docCoords[1], longitude: docCoords[0] },
        { latitude: nearCoords[1], longitude: nearCoords[0] }
      );

      if (maxDistance !== undefined && distance > maxDistance) return null;
      if (minDistance !== undefined && distance < minDistance) return null;

      const newDoc = { ...doc, [distanceField]: distance };
      if (includeLocs) newDoc[includeLocs] = docCoords;
      return newDoc;
    }).filter(Boolean);

    // geoNear always sorts by distance
    return results.sort((a, b) => a[distanceField] - b[distanceField]);
  }

  _findCoordinateField(doc) {
    // Basic heuristic: find a field that looks like coordinates
    for (const [key, val] of Object.entries(doc)) {
      if (extractCoordinates(val)) return key;
    }
    return 'location'; // default fallback
  }
}

module.exports = { Aggregate };