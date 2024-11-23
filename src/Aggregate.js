export class Aggregate {
  constructor(model, pipeline = []) {
    this.model = model;
    this.pipeline = [...pipeline];
    this._explain = false;
  }

  match(criteria) {
    this.pipeline.push({ $match: criteria });
    return this;
  }

  group(grouping) {
    this.pipeline.push({ $group: grouping });
    return this;
  }

  sort(sorting) {
    this.pipeline.push({ $sort: sorting });
    return this;
  }

  limit(n) {
    this.pipeline.push({ $limit: n });
    return this;
  }

  skip(n) {
    this.pipeline.push({ $skip: n });
    return this;
  }

  unwind(path) {
    this.pipeline.push({ $unwind: path });
    return this;
  }

  async exec() {
    let docs = await this.model._find();
    
    for (const stage of this.pipeline) {
      const operator = Object.keys(stage)[0];
      const operation = stage[operator];
      
      switch (operator) {
        case '$match':
          docs = docs.filter(doc => this.model._matchQuery(doc, operation));
          break;
          
        case '$group':
          docs = this._group(docs, operation);
          break;
          
        case '$sort':
          docs = this._sort(docs, operation);
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
      }
    }
    
    return docs;
  }

  _group(docs, grouping) {
    const groups = new Map();
    
    for (const doc of docs) {
      const key = grouping._id === null ? 'null' : 
        this._evaluateExpression(grouping._id, doc);
      
      if (!groups.has(key)) {
        const group = {};
        for (const [field, accumulator] of Object.entries(grouping)) {
          if (field === '_id') continue;
          group[field] = this._initializeAccumulator(accumulator);
        }
        groups.set(key, group);
      }
      
      const group = groups.get(key);
      for (const [field, accumulator] of Object.entries(grouping)) {
        if (field === '_id') continue;
        this._updateAccumulator(group, field, accumulator, doc);
      }
    }
    
    return Array.from(groups.entries()).map(([key, value]) => ({
      _id: key === 'null' ? null : key,
      ...value
    }));
  }

  _sort(docs, sorting) {
    return [...docs].sort((a, b) => {
      for (const [field, order] of Object.entries(sorting)) {
        const aVal = this._getFieldValue(a, field);
        const bVal = this._getFieldValue(b, field);
        if (aVal < bVal) return -1 * order;
        if (aVal > bVal) return 1 * order;
      }
      return 0;
    });
  }

  _unwind(docs, path) {
    const result = [];
    const fieldPath = path.startsWith('$') ? path.slice(1) : path;
    
    for (const doc of docs) {
      const array = this._getFieldValue(doc, fieldPath);
      if (!Array.isArray(array)) {
        result.push(doc);
        continue;
      }
      
      for (const item of array) {
        const newDoc = { ...doc };
        this._setFieldValue(newDoc, fieldPath, item);
        result.push(newDoc);
      }
    }
    
    return result;
  }

  _getFieldValue(doc, path) {
    return path.split('.').reduce((obj, key) => obj && obj[key], doc);
  }

  _setFieldValue(doc, path, value) {
    const parts = path.split('.');
    const last = parts.pop();
    const target = parts.reduce((obj, key) => obj[key], doc);
    target[last] = value;
  }

  _evaluateExpression(expr, doc) {
    if (typeof expr === 'string' && expr.startsWith('$')) {
      return this._getFieldValue(doc, expr.slice(1));
    }
    return expr;
  }

  _initializeAccumulator(accumulator) {
    const operator = Object.keys(accumulator)[0];
    switch (operator) {
      case '$sum': return 0;
      case '$avg': return { sum: 0, count: 0 };
      case '$min': return Infinity;
      case '$max': return -Infinity;
      default: return null;
    }
  }

  _updateAccumulator(group, field, spec, doc) {
    const operator = Object.keys(spec)[0];
    const fieldPath = spec[operator];
    const value = this._evaluateExpression(fieldPath, doc);
    
    switch (operator) {
      case '$sum':
        group[field] += value;
        break;
      case '$avg':
        group[field].sum += value;
        group[field].count++;
        group[field].value = group[field].sum / group[field].count;
        break;
      case '$min':
        group[field] = Math.min(group[field], value);
        break;
      case '$max':
        group[field] = Math.max(group[field], value);
        break;
    }
  }
}