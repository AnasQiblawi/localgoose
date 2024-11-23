import { validateType } from './utils.js';
import { QueryBuilder } from './QueryBuilder.js';
import { Document } from './Document.js';

export class Query {
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
  }

  where(path) {
    return new QueryBuilder(this, path);
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

  skip(n) {
    this._skip = n;
    return this;
  }

  limit(n) {
    this._limit = n;
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

  async exec() {
    let docs = await this.model._find(this.conditions);
    
    // Apply sort
    if (Object.keys(this._sort).length > 0) {
      docs.sort((a, b) => {
        for (const [field, order] of Object.entries(this._sort)) {
          if (a[field] < b[field]) return -1 * order;
          if (a[field] > b[field]) return 1 * order;
        }
        return 0;
      });
    }

    // Apply skip and limit
    if (this._skip) {
      docs = docs.slice(this._skip);
    }
    if (this._limit) {
      docs = docs.slice(0, this._limit);
    }

    // Convert to Documents and handle population
    const documents = docs.map(doc => new Document(doc, this.model.schema, this.model));
    
    if (this._populate.length > 0) {
      return Promise.all(documents.map(doc => this._populateDoc(doc)));
    }

    return documents;
  }

  // Rest of the Query class methods remain the same...
  box(path, box) {
    this._geometry = { type: 'box', path, coordinates: box };
    return this;
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

  geometry(path, geometry) {
    this._geometry = { type: 'geometry', path, coordinates: geometry };
    return this;
  }

  polygon(path, coordinates) {
    this._geometry = { type: 'polygon', path, coordinates };
    return this;
  }

  allowDiskUse(allow = true) {
    this._options.allowDiskUse = allow;
    return this;
  }

  batchSize(size) {
    this._batchSize = size;
    return this;
  }

  collation(collation) {
    this._options.collation = collation;
    return this;
  }

  hint(index) {
    this._hint = index;
    return this;
  }

  explain(explain = true) {
    this._explain = explain;
    return this;
  }

  all(path, values) {
    this.conditions[path] = { $all: values };
    return this;
  }

  and(conditions) {
    if (!this.conditions.$and) {
      this.conditions.$and = [];
    }
    this.conditions.$and.push(...conditions);
    return this;
  }

  elemMatch(path, criteria) {
    this.conditions[path] = { $elemMatch: criteria };
    return this;
  }

  mod(path, divisor, remainder) {
    this.conditions[path] = { $mod: [divisor, remainder] };
    return this;
  }

  nor(conditions) {
    if (!this.conditions.$nor) {
      this.conditions.$nor = [];
    }
    this.conditions.$nor.push(...conditions);
    return this;
  }

  orFail(error) {
    this._error = error || new Error('No document found');
    return this;
  }

  projection(fields) {
    this._fields = fields;
    return this;
  }

  slice(path, val) {
    this._fields[path] = { $slice: val };
    return this;
  }

  transform(fn) {
    this._transform = fn;
    return this;
  }

  within() {
    return this;
  }
}