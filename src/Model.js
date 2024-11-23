import { readJSON, writeJSON } from './utils.js';
import { ObjectId } from 'bson';
import path from 'path';
import { Query } from './Query.js';
import { Aggregate } from './Aggregate.js';
import { Document } from './Document.js';
import { EventEmitter } from 'events';
import fs from 'fs-extra';

export class Model {
  constructor(name, schema, connection) {
    this.name = name;
    this.schema = schema;
    this.connection = connection;
    this.collectionPath = path.join(connection.dbPath, `${name}.json`);
    this.collection = {
      name: this.name,
      collectionPath: this.collectionPath,
      async find(conditions = {}) {
        return readJSON(this.collectionPath);
      }
    };
    this.base = connection;
    this.db = connection;
    this.discriminators = null;
    this.events = new EventEmitter();
    this.modelName = name;
    this.baseModelName = null;
    this._indexes = new Map();
    
    this._initializeCollection();
    
    Object.entries(schema.statics).forEach(([name, fn]) => {
      this[name] = fn.bind(this);
    });

    Object.entries(schema.methods).forEach(([name, fn]) => {
      this[name] = fn;
    });
  }

  async _createOne(data) {
    const defaultedData = { ...data };
    
    for (const [field, schema] of Object.entries(this.schema.definition)) {
      if (defaultedData[field] === undefined && schema.default !== undefined) {
        defaultedData[field] = typeof schema.default === 'function' ? 
          schema.default() : schema.default;
      }
      
      if (schema.type === Date && typeof defaultedData[field] === 'string') {
        defaultedData[field] = new Date(defaultedData[field]);
      }
    }

    const errors = this.schema.validate(defaultedData);
    if (errors.length > 0) {
      throw new Error(errors.join(', '));
    }

    if (this.schema.middleware.pre.save) {
      for (const middleware of this.schema.middleware.pre.save) {
        await middleware.call(defaultedData);
      }
    }

    const docs = await readJSON(this.collectionPath);
    const now = new Date();
    const newDoc = { 
      _id: new ObjectId().toString(), 
      ...defaultedData,
      createdAt: now,
      updatedAt: now
    };

    docs.push(newDoc);
    await writeJSON(this.collectionPath, docs);

    if (this.schema.middleware.post.save) {
      for (const middleware of this.schema.middleware.post.save) {
        await middleware.call(newDoc);
      }
    }

    return new Document(newDoc, this.schema, this);
  }

  async _find(conditions = {}) {
    const docs = await readJSON(this.collectionPath);
    return docs.filter(doc => this._matchQuery(doc, conditions));
  }

  _matchQuery(doc, query) {
    return Object.entries(query).every(([key, value]) => {
      if (value && typeof value === 'object') {
        return Object.entries(value).every(([operator, operand]) => {
          switch (operator) {
            case '$gt': return doc[key] > operand;
            case '$gte': return doc[key] >= operand;
            case '$lt': return doc[key] < operand;
            case '$lte': return doc[key] <= operand;
            case '$ne': return doc[key] !== operand;
            case '$in': 
              const docValue = Array.isArray(doc[key]) ? doc[key] : [doc[key]];
              return operand.some(item => docValue.includes(item));
            case '$nin':
              const docVal = Array.isArray(doc[key]) ? doc[key] : [doc[key]];
              return !operand.some(item => docVal.includes(item));
            case '$regex':
              const regex = new RegExp(operand, value.$options);
              return regex.test(doc[key]);
            default:
              return false;
          }
        });
      }
      return doc[key] === value;
    });
  }

  async _initializeCollection() {
    try {
      await readJSON(this.collectionPath);
    } catch (error) {
      if (error.code === 'ENOENT') {
        await writeJSON(this.collectionPath, []);
      }
    }
  }

  find(conditions = {}) {
    return new Query(this, conditions);
  }

  async create(data) {
    if (Array.isArray(data)) {
      return Promise.all(data.map(item => this._createOne(item)));
    }
    return this._createOne(data);
  }

  async findOne(conditions = {}) {
    const docs = await this._find(conditions);
    return docs[0] ? new Document(docs[0], this.schema, this) : null;
  }

  async updateOne(conditions, update) {
    const docs = await readJSON(this.collectionPath);
    const index = docs.findIndex(doc => this._matchQuery(doc, conditions));
    if (index !== -1) {
      docs[index] = { ...docs[index], ...update, updatedAt: new Date() };
      await writeJSON(this.collectionPath, docs);
      return { modifiedCount: 1, upsertedCount: 0 };
    }
    return { modifiedCount: 0, upsertedCount: 0 };
  }

  async deleteMany(conditions = {}) {
    const docs = await readJSON(this.collectionPath);
    const remaining = docs.filter(doc => !this._matchQuery(doc, conditions));
    await writeJSON(this.collectionPath, remaining);
    return { deletedCount: docs.length - remaining.length };
  }

  aggregate(pipeline = []) {
    return new Aggregate(this, pipeline);
  }
}