class Aggregate {
  constructor(model, pipeline = []) {
    if (!model) {
      throw new Error('Model is required for aggregation');
    }
    this.model = model;
    this.pipeline = [...pipeline];
    this._explain = false;
    this._validatePipeline();
  }

  _validatePipeline() {
    const validStages = ['$match', '$group', '$sort', '$limit', '$skip', '$unwind', 
      '$project', '$lookup', '$addFields', '$densify', 
      '$facet', '$graphLookup', '$unionWith', '$sortByCount'];

    
    this.pipeline.forEach(stage => {
      const operator = Object.keys(stage)[0];
      if (!validStages.includes(operator)) {
        throw new Error(`Invalid pipeline stage: ${operator}`);
      }
    });
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

  project(projection) {
    this.pipeline.push({ $project: projection });
    return this;
  }

  lookup(lookupOptions) {
    this.pipeline.push({ $lookup: lookupOptions });
    return this;
  }

  addFields(fields) {
    this.pipeline.push({ $addFields: fields });
    return this;
  }

  densify(densifyOptions) {
    this.pipeline.push({ $densify: densifyOptions });
    return this;
  }

  facet(facets) {
    this.pipeline.push({ $facet: facets });
    return this;
  }

  graphLookup(options) {
    this.pipeline.push({ $graphLookup: options });
    return this;
  }

  unionWith(collection, pipeline = []) {
    this.pipeline.push({ $unionWith: { coll: collection, pipeline } });
    return this;
  }

  sortByCount(field) {
    this.pipeline.push({ $sortByCount: field });
    return this;
  }


  async exec() {
    if (!this.model._find) {
      throw new Error('_find method is not implemented in the model');
    }
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
        
        case '$project':
          docs = docs.map(doc => {
            const projectedDoc = {};
            for (const [field, spec] of Object.entries(operation)) {
              if (typeof spec === 'number') {
                if (spec === 1) {
                  projectedDoc[field] = this._getFieldValue(doc, field);
                }
              } else if (typeof spec === 'object') {
                projectedDoc[field] = this._evaluateExpression(spec, doc);
              }
            }
            return projectedDoc;
          });
          break;
        
        case '$lookup':
          docs = docs.map(doc => {
            const { from, localField, foreignField, as } = operation;
            
            // Assume we have access to another model/collection
            const foreignDocs = this.model._getCollection(from);
            
            doc[as] = foreignDocs.filter(foreignDoc =>
              foreignDoc[foreignField] === doc[localField]
            );

            return doc;
          });
          break;

        case '$addFields':
          docs = docs.map(doc => {
            const newDoc = { ...doc };
            for (const [field, value] of Object.entries(operation)) {
              newDoc[field] = this._evaluateExpression(value, doc);
            }
            return newDoc;
          });
          break;

        case '$densify':
          // Handle densify (requires additional logic to generate gaps and fill them)
          docs = this._densify(docs, operation);
          break;

        case '$facet':
          docs = [this._facet(docs, operation)];
          break;

        case '$graphLookup':
          docs = this._graphLookup(docs, operation);
          break;

        case '$unionWith':
          docs = this._unionWith(docs, operation);
          break;

        case '$sortByCount':
          docs = this._sortByCount(docs, operation);
          break;
      }
    }

    return docs;
  }

  _group(docs, grouping) {
    const groups = new Map();
    
    for (const doc of docs) {
      let key;
      if (grouping._id === null) {
        key = 'null';
      } else if (typeof grouping._id === 'object' && !Array.isArray(grouping._id)) {
        key = JSON.stringify(this._evaluateExpression(grouping._id, doc));
      } else {
        key = this._evaluateExpression(grouping._id, doc);
      }
      
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
        if (aVal < bVal) return -order;
        if (aVal > bVal) return order;
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
    
    if (typeof expr === 'object') {
      // Size operator
      if (expr.$size) {
        const array = this._evaluateExpression(expr.$size, doc);
        return Array.isArray(array) ? array.length : 0;
      }
  
      // Sum operator
      if (expr.$sum) {
        if (Array.isArray(expr.$sum)) {
          return expr.$sum.reduce((sum, val) => 
            sum + this._evaluateExpression(val, doc), 0);
        }
        const array = this._evaluateExpression(expr.$sum, doc);
        return Array.isArray(array) ? array.reduce((a, b) => a + (b || 0), 0) : 0;
      }
      
      // Arithmetic expressions
      if (expr.$add) {
        return expr.$add.reduce((sum, val) => 
          sum + this._evaluateExpression(val, doc), 0);
      }
      if (expr.$multiply) {
        return expr.$multiply.reduce((product, val) => 
          product * this._evaluateExpression(val, doc), 1);
      }
      
      // Comparison operators
      if (expr.$eq) return this._evaluateExpression(expr.$eq[0], doc) === this._evaluateExpression(expr.$eq[1], doc);
      if (expr.$gt) return this._evaluateExpression(expr.$gt[0], doc) > this._evaluateExpression(expr.$gt[1], doc);
      
      // Logical operators
      if (expr.$and) return expr.$and.every(condition => this._evaluateExpression(condition, doc));
      if (expr.$or) return expr.$or.some(condition => this._evaluateExpression(condition, doc));
      
      // Date operators
      if (expr.$year) {
        const date = this._evaluateExpression(expr.$year, doc);
        return new Date(date).getFullYear();
      }
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
      case '$push': return [];
      case '$first': return null;
      case '$last': return null;
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
      case '$push':
        group[field].push(value);
        break;
      case '$first':
        if (group[field] === null) {
          group[field] = value;
        }
        break;
      case '$last':
        group[field] = value;
        break;
    }
  }

  _densify(docs, options) {
    const { field, range, partitionByFields = [] } = options;
  
    if (!range || !field) {
      throw new Error('$densify requires both a "field" and a "range" property.');
    }
  
    const { step, unit, bounds } = range;
  
    // Helper function to increment based on unit
    const incrementValue = (value) => {
      switch (unit) {
        case 'days':
          return new Date(value.setDate(value.getDate() + step));
        case 'hours':
          return new Date(value.setHours(value.getHours() + step));
        case 'minutes':
          return new Date(value.setMinutes(value.getMinutes() + step));
        default:
          return value + step;
      }
    };
  
    const groups = partitionByFields.length
      ? this._groupByPartition(docs, partitionByFields)
      : { global: docs };
  
    const result = [];
  
    for (const group of Object.values(groups)) {
      const values = group.map(doc => doc[field]).sort((a, b) => a - b);
  
      const start = bounds && bounds[0] !== undefined 
        ? new Date(bounds[0]) 
        : new Date(Math.min(...values));
      const end = bounds && bounds[1] !== undefined 
        ? new Date(bounds[1]) 
        : new Date(Math.max(...values));
  
      let current = new Date(start);
      while (current <= end) {
        const existingDoc = group.find(doc => {
          const docDate = new Date(doc[field]);
          return docDate.getTime() === current.getTime();
        });
  
        if (existingDoc) {
          result.push(existingDoc);
        } else {
          const newDoc = {};
          for (const partitionField of partitionByFields) {
            newDoc[partitionField] = group[0][partitionField];
          }
          newDoc[field] = new Date(current);
          result.push(newDoc);
        }
  
        current = incrementValue(current);
      }
    }
  
    return result;
  }

  _facet(docs, facets) {
    const result = {};
    for (const [name, pipeline] of Object.entries(facets)) {
      const facetAgg = new Aggregate(this.model, pipeline);
      // Use a synchronous version of the pipeline execution
      result[name] = this._executePipelineSync(docs, pipeline);
    }
    return result;
  }
  
  _executePipelineSync(docs, pipeline) {
    let result = [...docs];
    for (const stage of pipeline) {
      // Apply each stage synchronously
      result = this._applyStageSync(result, stage);
    }
    return result;
  }

  async _applyStageSync(docs, stage) {
    const operator = Object.keys(stage)[0];
    const operation = stage[operator];

    switch (operator) {
      case '$lookup': {
        const { from, localField, foreignField, as } = operation;
        const foreignDocs = await this._readCollectionData(from);
        
        return docs.map(doc => {
          const localValue = this._getFieldValue(doc, localField);
          const matches = foreignDocs.filter(foreignDoc => 
            String(this._getFieldValue(foreignDoc, foreignField)) === String(localValue)
          );
          return {
            ...doc,
            [as]: matches
          };
        });
      }
      

      case '$project': {
        return docs.map(doc => {
          const projected = {};
          for (const [field, spec] of Object.entries(operation)) {
            if (spec === 1) {
              projected[field] = doc[field];
            } else if (typeof spec === 'object') {
              if (spec.$size) {
                const array = this._getFieldValue(doc, spec.$size.slice(1));
                projected[field] = Array.isArray(array) ? array.length : 0;
              } else if (spec.$sum) {
                if (typeof spec.$sum === 'string') {
                  const array = this._getFieldValue(doc, spec.$sum.slice(1));
                  projected[field] = Array.isArray(array) 
                    ? array.reduce((sum, item) => sum + (item || 0), 0)
                    : 0;
                } else {
                  projected[field] = spec.$sum;
                }
              }
            }
          }
          return projected;
        });
      }

      case '$sort': {
        return [...docs].sort((a, b) => {
          for (const [field, order] of Object.entries(operation)) {
            const aVal = this._getFieldValue(a, field) || 0;
            const bVal = this._getFieldValue(b, field) || 0;
            if (aVal < bVal) return -order;
            if (aVal > bVal) return order;
          }
          return 0;
        });
      }

      default:
        return docs;
    }
  }

  async _readCollectionData(collectionName) {
    try {
      const collection = await this.model._getCollection(collectionName);
      return collection || [];
    } catch (error) {
      console.error(`Error reading collection ${collectionName}:`, error);
      return [];
    }
  }

  _densify(docs, options) {
    const { field, range, partitionByFields = [] } = options;

    if (!range || !field) {
      throw new Error('$densify requires both a "field" and a "range" property.');
    }

    const { step, unit, bounds } = range;

    // Group by partition fields (if any)
    const groups = partitionByFields.length
      ? this._groupByPartition(docs, partitionByFields)
      : { global: docs };

    const result = [];

    for (const group of Object.values(groups)) {
      const values = group.map(doc => doc[field]).sort((a, b) => a - b);

      const start = bounds && bounds[0] !== undefined ? bounds[0] : Math.min(...values);
      const end = bounds && bounds[1] !== undefined ? bounds[1] : Math.max(...values);

      // Generate range values
      for (let i = start; i <= end; i += step) {
        const existingDoc = group.find(doc => doc[field] === i);
        if (existingDoc) {
          result.push(existingDoc);
        } else {
          const newDoc = {};
          for (const field of partitionByFields) {
            newDoc[field] = group[0][field];
          }
          newDoc[field] = i;
          result.push(newDoc);
        }
      }
    }

    return result;
  }

  _groupByPartition(docs, partitionFields) {
    const groups = {};
    for (const doc of docs) {
      const key = JSON.stringify(partitionFields.map(field => doc[field]));
      if (!groups[key]) groups[key] = [];
      groups[key].push(doc);
    }
    return groups;
  }

  _graphLookup(docs, options) {
    // Input validation
    if (!docs || !options) {
      throw new Error('Invalid input: docs and options are required');
    }

    const {
      from,                     // The collection to search
      startWith,                // Initial field to start the recursion
      connectFromField,         // Source field for connections
      connectToField,           // Target field for connections
      as,                       // Output array field
      maxDepth = Infinity,      // Optional depth limit
      depthField = null 
    } = options;

    // Additional validation
    if (!from || !startWith || !connectFromField || !connectToField || !as) {
      throw new Error('Missing required graph lookup parameters');
    }

    const foreignDocs = this.model._getCollection(from);
    if (!foreignDocs) {
      throw new Error(`Collection '${from}' not found`);
    }

    // Create an index for faster lookups
    const connectionsMap = new Map();
    foreignDocs.forEach(doc => {
      const key = this._getFieldValue(doc, connectFromField);
      if (!connectionsMap.has(key)) {
        connectionsMap.set(key, []);
      }
      connectionsMap.get(key).push(doc);
    });

    const results = [];
    const visitedDocs = new Set(); // To prevent circular references

    const buildConnections = (doc, currentDepth = 0) => {
      if (currentDepth > maxDepth) return [];

      const valueToMatch = this._getFieldValue(doc, startWith);
      const connectedDocs = connectionsMap.get(valueToMatch) || [];

      const connections = connectedDocs.filter(connectedDoc => {
        // Check if the connected doc matches the connectToField
        const toFieldValue = this._getFieldValue(connectedDoc, connectToField);

        // Prevent circular references
        const docKey = JSON.stringify(connectedDoc);
        if (visitedDocs.has(docKey)) return false;

        return toFieldValue === valueToMatch;
      }).map(connectedDoc => {
        const resultDoc = { ...connectedDoc };
        if (depthField) {
          resultDoc[depthField] = currentDepth;
        }

        // Recursive call with depth tracking
        resultDoc[as] = buildConnections(connectedDoc, currentDepth + 1);

        const docKey = JSON.stringify(connectedDoc);
        visitedDocs.delete(docKey);

        return resultDoc;
      });

      return connections;
    };

    for (const doc of docs) {
      visitedDocs.clear(); // Reset visited docs for each root document
      doc[as] = buildConnections(doc);
      results.push(doc);
    }

    return results;
  }


  _unionWith(docs, { coll, pipeline }) {
    const additionalDocs = this.model._getCollection(coll);
    const unionDocs = pipeline.length ? new Aggregate(this.model, pipeline).execSync(additionalDocs) : additionalDocs;
    return [...docs, ...unionDocs];
  }

  _sortByCount(docs, field) {
    const counts = docs.reduce((acc, doc) => {
      const key = this._getFieldValue(doc, field);
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {});

    return Object.entries(counts)
      .map(([key, count]) => ({ _id: key, count }))
      .sort((a, b) => b.count - a.count);
  }

}

module.exports = { Aggregate };