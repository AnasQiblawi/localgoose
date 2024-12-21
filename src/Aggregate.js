const { readJSON } = require('./utils.js');
const path = require('path');

class Aggregate {
  // === Core Functionality ===
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
          docs = await this._lookup(docs, operation);
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

        case '$merge':
          docs = await this._merge(docs, operation);
          break;

        case '$out':
          docs = await this._out(docs, operation);
          break;
      }
    }

    return docs;
  }

  async _lookup(docs, operation) {
    const { from, localField, foreignField, as } = operation;
    const foreignDocs = await this.model._getCollection(from);
    return docs.map(doc => {
      const localValue = this._getFieldValue(doc, localField);
      const matches = foreignDocs.filter(foreignDoc => 
        String(this._getFieldValue(foreignDoc, foreignField)) === String(localValue)
      );
      return { ...doc, [as]: matches };
    });
  }

  async _merge(docs, operation) {
    const { into, on, whenMatched, whenNotMatched } = operation;
    const targetCollectionPath = path.join(this.model.connection.dbPath, `${into}.json`);
    const targetDocs = await readJSON(targetCollectionPath);

    const mergedDocs = docs.map(doc => {
      const matchIndex = targetDocs.findIndex(targetDoc => targetDoc[on] === doc[on]);
      if (matchIndex !== -1) {
        switch (whenMatched) {
          case 'replace':
            targetDocs[matchIndex] = doc;
            break;
          case 'merge':
            targetDocs[matchIndex] = { ...targetDocs[matchIndex], ...doc };
            break;
          case 'keepExisting':
          default:
            break;
        }
      } else {
        if (whenNotMatched === 'insert') {
          targetDocs.push(doc);
        }
      }
      return doc;
    });

    await writeJSON(targetCollectionPath, targetDocs);
    return mergedDocs;
  }

  async _out(docs, collection) {
    const targetCollectionPath = path.join(this.model.connection.dbPath, `${collection}.json`);
    await writeJSON(targetCollectionPath, docs);
    return docs;
  }

  // === Pipeline Stage Methods ===
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

  count(fieldName = 'count') {
    this.pipeline.push({ $count: fieldName });
    return this;
  }
  
  out(collection) {
    this.pipeline.push({ $out: collection });
    return this;
  }
  
  merge(options) {
    this.pipeline.push({ $merge: options });
    return this;
  }
  
  replaceRoot(newRoot) {
    this.pipeline.push({ $replaceRoot: { newRoot } });
    return this;
  }
  
  set(fields) {
    this.pipeline.push({ $set: fields });
    return this;
  }
  
  unset(fields) {
    this.pipeline.push({ $unset: Array.isArray(fields) ? fields : [fields] });
    return this;
  }

  bucketAuto(options) {
    this.pipeline.push({ $bucketAuto: options });
    return this;
  }

  changeStream(options = {}) {
    this.pipeline.push({ $changeStream: options });
    return this;
  }

  documents(docs) {
    this.pipeline.push({ $documents: docs });
    return this;
  }

  fill(options) {
    this.pipeline.push({ $fill: options });
    return this;
  }

  sample(size) {
    this.pipeline.push({ $sample: { size } });
    return this;
  }

  setWindowFields(options) {
    this.pipeline.push({ $setWindowFields: options });
    return this;
  }

  // === Pipeline Stage Execution Helpers ===
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

  // === Utility Methods ===
  _getFieldValue(doc, path) {
    if (!path) return doc;
    
    // Handle dot notation for nested fields
    const parts = path.split('.');
    let value = doc;
    
    for (const part of parts) {
      if (value == null) return null;
      
      // Handle array field references (e.g., 'posts.likes')
      if (Array.isArray(value)) {
        // Map through array and get the specified field from each element
        return value.map(item => this._getFieldValue(item, part));
      }
      
      value = value[part];
    }
    
    return value;
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

  _calculateAccumulator(docs, accumulator) {
    const operator = Object.keys(accumulator)[0];
    const field = accumulator[operator];

    switch (operator) {
      case '$sum':
        return docs.reduce((sum, doc) => sum + (doc[field] || 0), 0);
      case '$avg':
        const values = docs.map(doc => doc[field]).filter(v => v !== null);
        return values.length ? values.reduce((a, b) => a + b, 0) / values.length : null;
      case '$max':
        return Math.max(...docs.map(doc => doc[field]).filter(v => v !== null));
      case '$min':
        return Math.min(...docs.map(doc => doc[field]).filter(v => v !== null));
    }
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

  // === Pipeline Execution Methods ===
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
  
      case '$bucket': {
        const { 
          groupBy,      // Expression to group by
          boundaries,   // Bucket boundaries
          default: defaultBucket,  // Optional bucket for values outside boundaries
          output = {}   // Optional output fields
        } = operation;
  
        const buckets = {};
  
        docs.forEach(doc => {
          // Evaluate the groupBy expression for the current document
          const value = this._evaluateExpression(groupBy, doc);
  
          // Find the appropriate bucket
          let bucketIndex = boundaries.findIndex(boundary => value < boundary);
          
          if (bucketIndex === -1) {
            if (defaultBucket !== undefined) {
              bucketIndex = 'default';
            } else {
              // Skip document if no suitable bucket and no default
              return;
            }
          } else {
            // Use the lower boundary as the bucket key
            bucketIndex = bucketIndex > 0 ? boundaries[bucketIndex - 1] : 0;
          }
  
          // Initialize bucket if it doesn't exist
          if (!buckets[bucketIndex]) {
            buckets[bucketIndex] = { 
              _id: bucketIndex,
              count: 0
            };
            
            // Initialize output fields
            for (const [field, accumulator] of Object.entries(output)) {
              buckets[bucketIndex][field] = this._initializeAccumulator(accumulator);
            }
          }
  
          // Update count
          buckets[bucketIndex].count++;
  
          // Update output fields
          for (const [field, accumulator] of Object.entries(output)) {
            this._updateAccumulator(
              buckets[bucketIndex], 
              field, 
              accumulator, 
              doc
            );
          }
        });
  
        // Convert buckets object to array and handle averages
        return Object.values(buckets).map(bucket => {
          // Convert average accumulator to final value
          for (const [field, value] of Object.entries(bucket)) {
            if (typeof value === 'object' && value.sum !== undefined) {
              bucket[field] = value.value || (value.sum / value.count);
            }
          }
          return bucket;
        });
      }
  
      case '$count': {
        return [{ [operation]: docs.length }];
      }
  
      case '$out': {
        // Determine the target collection path
        const targetCollectionPath = path.join(
          this.connection.dbPath, 
          `${operation}.json`
        );
      
        // Write the current docs to the target collection
        await writeJSON(targetCollectionPath, docs);
      
        // Optionally, you can return the docs or an empty array
        return docs;
      }
  
      case '$merge': {
        const { into, on, whenMatched, whenNotMatched } = operation;
        
        // Simulate merge logic
        const existingCollection = this.model._getCollection(into) || [];
        
        const mergedDocs = docs.map(doc => {
          // Find matching documents based on 'on' field
          const matchIndex = existingCollection.findIndex(
            existing => existing[on] === doc[on]
          );
  
          if (matchIndex !== -1) {
            // When matched
            switch (whenMatched) {
              case 'replace':
                existingCollection[matchIndex] = doc;
                break;
              case 'merge':
                existingCollection[matchIndex] = { 
                  ...existingCollection[matchIndex], 
                  ...doc 
                };
                break;
              case 'keepExisting':
              default:
                break;
            }
          } else {
            // When not matched
            switch (whenNotMatched) {
              case 'insert':
                existingCollection.push(doc);
                break;
              case 'discard':
              default:
                break;
            }
          }
  
          return doc;
        });
  
        return mergedDocs;
      }
  
      case '$replaceRoot': {
        const { newRoot } = operation;
        return docs.map(doc => {
          // Evaluate the new root expression
          return this._evaluateExpression(newRoot, doc);
        });
      }
  
      case '$set': {
        return docs.map(doc => {
          const updatedDoc = { ...doc };
          for (const [field, value] of Object.entries(operation)) {
            updatedDoc[field] = this._evaluateExpression(value, doc);
          }
          return updatedDoc;
        });
      }
  
      case '$unset': {
        return docs.map(doc => {
          const updatedDoc = { ...doc };
          for (const field of operation) {
            delete updatedDoc[field];
          }
          return updatedDoc;
        });
      }
  
      case '$bucketAuto': {
        const { groupBy, buckets, output = {} } = operation;
        const values = docs.map(doc => this._evaluateExpression(groupBy, doc)).sort((a, b) => a - b);
        const bucketSize = Math.ceil(values.length / buckets);
        
        return Array.from({ length: buckets }, (_, i) => {
          const start = i * bucketSize;
          const end = (i + 1) * bucketSize;
          
          const bucketDocs = docs.filter((doc, index) => 
            index >= start && index < Math.min(end, docs.length)
          );
          
          return {
            _id: { 
              min: values[start], 
              max: values[Math.min(end - 1, values.length - 1)] 
            },
            count: bucketDocs.length,
            ...this._computeOutputFields(bucketDocs, output)
          };
        });
      }
  
      case '$fill': {
        const { sortBy, output } = operation;
        const sortedDocs = sortBy ? this._sort(docs, sortBy) : [...docs];
        
        return sortedDocs.map(doc => {
          const filledDoc = { ...doc };
          
          for (const [field, method] of Object.entries(output)) {
            if (filledDoc[field] === null || filledDoc[field] === undefined) {
              switch (method) {
                case 'linear':
                  const docIndex = sortedDocs.indexOf(doc);
                  const prevDoc = sortedDocs[docIndex - 1];
                  const nextDoc = sortedDocs[docIndex + 1];
                  if (prevDoc && nextDoc) {
                    filledDoc[field] = (prevDoc[field] + nextDoc[field]) / 2;
                  }
                  break;
                case 'locf':
                  const lastValidDoc = sortedDocs.findLast(d => 
                    d[field] !== null && d[field] !== undefined
                  );
                  if (lastValidDoc) {
                    filledDoc[field] = lastValidDoc[field];
                  }
                  break;
              }
            }
          }
          
          return filledDoc;
        });
      }
  
      case '$documents': {
        return Array.isArray(operation) ? operation : [operation];
      }
  
      case '$sample': {
        const { size } = operation;
        return docs
          .sort(() => 0.5 - Math.random())
          .slice(0, size);
      }
  
      case '$setWindowFields': {
        const { partitionBy, sortBy, output } = operation;
        
        const partitionedDocs = partitionBy 
          ? this._partitionDocuments(docs, partitionBy) 
          : [docs];
        
        return partitionedDocs.flatMap(partition => 
          this._computeWindowFields(partition, sortBy, output)
        );
      }
  
      default:
        return docs;
    }
  }

  // === Data Access Methods ===
  async _readCollectionData(collectionName) {
    try {
      const collection = await this.model._getCollection(collectionName);
      return collection || [];
    } catch (error) {
      console.error(`Error reading collection ${collectionName}:`, error);
      return [];
    }
  }
}

module.exports = { Aggregate };