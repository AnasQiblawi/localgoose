const fs = require('fs-extra');
const path = require('path');
const { ObjectId } = require('bson');

async function readJSON(filePath, options = {}) {
  const { 
    defaultValue = [], 
    dateReviver = (value) => value instanceof Date ? value : new Date(value) 
  } = options;

  try {
    const data = await fs.readFile(filePath, 'utf8');
    
    if (!data.trim()) {
      await writeJSON(filePath, defaultValue);
      return defaultValue;
    }

    return JSON.parse(data, (key, value) => {
      // Enhanced date parsing
      if (typeof value === 'string' && 
          /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z$/.test(value)) {
        try {
          return dateReviver(value);
        } catch {
          return value;
        }
      }
      return value;
    });
    
  } catch (error) {
    if (error.code === 'ENOENT') {
      const dirPath = path.dirname(filePath);
      if (dirPath) {
        await fs.mkdir(dirPath, { recursive: true });
      }
      await writeJSON(filePath, defaultValue);
      return defaultValue;
    }
    throw error;
  }
}

async function writeJSON(filePath, data, options = {}) {
  const { 
    spaces = 2, 
    dateReplacer = (value) => value instanceof Date ? value.toISOString() : value 
  } = options;

  try {
    const jsonString = JSON.stringify(data, (key, value) => {
      // Enhanced date handling with custom replacer
      if (value instanceof Date) {
        return dateReplacer(value);
      }
      return value;
    }, spaces);
    await fs.writeFile(filePath, jsonString, 'utf8');
  } catch (error) {
    throw new Error(`Failed to write to ${filePath}: ${error.message}`);
  }
}

function validateType(value, type, options = {}) {
  const { 
    coerce = false,
    nullable = false 
  } = options;

  // Handle nullable option
  if ((value === undefined || value === null)) {
    return nullable;
  }

  // Type coercion support
  if (coerce) {
    if (type === String) return String(value);
    if (type === Number) return Number(value);
    if (type === Boolean) return Boolean(value);
    if (type === Date) return new Date(value);
    if (type === ObjectId) return new ObjectId(value);
    if (type === Buffer) return Buffer.from(value);
    if (type === BigInt) return BigInt(value);
  }

  // Enhanced type checking with custom class support
  if (type === String) return typeof value === 'string';
  if (type === Number) return typeof value === 'number' && !isNaN(value);
  if (type === Boolean) return typeof value === 'boolean';
  if (type === Date) return value instanceof Date || !isNaN(new Date(value).getTime());
  if (type === Array) return Array.isArray(value);
  if (type === Object) return typeof value === 'object' && !Array.isArray(value) && value !== null;
  if (type === Buffer) return Buffer.isBuffer(value);
  if (type === ObjectId) return value instanceof ObjectId;
  if (type === BigInt) return typeof value === 'bigint';
  if (type === Map) return value instanceof Map;
  
  // Custom class instance check
  if (typeof type === 'function') {
    return value instanceof type;
  }

  return true;
}

function formatOutput(obj, options = {}) {
  const { 
    seen = new WeakSet(),
    maxDepth = 10,
    currentDepth = 0 
  } = options;

  // Depth and circular reference protection
  if (currentDepth > maxDepth) {
    return '[Max Depth Reached]';
  }

  if (obj === null || typeof obj !== 'object') {
    return obj;
  }

  if (seen.has(obj)) {
    return '[Circular]';
  }

  // Clone the seen set to prevent cross-branch pollution
  const newSeen = new WeakSet(seen);
  newSeen.add(obj);

  if (Array.isArray(obj)) {
    return obj.map(item => formatOutput(item, {
      seen: newSeen,
      maxDepth,
      currentDepth: currentDepth + 1
    }));
  }

  const formatted = {};
  for (const [key, value] of Object.entries(obj)) {
    if (value && typeof value === 'object') {
      if (value instanceof Date) {
        formatted[key] = value.toISOString();
      } else {
        formatted[key] = formatOutput(value, {
          seen: newSeen,
          maxDepth,
          currentDepth: currentDepth + 1
        });
      }
    } else {
      formatted[key] = value;
    }
  }

  return formatted;
}

module.exports = {
  readJSON,
  writeJSON,
  validateType,
  formatOutput
};