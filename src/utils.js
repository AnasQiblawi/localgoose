const fs = require('fs-extra');
const path = require('path');
const { ObjectId } = require('bson');
const { LRUCache } = require('lru-cache');
const writeFileAtomic = require('write-file-atomic');

// === File Operations ===
const fileLocks = new Map();
const fileCache = new LRUCache({
  max: 500,
  // 1 hour TTL
  ttl: 1000 * 60 * 60,
  updateAgeOnGet: true
});
const flushTimers = new Map();
const pendingFlushes = new Map();

async function flushDisk() {
  while (pendingFlushes.size > 0) {
    const promises = Array.from(pendingFlushes.values()).map(fn => fn());
    pendingFlushes.clear();
    for (const timer of flushTimers.values()) clearTimeout(timer);
    flushTimers.clear();
    await Promise.all(promises);
  }
}

function _deepClone(obj) {
  if (obj === null || typeof obj !== 'object') return obj;
  if (obj instanceof Date) return new Date(obj.getTime());
  if (obj instanceof ObjectId) return new ObjectId(obj.toString());
  if (Array.isArray(obj)) return obj.map(_deepClone);
  const cloned = {};
  for (const k in obj) cloned[k] = _deepClone(obj[k]);
  return cloned;
}

function clearCache(dirPath) {
  // Selectively clear cache entries starting with dirPath
  for (const key of fileCache.keys()) {
    if (key.startsWith(dirPath)) {
      fileCache.delete(key);
      if (flushTimers.has(key)) {
        clearTimeout(flushTimers.get(key));
        flushTimers.delete(key);
      }
      pendingFlushes.delete(key);
    }
  }
}

async function withLock(filePath, fn) {
  const currentLock = fileLocks.get(filePath) || Promise.resolve();
  const resultPromise = currentLock.then(() => fn(), () => fn());
  fileLocks.set(filePath, resultPromise.catch(() => {}));
  return resultPromise;
}

async function readJSON(filePath, options = {}) {
  return withLock(filePath, async () => {
    const { defaultValue = [] } = options;
    if (fileCache.has(filePath)) {
      return _deepClone(fileCache.get(filePath));
    }
    try {
      const data = await fs.readFile(filePath, 'utf8');
      if (!data.trim()) {
        fileCache.set(filePath, _deepClone(defaultValue));
        await _writeJSONImpl(filePath, defaultValue, options);
        return defaultValue;
      }
      const parsed = JSON.parse(data, (key, value) => {
        if (typeof value === 'string' &&
            /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/.test(value)) {
          const d = new Date(value);
          if (!isNaN(d.getTime())) return d;
        }
        return value;
      });
      fileCache.set(filePath, parsed);
      return _deepClone(parsed);
    } catch (error) {
      if (error.code === 'ENOENT') {
        const dirPath = path.dirname(filePath);
        if (dirPath) await fs.mkdir(dirPath, { recursive: true });
        fileCache.set(filePath, _deepClone(defaultValue));
        await _writeJSONImpl(filePath, defaultValue, options);
        return defaultValue;
      }
      throw error;
    }
  });
}

async function _writeJSONImpl(filePath, data, options = {}) {
  const { spaces = 2 } = options;
  const jsonString = JSON.stringify(data, (key, value) => {
    if (value instanceof Date) return value.toISOString();
    if (value instanceof Map) return Object.fromEntries(value);
    if (typeof value === 'bigint') return value.toString();
    return value;
  }, spaces);

  const dirPath = path.dirname(filePath);
  if (dirPath) await fs.mkdir(dirPath, { recursive: true });
  
  await writeFileAtomic(filePath, jsonString, 'utf8');
}

function _scheduleFlush(filePath, data, options) {
  if (flushTimers.has(filePath)) clearTimeout(flushTimers.get(filePath));
  
  const flushFn = () => withLock(filePath, () => _writeJSONImpl(filePath, data, options));
  pendingFlushes.set(filePath, flushFn);

  const timer = setTimeout(() => {
    flushTimers.delete(filePath);
    pendingFlushes.delete(filePath);
    flushFn().catch(console.error);
  }, 50);
  flushTimers.set(filePath, timer);
}

async function writeJSON(filePath, data, options = {}) {
  fileCache.set(filePath, _deepClone(data));
  _scheduleFlush(filePath, data, options);
  return Promise.resolve();
}

// === Type Validation ===
function validateType(value, type, options = {}) {
  const { coerce = false, nullable = false } = options;
  if (value === undefined || value === null) return nullable;
  if (coerce) {
    if (type === String) return String(value);
    if (type === Number) return Number(value);
    if (type === Boolean) return Boolean(value);
    if (type === Date) return new Date(value);
    if (type === ObjectId) return new ObjectId(value);
    if (type === Buffer) return Buffer.from(value);
    if (type === BigInt) return BigInt(value);
  }
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
  if (typeof type === 'function') return value instanceof type;
  return true;
}

// === Dot-notation field access ===
function getNestedValue(obj, path) {
  if (!path || obj === null || obj === undefined) return undefined;
  const parts = path.split('.');
  let current = obj;
  for (const part of parts) {
    if (current === null || current === undefined) return undefined;
    if (Array.isArray(current)) {
      // Map over array for array field access (e.g. 'tags.name')
      current = current.map(item => (item && typeof item === 'object') ? item[part] : undefined);
    } else {
      current = current[part];
    }
  }
  return current;
}

function setNestedValue(obj, path, value) {
  const parts = path.split('.');
  let current = obj;
  for (let i = 0; i < parts.length - 1; i++) {
    if (current[parts[i]] === undefined || current[parts[i]] === null) {
      current[parts[i]] = {};
    }
    current = current[parts[i]];
  }
  current[parts[parts.length - 1]] = value;
}

// === Output Formatting ===
function formatOutput(obj, options = {}) {
  const { seen = new WeakSet(), maxDepth = 10, currentDepth = 0 } = options;
  if (currentDepth > maxDepth) return '[Max Depth Reached]';
  if (obj === null || typeof obj !== 'object') return obj;
  if (seen.has(obj)) return '[Circular]';
  const newSeen = new WeakSet(seen);
  newSeen.add(obj);
  if (Array.isArray(obj)) {
    return obj.map(item => formatOutput(item, { seen: newSeen, maxDepth, currentDepth: currentDepth + 1 }));
  }
  const formatted = {};
  for (const [key, value] of Object.entries(obj)) {
    if (value && typeof value === 'object') {
      formatted[key] = value instanceof Date
        ? value.toISOString()
        : formatOutput(value, { seen: newSeen, maxDepth, currentDepth: currentDepth + 1 });
    } else {
      formatted[key] = value;
    }
  }
  return formatted;
}

module.exports = { readJSON, writeJSON, validateType, formatOutput, getNestedValue,
  setNestedValue,
  formatOutput,
  flushDisk,
  clearCache
};