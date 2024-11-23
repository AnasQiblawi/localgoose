import fs from 'fs/promises';
import path from 'path';

export async function readJSON(filePath) {
  try {
    const data = await fs.readFile(filePath, 'utf8');
    
    if (!data.trim()) {
      await writeJSON(filePath, []);
      return [];
    }

    return JSON.parse(data, (key, value) => {
      if (typeof value === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z$/.test(value)) {
        return new Date(value);
      }
      return value;
    });
    
  } catch (error) {
    if (error.code === 'ENOENT') {
      const dirPath = path.dirname(filePath);
      if (dirPath) {
        await fs.mkdir(dirPath, { recursive: true });
      }
      await writeJSON(filePath, []);
      return [];
    }
    throw error;
  }
}

export async function writeJSON(filePath, data) {
  try {
    const jsonString = JSON.stringify(data, (key, value) => {
      if (value instanceof Date) {
        return value.toISOString();
      }
      return value;
    }, 2);
    await fs.writeFile(filePath, jsonString, 'utf8');
  } catch (error) {
    throw new Error(`Failed to write to ${filePath}: ${error.message}`);
  }
}

export function validateType(value, type) {
  if (value === undefined || value === null) return false;
  if (type === String) return typeof value === 'string';
  if (type === Number) return typeof value === 'number' && !isNaN(value);
  if (type === Boolean) return typeof value === 'boolean';
  if (type === Date) return value instanceof Date || !isNaN(new Date(value).getTime());
  if (type === Array) return Array.isArray(value);
  if (type === Object) return typeof value === 'object' && !Array.isArray(value) && value !== null;
  return true;
}

export function formatOutput(obj, seen = new WeakSet()) {
  if (obj === null || typeof obj !== 'object') {
    return obj;
  }

  if (seen.has(obj)) {
    return '[Circular]';
  }

  seen.add(obj);

  if (Array.isArray(obj)) {
    return obj.map(item => formatOutput(item, seen));
  }

  const formatted = {};
  for (const [key, value] of Object.entries(obj)) {
    if (value && typeof value === 'object') {
      if (value instanceof Date) {
        formatted[key] = value.toISOString();
      } else {
        formatted[key] = formatOutput(value, seen);
      }
    } else {
      formatted[key] = value;
    }
  }

  return formatted;
}