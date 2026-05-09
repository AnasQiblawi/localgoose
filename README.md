# Localgoose

A lightweight, file-based ODM (Object-Document Mapper) for Node.js, inspired by Mongoose but designed for local JSON storage. Perfect for prototypes, small applications, and scenarios where a full MongoDB setup isn't needed.

## Features

- 🚀 Mongoose-compatible API for a familiar development experience
- 📁 JSON file-based storage with atomic writes via `write-file-atomic`
- 🔄 Schema validation, type casting, and `enum` constraints
- 🎯 Rich, chainable query API (`.where()`, `.gt()`, `.lt()`, `.in()`, `.paginate()`, …)
- 📊 Aggregation pipeline support (20+ stages, 12 group accumulators)
- 🔌 Virtual properties, middleware hooks (pre/post), and schema plugins
- 🔗 Reference population — standard refs, array refs, virtual population
- 🌐 Geospatial queries (`$near`, `$nearSphere`, `$geoIntersects`, `$geoWithin`, …)
- 🔎 Full-text search (`$text` / `.text()`)
- 📑 Compound and text index support
- 🔄 Schema inheritance (`schema.add()`), discrimination, and extension
- 🗄️ Built-in backup and restore
- 🛠️ Efficient in-memory caching with LRU cache (`lru-cache`)
- 📦 Dependencies: `bson`, `fs-extra`, `geolib`, `lru-cache`, `write-file-atomic`

## Installation

```bash
npm install localgoose
```

## Quick Start

```javascript
const { localgoose } = require('localgoose');

// Connect to a local directory for storage
const db = await localgoose.connect('./mydb');

// Define schemas
const userSchema = new localgoose.Schema({
  username: { type: String, required: true },
  email:    { type: String, required: true },
  age:      { type: Number, required: true },
  tags:     { type: Array,  default: [] }
});

const postSchema = new localgoose.Schema({
  title:   { type: String, required: true },
  content: { type: String, required: true },
  author:  { type: localgoose.Schema.Types.ObjectId, ref: 'User' },
  likes:   { type: Number, default: 0 }
});

// Create models
const User = db.model('User', userSchema);
const Post = db.model('Post', postSchema);

// Create documents
const user = await User.create({
  username: 'john',
  email:    'john@example.com',
  age:      25,
  tags:     ['developer']
});

const post = await Post.create({
  title:   'Getting Started',
  content: 'Hello World!',
  author:  user._id
});

// Query with population
const posts = await Post.find()
  .populate('author')
  .sort('-likes')
  .exec();

// Aggregation pipeline
const stats = await Post.aggregate()
  .match({ author: user._id })
  .group({
    _id:        null,
    totalPosts: { $sum: 1 },
    avgLikes:   { $avg: '$likes' }
  })
  .exec();
```

---

## API Reference

### Connection

```javascript
const { localgoose } = require('localgoose');

// Connect (returns the connection)
const db = await localgoose.connect('./mydb');

// Create a separate, independent connection
const conn = await localgoose.createConnection('./otherdb');

// Flush all pending writes immediately (useful before process exit)
await localgoose.flushDisk();

// Top-level ObjectId constructor
const id = new localgoose.ObjectId();
```

#### Connection Methods

```javascript
// Disconnect and flush all pending writes
await db.disconnect();   // or db.close()

// Drop the entire database directory
await db.dropDatabase();

// Switch to a different database (returns a new Connection)
const otherDb = db.useDb('otherdb');

// Collection helpers
const col  = db.collection('users');   // returns collection metadata
const list = db.listCollections();     // [{ name, type }]
await db.dropCollection('users');      // true | false

// Model registry
const names = db.modelNames();         // ['User', 'Post', …]
db.deleteModel('User');                // unregister a model

// Plugin support
db.plugin(myPluginFn, { option: true });

// Session stubs (Mongoose API compatibility — no real ACID)
const session = await db.startSession();
await session.startTransaction();
await session.commitTransaction();
await session.abortTransaction();
await session.endSession();

// Connection state
// db.readyState: 0 = disconnected, 1 = connected, 2 = connecting, 3 = disconnecting
```

---

### Schema Definition

```javascript
const schema = new localgoose.Schema(
  {
    // ── Primitive types ──────────────────────────────────────────────
    name:      { type: String,  required: true },
    score:     { type: Number,  default: 0 },
    active:    { type: Boolean },
    createdAt: { type: Date,    default: Date.now },

    // ── BSON / special types ──────────────────────────────────────────
    userId:  { type: localgoose.Schema.Types.ObjectId, ref: 'User' },
    decimal: localgoose.Schema.Types.Decimal128,
    binary:  localgoose.Schema.Types.Buffer,
    uuid:    localgoose.Schema.Types.UUID,
    bigint:  localgoose.Schema.Types.BigInt,
    any:     localgoose.Schema.Types.Mixed,
    meta:    localgoose.Schema.Types.Map,

    // ── Enum constraint ───────────────────────────────────────────────
    role:    { type: String, enum: ['admin', 'user', 'guest'], default: 'user' },

    // ── Shorthand type ────────────────────────────────────────────────
    bio: String,

    // ── Arrays ────────────────────────────────────────────────────────
    tags:    { type: Array, default: [] },
    scores:  [{ type: Number }],

    // ── Nested object ─────────────────────────────────────────────────
    address: {
      street: String,
      city:   String
    }
  },
  {
    timestamps:  true,     // auto-adds createdAt / updatedAt
    versionKey:  '__v',    // set to false to disable
    strict:      true      // extra fields are ignored when true (default)
  }
);
```

#### Schema Types

| Type | Alias |
|------|-------|
| `String` | — |
| `Number` | — |
| `Boolean` | — |
| `Date` | — |
| `Array` | — |
| `Object` / `Mixed` | `Schema.Types.Mixed` |
| `Map` | `Schema.Types.Map` |
| `Buffer` | `Schema.Types.Buffer` |
| `ObjectId` | `Schema.Types.ObjectId` |
| `Decimal128` | `Schema.Types.Decimal128` |
| `UUID` | `Schema.Types.UUID` |
| `BigInt` | `Schema.Types.BigInt` |

#### Schema Options (per field)

| Option | Description |
|--------|-------------|
| `required` | `true` or `[true, 'message']` |
| `default` | static value or function |
| `enum` | array of allowed values |
| `min` / `max` | numeric bounds (value or `[value, 'msg']`) |
| `minlength` / `maxlength` | string length bounds |
| `match` | RegExp (string fields) |
| `validate` | function or `{ validator, message }` |
| `unique` | mark as unique (enforced in memory) |
| `ref` | model name for population |
| `index` | auto-create an index for this path |

#### Virtual Properties

```javascript
schema.virtual('fullName').get(function () {
  return `${this.firstName} ${this.lastName}`;
});

// Virtual population
schema.virtual('posts', {
  ref:          'Post',
  localField:   '_id',
  foreignField: 'author',
  justOne:      false,
  options:      { sort: { createdAt: -1 } }
});
```

#### Instance & Static Methods

```javascript
schema.method('getInfo', function () {
  return `${this.username} (${this.age})`;
});

// OR pass an object
schema.method({ greet() { return 'hi'; }, bye() { return 'bye'; } });

schema.static('findByEmail', function (email) {
  return this.findOne({ email });
});
```

#### Middleware Hooks

Supported hooks: `init`, `validate`, `save`, `remove`, `deleteOne`, `deleteMany`, `find`, `findOne`, `findOneAndUpdate`, `findOneAndRemove`, `findOneAndDelete`, `updateOne`, `updateMany`, `count`, `countDocuments`, `estimatedDocumentCount`, `aggregate`, `insertMany`

```javascript
// Document middleware
schema.pre('save', async function () {
  if (this.isModified('password')) {
    this.password = await hash(this.password);
  }
});

schema.post('save', function (doc) {
  console.log('Saved:', doc._id);
});

// Query middleware
schema.pre('find', function () {
  this.where({ isActive: true });
});

// Aggregation middleware — use this._pipeline directly
schema.pre('aggregate', function () {
  this._pipeline.unshift({ $match: { isDeleted: false } });
});
```

#### Indexes

```javascript
schema.index({ email: 1 }, { unique: true });
schema.index({ title: 'text', content: 'text' });  // text index for full-text search
schema.index({ location: '2dsphere' });             // geospatial index

// Retrieve / manage indexes
schema.indexes();               // array of all index definitions
schema.removeIndex('indexName');
schema.clearIndexes();
schema.searchIndex({ name: 'mySearch', definition: { mappings: {} } });
```

#### Other Schema Methods

```javascript
// Add fields or merge another schema
schema.add({ newField: String });
schema.add(anotherSchema);

// Schema manipulation
const cloned  = schema.clone();
const trimmed = schema.omit(['password', 'salt']);     // returns new schema
const minimal = schema.pick(['_id', 'username']);      // returns new schema
schema.extend(anotherSchema);                          // alias for schema.add(schema)

// Field aliases
schema.alias('email', 'emailAddress');

// Load methods/statics from a class
schema.loadClass(MyClass);

// Schema plugins
schema.plugin(myPlugin, { option: true });

// Path introspection
const st  = schema.path('email');         // returns SchemaType
const typ = schema.pathType('email');     // 'real' | 'virtual' | 'reserved' | 'adhoc'
schema.eachPath((pathName, schemaType) => { /* … */ });
schema.requiredPaths();                   // ['username', 'email', …]

// JSON Schema export
const jsonSchema = schema.toJSONSchema();
const bsonSchema = schema.toJSONSchema({ useBsonType: true });

// Option accessors
schema.get('timestamps');
schema.set('strict', false);
```

---

### Model Operations

#### Create

```javascript
// Single document
const doc = await Model.create({ field: 'value' });

// Multiple documents
const docs = await Model.create([{ field: 'a' }, { field: 'b' }]);

// Alias for single create
const doc = await Model.insertOne({ field: 'value' });

// Insert many with options
const result = await Model.insertMany(
  [{ field: 'a' }, { field: 'b' }],
  { ordered: true, lean: false }
);
```

#### Read

```javascript
// Find all
const docs = await Model.find();

// Find with filter
const docs = await Model.find({ status: 'active', score: { $gt: 10 } });

// Find one
const doc = await Model.findOne({ email: 'a@b.com' });

// Find by ID
const doc = await Model.findById(id);

// Count
const n = await Model.countDocuments({ status: 'active' });
const n = await Model.estimatedDocumentCount();

// Check existence
const exists = await Model.exists({ email: 'a@b.com' });  // { _id } or null

// Distinct values
const emails = await Model.distinct('email', { active: true });

// Populate on find
const doc = await Model.findOne({ slug: 'post' }).populate('author').exec();

// Hydrate a plain object into a Document
const doc = Model.hydrate({ _id: '…', name: 'John' });

// Cast an object through the schema without saving
const cast = Model.castObject({ score: '42' });  // { score: 42 }
```

#### Update

```javascript
// Update one / many
const result = await Model.updateOne(
  { field: 'value' },
  { $set: { newField: 'new' } },
  { upsert: true }
);

const result = await Model.updateMany(
  { status: 'old' },
  { $set: { status: 'new' } }
);

// Find and update (returns document)
const doc = await Model.findOneAndUpdate(
  { field: 'value' },
  { $set: { field: 'new' } },
  { new: true, upsert: true }   // new: true → return updated document
);

const doc = await Model.findByIdAndUpdate(id, { $set: { field: 'new' } }, { new: true });

// Replace entire document
const result = await Model.replaceOne({ field: 'value' }, { newDocument: true });

const doc = await Model.findOneAndReplace(
  { field: 'value' },
  { replacement: true },
  { upsert: false }
);

// Increment a field
const result = await Model.increment(
  { field: 'value' }, // filter
  'counter',          // field
  5                   // amount (default: 1)
);
```

#### Delete

```javascript
const result = await Model.deleteOne({ field: 'value' });
const result = await Model.deleteMany({ status: 'inactive' });

const doc = await Model.findOneAndDelete({ field: 'value' });
const doc = await Model.findByIdAndDelete(id);
const doc = await Model.findByIdAndRemove(id);  // alias
```

#### Bulk Operations

```javascript
const result = await Model.bulkWrite([
  {
    insertOne: { document: { field: 'value' } }
  },
  {
    updateOne: {
      filter: { field: 'value' },
      update: { $set: { field: 'new' } }
    }
  },
  {
    updateMany: {
      filter: { status: 'old' },
      update: { $set: { status: 'new' } }
    }
  },
  {
    replaceOne: {
      filter: { field: 'old' },
      replacement: { field: 'new' }
    }
  },
  {
    deleteOne:  { filter: { field: 'del' } }
  },
  {
    deleteMany: { filter: { status: 'gone' } }
  }
], { ordered: true });

// result: { insertedCount, modifiedCount, deletedCount, upsertedCount, … }

// Bulk save Document instances
const result = await Model.bulkSave([
  new Model({ field: 'a' }),
  new Model({ field: 'b' })
], { ordered: true });
```

#### Index Management

```javascript
await Model.createIndexes();
await Model.syncIndexes();
await Model.ensureIndexes();
const list = await Model.listIndexes();
const diff = await Model.diffIndexes();

// Atlas Search index stubs (API compatibility)
await Model.createSearchIndex({ name: 'default', definition: {} });
await Model.updateSearchIndex('default', { definition: {} });
const indexes = await Model.listSearchIndexes();
await Model.dropSearchIndex('default');
```

#### Other Model Methods

```javascript
// Discriminator
const AdminModel = Model.discriminator('Admin', adminSchema);

// Static `.where()` shorthand
Model.where('status').equals('active').exec();

// Deprecated shims (Mongoose v5 API compat)
await Model.count({ status: 'active' });   // alias for countDocuments
await Model.remove({ old: true });         // alias for deleteMany
await Model.update(filter, update, opts);  // alias for updateOne

// Session stub (Mongoose API compat — no real transactions)
const session = await Model.startSession();

// watch() is intentionally unsupported
Model.watch(); // throws: 'Watch is not supported in file-based storage'
```

---

### Backup and Restore

```javascript
// Create a timestamped backup; returns the backup file path
const backupPath = await Model.backup();
console.log('Backup at:', backupPath);

// Restore from a backup file
await Model.restore(backupPath);

// List all available backups
const backups = await Model.listBackups();

// Delete old backups
await Model.cleanupBackups();
```

---

### Query API

Every `Model.find()` / `Model.findOne()` call returns a `Query` object. Queries are **thenable** — you can `await` them directly without calling `.exec()`.

```javascript
// Chainable query methods
const docs = await Model.find()
  .where('field').equals('value')
  .where('score').gt(10).lt(100)
  .where('tags').in(['node', 'js'])
  .select('field score tags')      // inclusion
  .select('-password')             // exclusion
  .sort('-score field')
  .skip(20)
  .limit(10)
  .lean()                          // return plain objects (faster, no Document overhead)
  .populate('author')
  .exec();                         // optional — query is already awaitable

// Pagination helper
const page2 = await Model.find()
  .paginate(2, 10);  // page 2, 10 per page  →  skip(10).limit(10)

// Standalone text search
const results = await Model.find().text('search keyword').exec();

// Logical operators as chain methods
const docs = await Model.find()
  .or([{ status: 'active' }, { score: { $gt: 50 } }]);

const docs = await Model.find()
  .and([{ role: 'admin' }, { active: true }]);

const docs = await Model.find()
  .nor([{ banned: true }, { deleted: true }]);

// Per-field operators
Model.find().where('score').ne(0);
Model.find().where('tags').all(['node', 'js']);
Model.find().where('reviews').elemMatch({ rating: { $gte: 4 } });

// Throw if nothing found
const doc = await Model.findOne({ slug: 'missing' })
  .orFail(new Error('Not found'));

// Transform results
const names = await Model.find().transform(docs => docs.map(d => d.name));

// Async for-of (async iterator)
for await (const doc of Model.find({ active: true })) {
  console.log(doc.name);
}
```

#### Geospatial Queries

```javascript
// Near a point
const nearby = await Model.find()
  .where('location')
  .near({ center: [longitude, latitude], maxDistance: 5000 })
  .exec();

// Within a box
const boxed = await Model.find()
  .where('location')
  .box([[-180, -90], [180, 90]])
  .exec();

// Within a circle
const circled = await Model.find()
  .where('location')
  .center([longitude, latitude], radius)
  .exec();

// Within a polygon
const poly = await Model.find()
  .where('location')
  .polygon([[x1,y1], [x2,y2], [x3,y3]])
  .exec();

// Geo-intersects
const intersects = await Model.find()
  .where('location')
  .intersects({ type: 'Polygon', coordinates: [[[…]]] })
  .exec();
```

---

### Aggregation Pipeline

```javascript
const results = await Model.aggregate()
  .match({ status: 'active' })
  .group({
    _id:     '$department',
    total:   { $sum: 1 },
    avg:     { $avg: '$salary' },
    maxSal:  { $max: '$salary' },
    minSal:  { $min: '$salary' },
    names:   { $push: '$name' },
    unique:  { $addToSet: '$role' },
    first:   { $first: '$name' },
    last:    { $last: '$name' },
    stdPop:  { $stdDevPop: '$salary' },
    stdSamp: { $stdDevSamp: '$salary' },
    merged:  { $mergeObjects: '$meta' },
    count:   { $count: {} }
  })
  .sort({ total: -1 })
  .limit(5)
  .exec();
```

#### Supported Aggregation Stages

| Stage | Description |
|-------|-------------|
| `$match` | Filter documents |
| `$group` | Group by expression |
| `$sort` | Sort documents |
| `$limit` | Limit result count |
| `$skip` | Skip N documents |
| `$unwind` | Deconstruct array field |
| `$lookup` | Left outer join |
| `$project` | Reshape documents |
| `$addFields` / `$set` | Add or update fields |
| `$unset` | Remove fields |
| `$replaceRoot` | Replace input document |
| `$facet` | Multiple aggregation pipelines |
| `$bucket` | Categorize into fixed buckets |
| `$bucketAuto` | Auto-sized buckets |
| `$sortByCount` | Group and count |
| `$count` | Count documents |
| `$out` | Write result to a file |
| `$merge` | Merge result into a collection |
| `$densify` | Fill gaps in time-series data |
| `$graphLookup` | Recursive search |
| `$unionWith` | Combine documents from another collection |
| `$sample` | Random sample of N documents |
| `$fill` | Fill missing field values |
| `$setWindowFields` | Window functions |
| `$redact` | Field-level access control |
| `$search` | (passthrough stub for Atlas Search compat) |
| `$geoNear` | Geospatial proximity sort |

> **Note:** `$changeStream` and `$documents` are listed as valid stage names for API compatibility but are silently ignored (no-op).

#### Group Accumulators

`$sum`, `$avg`, `$min`, `$max`, `$push`, `$addToSet`, `$first`, `$last`, `$stdDevPop`, `$stdDevSamp`, `$mergeObjects`, `$count`

---

### Supported Update Operators

#### Field Operators
| Operator | Description |
|----------|-------------|
| `$set` | Set field value |
| `$unset` | Remove field |
| `$rename` | Rename field |
| `$setOnInsert` | Set value only on upsert insert |

#### Numeric Operators
| Operator | Description |
|----------|-------------|
| `$inc` | Increment by amount |
| `$mul` | Multiply by amount |
| `$min` | Update if new value is less |
| `$max` | Update if new value is greater |

#### Array Operators
| Operator | Description |
|----------|-------------|
| `$push` | Add item to array (supports `$each`, `$slice`, `$sort`, `$position`) |
| `$pull` | Remove elements matching a condition |
| `$pullAll` | Remove all matching values |
| `$addToSet` | Add if not already present |
| `$pop` | Remove first (`-1`) or last (`1`) element |

#### `$push` Modifiers
```javascript
await Model.updateOne({ _id: id }, {
  $push: {
    scores: {
      $each:     [90, 85, 92],   // push multiple values
      $slice:    -3,             // keep only last 3
      $sort:     -1,             // sort descending before slice
      $position: 0              // insert at index 0
    }
  }
});
```

#### Other Operators
| Operator | Description |
|----------|-------------|
| `$bit` | Bitwise AND / OR / XOR on integer fields |
| `$currentDate` | Set field to current date |

---

### Supported Query Operators

#### Comparison
`$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`

#### Logical
`$and`, `$or`, `$nor`, `$not`

#### Element
`$exists`, `$type`

#### Evaluation
`$regex`, `$mod`, `$text`, `$where`, `$expr`, `$jsonSchema`

#### Array
`$size`, `$all`, `$elemMatch`

#### Geospatial
`$near`, `$nearSphere`, `$geoIntersects`, `$geoWithin`, `$box`, `$center`, `$centerSphere`, `$polygon`

---

## Advanced Features

### Schema Validation

```javascript
const schema = new localgoose.Schema({
  email: {
    type:     String,
    required: true,
    validate: {
      validator: v => /\S+@\S+\.\S+/.test(v),
      message:   props => `${props.value} is not a valid email!`
    }
  },
  age: {
    type: Number,
    min:  [18, 'Must be at least 18'],
    max:  [120, 'Must be no more than 120']
  },
  password: {
    type:      String,
    minlength: 8,
    match:     /^(?=.*\d)(?=.*[a-z])(?=.*[A-Z])/
  },
  role: {
    type:    String,
    enum:    ['admin', 'user', 'guest'],
    default: 'user'
  }
});
```

### Schema Plugins

```javascript
function timestampPlugin(schema, options) {
  schema.add({ createdAt: { type: Date, default: Date.now } });
  schema.pre('save', function () { this.createdAt = new Date(); });
}

schema.plugin(timestampPlugin, { option: true });
```

### Schema Inheritance

```javascript
const baseSchema = new localgoose.Schema({
  createdAt: { type: Date, default: Date.now }
});

const userSchema = new localgoose.Schema({
  username: String,
  email:    String
});

// Merge baseSchema into userSchema
userSchema.add(baseSchema);

// Or use extend (alias)
userSchema.extend(baseSchema);
```

### Schema Discrimination

```javascript
const animalSchema = new localgoose.Schema({ name: String });
const Animal = db.model('Animal', animalSchema);

// Discriminator adds a `_type` field to distinguish subtypes
const dogSchema  = new localgoose.Schema({ breed: String });
const catSchema  = new localgoose.Schema({ indoor: Boolean });

const Dog = Animal.discriminator('Dog', dogSchema);
const Cat = Animal.discriminator('Cat', catSchema);
```

### `localgoose.ObjectId` and `localgoose.Types`

```javascript
// Generate a new ObjectId
const id = new localgoose.ObjectId();
const id = new localgoose.Types.ObjectId();

// Check type
console.log(id instanceof localgoose.Types.ObjectId); // true
```

### `localgoose.flushDisk()`

Writes are buffered with a 50 ms delay for performance. Call `flushDisk()` to force an immediate write — e.g. before process exit or before a backup.

```javascript
await localgoose.flushDisk();
```

---

## File Structure

Each model's data is stored in a separate JSON file in the database directory:

```
mydb/
  ├── User.json
  ├── Post.json
  └── Comment.json
```

---

## Error Handling

Localgoose provides detailed error messages for:
- Schema validation failures (required fields, enum, min/max, custom validators)
- Type casting errors
- Unique constraint violations
- Query execution errors
- Reference population errors

---

## Best Practices

1. **Always `await` the connection** — `localgoose.connect()` is async.
2. **Use lean queries** when you only need plain data, not Document instances.
3. **Paginate large result sets** with `.paginate(page, limit)` or `.skip().limit()`.
4. **Backup before destructive operations** and call `localgoose.flushDisk()` first.
5. **Keep collections small** — the entire JSON file is loaded into memory per operation.
6. **Use indexes** for frequently-queried fields to reduce scan time.
7. **Use `schema.plugin()`** to share common behaviour (timestamps, soft deletes, etc.) across schemas.

---

## Limitations

- **Not suitable for large datasets** — the entire collection file is loaded into memory on every operation. Keep files under ~10 MB per collection.
- **No real ACID transactions** — `startSession()`, `startTransaction()`, `commitTransaction()`, and `abortTransaction()` are stubs for Mongoose API compatibility only.
- **Limited query performance** — all filtering is done in JavaScript (linear scan); no B-tree indexes.
- **In-memory population** — `populate()` is fully supported but all joins are performed in memory; no server-side `$lookup` optimization.
- **No real-time updates** — `watch()` throws `'Watch is not supported in file-based storage'`.
- **No cursor support** — `query.cursor()` throws `'Cursors are not supported in file-based storage'`.
- **No `mapReduce`** — `Model.mapReduce()` throws an error.
- **No distributed / cross-process safety** — file locks are in-process only; simultaneous access from multiple Node.js processes is unsafe.
- **Asynchronous write flush** — writes are batched with a 50 ms delay. A crash within that window can lose the latest write. Use `localgoose.flushDisk()` to mitigate.
- **`$changeStream` / `$documents` aggregation stages** — listed as valid for API compatibility but silently no-op.
- **No distributed operations** — designed for single-machine, single-process use.

---

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## License

MIT

## Author

[Anas Qiblawi](https://github.com/AnasQiblawi)

## Acknowledgments

Inspired by [Mongoose](https://mongoosejs.com/), the elegant MongoDB ODM for Node.js.