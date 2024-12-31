# Localgoose

A lightweight, file-based ODM (Object-Document Mapper) for Node.js, inspired by Mongoose but designed for local JSON storage. Perfect for prototypes, small applications, and scenarios where a full MongoDB setup isn't needed.

## Features

- 🚀 Mongoose-like API for familiar development experience
- 📁 JSON file-based storage
- 🔄 Schema validation and type casting
- 🎯 Rich query API with chainable methods
- 📊 Aggregation pipeline support
- 🔌 Virtual properties and middleware hooks
- 🏃‍♂️ Zero external dependencies (except BSON for ObjectIds)
- 🔗 Support for related models and references
- 📝 Comprehensive CRUD operations
- 🔍 Advanced querying and filtering
- 🔎 Full-text search capabilities
- 📑 Compound indexing support
- 🔄 Schema inheritance and discrimination
- 🎨 Custom type casting and validation
- 🗄️ Backup and restore functionality
- 🧩 Custom types and schema inheritance
- 🛠️ Middleware hooks for documents, queries, and aggregations
- 🌐 Geospatial queries and indexing
- 📅 Date operators and bitwise operators

## Installation

```bash
npm install localgoose
```

## Quick Start

```javascript
const { localgoose } = require('localgoose');

// Connect to a local directory for storage
const db = localgoose.connect('./mydb');

// Define schemas for related models
const userSchema = new localgoose.Schema({
  username: { type: String, required: true },
  email: { type: String, required: true },
  age: { type: Number, required: true },
  tags: { type: Array, default: [] }
});

const postSchema = new localgoose.Schema({
  title: { type: String, required: true },
  content: { type: String, required: true },
  author: { type: localgoose.Schema.Types.ObjectId, ref: 'User' },
  likes: { type: Number, default: 0 }
});

// Create models
const User = db.model('User', userSchema);
const Post = db.model('Post', postSchema);

// Create a user
const user = await User.create({
  username: 'john',
  email: 'john@example.com',
  age: 25,
  tags: ['developer']
});

// Create a post with reference to user
const post = await Post.create({
  title: 'Getting Started',
  content: 'Hello World!',
  author: user._id
});

// Query with population
const posts = await Post.find()
  .populate('author')
  .sort('-likes')
  .exec();

// Use aggregation pipeline
const stats = await Post.aggregate()
  .match({ author: user._id })
  .group({
    _id: null,
    totalPosts: { $sum: 1 },
    avgLikes: { $avg: '$likes' }
  })
  .exec();
```

## API Reference

### Connection

```javascript
// Connect to database
const db = await localgoose.connect('./mydb');

// Create separate connection
const connection = await localgoose.createConnection('./mydb');
```

### Schema Definition

```javascript
const schema = new localgoose.Schema({
  // Basic types
  string: { type: String, required: true },
  number: { type: Number, default: 0 },
  boolean: { type: Boolean },
  date: { type: Date, default: Date.now },
  objectId: { type: localgoose.Schema.Types.ObjectId, ref: 'OtherModel' },
  buffer: localgoose.Schema.Types.Buffer,
  uuid: localgoose.Schema.Types.UUID,
  bigInt: localgoose.Schema.Types.BigInt,
  mixed: localgoose.Schema.Types.Mixed,
  map: localgoose.Schema.Types.Map,
  
  // Arrays and Objects
  array: { type: Array, default: [] },
  object: {
    type: Object,
    default: {
      key: 'value'
    }
  }
});

// Virtual properties
schema.virtual('fullName').get(function() {
  return `${this.firstName} ${this.lastName}`;
});

// Instance methods
schema.method('getInfo', function() {
  return `${this.username} (${this.age})`;
});

// Static methods
schema.static('findByEmail', function(email) {
  return this.findOne({ email });
});

// Middleware
schema.pre('save', function() {
  this.updatedAt = new Date();
});

schema.post('save', function() {
  console.log('Document saved:', this._id);
});

// Indexes
schema.index({ email: 1 }, { unique: true });
schema.index({ title: 'text', content: 'text' });
```

### Model Operations

#### Create
```javascript
// Single document
const doc = await Model.create({
  field: 'value'
});

// Multiple documents
const docs = await Model.create([
  { field: 'value1' },
  { field: 'value2' }
]);
```

#### Read
```javascript
// Find all
const docs = await Model.find();

// Find with conditions
const docs = await Model.find({
  field: 'value'
});

// Find one
const doc = await Model.findOne({
  field: 'value'
});

// Find by ID
const doc = await Model.findById(id);

// Find with population
const doc = await Model.findOne({ field: 'value' })
  .populate('reference')
  .exec();
```

#### Update
```javascript
// Update one
const result = await Model.updateOne(
  { field: 'value' },
  { $set: { newField: 'newValue' }}
);

// Update many
const result = await Model.updateMany(
  { field: 'value' },
  { $set: { newField: 'newValue' }}
);

// Save changes to document
doc.field = 'new value';
await doc.save();
```

#### Delete
```javascript
// Delete one
const result = await Model.deleteOne({
  field: 'value'
});

// Delete many
const result = await Model.deleteMany({
  field: 'value'
});
```

### Query API

```javascript
// Chainable query methods
const docs = await Model.find()
  .where('field').equals('value')
  .where('number').gt(10).lt(20)
  .where('tags').in(['tag1', 'tag2'])
  .select('field1 field2')
  .sort('-field')
  .skip(10)
  .limit(5)
  .populate('reference')
  .exec();

// Advanced queries with geospatial support
const docs = await Model.find()
  .where('location')
  .near({
    center: [longitude, latitude],
    maxDistance: 5000
  })
  .exec();

// Text search
const docs = await Model.find()
  .where('$text')
  .equals({ $search: 'keyword' })
  .exec();
```

### Aggregation Pipeline

```javascript
const results = await Model.aggregate()
  .match({ field: 'value' })
  .group({
    _id: '$groupField',
    total: { $sum: 1 },
    avg: { $avg: '$numField' }
  })
  .sort({ total: -1 })
  .limit(5)
  .exec();
```

## Backup and Restore

```javascript
// Create backup
const backupPath = await Model.backup();

// Restore from backup
await Model.restore(backupPath);

// List backups
const backups = await Model.listBackups();

// Clean up old backups
await Model.cleanupBackups();
```

### Supported Update Operators

#### Field Update Operators
- `$set`: Sets the value of a field
- `$unset`: Removes the specified field from a document
- `$rename`: Renames a field
- `$setOnInsert`: Sets the value of a field if an update results in an insert

#### Increment/Decrement Operators
- `$inc`: Increments the value of a field by the specified amount
- `$mul`: Multiplies the value of a field by the specified amount
- `$min`: Updates the field only if the specified value is less than the existing value
- `$max`: Updates the field only if the specified value is greater than the existing value

#### Array Update Operators
- `$push`: Adds an item to an array
- `$pull`: Removes all array elements that match a specified query
- `$addToSet`: Adds elements to an array only if they do not already exist
- `$pop`: Removes the first or last item from an array
- `$pullAll`: Removes all matching values from an array

#### Bitwise Operators
- `$bit`: Performs bitwise AND, OR, and XOR updates of integer values

#### Date Operators
- `$currentDate`: Sets the value of a field to the current date

### Supported Query Operators

- `equals`: Exact match
- `gt`: Greater than
- `gte`: Greater than or equal
- `lt`: Less than
- `lte`: Less than or equal
- `in`: Match any value in array
- `nin`: Not match any value in array
- `regex`: Regular expression match
- `exists`: Check for existence of a field
- `size`: Match the size of an array
- `mod`: Match documents where the value of a field modulo some divisor is equal to a specified remainder
- `near`: Find documents near a specified point
- `maxDistance`: Limit the results to documents within a specified distance from the point
- `within`: Find documents within a specified shape
- `box`: Find documents within a rectangular box
- `center`: Find documents within a specified circle
- `centerSphere`: Find documents within a specified spherical circle
- `polygon`: Find documents within a specified polygon
- `geoIntersects`: Find documents that intersect a specified geometry
- `nearSphere`: Find documents near a specified point using spherical geometry
- `text`: Full-text search
- `or`: Logical OR
- `nor`: Logical NOR
- `and`: Logical AND
- `elemMatch`: Match documents that contain an array field with at least one element that matches all the specified query criteria

### Supported Aggregation Operators

- `$match`: Filter documents
- `$group`: Group documents by expression
- `$sort`: Sort documents
- `$limit`: Limit number of documents
- `$skip`: Skip number of documents
- `$unwind`: Deconstruct array field
- `$lookup`: Perform left outer join
- `$project`: Reshape documents
- `$addFields`: Add new fields
- `$facet`: Process multiple aggregation pipelines
- `$bucket`: Categorize documents into buckets
- `$sortByCount`: Group and count documents
- `$densify`: Fill gaps in time-series data
- `$graphLookup`: Perform recursive search on a collection
- `$unionWith`: Combine documents from another collection
- `$count`: Count the number of documents
- `$out`: Write the result to a collection
- `$merge`: Merge the result with a collection
- `$replaceRoot`: Replace the input document with the specified document
- `$set`: Add new fields or update existing fields in documents
- `$unset`: Remove specified fields from documents

### Supported Group Accumulators

- `$sum`: Calculate sum
- `$avg`: Calculate average
- `$min`: Get minimum value
- `$max`: Get maximum value
- `$push`: Accumulate values into array
- `$first`: Get first value
- `$last`: Get last value
- `$addToSet`: Add unique values to array
- `$stdDevPop`: Calculate population standard deviation
- `$stdDevSamp`: Calculate sample standard deviation
- `$mergeObjects`: Merge objects into a single object

## Advanced Features

### Schema Validation

```javascript
const schema = new localgoose.Schema({
  email: {
    type: String,
    required: true,
    validate: {
      validator: function(v) {
        return /\S+@\S+\.\S+/.test(v);
      },
      message: props => `${props.value} is not a valid email!`
    }
  },
  age: {
    type: Number,
    min: [18, 'Must be at least 18'],
    max: [120, 'Must be no more than 120']
  },
  password: {
    type: String,
    minlength: 8,
    match: /^(?=.*\d)(?=.*[a-z])(?=.*[A-Z])/
  }
});
```

### Middleware Hooks

```javascript
// Document middleware
schema.pre('save', async function() {
  if (this.isModified('password')) {
    this.password = await hash(this.password);
  }
});

// Query middleware
schema.pre('find', function() {
  this.where({ isActive: true });
});

// Aggregation middleware
schema.pre('aggregate', function() {
  this.pipeline().unshift({ $match: { isDeleted: false } });
});
```

### Virtual Population

```javascript
schema.virtual('posts', {
  ref: 'Post',
  localField: '_id',
  foreignField: 'author',
  justOne: false,
  options: { sort: { createdAt: -1 } }
});
```

### Schema Inheritance

```javascript
const baseSchema = new localgoose.Schema({
  name: String,
  createdAt: Date
});

const userSchema = new localgoose.Schema({
  email: String,
  password: String
});

userSchema.add(baseSchema);
```

### Custom Types

```javascript
class Point {
  constructor(x, y) {
    this.x = x;
    this.y = y;
  }
}

const pointSchema = new localgoose.Schema({
  location: {
    type: Point,
    validate: {
      validator: v => v instanceof Point,
      message: 'Invalid point'
    }
  }
});
```

## File Structure

Each model's data is stored in a separate JSON file:

```
mydb/
  ├── User.json
  ├── Post.json
  └── Comment.json
```

## Error Handling

Localgoose provides detailed error messages for:
- Schema validation failures
- Required field violations
- Type casting errors
- Query execution errors
- Reference population errors

## Best Practices

1. **Schema Design**
   - Define schemas with proper types and validation
   - Use references for related data
   - Implement virtual properties for computed fields
   - Add middleware for common operations

2. **Querying**
   - Use proper query operators
   - Limit result sets for better performance
   - Use projection to select only needed fields
   - Populate references only when needed

3. **File Management**
   - Regularly backup your JSON files
   - Monitor file sizes
   - Implement proper error handling
   - Use atomic operations when possible

4. **Performance Optimization**
   - Use indexes for frequently queried fields
   - Implement pagination for large datasets
   - Cache frequently accessed data
   - Use lean queries when possible

5. **Data Integrity**
   - Implement proper validation
   - Use transactions when needed
   - Handle errors gracefully
   - Keep backups up to date

## Limitations

- Not suitable for large datasets (>10MB per collection)
- No support for transactions
- Limited query performance compared to real databases
- Basic relationship support through references
- No real-time updates or change streams
- No distributed operations

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## License

MIT

## Author

[Anas Qiblawi](https://github.com/AnasQiblawi)

## Acknowledgments

Inspired by Mongoose, the elegant MongoDB ODM for Node.js.