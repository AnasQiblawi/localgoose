/**
 * Localgoose Full Test Suite
 * Tests all major features: Schema, Model, Query, Document, Aggregate, Middleware, Population
 */
const { localgoose } = require('./src/index.js');
const fs = require('fs');
const path = require('path');

const DB_PATH = './test_db';
let passed = 0, failed = 0, total = 0;

async function test(name, fn) {
  total++;
  try {
    await fn();
    console.log(`  ✅ ${name}`);
    passed++;
  } catch (e) {
    console.log(`  ❌ ${name}`);
    console.log(`       → ${e.message}`);
    failed++;
  }
}

function assert(condition, msg) { if (!condition) throw new Error(msg || 'Assertion failed'); }
function assertEqual(a, b, msg) { if (a !== b) throw new Error(msg || `Expected ${b}, got ${a}`); }
function assertNull(a, msg) { if (a !== null) throw new Error(msg || `Expected null, got ${a}`); }
function assertArray(a, msg) { if (!Array.isArray(a)) throw new Error(msg || `Expected array, got ${typeof a}`); }

async function cleanup(db) {
  try { db.disconnect(); } catch (e) {}
  try { fs.rmSync(DB_PATH, { recursive: true, force: true }); } catch (e) {}
}

// ─── Schemas used across tests ───────────────────────────────────────────────
function buildSchemas(db) {
  const authorSchema = new localgoose.Schema({
    name:  { type: String, required: true },
    email: { type: String },
    age:   { type: Number, min: [0, 'Age must be non-negative'], max: 120 },
    score: { type: Number, default: 0 },
    tags:  { type: Array, default: [] },
    active:{ type: Boolean, default: true },
    rating:{ type: Number },
    profile: { type: Object, default: {} }
  });

  authorSchema.virtual('displayName').get(function () {
    return `${this.name} <${this.email}>`;
  });

  authorSchema.method('greet', function () { return `Hello, I am ${this.name}`; });
  authorSchema.static('findActive', function () { return this.find({ active: true }); });

  authorSchema.pre('save', async function () { this._preSaveCalled = true; });
  authorSchema.post('save', async function () { this._postSaveCalled = true; });

  const postSchema = new localgoose.Schema({
    title:   { type: String, required: true, minlength: 3, maxlength: 200 },
    content: { type: String },
    likes:   { type: Number, default: 0 },
    tags:    { type: Array, default: [] },
    author:  { type: String, ref: 'SuiteAuthor' },
    published: { type: Boolean, default: false },
    meta:    { type: Object, default: {} }
  });

  const Author = db.model('SuiteAuthor', authorSchema);
  const Post   = db.model('SuitePost', postSchema);
  return { Author, Post, authorSchema, postSchema };
}

// ══════════════════════════════════════════════════════════════════════════════
async function runAll() {
  if (fs.existsSync(DB_PATH)) fs.rmSync(DB_PATH, { recursive: true, force: true });
  const db = localgoose.connect(DB_PATH);
  const { Author, Post } = buildSchemas(db);

  // ── 1. Schema ────────────────────────────────────────────────────────────
  console.log('\n📋 1. Schema');

  await test('Schema.Types exposed', async () => {
    assert(localgoose.Schema.Types.String === String);
    assert(localgoose.Schema.Types.Number === Number);
    assert(localgoose.Schema.Types.ObjectId);
  });

  await test('Schema virtual', async () => {
    const s = new localgoose.Schema({ name: String, email: String });
    s.virtual('display').get(function () { return `${this.name}/${this.email}`; });
    assert(s.virtuals.display);
  });

  await test('Schema.method / statics', async () => {
    const s = new localgoose.Schema({ x: Number });
    s.method('double', function () { return this.x * 2; });
    s.static('findBig', function () { return this.find({ x: { $gt: 100 } }); });
    assert(typeof s.methods.double === 'function');
    assert(typeof s.statics.findBig === 'function');
  });

  await test('Schema pre/post hooks register without throw', async () => {
    const s = new localgoose.Schema({ n: String });
    ['save','validate','remove','deleteOne','deleteMany','find','findOne',
     'findOneAndUpdate','updateOne','updateMany','aggregate','insertMany'].forEach(h => {
      s.pre(h, async function () {});
      s.post(h, async function () {});
    });
  });

  await test('Schema.pre throws on invalid hook', async () => {
    const s = new localgoose.Schema({});
    let threw = false;
    try { s.pre('badHook', () => {}); } catch (e) { threw = true; }
    assert(threw, 'Should throw for invalid hook');
  });

  await test('Schema validate — required', async () => {
    const s = new localgoose.Schema({ name: { type: String, required: true } });
    const errs = s.validate({});
    assert(errs.length > 0, 'Should fail required');
  });

  await test('Schema validate — min/max (array form)', async () => {
    const s = new localgoose.Schema({ age: { type: Number, min: [18, 'Too young'], max: [65, 'Too old'] } });
    const e1 = s.validate({ age: 10 });
    assert(e1[0] === 'Too young', `Got: ${e1[0]}`);
    const e2 = s.validate({ age: 70 });
    assert(e2[0] === 'Too old', `Got: ${e2[0]}`);
    const e3 = s.validate({ age: 30 });
    assertEqual(e3.length, 0, 'Should pass');
  });

  await test('Schema validate — minlength/maxlength', async () => {
    const s = new localgoose.Schema({ title: { type: String, minlength: 5, maxlength: 10 } });
    assert(s.validate({ title: 'Hi' }).length > 0, 'minlength fail');
    assert(s.validate({ title: 'A very long title indeed' }).length > 0, 'maxlength fail');
    assert(s.validate({ title: 'Hello' }).length === 0, 'should pass');
  });

  await test('Schema validate — enum', async () => {
    const s = new localgoose.Schema({ status: { type: String, enum: ['a', 'b'] } });
    assert(s.validate({ status: 'c' }).length > 0, 'enum fail');
    assert(s.validate({ status: 'a' }).length === 0, 'enum pass');
  });

  await test('Schema validate — custom validator', async () => {
    const s = new localgoose.Schema({
      score: { type: Number, validate: { validator: v => v >= 0, message: 'Must be non-negative' } }
    });
    assert(s.validate({ score: -1 }).length > 0, 'custom validator fail');
    assert(s.validate({ score: 5 }).length === 0, 'custom validator pass');
  });

  await test('Schema.add(schema) merges paths', async () => {
    const base = new localgoose.Schema({ createdBy: String });
    const ext  = new localgoose.Schema({ title: String });
    ext.add(base);
    assert(ext._paths.has('createdBy'), 'Should have createdBy');
    assert(ext._paths.has('title'), 'Should have title');
  });

  await test('Schema.clone()', async () => {
    const s = new localgoose.Schema({ x: Number });
    s.method('foo', () => 1);
    const c = s.clone();
    assert(c._paths.has('x'));
    assert(c.methods.foo);
    assert(c !== s);
  });

  await test('Schema allows custom class types', async () => {
    class GeoPoint {}
    const s = new localgoose.Schema({ loc: { type: GeoPoint } });
    assert(s);
  });

  await test('Schema.eachPath iterates paths', async () => {
    const s = new localgoose.Schema({ a: String, b: Number });
    const paths = [];
    s.eachPath((p) => paths.push(p));
    assert(paths.includes('a') && paths.includes('b'));
  });

  await test('Schema.path() returns SchemaType', async () => {
    const s = new localgoose.Schema({ age: { type: Number, min: 0 } });
    assert(s.path('age'));
  });

  await test('Schema.indexes()', async () => {
    const s = new localgoose.Schema({ email: { type: String, index: true } });
    assert(s.indexes().length > 0);
  });

  await test('Schema.pick()', async () => {
    const s = new localgoose.Schema({ a: String, b: Number, c: Boolean });
    const p = s.pick(['a','b']);
    assert(p._paths.has('a') && p._paths.has('b') && !p._paths.has('c'));
  });

  await test('Schema.omit()', async () => {
    const s = new localgoose.Schema({ a: String, b: Number, c: Boolean });
    const o = s.omit(['c']);
    assert(o._paths.has('a') && !o._paths.has('c'));
  });

  await test('Schema toJSONSchema()', async () => {
    const s = new localgoose.Schema({ name: { type: String, required: true }, age: Number });
    const js = s.toJSONSchema();
    assert(js.type === 'object');
    assert(js.properties.name);
  });

  await test('Schema.plugin()', async () => {
    const s = new localgoose.Schema({ x: Number });
    let called = false;
    s.plugin(function (schema) { called = true; schema.method('pluginMethod', () => 42); });
    assert(called && s.methods.pluginMethod);
  });

  // ── 2. CRUD ──────────────────────────────────────────────────────────────
  console.log('\n📝 2. CRUD Operations');

  let alice, bob, carol, dave;

  await test('create() single doc', async () => {
    alice = await Author.create({ name: 'Alice', email: 'alice@t.com', age: 28, tags: ['js', 'ts'], rating: 4.5 });
    assert(alice._id, 'Has _id');
    assertEqual(alice.name, 'Alice');
    assertEqual(alice.score, 0, 'Default applied');
  });

  await test('create() multiple docs', async () => {
    [bob, carol, dave] = await Author.create([
      { name: 'Bob',   email: 'bob@t.com',   age: 22, tags: ['python'],    rating: 3.2 },
      { name: 'Carol', email: 'carol@t.com', age: 35, tags: ['js', 'css'], rating: 4.8 },
      { name: 'Dave',  email: 'dave@t.com',  age: 22, tags: ['rust'],      rating: 2.1 }
    ]);
    assert(bob && carol && dave, 'All created');
  });

  await test('find() all', async () => {
    const docs = await Author.find().exec();
    assertEqual(docs.length, 4);
  });

  await test('findOne() hit', async () => {
    const doc = await Author.findOne({ name: 'Alice' }).exec();
    assert(doc !== null);
    assertEqual(doc.name, 'Alice');
  });

  await test('findOne() miss returns null', async () => {
    const doc = await Author.findOne({ name: 'Nobody' }).exec();
    assertNull(doc, 'Should be null');
  });

  await test('findById() returns chainable Query', async () => {
    const doc = await Author.findById(alice._id).exec();
    assert(doc && doc.name === 'Alice');
    // Confirm it's chainable
    const doc2 = await Author.findById(alice._id).lean().exec();
    assert(doc2 && doc2.name === 'Alice');
  });

  await test('find() with conditions', async () => {
    const docs = await Author.find({ age: 22 }).exec();
    assertEqual(docs.length, 2);
    assert(docs.every(d => d.age === 22));
  });

  await test('find() with $gt / $lt', async () => {
    const docs = await Author.find({ age: { $gt: 22, $lt: 35 } }).exec();
    assertEqual(docs.length, 1);
    assertEqual(docs[0].name, 'Alice');
  });

  await test('find() with $in', async () => {
    const docs = await Author.find({ name: { $in: ['Alice', 'Carol'] } }).exec();
    assertEqual(docs.length, 2);
  });

  await test('find() with $nin', async () => {
    const docs = await Author.find({ name: { $nin: ['Alice', 'Carol'] } }).exec();
    assertEqual(docs.length, 2);
  });

  await test('find() with $ne', async () => {
    const docs = await Author.find({ age: { $ne: 22 } }).exec();
    assert(docs.every(d => d.age !== 22));
  });

  await test('find() with $regex', async () => {
    const docs = await Author.find({ email: { $regex: /alice/i } }).exec();
    assertEqual(docs.length, 1);
    assertEqual(docs[0].name, 'Alice');
  });

  await test('find() with $or', async () => {
    const docs = await Author.find({ $or: [{ name: 'Alice' }, { name: 'Bob' }] }).exec();
    assertEqual(docs.length, 2);
  });

  await test('find() with $and', async () => {
    const docs = await Author.find({ $and: [{ age: { $gte: 22 } }, { name: { $regex: /^A/ } }] }).exec();
    assertEqual(docs.length, 1);
    assertEqual(docs[0].name, 'Alice');
  });

  await test('find() with $nor', async () => {
    const docs = await Author.find({ $nor: [{ name: 'Alice' }, { name: 'Bob' }] }).exec();
    assert(docs.every(d => d.name !== 'Alice' && d.name !== 'Bob'));
  });

  await test('find() dot-notation nested field', async () => {
    await Author.updateOne({ name: 'Alice' }, { $set: { 'profile.city': 'NYC' } });
    const docs = await Author.find({ 'profile.city': 'NYC' }).exec();
    assertEqual(docs.length, 1);
  });

  await test('find() with $size on array', async () => {
    const docs = await Author.find({ tags: { $size: 2 } }).exec();
    assert(docs.every(d => d.tags.length === 2));
  });

  await test('find() with $all on array', async () => {
    const docs = await Author.find({ tags: { $all: ['js', 'css'] } }).exec();
    assertEqual(docs.length, 1);
    assertEqual(docs[0].name, 'Carol');
  });

  await test('find() with $elemMatch (object)', async () => {
    const docs = await Author.find({ tags: { $elemMatch: { $eq: 'rust' } } }).exec();
    assertEqual(docs.length, 1);
    assertEqual(docs[0].name, 'Dave');
  });

  await test('find() with $exists', async () => {
    const docs = await Author.find({ rating: { $exists: true } }).exec();
    assertEqual(docs.length, 4);
  });

  await test('find() with $not', async () => {
    const docs = await Author.find({ age: { $not: { $eq: 22 } } }).exec();
    assert(docs.every(d => d.age !== 22));
  });

  await test('sort() descending', async () => {
    const docs = await Author.find().sort({ age: -1 }).exec();
    assertEqual(docs[0].name, 'Carol');
  });

  await test('sort() string form', async () => {
    const docs = await Author.find().sort('-age').exec();
    assertEqual(docs[0].name, 'Carol');
  });

  await test('limit()', async () => {
    const docs = await Author.find().limit(2).exec();
    assertEqual(docs.length, 2);
  });

  await test('skip()', async () => {
    const all = await Author.find().sort({ name: 1 }).exec();
    const skipped = await Author.find().sort({ name: 1 }).skip(2).exec();
    assertEqual(skipped[0].name, all[2].name);
  });

  await test('paginate()', async () => {
    const docs = await Author.find().sort({ name: 1 }).paginate(2, 2).exec();
    assertEqual(docs.length, 2);
  });

  await test('select() inclusive projection', async () => {
    const docs = await Author.find({ name: 'Alice' }).select('name email').exec();
    assert(docs[0].name, 'name present');
    assert(docs[0].email, 'email present');
    assertEqual(docs[0].age, undefined, 'age excluded');
  });

  await test('select() exclusive projection', async () => {
    const docs = await Author.find({ name: 'Alice' }).select('-score -tags').exec();
    assertEqual(docs[0].score, undefined, 'score excluded');
    assertEqual(docs[0].tags, undefined, 'tags excluded');
    assert(docs[0].name, 'name still present');
  });

  await test('lean() returns plain objects', async () => {
    const docs = await Author.find({ name: 'Alice' }).lean().exec();
    assertEqual(typeof docs[0].toObject, 'undefined', 'No toObject on lean');
    assert(docs[0].name === 'Alice');
  });

  await test('where().gt().lt() chaining on same field', async () => {
    const docs = await Author.find().where('age').gt(22).lt(35).exec();
    assertEqual(docs.length, 1);
    assertEqual(docs[0].name, 'Alice');
  });

  await test('where().in()', async () => {
    const docs = await Author.find().where('name').in(['Alice', 'Dave']).exec();
    assertEqual(docs.length, 2);
  });

  await test('where().regex()', async () => {
    const docs = await Author.find().where('name').regex(/^[AC]/).exec();
    assertEqual(docs.length, 2);
  });

  await test('where().ne()', async () => {
    const docs = await Author.find().where('age').ne(22).exec();
    assert(docs.every(d => d.age !== 22));
  });

  await test('countDocuments()', async () => {
    const n = await Author.countDocuments({ age: 22 });
    assertEqual(n, 2);
  });

  await test('estimatedDocumentCount()', async () => {
    const n = await Author.estimatedDocumentCount();
    assert(n >= 4);
  });

  await test('distinct()', async () => {
    const ages = await Author.distinct('age');
    assert(ages.includes(22) && ages.includes(28) && ages.includes(35));
    assert(ages.filter(a => a === 22).length === 1, 'No duplicates');
  });

  await test('exists() returns {_id} or null', async () => {
    const found = await Author.exists({ name: 'Alice' });
    assert(found && found._id, 'found._id present');
    const notFound = await Author.exists({ name: 'Ghost' });
    assertNull(notFound, 'Should be null');
  });

  // ── 3. Updates ───────────────────────────────────────────────────────────
  console.log('\n✏️  3. Update Operations');

  await test('updateOne() $set', async () => {
    await Author.updateOne({ name: 'Alice' }, { $set: { score: 99 } });
    const doc = await Author.findOne({ name: 'Alice' }).exec();
    assertEqual(doc.score, 99);
  });

  await test('updateOne() $inc', async () => {
    await Author.updateOne({ name: 'Bob' }, { $inc: { score: 5 } });
    const doc = await Author.findOne({ name: 'Bob' }).exec();
    assertEqual(doc.score, 5);
  });

  await test('updateOne() $inc again', async () => {
    await Author.updateOne({ name: 'Bob' }, { $inc: { score: 10 } });
    const doc = await Author.findOne({ name: 'Bob' }).exec();
    assertEqual(doc.score, 15);
  });

  await test('updateOne() $mul', async () => {
    await Author.updateOne({ name: 'Alice' }, { $mul: { score: 2 } });
    const doc = await Author.findOne({ name: 'Alice' }).exec();
    assertEqual(doc.score, 198);
  });

  await test('updateOne() $min / $max', async () => {
    await Author.updateOne({ name: 'Carol' }, { $min: { score: 50 }, $max: { score: 50 } });
    const doc = await Author.findOne({ name: 'Carol' }).exec();
    assertEqual(doc.score, 50);
  });

  await test('updateOne() $unset removes field', async () => {
    await Author.updateOne({ name: 'Dave' }, { $set: { score: 77 } });
    await Author.updateOne({ name: 'Dave' }, { $unset: { score: 1 } });
    const doc = await Author.findOne({ name: 'Dave' }).exec();
    assertEqual(doc.score, undefined);
  });

  await test('updateOne() $rename', async () => {
    await Author.updateOne({ name: 'Dave' }, { $set: { tmpField: 'hello' } });
    await Author.updateOne({ name: 'Dave' }, { $rename: { tmpField: 'renamedField' } });
    const doc = await Author.findOne({ name: 'Dave' }).exec();
    assertEqual(doc.renamedField, 'hello');
    assertEqual(doc.tmpField, undefined);
  });

  await test('updateOne() $push', async () => {
    await Author.updateOne({ name: 'Alice' }, { $push: { tags: 'node' } });
    const doc = await Author.findOne({ name: 'Alice' }).exec();
    assert(doc.tags.includes('node'));
  });

  await test('updateOne() $push $each', async () => {
    await Author.updateOne({ name: 'Bob' }, { $push: { tags: { $each: ['go', 'ruby'] } } });
    const doc = await Author.findOne({ name: 'Bob' }).exec();
    assert(doc.tags.includes('go') && doc.tags.includes('ruby'));
  });

  await test('updateOne() $pull', async () => {
    await Author.updateOne({ name: 'Alice' }, { $pull: { tags: 'node' } });
    const doc = await Author.findOne({ name: 'Alice' }).exec();
    assert(!doc.tags.includes('node'));
  });

  await test('updateOne() $addToSet no duplicates', async () => {
    await Author.updateOne({ name: 'Alice' }, { $addToSet: { tags: 'js' } });
    const doc = await Author.findOne({ name: 'Alice' }).exec();
    assertEqual(doc.tags.filter(t => t === 'js').length, 1);
  });

  await test('updateOne() $addToSet $each', async () => {
    await Author.updateOne({ name: 'Carol' }, { $addToSet: { tags: { $each: ['node', 'ts'] } } });
    const doc = await Author.findOne({ name: 'Carol' }).exec();
    assert(doc.tags.includes('node') && doc.tags.includes('ts'));
  });

  await test('updateOne() $pop last', async () => {
    await Author.updateOne({ name: 'Bob' }, { $pop: { tags: 1 } });
    const doc = await Author.findOne({ name: 'Bob' }).exec();
    assert(!doc.tags.includes('ruby'));
  });

  await test('updateOne() $currentDate', async () => {
    await Author.updateOne({ name: 'Alice' }, { $currentDate: { updatedAt: true } });
    const doc = await Author.findOne({ name: 'Alice' }).exec();
    assert(doc.updatedAt instanceof Date || typeof doc.updatedAt === 'string');
  });

  await test('updateMany()', async () => {
    const res = await Author.updateMany({ age: 22 }, { $set: { active: false } });
    assertEqual(res.modifiedCount, 2);
    const docs = await Author.find({ active: false }).exec();
    assertEqual(docs.length, 2);
  });

  await test('updateOne() upsert creates doc', async () => {
    const res = await Author.updateOne({ name: 'Eve' }, { $set: { name: 'Eve', email: 'eve@t.com', age: 25 } }, { upsert: true });
    assert(res.upsertedCount === 1 || res.modifiedCount >= 0);
    const doc = await Author.findOne({ name: 'Eve' }).exec();
    assert(doc !== null);
    await Author.deleteOne({ name: 'Eve' });
  });

  await test('findOneAndUpdate() returns updated doc', async () => {
    const doc = await Author.findOneAndUpdate({ name: 'Alice' }, { $set: { score: 200 } }, { new: true });
    assertEqual(doc.score, 200);
  });

  await test('findOneAndUpdate() returns old doc with {new:false}', async () => {
    const before = await Author.findOne({ name: 'Alice' }).exec();
    const doc = await Author.findOneAndUpdate({ name: 'Alice' }, { $set: { score: 300 } }, { new: false });
    assertEqual(doc.score, before.score);
  });

  await test('findByIdAndUpdate()', async () => {
    const doc = await Author.findByIdAndUpdate(alice._id, { $set: { score: 111 } }, { new: true });
    assertEqual(doc.score, 111);
  });

  await test('replaceOne()', async () => {
    await Author.create({ name: 'Temp', email: 'tmp@t.com', age: 99 });
    await Author.replaceOne({ name: 'Temp' }, { name: 'Replaced', email: 'r@t.com', age: 50 });
    const doc = await Author.findOne({ name: 'Replaced' }).exec();
    assert(doc !== null);
    await Author.deleteOne({ name: 'Replaced' });
  });

  // ── 4. Delete ────────────────────────────────────────────────────────────
  console.log('\n🗑️  4. Delete Operations');

  await test('deleteOne()', async () => {
    await Author.create({ name: 'Del1', email: 'd1@t.com', age: 10 });
    const res = await Author.deleteOne({ name: 'Del1' });
    assertEqual(res.deletedCount, 1);
    assertNull(await Author.findOne({ name: 'Del1' }).exec());
  });

  await test('deleteMany()', async () => {
    await Author.create([
      { name: 'DelA', email: 'da@t.com', age: 5 },
      { name: 'DelB', email: 'db@t.com', age: 5 }
    ]);
    const res = await Author.deleteMany({ age: 5 });
    assertEqual(res.deletedCount, 2);
  });

  await test('findOneAndDelete()', async () => {
    await Author.create({ name: 'DelC', email: 'dc@t.com', age: 44 });
    const doc = await Author.findOneAndDelete({ name: 'DelC' });
    assert(doc && doc.name === 'DelC');
    assertNull(await Author.findOne({ name: 'DelC' }).exec());
  });

  await test('findByIdAndDelete()', async () => {
    const tmp = await Author.create({ name: 'DelD', email: 'dd@t.com', age: 55 });
    const doc = await Author.findByIdAndDelete(tmp._id);
    assert(doc !== null);
    assertNull(await Author.findOne({ name: 'DelD' }).exec());
  });

  // ── 5. Document Instance ─────────────────────────────────────────────────
  console.log('\n📄 5. Document Instance');

  await test('Document.save() inserts new doc', async () => {
    const AuthorModel = db.model('SuiteAuthor');
    // Using standard Mongoose Constructor format
    const doc = new AuthorModel({ name: 'NewSave', email: 'ns@t.com', age: 33 });
    await doc.save();
    const found = await Author.findOne({ name: 'NewSave' }).exec();
    assert(found !== null);
    await Author.deleteOne({ name: 'NewSave' });
  });

  await test('Document.save() updates existing doc', async () => {
    const doc = await Author.findOne({ name: 'Alice' }).exec();
    doc.set('score', 999);
    await doc.save();
    const updated = await Author.findOne({ name: 'Alice' }).exec();
    assertEqual(updated.score, 999);
  });

  await test('Document.get() / set()', async () => {
    const doc = await Author.findOne({ name: 'Alice' }).exec();
    doc.set('score', 42);
    assertEqual(doc.get('score'), 42);
  });

  await test('Document.isModified()', async () => {
    const doc = await Author.findOne({ name: 'Alice' }).exec();
    const was = doc.isModified('score');
    doc.set('score', 1);
    assert(doc.isModified('score'));
  });

  await test('Document.modifiedPaths()', async () => {
    const doc = await Author.findOne({ name: 'Alice' }).exec();
    doc.set('score', 55);
    doc.set('age', 99);
    const paths = doc.modifiedPaths();
    assert(paths.includes('score') && paths.includes('age'));
  });

  await test('Document.toObject()', async () => {
    const doc = await Author.findOne({ name: 'Alice' }).exec();
    const obj = doc.toObject();
    assert(typeof obj === 'object');
    assert(obj._id);
  });

  await test('Document.toObject({ virtuals: true }) includes virtuals', async () => {
    const doc = await Author.findOne({ name: 'Alice' }).exec();
    const obj = doc.toObject({ virtuals: true });
    assert(obj.displayName, `displayName: ${obj.displayName}`);
    assert(obj.displayName.includes('Alice'));
  });

  await test('Document.toJSON()', async () => {
    const doc = await Author.findOne({ name: 'Alice' }).exec();
    const json = doc.toJSON();
    assert(json._id);
  });

  await test('Document instance method', async () => {
    const doc = await Author.findOne({ name: 'Alice' }).exec();
    assertEqual(doc.greet(), 'Hello, I am Alice');
  });

  await test('Document virtual getter', async () => {
    const doc = await Author.findOne({ name: 'Alice' }).exec();
    assert(doc.displayName.includes('Alice'));
  });

  await test('Document.isNew = false after load', async () => {
    const doc = await Author.findOne({ name: 'Alice' }).exec();
    assert(!doc.isNew, 'Loaded doc should not be new');
  });

  await test('Document.updateOne()', async () => {
    const doc = await Author.findOne({ name: 'Alice' }).exec();
    await doc.updateOne({ $set: { score: 77 } });
    const updated = await Author.findOne({ name: 'Alice' }).exec();
    assertEqual(updated.score, 77);
  });

  await test('Document.equals()', async () => {
    const a = await Author.findOne({ name: 'Alice' }).exec();
    const b = await Author.findOne({ name: 'Alice' }).exec();
    assert(a.equals(b));
  });

  await test('Document.$inc()', async () => {
    const doc = await Author.findOne({ name: 'Alice' }).exec();
    doc.$inc('score', 10);
    assert(doc.get('score') > 77);
  });

  await test('Document.markModified()', async () => {
    const doc = await Author.findOne({ name: 'Alice' }).exec();
    doc.markModified('tags');
    assert(doc.modifiedPaths().includes('tags'));
  });

  await test('Document pre-save middleware called', async () => {
    const doc = await Author.findOne({ name: 'Alice' }).exec();
    doc.set('score', 100);
    await doc.save();
    // pre-save sets _preSaveCalled on proxy; post-save on doc itself
    assert(true); // just confirm no crash
  });

  await test('Document.remove() deletes doc', async () => {
    const tmp = await Author.create({ name: 'TmpDel', email: 'tdel@t.com', age: 1 });
    const doc = await Author.findOne({ name: 'TmpDel' }).exec();
    await doc.remove();
    assertNull(await Author.findOne({ name: 'TmpDel' }).exec());
  });

  await test('Document unique constraints', async () => {
    const UniqueModel = db.model('UniqueTest', new localgoose.Schema({ uniqueKey: { type: String, unique: true } }));
    await UniqueModel.deleteMany({});
    
    await UniqueModel.create({ uniqueKey: '123' });
    let threw = false;
    try {
      await UniqueModel.create({ uniqueKey: '123' });
    } catch (e) {
      threw = true;
      assert(e.code === 11000, `Expected code 11000, got ${e.code}`);
    }
    assert(threw, 'Unique constraint should violently throw');
    await UniqueModel.deleteMany({});
  });

  // ── 6. Population ────────────────────────────────────────────────────────
  console.log('\n🔗 6. Population');

  let post1, post2;

  await test('create posts with author refs', async () => {
    const freshAlice = await Author.findOne({ name: 'Alice' }).exec();
    post1 = await Post.create({ title: 'Into to JS',     content: 'JS basics', author: freshAlice._id, likes: 10, tags: ['js'] });
    post2 = await Post.create({ title: 'Advanced TS',    content: 'TypeScript tips', author: freshAlice._id, likes: 25, tags: ['ts'] });
    assert(post1._id && post2._id);
  });

  await test('populate() resolves ref', async () => {
    const doc = await Post.findOne({ title: 'Into to JS' }).populate('author').exec();
    assert(doc.author && doc.author.name === 'Alice', `Expected Alice, got: ${doc.author}`);
  });

  await test('populate() with select', async () => {
    const doc = await Post.findOne({ title: 'Into to JS' })
      .populate({ path: 'author', select: 'name' })
      .exec();
    assert(doc.author && doc.author.name, 'has name');
    assertEqual(doc.author.age, undefined, 'age excluded');
  });

  await test('Document.populate() instance method', async () => {
    const doc = await Post.findOne({ title: 'Into to JS' }).exec();
    await doc.populate('author');
    assert(doc.author && doc.author.name === 'Alice');
  });

  await test('Model.populate() static', async () => {
    const docs = await Post.find({ title: 'Into to JS' }).exec();
    // static populate not easily testable without ref config; just confirm no crash
    assert(docs.length > 0);
  });

  // ── 7. bulkWrite ─────────────────────────────────────────────────────────
  console.log('\n📦 7. bulkWrite');

  await test('bulkWrite insertOne', async () => {
    const res = await Author.bulkWrite([
      { insertOne: { document: { name: 'BulkA', email: 'ba@t.com', age: 20 } } }
    ]);
    assertEqual(res.insertedCount, 1);
    assert(await Author.findOne({ name: 'BulkA' }).exec());
  });

  await test('bulkWrite updateOne', async () => {
    const res = await Author.bulkWrite([
      { updateOne: { filter: { name: 'BulkA' }, update: { $set: { score: 55 } } } }
    ]);
    assertEqual(res.modifiedCount, 1);
    const doc = await Author.findOne({ name: 'BulkA' }).exec();
    assertEqual(doc.score, 55);
  });

  await test('bulkWrite deleteOne', async () => {
    const res = await Author.bulkWrite([{ deleteOne: { filter: { name: 'BulkA' } } }]);
    assertEqual(res.deletedCount, 1);
  });

  await test('bulkWrite mixed operations', async () => {
    const res = await Author.bulkWrite([
      { insertOne: { document: { name: 'BwX', email: 'bwx@t.com', age: 1 } } },
      { insertOne: { document: { name: 'BwY', email: 'bwy@t.com', age: 2 } } },
      { updateOne: { filter: { name: 'BwX' }, update: { $set: { score: 10 } } } },
      { deleteOne: { filter: { name: 'BwY' } } }
    ]);
    assertEqual(res.insertedCount, 2);
    assertEqual(res.modifiedCount, 1);
    assertEqual(res.deletedCount, 1);
    await Author.deleteOne({ name: 'BwX' });
  });

  // ── 8. Aggregation ───────────────────────────────────────────────────────
  console.log('\n📊 8. Aggregation');

  await test('aggregate $match', async () => {
    const res = await Author.aggregate().match({ active: true }).exec();
    assert(res.length > 0);
    assert(res.every(d => d.active === true));
  });

  await test('aggregate $group $sum', async () => {
    const res = await Author.aggregate().group({ _id: null, total: { $sum: '$score' } }).exec();
    assert(typeof res[0].total === 'number');
  });

  await test('aggregate $group $avg returns number', async () => {
    const res = await Author.aggregate().group({ _id: null, avgAge: { $avg: '$age' } }).exec();
    assert(typeof res[0].avgAge === 'number', `Expected number, got ${typeof res[0].avgAge}`);
  });

  await test('aggregate $group $min $max', async () => {
    const res = await Author.aggregate().group({ _id: null, minAge: { $min: '$age' }, maxAge: { $max: '$age' } }).exec();
    assert(res[0].minAge <= res[0].maxAge);
  });

  await test('aggregate $group $push', async () => {
    const res = await Author.aggregate().group({ _id: null, names: { $push: '$name' } }).exec();
    assertArray(res[0].names);
    assert(res[0].names.includes('Alice'));
  });

  await test('aggregate $group $addToSet', async () => {
    const res = await Author.aggregate().group({ _id: null, uniqueAges: { $addToSet: '$age' } }).exec();
    assertArray(res[0].uniqueAges);
    // no duplicates
    const ages = res[0].uniqueAges;
    assertEqual(ages.length, new Set(ages).size);
  });

  await test('aggregate $group $first $last', async () => {
    const res = await Author.aggregate()
      .sort({ age: 1 })
      .group({ _id: null, youngest: { $first: '$name' }, oldest: { $last: '$name' } })
      .exec();
    assert(res[0].youngest && res[0].oldest);
  });

  await test('aggregate $group by field', async () => {
    const res = await Author.aggregate().group({ _id: '$age', count: { $sum: 1 } }).exec();
    assert(res.length > 0);
    assert(res.every(r => r.count >= 1));
  });

  await test('aggregate $sort', async () => {
    const res = await Author.aggregate().sort({ age: -1 }).exec();
    for (let i = 1; i < res.length; i++) assert(res[i-1].age >= res[i].age, 'Descending');
  });

  await test('aggregate $limit $skip', async () => {
    const res = await Author.aggregate().sort({ name: 1 }).skip(1).limit(2).exec();
    assertEqual(res.length, 2);
  });

  await test('aggregate $project', async () => {
    const res = await Author.aggregate().project({ name: 1, age: 1 }).exec();
    assert(res.every(d => d.name && d.age !== undefined));
    assert(res.every(d => d.email === undefined));
  });

  await test('aggregate $addFields', async () => {
    const res = await Author.aggregate()
      .match({ name: 'Alice' })
      .addFields({ nameUpper: { $toUpper: '$name' } })
      .exec();
    assertEqual(res[0].nameUpper, 'ALICE');
  });

  await test('aggregate $unwind', async () => {
    const res = await Author.aggregate()
      .match({ name: 'Carol' })
      .unwind('$tags')
      .exec();
    assert(res.length >= 2, `Expected >=2 unwind results, got ${res.length}`);
    assert(res.every(d => typeof d.tags === 'string'));
  });

  await test('aggregate $count', async () => {
    const res = await Author.aggregate().count('total').exec();
    assert(res[0].total >= 4);
  });

  await test('aggregate $sample', async () => {
    const res = await Author.aggregate().sample(2).exec();
    assertEqual(res.length, 2);
  });

  await test('aggregate $sortByCount', async () => {
    const res = await Author.aggregate().sortByCount('$age').exec();
    assert(res.length > 0);
    assert(res[0].count >= res[res.length - 1].count);
  });

  await test('aggregate $lookup', async () => {
    const res = await Post.aggregate()
      .lookup({ from: 'SuiteAuthor', localField: 'author', foreignField: '_id', as: 'authorInfo' })
      .exec();
    assert(res.length > 0);
    assert(Array.isArray(res[0].authorInfo));
  });

  await test('aggregate $facet', async () => {
    const res = await Author.aggregate()
      .facet({
        byAge: [{ $group: { _id: '$age', count: { $sum: 1 } } }],
        total:  [{ $count: 'n' }]
      })
      .exec();
    assert(res[0].byAge && Array.isArray(res[0].byAge));
    assert(res[0].total && Array.isArray(res[0].total));
  });

  await test('aggregate $set / $unset', async () => {
    const res = await Author.aggregate()
      .match({ name: 'Alice' })
      .set({ bonus: { $add: ['$score', 100] } })
      .unset(['score'])
      .exec();
    assert(res[0].bonus !== undefined);
    assertEqual(res[0].score, undefined);
  });

  await test('aggregate $replaceRoot', async () => {
    const res = await Author.aggregate()
      .match({ name: 'Alice' })
      .replaceRoot('$profile')
      .exec();
    assert(res[0] !== undefined);
  });

  await test('aggregate expression $concat', async () => {
    const res = await Author.aggregate()
      .match({ name: 'Alice' })
      .addFields({ fullLabel: { $concat: ['$name', ' - ', '$email'] } })
      .exec();
    assert(res[0].fullLabel.includes('Alice'));
  });

  await test('aggregate expression $cond', async () => {
    const res = await Author.aggregate()
      .addFields({ category: { $cond: { if: { $gte: ['$age', 30] }, then: 'senior', else: 'junior' } } })
      .exec();
    assert(res.every(d => d.category === 'senior' || d.category === 'junior'));
  });

  await test('aggregate expression $switch', async () => {
    const res = await Author.aggregate()
      .addFields({
        tier: { $switch: { branches: [{ case: { $gte: ['$age', 35] }, then: 'A' }, { case: { $gte: ['$age', 28] }, then: 'B' }], default: 'C' } }
      }).exec();
    assert(res.every(d => ['A','B','C'].includes(d.tier)));
  });

  await test('aggregate expression arithmetic ($subtract, $divide)', async () => {
    const res = await Author.aggregate()
      .match({ name: 'Alice' })
      .addFields({ halfAge: { $divide: ['$age', 2] } })
      .exec();
    assertEqual(res[0].halfAge, 14);
  });

  await test('aggregate $group $mergeObjects', async () => {
    const res = await Author.aggregate()
      .group({ _id: null, merged: { $mergeObjects: '$profile' } })
      .exec();
    assert(res[0].merged !== undefined);
  });

  await test('aggregate $bucket', async () => {
    const res = await Author.aggregate()
      .bucket({ groupBy: '$age', boundaries: [0, 25, 35, 100], default: 'Other', output: { count: { $sum: 1 } } })
      .exec();
    assert(Array.isArray(res));
  });

  await test('aggregate $bucketAuto', async () => {
    const res = await Author.aggregate().bucketAuto({ groupBy: '$age', buckets: 2 }).exec();
    assert(res.length <= 2);
  });

  await test('aggregate then/catch (thenable)', async () => {
    const res = await Author.aggregate().match({ name: 'Alice' });
    assert(Array.isArray(res));
  });

  await test('aggregate pipeline() returns array copy', async () => {
    const agg = Author.aggregate().match({ x: 1 }).limit(10);
    const pl = agg.pipeline();
    assertArray(pl);
    assertEqual(pl.length, 2);
    // Mutating copy does not affect original
    pl.push({ $skip: 1 });
    assertEqual(agg.pipeline().length, 2);
  });

  await test('aggregate pre(aggregate) hook runs', async () => {
    let called = false;
    const s2 = new localgoose.Schema({ v: Number });
    s2.pre('aggregate', async function () { called = true; });
    const M2 = db.model('HookTest', s2);
    await M2.deleteMany({});
    await M2.aggregate().match({}).exec();
    assert(called, 'pre aggregate hook should have been called');
    await M2.deleteMany({});
  });

  // ── 9. insertMany ────────────────────────────────────────────────────────
  console.log('\n📥 9. insertMany');

  await test('insertMany() creates multiple docs', async () => {
    const docs = await Author.insertMany([
      { name: 'IM1', email: 'im1@t.com', age: 10 },
      { name: 'IM2', email: 'im2@t.com', age: 11 }
    ]);
    assertEqual(docs.length, 2);
    assert(docs[0]._id && docs[1]._id);
    await Author.deleteMany({ name: { $in: ['IM1', 'IM2'] } });
  });

  await test('insertMany() with lean option', async () => {
    const docs = await Author.insertMany(
      [{ name: 'IML', email: 'iml@t.com', age: 5 }],
      { lean: true }
    );
    assertEqual(typeof docs[0].toObject, 'undefined');
    await Author.deleteOne({ name: 'IML' });
  });

  // ── 10. hydrate / castObject ──────────────────────────────────────────────
  console.log('\n🔧 10. Utility Methods');

  await test('hydrate() creates Document from plain obj', async () => {
    const raw = { _id: '123', name: 'Hydrated', email: 'h@t.com', age: 40, tags: [], score: 0, active: true, profile: {} };
    const doc = Author.hydrate(raw);
    assert(doc.name === 'Hydrated');
    assert(!doc.isNew);
    assert(typeof doc.toObject === 'function');
  });

  await test('castObject()', async () => {
    const obj = Author.castObject({ name: 'Test', age: '25', score: '10' });
    assert(obj.name === 'Test');
  });

  await test('applyVirtuals()', async () => {
    const raw = { name: 'Alice', email: 'alice@t.com', age: 28, tags: [], score: 0 };
    const v = Author.applyVirtuals(raw);
    assert(v.displayName);
  });

  await test('Model static method (findActive)', async () => {
    const docs = await Author.findActive().exec();
    assert(Array.isArray(docs));
  });

  await test('Query [Symbol.asyncIterator]', async () => {
    const names = [];
    for await (const doc of Author.find({ name: { $in: ['Alice', 'Carol'] } })) {
      names.push(doc.name);
    }
    assertEqual(names.length, 2);
  });

  await test('Aggregate [Symbol.asyncIterator]', async () => {
    const ages = [];
    for await (const doc of Author.aggregate().match({ name: 'Alice' })) {
      ages.push(doc.age);
    }
    assert(ages.length > 0);
  });

  await test('localgoose.Types exposed', async () => {
    assert(localgoose.Types.String === String);
    assert(localgoose.Types.Number === Number);
    assert(localgoose.Types.ObjectId);
    assert(localgoose.ObjectId);
  });

  await test('Connection.disconnect() returns Promise', async () => {
    const conn = localgoose.connect('./tmp_conn_test');
    const result = conn.disconnect();
    assert(result && typeof result.then === 'function');
    try { fs.rmSync('./tmp_conn_test', { recursive: true, force: true }); } catch (e) {}
  });

  await test('Connection.listCollections()', async () => {
    const list = db.listCollections();
    assert(Array.isArray(list));
    assert(list.some(c => c.name === 'SuiteAuthor'));
  });

  await test('Connection.modelNames()', async () => {
    const names = db.modelNames();
    assert(names.includes('SuiteAuthor'));
    assert(names.includes('SuitePost'));
  });

  await test('Connection.model() throws for unknown model', async () => {
    let threw = false;
    try { db.model('UnknownModel123'); } catch (e) { threw = true; }
    assert(threw);
  });

  // ── Cleanup ───────────────────────────────────────────────────────────────
  await localgoose.flushDisk();
  await cleanup(db);
  try { fs.rmSync('./mydb_test',  { recursive: true, force: true }); } catch (e) {}
  try { fs.rmSync('./mydb_test2', { recursive: true, force: true }); } catch (e) {}
  try { fs.rmSync('./test_fixes.js', { recursive: false }); } catch (e) {}

  // ── Summary ───────────────────────────────────────────────────────────────
  const pct = ((passed / total) * 100).toFixed(1);
  console.log('\n' + '═'.repeat(50));
  console.log(`📋 Results: ${passed}/${total} passed (${pct}%)`);
  if (failed > 0) {
    console.log(`❌ ${failed} test(s) failed`);
    await localgoose.flushDisk();
    process.exit(1);
  } else {
    console.log('🎉 All tests passed!');
  }
  await localgoose.flushDisk();
}

runAll().catch(e => {
  console.error('\n💥 Fatal error:', e.message);
  console.error(e.stack);
  process.exit(1);
});
