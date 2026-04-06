const { localgoose } = require("../src/index.js");
const path = require('path');

/**
 * Level 4: Maximum Capability Showcase
 * This example uses EVERY feature available in localgoose:
 * - Complex Schema definitions & Inheritance (schema.add)
 * - Custom Validators & All Middleware hooks (pre/post save, validate, aggregate)
 * - Advanced Update Operators ($inc, $push, $addToSet, $currentDate, $unset)
 * - Multi-stage Aggregation ($match, $lookup, $unwind, $group, $project, $facet, $out)
 * - Bulk Operations (bulkWrite) and System ops (backup, restore, flushDisk)
 * - Error Handling for unique constraints
 */
const main = async () => {
  const DB_PATH = './max_capability_db';
  try {
    console.log('--- Phase 1: Setup & Schema definition ---');
    const db = localgoose.connect(DB_PATH);

    // 1. Base Schema (Inheritance)
    const timestampsSchema = new localgoose.Schema({}, { timestamps: true });

    // 2. Complex User Schema
    const userSchema = new localgoose.Schema({
      username: { type: String, required: true, unique: true },
      email: { 
        type: String, 
        required: true, 
        validate: {
          validator: (v) => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(v),
          message: props => `${props.value} is not a valid email!`
        }
      },
      profile: {
        bio: String,
        social: { twitter: String, github: String }
      },
      tags: [{ type: String }],
      loginHistory: [{
        at: { type: Date, default: Date.now },
        ip: String
      }],
      __v: { type: Number, default: 0 }
    });
    userSchema.add(timestampsSchema); // Inherit timestamps

    // 3. All Middleware Hooks
    userSchema.pre('validate', function() { console.log('  [Hook] Pre-validate'); });
    userSchema.post('validate', function() { console.log('  [Hook] Post-validate'); });
    userSchema.pre('save', function() { console.log('  [Hook] Pre-save'); });
    userSchema.post('save', function() { console.log('  [Hook] Post-save'); });
    userSchema.pre('aggregate', function() { console.log('  [Hook] Pre-aggregate'); });

    // 4. Virtuals, Methods & Statics
    userSchema.virtual('domain').get(function() { return this.email.split('@')[1]; });
    userSchema.method('addTag', function(tag) { this.tags.push(tag); return this.save(); });
    userSchema.static('findByTag', function(tag) { return this.find({ tags: tag }); });

    const User = db.model('User', userSchema);
    const Log  = db.model('Log', new localgoose.Schema({ 
        action: String, 
        userId: { type: localgoose.Schema.Types.ObjectId, ref: 'User' } 
    }));

    console.log('\n--- Phase 2: Bulk Operations & Validation ---');
    // bulkWrite: insert, update, and delete
    const bulkResults = await User.bulkWrite([
      { insertOne: { document: { username: 'dev_guru', email: 'dev@guru.com', tags: ['node', 'db'] } } },
      { insertOne: { document: { username: 'ops_lead', email: 'ops@lead.com', tags: ['cloud', 'linux'] } } }
    ]);
    console.log('Bulk Insert counts:', bulkResults.insertedCount);

    // Demonstration of Validation Error
    try {
      await User.create({ username: 'invalid', email: 'bad-email' });
    } catch (e) {
      console.log('Validation Error Caught:', e.message);
    }

    // Demonstration of Unique Constraint Error
    try {
      await User.create({ username: 'dev_guru', email: 'duplicate@guru.com' });
    } catch (e) {
      console.log('Unique Constraint Error Caught (Duplicate Username)');
    }

    console.log('\n--- Phase 3: Advanced Updates ---');
    const guru = await User.findOne({ username: 'dev_guru' });
    await User.updateOne({ _id: guru._id }, {
      $inc: { __v: 1 },
      $addToSet: { tags: 'javascript' },
      $push: { loginHistory: { ip: '127.0.0.1' } },
      $set: { 'profile.bio': 'Senior Developer' },
      $currentDate: { 'profile.lastUpdated': true }
    });
    console.log('User guru updated with complex operators.');

    console.log('\n--- Phase 4: Extreme Aggregation ($facet, $lookup, $unwind) ---');
    // Add some logs for joining
    await Log.create({ action: 'login', userId: guru._id });

    const megaAggregate = await User.aggregate()
      .facet({
        'tagStats': [
          { $unwind: '$tags' },
          { $group: { _id: '$tags', count: { $sum: 1 } } },
          { $sort: { count: -1 } }
        ],
        'userProfiles': [
          { $lookup: { from: 'Log', localField: '_id', foreignField: 'userId', as: 'logs' } },
          { $project: { username: 1, logCount: { $size: '$logs' } } }
        ]
      })
      .exec();
    console.log('Aggregate Facet Results:', JSON.stringify(megaAggregate, null, 2));

    console.log('\n--- Phase 5: System Operations (Backup, Restore, Flush) ---');
    // Save to disk immediately
    await localgoose.flushDisk();
    console.log('Disk flushed.');

    // Backup
    const backupFile = path.join(process.cwd(), 'user_backup.json');
    await User.backup(backupFile);
    console.log('Backup created at:', backupFile);

    // Destructive op then Restore
    await User.deleteMany({});
    console.log('All users deleted.');
    await User.restore(backupFile);
    const restoredCount = await User.countDocuments();
    console.log('Users restored! Count:', restoredCount);

    console.log('\n--- Phase 6: Teardown ---');
    // await db.dropDatabase(); // Uncomment to wipe everything
    await db.disconnect();
    console.log('Database disconnected. Showcase complete.');

  } catch (error) {
    console.error('CRITICAL ERROR in Showcase:', error);
  }
};

main();
