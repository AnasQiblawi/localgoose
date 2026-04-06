const { localgoose } = require("../src/index.js");

/**
 * Level 3: Professional
 * Focus: Aggregation pipelines, Advanced queries, Complex update operators, and Schema Inheritance.
 */
const main = async () => {
  try {
    // 1. Connect to the database
    const db = localgoose.connect('./professional_db');
    console.log('Connected to professional database.');

    // --- Schema Inheritance ---
    const personProperties = {
      name: { type: String, required: true },
      email: { type: String, required: true, unique: true },
      age: { type: Number, min: 0 }
    };

    const userSchema = new localgoose.Schema({
      ...personProperties,
      username: { type: String, required: true },
      tags: { type: Array, default: [] },
      stats: {
        type: Object,
        default: { loginCount: 0, lastSeen: null }
      }
    }, { timestamps: true });

    const User = db.model('User', userSchema);
    const Post = db.model('Post', new localgoose.Schema({
      title: { type: String, required: true },
      author: { type: localgoose.Schema.Types.ObjectId, ref: 'User' },
      likes: { type: Number, default: 0 },
      tags: { type: Array, default: [] }
    }));

    // 2. Data population with Advanced Queries
    console.log('\n--- Creating Users & Posts ---');
    const user1 = await User.create({
      name: 'John Doe',
      username: 'john_doe',
      email: 'john@example.com',
      age: 28,
      tags: ['nodejs', 'javascript']
    });

    const user2 = await User.create({
      name: 'Jane Roe',
      username: 'jane_roe',
      email: 'jane@example.com',
      age: 32,
      tags: ['ui', 'design']
    });

    await Post.create([
      { title: 'Advanced localgoose', author: user1._id, likes: 50, tags: ['db', 'performance'] },
      { title: 'Modern UI Design', author: user2._id, likes: 30, tags: ['ui', 'design'] },
      { title: 'Node.js Internals', author: user1._id, likes: 100, tags: ['nodejs', 'advanced'] }
    ]);

    // 3. Complex Queries (Regex, GT, IN)
    console.log('\n--- Complex Queries ---');
    const filteredPosts = await Post.find()
      .where('likes').gt(20)
      .where('tags').in(['nodejs', 'ui'])
      .exec();
    console.log('Found filtered posts:', filteredPosts.map(p => p.title));

    const regexQuery = await Post.find({ title: /localgoose/i }).exec();
    console.log('Regex found post:', regexQuery.map(p => p.title));

    // 4. Advanced Updates ($inc, $push, $addToSet)
    console.log('\n--- Advanced Updates ---');
    await User.updateOne({ _id: user1._id }, {
      $inc: { 'stats.loginCount': 1 },
      $addToSet: { tags: 'backend' },
      $push: { tags: 'scaling' }
    });

    const updatedUser1 = await User.findById(user1._id);
    console.log('Login count after update:', updatedUser1.stats.loginCount);
    console.log('User 1 tags after update:', updatedUser1.tags);

    // 5. Aggregation Pipelines
    console.log('\n--- Aggregation Pipeline ---');
    const postStats = await Post.aggregate()
      .match({ likes: { $gt: 10 } })
      .group({
        _id: '$author',
        totalLikes: { $sum: '$likes' },
        avgLikes: { $avg: '$likes' },
        postCount: { $sum: 1 }
      })
      .sort({ totalLikes: -1 })
      .exec();

    console.log('Author aggregate stats:', postStats);

    // 6. Demonstrate lookup (Join)
    console.log('\n--- Lookup Aggregation ---');
    const userWithPosts = await User.aggregate()
      .match({ username: 'john_doe' })
      .lookup({
        from: 'Post',
        localField: '_id',
        foreignField: 'author',
        as: 'user_posts'
      })
      .exec();

    console.log(`${userWithPosts[0].username} has ${userWithPosts[0].user_posts.length} posts.`);

    // 7. Clean up & Disconnect
    await User.deleteMany({});
    await Post.deleteMany({});
    await db.disconnect();
    console.log('\nDatabase connection closed.');

  } catch (error) {
    console.error('Error:', error.message);
  }
};

main();
