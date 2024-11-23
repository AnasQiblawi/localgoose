import { localgoose } from "./src/index.js";

const logOutput = (label, data) => {
  console.log(`\n${label}`);
  try {
    if (Array.isArray(data)) {
      console.log(JSON.stringify(data.map(doc => 
        doc && typeof doc.toObject === 'function' ? doc.toObject() : doc
      ), null, 2));
    } else if (data && typeof data === 'object') {
      console.log(JSON.stringify(
        data.toObject ? data.toObject() : data, 
        null, 
        2
      ));
    } else {
      console.log(JSON.stringify(data, null, 2));
    }
  } catch (error) {
    console.log(data);
  }
};

const main = async () => {
  try {
    // Connect to database
    const db = await localgoose.connect('./mydb');

    // Define User schema
    const userSchema = new localgoose.Schema({
      username: { type: String, required: true },
      email: { type: String, required: true },
      age: { type: Number, required: true },
      isActive: { type: Boolean, default: true },
      tags: { type: Array, default: [] },
      profile: {
        type: Object,
        default: {
          avatar: 'default.png',
          bio: ''
        }
      },
      lastLogin: { type: Date },
      createdAt: { type: Date, default: Date.now }
    });

    // Define Post schema with proper ObjectId reference
    const postSchema = new localgoose.Schema({
      title: { type: String, required: true },
      content: { type: String, required: true },
      author: { type: localgoose.Schema.Types.ObjectId, ref: 'User', required: true },
      tags: { type: Array, default: [] },
      likes: { type: Number, default: 0 },
      published: { type: Boolean, default: true },
      createdAt: { type: Date, default: Date.now }
    });

    // Add virtual properties to User
    userSchema.virtual('isAdult').get(function () {
      return this.age >= 18;
    });

    // Add instance methods to User
    userSchema.method('getFullInfo', function () {
      return `${this.username} (${this.age}) - ${this.email}`;
    });

    // Add static methods to User
    userSchema.static('findByEmail', async function (email) {
      return this.findOne({ email });
    });

    // Add middleware to User
    userSchema.pre('save', function () {
      console.log('Before saving user:', this.username);
      this.lastLogin = new Date();
    });

    userSchema.post('save', function () {
      console.log('After saving user:', this.username);
    });

    // Create models
    const User = db.model('User', userSchema);
    const Post = db.model('Post', postSchema);

    // Clear existing data
    await User.deleteMany({});
    await Post.deleteMany({});

    // Create users
    console.log('\n--- Creating Users ---');
    const john = await User.create({
      username: 'john',
      email: 'john@example.com',
      age: 25,
      tags: ['developer', 'nodejs'],
      profile: {
        avatar: 'john.jpg',
        bio: 'Node.js developer'
      }
    });
    logOutput('Created user:', john);

    const jane = await User.create({
      username: 'jane',
      email: 'jane@example.com',
      age: 30,
      tags: ['designer', 'ui/ux'],
      profile: {
        avatar: 'jane.jpg',
        bio: 'UI/UX Designer'
      }
    });
    logOutput('Created user:', jane);

    // Create posts with ObjectId references
    console.log('\n--- Creating Posts ---');
    const post1 = await Post.create({
      title: 'Getting Started with Node.js',
      content: 'Node.js is a JavaScript runtime built on Chrome\'s V8 JavaScript engine.',
      author: john._id,
      tags: ['nodejs', 'javascript', 'tutorial'],
      likes: 10
    });
    logOutput('Created post:', post1);

    const post2 = await Post.create({
      title: 'UI/UX Design Principles',
      content: 'Learn the fundamental principles of UI/UX design.',
      author: jane._id,
      tags: ['design', 'ui/ux', 'tutorial'],
      likes: 15
    });
    logOutput('Created post:', post2);

    // Query posts with populated author
    console.log('\n--- Posts with Populated Author ---');
    const populatedPosts = await Post.find()
      .populate('author')
      .sort('-likes')
      .exec();
    logOutput('Posts with author details:', populatedPosts);

    // Query users
    console.log('\n--- User Queries ---');
    const allUsers = await User.find().exec();
    logOutput('All users:', allUsers);

    const adultUsers = await User.find()
      .where('age').gte(18)
      .select('username email age')
      .sort('-age')
      .exec();
    logOutput('Adult users:', adultUsers);

    // Query posts
    console.log('\n--- Post Queries ---');
    const allPosts = await Post.find()
      .sort('-likes')
      .exec();
    logOutput('All posts:', allPosts);

    const tutorialPosts = await Post.find()
      .where('tags').in(['tutorial'])
      .select('title author tags likes')
      .populate('author')
      .exec();
    logOutput('Tutorial posts:', tutorialPosts);

    // Aggregations
    console.log('\n--- User Aggregations ---');
    const userAgeStats = await User.aggregate()
      .group({
        _id: null,
        avgAge: { $avg: '$age' },
        minAge: { $min: '$age' },
        maxAge: { $max: '$age' },
        total: { $sum: 1 }
      })
      .exec();
    logOutput('User age statistics:', userAgeStats);

    const usersByTags = await User.aggregate()
      .unwind('$tags')
      .group({
        _id: '$tags',
        users: { $sum: 1 },
        avgAge: { $avg: '$age' }
      })
      .sort({ users: -1 })
      .exec();
    logOutput('Users grouped by tags:', usersByTags);

    console.log('\n--- Post Aggregations ---');
    const postStats = await Post.aggregate()
      .group({
        _id: null,
        totalPosts: { $sum: 1 },
        avgLikes: { $avg: '$likes' },
        maxLikes: { $max: '$likes' }
      })
      .exec();
    logOutput('Post statistics:', postStats);

    const postsByTags = await Post.aggregate()
      .unwind('$tags')
      .group({
        _id: '$tags',
        posts: { $sum: 1 },
        totalLikes: { $sum: '$likes' }
      })
      .sort({ posts: -1 })
      .exec();
    logOutput('Posts grouped by tags:', postsByTags);

    // Clean up
    await db.disconnect();
    console.log('\nDatabase connection closed');
  } catch (error) {
    console.error('Error:', error.message);
    process.exit(1);
  }
};

main();