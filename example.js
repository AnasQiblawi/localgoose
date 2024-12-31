const { localgoose } = require("./src/index.js");

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
    const db = localgoose.connect('./mydb');

    // Define base Person schema for inheritance
    const personSchema = new localgoose.Schema({
      name: { type: String, required: true },
      email: { type: String, required: true },
      age: { type: Number, min: 0, required: true }
    });

    // Add instance method to Person
    personSchema.method('getFullName', function() {
      return `${this.name} (${this.age})`;
    });

    // Define User schema inheriting from Person
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
    }, { timestamps: true, versionKey: '__v' });

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

    // Define Comment schema
    const commentSchema = new localgoose.Schema({
      content: { type: String, required: true },
      post: { type: localgoose.Schema.Types.ObjectId, ref: 'Post', required: true },
      author: { type: localgoose.Schema.Types.ObjectId, ref: 'User', required: true },
      likes: { type: Number, default: 0 },
      createdAt: { type: Date, default: Date.now }
    });

    // Define Category schema
    const categorySchema = new localgoose.Schema({
      name: { type: String, required: true },
      slug: { type: String, required: true },
      description: String,
      parent: { type: localgoose.Schema.Types.ObjectId, ref: 'Category' },
      isActive: { type: Boolean, default: true }
    });

    // Add virtual properties to User
    userSchema.virtual('isAdult').get(function () {
      return this.age >= 18;
    });

    // Add compound virtual property
    userSchema.virtual('displayName').get(function() {
      return `${this.username} (${this.email})`;
    });

    // Add virtual with dependencies
    userSchema.virtual('status').get(function() {
      if (!this.isActive) return 'inactive';
      if (!this.lastLogin) return 'new';
      const daysSinceLogin = (Date.now() - this.lastLogin) / (1000 * 60 * 60 * 24);
      return daysSinceLogin > 30 ? 'dormant' : 'active';
    });

    // Add instance methods to User
    userSchema.method('getFullInfo', function () {
      return `${this.username} (${this.age}) - ${this.email}`;
    });

    userSchema.method('updateProfile', async function(profileData) {
      Object.assign(this.profile, profileData);
      return this.save();
    });

    // Add static methods to User
    userSchema.static('findByEmail', async function (email) {
      return this.findOne({ email });
    });

    userSchema.static('findActiveUsers', async function() {
      return this.find({ isActive: true });
    });

    // Add middleware to User
    userSchema.pre('save', function () {
      console.log('Before saving user:', this.username);
      this.lastLogin = new Date();
    });

    userSchema.post('save', function () {
      console.log('After saving user:', this.username);
    });

    // Add validation to Post
    postSchema.pre('save', function() {
      if (this.content.length < 10) {
        throw new Error('Post content must be at least 10 characters long');
      }
    });

    // Add instance method to Post
    postSchema.method('addTag', async function(tag) {
      if (!this.tags.includes(tag)) {
        this.tags.push(tag);
        return this.save();
      }
      return this;
    });

    // Create models
    const User = db.model('User', userSchema);
    const Post = db.model('Post', postSchema);
    const Comment = db.model('Comment', commentSchema);
    const Category = db.model('Category', categorySchema);

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

    // Create categories
    console.log('\n--- Creating Categories ---');
    const techCategory = await Category.create({
      name: 'Technology',
      slug: 'technology',
      description: 'Technology related posts'
    });
    logOutput('Created category:', techCategory);

    const designCategory = await Category.create({
      name: 'Design',
      slug: 'design',
      description: 'Design related posts',
    });
    logOutput('Created category:', designCategory);

    // Create posts
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

    // Create comments
    console.log('\n--- Creating Comments ---');
    const comment1 = await Comment.create({
      content: 'Great introduction to Node.js!',
      post: post1._id,
      author: jane._id,
      likes: 5
    });
    logOutput('Created comment:', comment1);

    const comment2 = await Comment.create({
      content: 'Very helpful design principles!',
      post: post2._id,
      author: john._id,
      likes: 3
    });
    logOutput('Created comment:', comment2);

    // Demonstrate virtual properties
    console.log('\n--- Virtual Properties ---');
    const users = await User.find();
    users.forEach(user => {
      console.log(`${user.username}:`);
      console.log('- Is Adult:', user.isAdult);
      console.log('- Display Name:', user.displayName);
      console.log('- Status:', user.status);
    });

    // Demonstrate instance methods
    console.log('\n--- Instance Methods ---');
    const johnInfo = john.getFullInfo();
    console.log('John\'s full info:', johnInfo);

    await john.updateProfile({
      bio: 'Senior Node.js Developer',
      avatar: 'john_new.jpg'
    });
    logOutput('Updated John\'s profile:', john);

    // Demonstrate static methods
    console.log('\n--- Static Methods ---');
    const userByEmail = await User.findByEmail('jane@example.com');
    logOutput('Found user by email:', userByEmail);

    const activeUsers = await User.findActiveUsers();
    logOutput('Active users:', activeUsers);

    // Query posts with populated author and comments
    console.log('\n--- Complex Queries with Population ---');
    const populatedPosts = await Post.find()
      .populate('author')
      .sort('-likes')
      .exec();
    logOutput('Posts with author details:', populatedPosts);

    // Demonstrate advanced queries
    console.log('\n--- Advanced Queries ---');
    const popularPosts = await Post.find()
      .where('likes').gt(5)
      .where('tags').in(['tutorial'])
      .select('title author likes')
      .populate('author')
      .sort('-likes')
      .exec();
    logOutput('Popular tutorial posts:', popularPosts);

    // Demonstrate regex queries
    console.log('\n--- Regex Queries ---');
    const nodeJsPosts = await Post.find()
      .where('title').regex(/node\.js/i)
      .exec();
    logOutput('Node.js related posts:', nodeJsPosts);

    // Demonstrate aggregations
    console.log('\n--- Advanced Aggregations ---');
    const postStats = await Post.aggregate()
      .match({ likes: { $gt: 5 } })
      .group({
        _id: '$author',
        totalPosts: { $sum: 1 },
        avgLikes: { $avg: '$likes' },
        tags: { $push: '$tags' }
      })
      .sort({ avgLikes: -1 })
      .exec();
    logOutput('Post statistics by author:', postStats);

    // Demonstrate nested aggregations
    const userPostStats = await User.aggregate()
      .lookup({
        from: 'Post',
        localField: '_id',
        foreignField: 'author',
        as: 'posts'
      })
      .project({
        username: 1,
        postCount: { $size: '$posts' },
        totalLikes: { $sum: '$posts.likes' }
      })
      .sort({ totalLikes: -1 })
      .exec();
    logOutput('User post statistics:', userPostStats);

    // Demonstrate updating data
    console.log('\n--- Updating Data ---');
    const updatedJohn = await User.findByIdAndUpdate(john._id, { age: 26 }, { new: true });
    logOutput('Updated John\'s age:', updatedJohn);

    const updatedPost1 = await Post.findByIdAndUpdate(post1._id, { likes: 20 }, { new: true });
    logOutput('Updated Post1 likes:', updatedPost1);

    // Demonstrate populate method
    console.log('\n--- Populating Author in Posts ---');
    const populatedPost1 = await Post.findOne({ _id: post1._id }).populate('author').exec();
    logOutput('Populated post1:', populatedPost1);

    const populatedPost2 = await Post.findOne({ _id: post2._id }).populate('author').exec();
    logOutput('Populated post2:', populatedPost2);

    // Clean up: Delete all records before disconnecting
    console.log('\n--- Cleaning up Database ---');
    await User.deleteMany({});
    await Post.deleteMany({});
    await Comment.deleteMany({});
    await Category.deleteMany({});

    // Clean up
    await db.disconnect();
    console.log('\nDatabase connection closed');
  } catch (error) {
    console.error('Error:', error.message);
    process.exit(1);
  }
};

main();