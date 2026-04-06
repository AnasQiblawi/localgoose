const { localgoose } = require("../src/index.js");

/**
 * Level 2: Intermediate
 * Focus: Schema Virtuals, Instance & Static methods, Middleware (Hooks), and Population.
 */
const main = async () => {
  try {
    // 1. Connect to the database
    const db = localgoose.connect('./intermediate_db');
    console.log('Connected to intermediate database.');

    // 2. Define Schema with Virtuals and Methods
    const userSchema = new localgoose.Schema({
      username: { type: String, required: true },
      email: { type: String, required: true },
      age: { type: Number, required: true },
      isActive: { type: Boolean, default: true }
    });

    // --- Virtual property ---
    userSchema.virtual('isAdult').get(function () {
      return this.age >= 18;
    });

    // --- Instance method ---
    userSchema.method('greet', function () {
      return `Hello, my name is ${this.username}!`;
    });

    // --- Static method ---
    userSchema.static('findActive', function () {
      return this.find({ isActive: true });
    });

    // --- Middleware (Hooks) ---
    userSchema.pre('save', function () {
      console.log(`Pre-save: Saving user ${this.username}`);
      this.username = this.username.toLowerCase();
    });

    userSchema.post('save', function () {
      console.log(`Post-save: User ${this.username} was successfully saved.`);
    });

    // 3. Define another Schema for Population
    const postSchema = new localgoose.Schema({
      title: { type: String, required: true },
      content: { type: String, required: true },
      author: { type: localgoose.Schema.Types.ObjectId, ref: 'User', required: true }
    });

    const User = db.model('User', userSchema);
    const Post = db.model('Post', postSchema);

    // 4. Create data
    console.log('\n--- Creating User & Post ---');
    const author = await User.create({
      username: 'BobSmith',
      email: 'bob@example.com',
      age: 30
    });

    console.log(author.greet());
    console.log('Is Adult:', author.isAdult);

    const post = await Post.create({
      title: 'Hello localgoose!',
      content: 'This is an awesome library.',
      author: author._id
    });

    // 5. Demonstrate Population
    console.log('\n--- Populating Post Author ---');
    const populatedPost = await Post.findOne({ _id: post._id }).populate('author').exec();
    console.log('Post Title:', populatedPost.title);
    console.log('Author Name:', populatedPost.author.username);

    // 6. Demonstrate Static Method
    console.log('\n--- Finding Active Users ---');
    const activeUsers = await User.findActive();
    console.log('Active users count:', activeUsers.length);

    // Clean up & Disconnect
    await User.deleteMany({});
    await Post.deleteMany({});
    await db.disconnect();
    console.log('\nDatabase connection closed.');

  } catch (error) {
    console.error('Error:', error.message);
  }
};

main();
