const { localgoose } = require("../src/index.js");

/**
 * Level 1: Beginner
 * Focus: Database connection, Schema definition, and basic CRUD operations.
 */
const main = async () => {
  try {
    // 1. Connect to the database (creates the folder if it doesn't exist)
    const db = localgoose.connect('./beginner_db');
    console.log('Connected to local database.');

    // 2. Define a simple Schema
    const userSchema = new localgoose.Schema({
      name: { type: String, required: true },
      email: { type: String, required: true },
      age: { type: Number, default: 18 }
    });

    // 3. Create a Model
    const User = db.model('User', userSchema);

    // 4. Create a new document
    console.log('\n--- Creating User ---');
    const newUser = await User.create({
      name: 'Alice Berry',
      email: 'alice@example.com',
      age: 25
    });
    console.log('User created:', newUser.toObject());

    // 5. Find documents
    console.log('\n--- Finding Users ---');
    const allUsers = await User.find();
    console.log('All users count:', allUsers.length);

    const specificUser = await User.findOne({ name: 'Alice Berry' });
    console.log('Found Alice:', specificUser.name);

    // 6. Update a document
    console.log('\n--- Updating User ---');
    await User.findByIdAndUpdate(newUser._id, { age: 26 });
    const updatedUser = await User.findById(newUser._id);
    console.log('Updated Age:', updatedUser.age);

    // 7. Delete a document
    console.log('\n--- Deleting User ---');
    await User.deleteOne({ _id: newUser._id });
    console.log('User deleted.');

    // 8. Disconnect
    await db.disconnect();
    console.log('\nDatabase connection closed.');

  } catch (error) {
    console.error('Error:', error.message);
  }
};

main();
