import mongoose from 'mongoose';

export const connectDatabase = async (): Promise<void> => {
    const mongoUri = process.env.MONGODB_URI;

    if (!mongoUri) {
        throw new Error('MONGODB_URI is not defined in environment variables');
    }

    try {
        await mongoose.connect(mongoUri);
        console.log('[DATABASE] ✅ Connected to MongoDB');
    } catch (error) {
        console.error('[DATABASE] ❌ MongoDB connection error:', error);
        throw error;
    }
};

export const disconnectDatabase = async (): Promise<void> => {
    try {
        await mongoose.connection.close();
        console.log('[DATABASE] 🔌 MongoDB connection closed gracefully');
    } catch (error) {
        console.error('[DATABASE] ❌ Error closing MongoDB connection:', error);
        throw error;
    }
};

mongoose.connection.on('disconnected', () => {
    console.log('[DATABASE] ⚠️ MongoDB disconnected');
});

mongoose.connection.on('error', (err) => {
    console.error('[DATABASE] ❌ MongoDB error:', err);
});
