import { Redis } from 'ioredis';

let redisConnection: Redis | null = null;

// Parse Redis URL into host/port/password for BullMQ
const parseRedisUrl = (url: string) => {
    const parsed = new URL(url);
    return {
        host: parsed.hostname,
        port: parseInt(parsed.port || '6379', 10),
        password: parsed.password || undefined,
        username: parsed.username || undefined,
    };
};

// Connection config for BullMQ (it creates its own connections)
export const getRedisConfig = () => {
    const redisUrl = process.env.REDIS_URL;

    if (!redisUrl) {
        throw new Error('REDIS_URL is not defined in environment variables');
    }

    const isTls = redisUrl.startsWith('rediss://');
    const parsed = parseRedisUrl(redisUrl);

    return {
        ...parsed,
        maxRetriesPerRequest: null,
        ...(isTls && { tls: { rejectUnauthorized: false } }),
    };
};

// Shared connection for non-BullMQ usage
export const getRedisConnection = (): Redis => {
    if (redisConnection) {
        return redisConnection;
    }

    const redisUrl = process.env.REDIS_URL;
    if (!redisUrl) {
        throw new Error('REDIS_URL is not defined in environment variables');
    }

    const isTls = redisUrl.startsWith('rediss://');

    redisConnection = new Redis(redisUrl, {
        maxRetriesPerRequest: null,
        ...(isTls && { tls: { rejectUnauthorized: false } }),
    });

    redisConnection.on('connect', () => {
        console.log('[REDIS] ✅ Connected to Redis');
    });

    redisConnection.on('error', (err) => {
        console.error('[REDIS] ❌ Redis error:', err);
    });

    return redisConnection;
};

export const disconnectRedis = async (): Promise<void> => {
    if (redisConnection) {
        try {
            // Check if connection is already closed before attempting to quit
            const status = redisConnection.status;
            if (status === 'end' || status === 'close') {
                console.log('[REDIS] ℹ️ Redis connection already closed');
                redisConnection = null;
                return;
            }
            await redisConnection.quit();
            redisConnection = null;
            console.log('[REDIS] 🔌 Redis connection closed gracefully');
        } catch (error: any) {
            // Handle "Connection is closed" error gracefully
            if (error?.message?.includes('Connection is closed')) {
                console.log('[REDIS] ℹ️ Redis connection was already closed');
                redisConnection = null;
                return;
            }
            console.error('[REDIS] ❌ Error closing Redis connection:', error);
            redisConnection = null;
        }
    }
};
