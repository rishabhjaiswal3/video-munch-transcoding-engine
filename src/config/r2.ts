import { S3Client } from '@aws-sdk/client-s3';

let r2Client: S3Client | null = null;

export const getR2Client = (): S3Client => {
    if (r2Client) {
        return r2Client;
    }

    const accountId = process.env.R2_ACCOUNT_ID;
    const accessKeyId = process.env.R2_ACCESS_KEY_ID;
    const secretAccessKey = process.env.R2_SECRET_ACCESS_KEY;

    if (!accountId || !accessKeyId || !secretAccessKey) {
        throw new Error('R2 credentials are not defined in environment variables');
    }

    r2Client = new S3Client({
        region: 'auto',
        endpoint: `https://${accountId}.r2.cloudflarestorage.com`,
        credentials: {
            accessKeyId,
            secretAccessKey,
        },
    });

    console.log('[R2] ✅ R2 Client initialized');

    return r2Client;
};

export const R2_BUCKETS = {
    RAW: process.env.R2_BUCKET_RAW || 'videos-raw-upload',
    TRANSCODED: process.env.R2_BUCKET_TRANSCODED || 'videos-transcoded-data',
    THUMBNAILS: process.env.R2_BUCKET_THUMBNAILS || 'video-thumbnails',
};
