import { PutObjectCommand, GetObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { getR2Client, R2_BUCKETS } from '../config/r2.js';
import { createWriteStream, createReadStream, statSync } from 'fs';
import { pipeline } from 'stream/promises';
import { Readable } from 'stream';

export const r2Service = {
    /**
     * Check if a file exists in R2
     */
    async fileExists(bucket: string, key: string): Promise<boolean> {
        const client = getR2Client();

        try {
            await client.send(
                new HeadObjectCommand({
                    Bucket: bucket,
                    Key: key,
                })
            );
            return true;
        } catch (error: any) {
            if (error.name === 'NotFound' || error.$metadata?.httpStatusCode === 404) {
                return false;
            }
            throw error;
        }
    },

    /**
     * Get file metadata from R2
     */
    async getFileMetadata(
        bucket: string,
        key: string
    ): Promise<{ size: number; contentType: string } | null> {
        const client = getR2Client();

        try {
            const response = await client.send(
                new HeadObjectCommand({
                    Bucket: bucket,
                    Key: key,
                })
            );

            return {
                size: response.ContentLength || 0,
                contentType: response.ContentType || 'application/octet-stream',
            };
        } catch (error: any) {
            if (error.name === 'NotFound' || error.$metadata?.httpStatusCode === 404) {
                return null;
            }
            throw error;
        }
    },

    /**
     * Get public URL for an object, with optional bucket-specific base
     */
    getPublicUrl(key: string, bucket?: string): string {
        const isThumbnail = bucket === R2_BUCKETS.THUMBNAILS;
        const publicUrl = isThumbnail
            ? (process.env.R2_PUBLIC_URL_THUMBNAILS || process.env.R2_PUBLIC_URL)
            : process.env.R2_PUBLIC_URL;

        if (!publicUrl) {
            throw new Error('R2_PUBLIC_URL is not defined');
        }
        return `${publicUrl}/${key}`;
    },

    /**
     * Download a file from R2 to a local path using streams.
     */
    async downloadToFile(
        bucket: string,
        key: string,
        localPath: string
    ): Promise<void> {
        const client = getR2Client();

        console.log(`[R2] 📥 Downloading from R2`);
        console.log(`[R2] 📂 Bucket: ${bucket}, Key: ${key}`);
        console.log(`[R2] 💾 Local path: ${localPath}`);

        const command = new GetObjectCommand({
            Bucket: bucket,
            Key: key,
        });

        const response = await client.send(command);

        if (!response.Body) {
            throw new Error(`No body returned for key: ${key}`);
        }

        const readableStream = response.Body as Readable;
        const writeStream = createWriteStream(localPath);

        await pipeline(readableStream, writeStream);
        console.log(`[R2] ✅ Download complete`);
    },

    /**
     * Upload a local file to R2 using streams.
     */
    async uploadFromFile(
        bucket: string,
        key: string,
        localPath: string,
        contentType: string
    ): Promise<{ size: number }> {
        const client = getR2Client();
        const fileStream = createReadStream(localPath);
        const fileStats = statSync(localPath);

        console.log(`[R2] 📤 Uploading to R2`);
        console.log(`[R2] 📂 Bucket: ${bucket}, Key: ${key}`);
        console.log(`[R2] 📦 Size: ${(fileStats.size / (1024 * 1024)).toFixed(2)} MB`);

        // Determine cache control based on file type
        // - Video segments (.ts) and thumbnails are immutable, cache for 1 year
        // - Playlists (.m3u8) should have shorter cache for potential updates
        let cacheControl = 'public, max-age=31536000, immutable'; // 1 year for segments
        if (key.endsWith('.m3u8')) {
            cacheControl = 'public, max-age=3600'; // 1 hour for playlists
        }

        const command = new PutObjectCommand({
            Bucket: bucket,
            Key: key,
            Body: fileStream,
            ContentType: contentType,
            ContentLength: fileStats.size,
            CacheControl: cacheControl,
        });

        await client.send(command);
        console.log(`[R2] ✅ Upload complete (Cache: ${cacheControl})`);

        return { size: fileStats.size };
    },
};
