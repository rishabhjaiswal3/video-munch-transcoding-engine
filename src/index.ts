import 'dotenv/config';
import { Worker, Job } from 'bullmq';
import { getRedisConnection, disconnectRedis } from './config/redis.js';
import { connectDatabase, disconnectDatabase } from './config/database.js';
import { Video } from './models/Video.js';
import { TranscodeJobData } from './types/index.js';
import { r2Service } from './services/r2Service.js';
import { R2_BUCKETS } from './config/r2.js';
import { ffmpegService, QualityPreset } from './services/ffmpegService.js';
import fs from 'fs/promises';
import path from 'path';
import os from 'os';

// console.log(`\n${'='.repeat(80)}`);
// console.log('🎬 I_TUBE TRANSCODING ENGINE');
// console.log('   Standalone Video Processing Worker');
// console.log(`${'='.repeat(80)}\n`);

// Quality presets for FFmpeg
const QUALITY_PRESETS: Record<string, QualityPreset> = {
    '1080p': { width: 1920, height: 1080, bitrate: '5000k', audioBitrate: '192k' },
    '720p': { width: 1280, height: 720, bitrate: '2500k', audioBitrate: '128k' },
    '480p': { width: 854, height: 480, bitrate: '1000k', audioBitrate: '96k' },
    '360p': { width: 640, height: 360, bitrate: '600k', audioBitrate: '64k' },
    '240p': { width: 426, height: 240, bitrate: '400k', audioBitrate: '48k' },
};

const TEMP_BASE_DIR = path.join(os.tmpdir(), 'videomanch-transcoding');

// Track workers for graceful shutdown
let userWorker: Worker<TranscodeJobData> | null = null;
let creatorWorker: Worker<TranscodeJobData> | null = null;
let isShuttingDown = false;
let activeJobs = 0;

const processJob = async (job: Job<TranscodeJobData>) => {
    const { videoId, userId, userType, r2Key, qualities } = job.data;
    const tempDir = path.join(TEMP_BASE_DIR, videoId);

    // Track active jobs for graceful shutdown
    activeJobs++;

    // Performance tracking
    const kpis = {
        timings: {
            queueWait: 0,
            download: 0,
            transcode: 0,
            upload: 0,
            total: 0,
        },
        sizes: {
            original: 0,
            '1080p': 0,
            '720p': 0,
            '480p': 0,
            '360p': 0,
            '240p': 0,
            total: 0,
        },
    };
    const jobStartTime = Date.now();

    console.log(`\n${'='.repeat(80)}`);
    console.log(`[TRANSCODE] 🎬 Starting transcoding job ${job.id}`);
    console.log(`[TRANSCODE] 🎥 Video ID: ${videoId}`);
    console.log(`[TRANSCODE] 👤 User: ${userId} (${userType})`);
    console.log(`[TRANSCODE] 🎯 Target qualities: ${qualities.join(', ')}`);
    console.log(`[TRANSCODE] 📁 Temp directory: ${tempDir}`);
    console.log(`${'='.repeat(80)}\n`);

    try {
        // --- Setup ---
        console.log(`[TRANSCODE] 📂 Creating temp directory...`);
        await fs.mkdir(tempDir, { recursive: true });
        console.log(`[TRANSCODE] ✅ Temp directory created`);

        console.log(`[TRANSCODE] 💾 Updating video status to 'processing'...`);
        await Video.findOneAndUpdate(
            { videoId },
            {
                status: 'processing',
                'transcoding.startedAt': new Date(),
                'transcoding.progress': 0,
            }
        );
        console.log(`[TRANSCODE] ✅ Video status updated to 'processing'`);

        // Weighted progress: download=10, each quality=20, thumbnail=5, upload=5
        const totalWeight = 10 + (qualities.length * 20) + 5 + 5;
        let completedWeight = 0;

        const reportProgress = async (weight: number) => {
            completedWeight += weight;
            const percent = Math.min(Math.round((completedWeight / totalWeight) * 100), 99);
            console.log(`[TRANSCODE] 📊 Progress: ${percent}% (${completedWeight}/${totalWeight} weight units)`);
            await job.updateProgress(percent);
            await updateVideoProgress(videoId, percent);
        };

        // --- Phase 1: Download original from R2 ---
        console.log(`\n[TRANSCODE] 📥 PHASE 1: Downloading original video from R2`);
        const downloadStartTime = Date.now();
        const originalExt = path.extname(r2Key) || '.mp4';
        const inputPath = path.join(tempDir, `original${originalExt}`);

        console.log(`[TRANSCODE] ☁️ R2 Key: ${r2Key}`);
        console.log(`[TRANSCODE] 💾 Local path: ${inputPath}`);
        await r2Service.downloadToFile(R2_BUCKETS.RAW, r2Key, inputPath);
        kpis.timings.download = Date.now() - downloadStartTime;

        // Get original file size
        const stats = await fs.stat(inputPath);
        kpis.sizes.original = stats.size;

        await reportProgress(10);
        console.log(`[TRANSCODE] ✅ Download complete (${(kpis.sizes.original / (1024 * 1024)).toFixed(2)} MB in ${(kpis.timings.download / 1000).toFixed(1)}s)`);

        // --- Phase 2: Probe video for metadata ---
        console.log(`\n[TRANSCODE] 🔍 PHASE 2: Probing video metadata`);
        const probeData = await ffmpegService.probe(inputPath);
        const duration = probeData.format.duration || 0;
        const videoStream = probeData.streams.find(s => s.codec_type === 'video');
        const originalWidth = videoStream?.width || 0;
        const originalHeight = videoStream?.height || 0;
        const originalCodec = videoStream?.codec_name;
        const originalBitrate = parseInt(String(probeData.format.bit_rate || '0'));
        const originalFps = videoStream?.r_frame_rate ? eval(videoStream.r_frame_rate) : 0;

        console.log(`[TRANSCODE] ⏱️ Video duration: ${duration.toFixed(2)}s`);
        console.log(`[TRANSCODE] 📐 Video dimensions: ${originalWidth}x${originalHeight}`);
        console.log(`[TRANSCODE] 🎞️ Codec: ${originalCodec}, Bitrate: ${(originalBitrate / 1000).toFixed(0)}k, FPS: ${originalFps.toFixed(1)}`);
        console.log(`[TRANSCODE] ✅ Probe complete`);

        // Store original metadata
        await Video.findOneAndUpdate(
            { videoId },
            {
                originalMetadata: {
                    width: originalWidth,
                    height: originalHeight,
                    duration,
                    codec: originalCodec,
                    bitrate: originalBitrate,
                    fps: originalFps,
                },
            }
        );

        // Smart quality selection - don't upscale and skip same-resolution re-encoding
        const smartQualities = qualities.filter(quality => {
            const preset = QUALITY_PRESETS[quality];
            if (!preset) return false;

            // Skip if target resolution is higher than source
            if (preset.height > originalHeight) {
                console.log(`[TRANSCODE] ⚠️ Skipping ${quality} (${preset.height}p) - source is only ${originalHeight}p`);
                return false;
            }

            // Skip 1080p re-encoding if source is 1080p (saves memory/time)
            // The 720p version will be the highest quality variant
            if (preset.height === 1080 && originalHeight === 1080) {
                console.log(`[TRANSCODE] ⚠️ Skipping ${quality} - source is already 1080p, using 720p as highest`);
                return false;
            }

            return true;
        });

        if (smartQualities.length === 0) {
            console.log(`[TRANSCODE] ⚠️ No valid qualities after filtering, using original quality`);
            // Find closest quality that doesn't upscale
            const closestQuality = Object.keys(QUALITY_PRESETS)
                .filter(q => QUALITY_PRESETS[q].height <= originalHeight)
                .sort((a, b) => QUALITY_PRESETS[b].height - QUALITY_PRESETS[a].height)[0] || '360p';
            smartQualities.push(closestQuality);
        }

        console.log(`[TRANSCODE] ✅ Smart quality selection: ${smartQualities.join(', ')}`);

        // --- Phase 3: Transcode each quality to HLS ---
        console.log(`\n[TRANSCODE] ⚙️ PHASE 3: Transcoding to HLS (adaptive streaming)`);
        const transcodeStartTime = Date.now();

        interface HLSQualityOutput {
            quality: string;
            playlistPath: string;
            segmentFiles: string[];
            segmentCount: number;
        }

        const hlsOutputs: HLSQualityOutput[] = [];
        const masterPlaylistVariants: Array<{
            quality: string;
            bandwidth: number;
            resolution: string;
            playlistFilename: string;
        }> = [];

        for (let i = 0; i < smartQualities.length; i++) {
            // Check for shutdown signal between qualities
            if (isShuttingDown) {
                console.log(`[TRANSCODE] ⚠️ Shutdown requested, completing current quality before stopping...`);
            }

            const quality = smartQualities[i];
            const preset = QUALITY_PRESETS[quality];
            if (!preset) {
                // console.warn(`[TRANSCODE] ⚠️ Unknown quality preset: ${quality}, skipping`);
                continue;
            }

            // console.log(`\n[TRANSCODE] 🎞️ Transcoding quality ${i + 1}/${qualities.length}: ${quality}`);
            // console.log(`[TRANSCODE] 📐 Target resolution: ${preset.width}x${preset.height}`);
            // console.log(`[TRANSCODE] 🎚️ Video bitrate: ${preset.bitrate}, Audio bitrate: ${preset.audioBitrate}`);
            // console.log(`[TRANSCODE] ⏱️ Video duration: ${duration.toFixed(2)}s`);
            // console.log(`[TRANSCODE] 🔄 Starting HLS encoding...`);

            const startTime = Date.now();
            let lastLoggedPercent = 0;

            const hlsOutput = await ffmpegService.transcodeToHLS(
                inputPath,
                tempDir,
                quality,
                preset,
                (percent) => {
                    // Fine-grained progress to BullMQ/Redis only (not MongoDB)
                    const qualityWeight = (percent / 100) * 20;
                    const overallPercent = Math.round(
                        ((completedWeight + qualityWeight) / totalWeight) * 100
                    );

                    // Log every 5% for detailed tracking
                    if (percent >= lastLoggedPercent + 5 || percent === 100) {
                        const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
                        const progressBar = '█'.repeat(Math.floor(percent / 5)) + '░'.repeat(20 - Math.floor(percent / 5));
                        console.log(`[TRANSCODE] 🔄 ${quality}: [${progressBar}] ${percent}% | Time: ${elapsed}s | Overall: ${overallPercent}%`);
                        lastLoggedPercent = percent;
                    }

                    job.updateProgress(overallPercent).catch(() => { });
                }
            );

            const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
            await reportProgress(20);
            console.log(`[TRANSCODE] ✅ ${quality} HLS transcode complete in ${elapsed}s`);
            console.log(`[TRANSCODE] 📊 Generated ${hlsOutput.segmentCount} .ts segments + 1 .m3u8 playlist`);

            hlsOutputs.push({
                quality,
                playlistPath: hlsOutput.playlistPath,
                segmentFiles: hlsOutput.segmentFiles,
                segmentCount: hlsOutput.segmentCount
            });

            // Add to master playlist variants
            const bandwidth = parseInt(preset.bitrate) * 1000; // Convert to bits/sec
            masterPlaylistVariants.push({
                quality,
                bandwidth,
                resolution: `${preset.width}x${preset.height}`,
                playlistFilename: `${quality}.m3u8`
            });
        }

        console.log(`[TRANSCODE] ✅ All ${hlsOutputs.length} qualities transcoded to HLS`);
        kpis.timings.transcode = Date.now() - transcodeStartTime;
        console.log(`[TRANSCODE] ⏱️ Total transcoding time: ${(kpis.timings.transcode / 1000).toFixed(1)}s`);

        // Generate master playlist
        console.log(`\n[TRANSCODE] 📝 Generating master playlist...`);
        const masterPlaylistPath = await ffmpegService.generateMasterPlaylist(
            tempDir,
            masterPlaylistVariants
        );
        console.log(`[TRANSCODE] ✅ Master playlist generated`);

        // --- Phase 4: Generate thumbnails (multiple for selection) ---
        console.log(`\n[TRANSCODE] 🖼️ PHASE 4: Generating thumbnails`);
        const thumbnailPaths: string[] = [];
        const thumbnailTimestamps = [
            Math.min(duration * 0.1, 2),      // 10% or 2s
            Math.min(duration * 0.3, 10),     // 30% or 10s
            Math.min(duration * 0.5, 20),     // 50% or 20s
        ];

        for (let i = 0; i < thumbnailTimestamps.length; i++) {
            const thumbnailPath = path.join(tempDir, `thumbnail_${i + 1}.jpg`);
            const timestamp = thumbnailTimestamps[i];

            console.log(`[TRANSCODE] 🖼️ Generating thumbnail ${i + 1}/3 at ${timestamp.toFixed(1)}s`);
            await ffmpegService.generateThumbnail(inputPath, thumbnailPath, timestamp);
            thumbnailPaths.push(thumbnailPath);
        }

        await reportProgress(5);
        console.log(`[TRANSCODE] ✅ Generated ${thumbnailPaths.length} thumbnails`);

        const thumbnailKeys = thumbnailPaths.map((_, i) =>
            `${userType}/${userId}/${videoId}/thumbnail_${i + 1}.jpg`
        );

        // --- Phase 5: Upload HLS files to R2 TRANSCODED bucket ---
        console.log(`\n[TRANSCODE] ☁️ PHASE 5: Uploading HLS files to R2`);
        const uploadedOutputs = [];
        let totalFilesUploaded = 0;

        // Upload each quality's HLS files
        for (let i = 0; i < hlsOutputs.length; i++) {
            const output = hlsOutputs[i];
            console.log(`\n[TRANSCODE] 📤 Uploading quality ${i + 1}/${hlsOutputs.length}: ${output.quality}`);
            console.log(`[TRANSCODE] 📊 Files: 1 playlist + ${output.segmentCount} segments`);

            // Upload playlist (.m3u8)
            const playlistKey = `${userType}/${userId}/${videoId}/${output.quality}.m3u8`;
            console.log(`[TRANSCODE] 📤 Uploading playlist: ${output.quality}.m3u8`);
            await r2Service.uploadFromFile(
                R2_BUCKETS.TRANSCODED,
                playlistKey,
                output.playlistPath,
                'application/vnd.apple.mpegurl'
            );
            totalFilesUploaded++;

            // Upload all .ts segments
            let totalSegmentSize = 0;
            for (let j = 0; j < output.segmentFiles.length; j++) {
                const segmentFile = output.segmentFiles[j];
                const segmentFilename = path.basename(segmentFile);
                const segmentKey = `${userType}/${userId}/${videoId}/${segmentFilename}`;

                const { size } = await r2Service.uploadFromFile(
                    R2_BUCKETS.TRANSCODED,
                    segmentKey,
                    segmentFile,
                    'video/mp2t'
                );
                totalSegmentSize += size;
                totalFilesUploaded++;

                // Log progress every 10 segments
                if ((j + 1) % 10 === 0 || j === output.segmentFiles.length - 1) {
                    console.log(`[TRANSCODE] 📤 Uploaded ${j + 1}/${output.segmentFiles.length} segments`);
                }
            }

            // console.log(`[TRANSCODE] ✅ ${output.quality} uploaded (${(totalSegmentSize / (1024 * 1024)).toFixed(2)} MB)`);

            // Track size in KPIs
            if (output.quality === '1080p') kpis.sizes['1080p'] = totalSegmentSize;
            else if (output.quality === '720p') kpis.sizes['720p'] = totalSegmentSize;
            else if (output.quality === '480p') kpis.sizes['480p'] = totalSegmentSize;
            else if (output.quality === '360p') kpis.sizes['360p'] = totalSegmentSize;
            else if (output.quality === '240p') kpis.sizes['240p'] = totalSegmentSize;
            kpis.sizes.total = (kpis.sizes.total || 0) + totalSegmentSize;

            uploadedOutputs.push({
                quality: output.quality,
                playlistUrl: r2Service.getPublicUrl(playlistKey),
                segmentCount: output.segmentCount,
                size: totalSegmentSize,
            });
        }

        // Upload master playlist
        // console.log(`\n[TRANSCODE] 📤 Uploading master playlist...`);
        const masterPlaylistKey = `${userType}/${userId}/${videoId}/master.m3u8`;
        await r2Service.uploadFromFile(
            R2_BUCKETS.TRANSCODED,
            masterPlaylistKey,
            masterPlaylistPath,
            'application/vnd.apple.mpegurl'
        );
        totalFilesUploaded++;
        // console.log(`[TRANSCODE] ✅ Master playlist uploaded`);

        // Upload thumbnails
        const uploadStartTime = Date.now();
        // console.log(`\n[TRANSCODE] 📤 Uploading ${thumbnailPaths.length} thumbnails...`);
        const thumbnailUrls: string[] = [];

        for (let i = 0; i < thumbnailPaths.length; i++) {
            await r2Service.uploadFromFile(
                R2_BUCKETS.THUMBNAILS,
                thumbnailKeys[i],
                thumbnailPaths[i],
                'image/jpeg'
            );
            thumbnailUrls.push(r2Service.getPublicUrl(thumbnailKeys[i], R2_BUCKETS.THUMBNAILS));
            totalFilesUploaded++;
        }

        kpis.timings.upload = Date.now() - uploadStartTime;
        // console.log(`[TRANSCODE] ✅ All ${thumbnailPaths.length} thumbnails uploaded`);

        await reportProgress(5);
        kpis.timings.total = Date.now() - jobStartTime;
        // console.log(`[TRANSCODE] ✅ All uploads complete (${totalFilesUploaded} files)`);
        // console.log(`[TRANSCODE] ⏱️ Upload time: ${(kpis.timings.upload / 1000).toFixed(1)}s`);
        // console.log(`[TRANSCODE] ⏱️ Total job time: ${(kpis.timings.total / 1000).toFixed(1)}s`);


        // --- Phase 6: Finalize in MongoDB ---
        // console.log(`\n[TRANSCODE] 💾 PHASE 6: Finalizing in database`);
        await job.updateProgress(100);

        const masterPlaylistUrl = r2Service.getPublicUrl(masterPlaylistKey);

        await Video.findOneAndUpdate(
            { videoId },
            {
                status: 'completed',
                masterPlaylistUrl,  // HLS master playlist URL
                outputs: uploadedOutputs,  // Array of quality variants
                thumbnail: thumbnailUrls[0],  // Primary thumbnail
                thumbnails: thumbnailUrls,    // All thumbnails
                duration: Math.round(duration),
                kpis,  // Performance metrics
                'transcoding.progress': 100,
                'transcoding.completedAt': new Date(),
            }
        );

        // console.log(`[TRANSCODE] ✅ Video status updated to 'completed'`);
        // console.log(`[TRANSCODE] 🎬 Master playlist: ${masterPlaylistUrl}`);
        // console.log(`[TRANSCODE] 🎉 Job ${job.id} completed successfully!`);
        // console.log(`[TRANSCODE] 📊 Generated ${uploadedOutputs.length} HLS quality variants`);
        // console.log(`[TRANSCODE] 📦 Total files uploaded: ${totalFilesUploaded}`);
        // console.log(`${'='.repeat(80)}\n`);

        activeJobs--;
        return { success: true, masterPlaylistUrl, outputs: uploadedOutputs };
    } catch (error: any) {
        // console.error(`\n[TRANSCODE] ❌ Job ${job.id} FAILED`);
        // console.error(`[TRANSCODE] 💥 Error: ${error.message}`);
        // console.error(`[TRANSCODE] 📋 Stack trace:`, error.stack);

        // console.log(`[TRANSCODE] 💾 Updating video status to 'failed'...`);
        await Video.findOneAndUpdate(
            { videoId },
            {
                status: 'failed',
                'transcoding.error': error.message || 'Unknown transcoding error',
            }
        );
        // console.log(`[TRANSCODE] ✅ Video status updated to 'failed'`);
        // console.log(`${'='.repeat(80)}\n`);

        activeJobs--;
        throw error;
    } finally {
        // Always clean up temp files
        // console.log(`[TRANSCODE] 🧹 Cleaning up temp directory: ${tempDir}`);
        try {
            await fs.rm(tempDir, { recursive: true, force: true });
            // console.log(`[TRANSCODE] ✅ Temp directory cleaned up`);
        } catch (cleanupError) {
            // console.error(`[TRANSCODE] ⚠️ Failed to clean up temp directory: ${tempDir}`, cleanupError);
        }
    }
};

const updateVideoProgress = async (videoId: string, progress: number) => {
    try {
        await Video.findOneAndUpdate(
            { videoId },
            { 'transcoding.progress': progress }
        );
    } catch (error) {
        console.error(`[TRANSCODE] ⚠️ Failed to update progress for ${videoId}:`, error);
    }
};

// Graceful shutdown function
const gracefulShutdown = async (signal: string) => {
    if (isShuttingDown) {
        console.log('[SHUTDOWN] Already shutting down...');
        return;
    }

    console.log(`\n[SHUTDOWN] Received ${signal}. Starting graceful shutdown...`);
    isShuttingDown = true;

    // Stop accepting new jobs
    console.log('[SHUTDOWN] Pausing workers (no new jobs will be accepted)...');

    try {
        if (userWorker) {
            await userWorker.pause();
            console.log('[SHUTDOWN] User worker paused');
        }
        if (creatorWorker) {
            await creatorWorker.pause();
            console.log('[SHUTDOWN] Creator worker paused');
        }
    } catch (error) {
        console.error('[SHUTDOWN] Error pausing workers:', error);
    }

    // Wait for active jobs to complete (max 5 minutes)
    const maxWaitTime = 5 * 60 * 1000;
    const startTime = Date.now();

    console.log(`[SHUTDOWN] Waiting for ${activeJobs} active job(s) to complete...`);

    while (activeJobs > 0 && (Date.now() - startTime) < maxWaitTime) {
        await new Promise(resolve => setTimeout(resolve, 1000));
        console.log(`[SHUTDOWN] Still waiting for ${activeJobs} job(s)...`);
    }

    if (activeJobs > 0) {
        // console.warn(`[SHUTDOWN] ⚠️ Force closing with ${activeJobs} job(s) still active`);
    }

    // Close workers
    console.log('[SHUTDOWN] Closing workers...');
    try {
        if (userWorker) {
            await userWorker.close();
            console.log('[SHUTDOWN] User worker closed');
        }
        if (creatorWorker) {
            await creatorWorker.close();
            console.log('[SHUTDOWN] Creator worker closed');
        }
    } catch (error) {
        console.error('[SHUTDOWN] Error closing workers:', error);
    }

    // Close connections
    console.log('[SHUTDOWN] Closing database connections...');
    try {
        await Promise.allSettled([
            disconnectDatabase(),
            disconnectRedis(),
        ]);
        console.log('[SHUTDOWN] All connections closed');
    } catch (error) {
        console.error('[SHUTDOWN] Error closing connections:', error);
    }

    console.log('[SHUTDOWN] Graceful shutdown complete. Exiting...');
    process.exit(0);
};

// Handle uncaught exceptions
process.on('uncaughtException', (error) => {
    console.error('[FATAL] Uncaught Exception:', error);
    gracefulShutdown('uncaughtException');
});

// Handle unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
    console.error('[FATAL] Unhandled Rejection at:', promise, 'reason:', reason);
    gracefulShutdown('unhandledRejection');
});

// Handle termination signals
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

const startWorker = async () => {
    try {
        console.log('[STARTUP] Connecting to MongoDB...');
        await connectDatabase();

        console.log('[STARTUP] Connecting to Redis...');
        const connection = getRedisConnection();

        console.log('[STARTUP] Initializing workers...');

        // Worker settings with stalled job recovery enabled
        // Now using Railway Redis (unlimited requests), so we can enable proper job recovery
        const workerSettings = {
            connection,
            // Use blocking wait when no jobs (30 seconds block timeout)
            drainDelay: 30000,
            // Enable stalled check to recover interrupted jobs on redeploy
            skipStalledCheck: false,
            // Skip lock renewal for idle workers
            skipLockRenewal: false,
            // Lock renew interval - only when actively processing
            lockRenewTime: 30000,
            // Job recovery settings
            settings: {
                // Lock duration before considering job stalled (10 minutes)
                lockDuration: 600000,
                // Check for stalled jobs every 2 minutes
                stalledInterval: 120000,
                // Max stalled count before marking as failed
                maxStalledCount: 2,
            },
        };

        // Single worker handles both queues to minimize Redis connections
        // User queue worker
        userWorker = new Worker<TranscodeJobData>(
            'user-transcode-queue',
            processJob,
            {
                ...workerSettings,
                concurrency: 1,  // Reduced to 1 to save memory/Redis calls
                lockDuration: 600000, // 10 min timeout per job
            }
        );

        // Only start creator worker if explicitly enabled (saves Redis calls)
        const enableCreatorWorker = process.env.ENABLE_CREATOR_WORKER === 'true';
        if (enableCreatorWorker) {
            creatorWorker = new Worker<TranscodeJobData>(
                'creator-transcode-queue',
                processJob,
                {
                    ...workerSettings,
                    concurrency: 1,
                    lockDuration: 1800000, // 30 min timeout (larger files)
                }
            );
        } else {
            console.log('[STARTUP] Creator worker disabled (set ENABLE_CREATOR_WORKER=true to enable)');
        }

        // Event handlers
        const setupEventHandlers = (worker: Worker, name: string) => {
            worker.on('completed', (job) => {
                console.log(`[${name}] ✅ Job ${job.id} completed`);
            });

            worker.on('failed', (job, err) => {
                console.error(`[${name}] ❌ Job ${job?.id} failed:`, err.message);
            });

            worker.on('error', (err) => {
                console.error(`[${name}] ⚠️ Worker error:`, err);
            });

            worker.on('stalled', (jobId) => {
                // console.warn(`[${name}] ⚠️ Job ${jobId} stalled`);
            });
        };

        setupEventHandlers(userWorker, 'USER');
        if (creatorWorker) {
            setupEventHandlers(creatorWorker, 'CREATOR');
        }

        console.log(`\n${'='.repeat(80)}`);
        console.log('✅ TRANSCODING ENGINE READY');
        console.log('   - User queue worker (concurrency: 1, timeout: 10min)');
        if (creatorWorker) {
            console.log('   - Creator queue worker (concurrency: 1, timeout: 30min)');
        }
        console.log('   - Stalled job recovery: enabled (checks every 2 min)');
        console.log('   - Auto-retry on restart: yes (up to 3 attempts)');
        console.log('   - Listening for transcoding jobs...');
        console.log(`${'='.repeat(80)}\n`);
    } catch (error) {
        console.error('[FATAL] Failed to start transcoding engine:', error);
        process.exit(1);
    }
};

startWorker();
