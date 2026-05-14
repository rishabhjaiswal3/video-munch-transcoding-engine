import 'dotenv/config';
import { Worker, Job } from 'bullmq';
import { getRedisConfig, disconnectRedis, getRedisConnection } from './config/redis.js';
import { connectDatabase, disconnectDatabase } from './config/database.js';
import { Video } from './models/Video.js';
import { TranscodeJobData } from './types/index.js';
import { r2Service } from './services/r2Service.js';
import { R2_BUCKETS } from './config/r2.js';
import { ffmpegService, QualityPreset } from './services/ffmpegService.js';
import fs from 'fs/promises';
import path from 'path';
import os from 'os';
import { randomUUID } from 'crypto';
import { execSync } from 'child_process';

// ============================================================================
// CONFIGURATION
// ============================================================================

// Railway provides ~8GB disk space in /tmp by default
// But it's shared, so be conservative
const MIN_REQUIRED_DISK_SPACE_MULTIPLIER = 3; // Need ~3x file size (input + outputs)
const MIN_FREE_DISK_BYTES = 500 * 1024 * 1024; // Minimum 500MB free

// Memory thresholds (Railway free tier: 512MB, Pro: up to 8GB)
const MEMORY_THRESHOLD_PERCENT = 85; // Pause if memory usage > 85%

// Progress update throttling (prevent Redis overwhelm)
const PROGRESS_UPDATE_INTERVAL_MS = 2000; // Max 1 update per 2 seconds

// FFmpeg timeout (prevent hanging on corrupted files)
const FFMPEG_TIMEOUT_MS = 60 * 60 * 1000; // 60 minutes max per quality

// Instance metadata (useful on Railway multi-replica deployments)
const INSTANCE_ID =
    process.env.RAILWAY_REPLICA_ID ||
    process.env.RAILWAY_PRIVATE_DOMAIN ||
    process.env.RAILWAY_DEPLOYMENT_ID ||
    process.env.HOSTNAME ||
    `pid-${process.pid}`;

const parsePositiveIntEnv = (name: string): number | null => {
    const raw = process.env[name];
    if (!raw) return null;
    const parsed = Number.parseInt(raw, 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
};

const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));

const detectAvailableCpu = (): number => {
    const maybeAvailableParallelism = (os as any).availableParallelism;
    if (typeof maybeAvailableParallelism === 'function') {
        const value = maybeAvailableParallelism();
        if (Number.isFinite(value) && value > 0) return Math.floor(value);
    }

    const value = os.cpus()?.length || 1;
    return Math.max(1, Math.floor(value));
};

const computeConcurrencyPlan = (enableCreatorWorker: boolean, enableAdWorker: boolean) => {
    const availableCpu = detectAvailableCpu();
    const cpuTarget = clamp(Number.parseFloat(process.env.CPU_UTILIZATION_TARGET || '0.8') || 0.8, 0.2, 1);
    const reservedCpu = Math.max(0, parsePositiveIntEnv('RESERVED_CPU_CORES') || 1);
    const globalOverride = parsePositiveIntEnv('WORKER_CONCURRENCY');
    const userOverride = parsePositiveIntEnv('USER_WORKER_CONCURRENCY');
    const creatorOverride = parsePositiveIntEnv('CREATOR_WORKER_CONCURRENCY');
    const adOverride = parsePositiveIntEnv('AD_WORKER_CONCURRENCY');

    const autoBudget = Math.max(1, Math.floor(availableCpu * cpuTarget) - reservedCpu);

    if (!enableCreatorWorker && !enableAdWorker) {
        const userConcurrency = userOverride || globalOverride || autoBudget;
        return {
            availableCpu,
            cpuTarget,
            reservedCpu,
            autoBudget,
            userConcurrency: Math.max(1, userConcurrency),
            creatorConcurrency: 0,
            adConcurrency: 0,
            totalConcurrency: Math.max(1, userConcurrency),
        };
    }

    if (userOverride || creatorOverride || adOverride || globalOverride) {
        const userConcurrency = userOverride || globalOverride || 1;
        const creatorConcurrency = enableCreatorWorker ? (creatorOverride || globalOverride || 1) : 0;
        const adConcurrency = enableAdWorker ? (adOverride || globalOverride || 1) : 0;
        const total = Math.max(1, userConcurrency) + Math.max(0, creatorConcurrency) + Math.max(0, adConcurrency);
        return {
            availableCpu,
            cpuTarget,
            reservedCpu,
            autoBudget,
            userConcurrency: Math.max(1, userConcurrency),
            creatorConcurrency: Math.max(0, creatorConcurrency),
            adConcurrency: Math.max(0, adConcurrency),
            totalConcurrency: total,
        };
    }

    // Auto split: user 55%, creator 35%, ad 10%
    const userConcurrency = Math.max(1, Math.ceil(autoBudget * (enableCreatorWorker ? 0.55 : 0.65)));
    const creatorConcurrency = enableCreatorWorker ? Math.max(1, Math.floor(autoBudget * 0.35)) : 0;
    const adConcurrency = enableAdWorker ? Math.max(1, Math.floor(autoBudget * 0.10)) : 0;
    return {
        availableCpu,
        cpuTarget,
        reservedCpu,
        autoBudget,
        userConcurrency,
        creatorConcurrency,
        adConcurrency,
        totalConcurrency: userConcurrency + creatorConcurrency + adConcurrency,
    };
};

class CpuSlotTracker {
    private available: number[] = [];

    constructor(size: number) {
        this.reset(size);
    }

    reset(size: number) {
        const safeSize = Math.max(1, Math.floor(size));
        this.available = Array.from({ length: safeSize }, (_, idx) => idx + 1);
    }

    acquire(): number | null {
        if (this.available.length === 0) return null;
        return this.available.shift() || null;
    }

    release(slot: number | null) {
        if (!slot) return;
        if (!this.available.includes(slot)) {
            this.available.push(slot);
            this.available.sort((a, b) => a - b);
        }
    }
}

const cpuSlotTracker = new CpuSlotTracker(1);
const GLOBAL_SEMAPHORE_KEY = process.env.GLOBAL_TRANSCODE_SEMAPHORE_KEY || 'videomanch:transcode:global:slots';
const GLOBAL_SLOT_LEASE_MS = clamp(parsePositiveIntEnv('GLOBAL_SLOT_LEASE_MS') || 180000, 30000, 15 * 60 * 1000);
const GLOBAL_SLOT_WAIT_TIMEOUT_MS = clamp(parsePositiveIntEnv('GLOBAL_SLOT_WAIT_TIMEOUT_MS') || 5 * 60 * 1000, 30000, 30 * 60 * 1000);
const GLOBAL_SLOT_WAIT_POLL_MS = clamp(parsePositiveIntEnv('GLOBAL_SLOT_WAIT_POLL_MS') || 2000, 500, 15000);

class GlobalJobLimiter {
    private readonly maxSlots: number | null;

    constructor(maxSlots: number | null) {
        this.maxSlots = maxSlots && maxSlots > 0 ? Math.floor(maxSlots) : null;
    }

    isEnabled(): boolean {
        return this.maxSlots !== null;
    }

    getConfiguredMaxSlots(): number | null {
        return this.maxSlots;
    }

    getLeaseMs(): number {
        return GLOBAL_SLOT_LEASE_MS;
    }

    async acquire(jobCtx: string): Promise<string | null> {
        if (!this.maxSlots) return null;
        const redis = getRedisConnection();
        const token = `${INSTANCE_ID}:${randomUUID()}`;
        const deadline = Date.now() + GLOBAL_SLOT_WAIT_TIMEOUT_MS;

        while (Date.now() < deadline) {
            const acquired = await redis.eval(
                `
                local key = KEYS[1]
                local token = ARGV[1]
                local now = tonumber(ARGV[2])
                local leaseMs = tonumber(ARGV[3])
                local limit = tonumber(ARGV[4])
                redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
                local used = redis.call('ZCARD', key)
                if used < limit then
                    redis.call('ZADD', key, now + leaseMs, token)
                    redis.call('PEXPIRE', key, leaseMs * 2)
                    return 1
                end
                return 0
                `,
                1,
                GLOBAL_SEMAPHORE_KEY,
                token,
                Date.now(),
                GLOBAL_SLOT_LEASE_MS,
                this.maxSlots
            );

            if (Number(acquired) === 1) {
                const activeSlots = await redis.zcard(GLOBAL_SEMAPHORE_KEY);
                console.log(`${jobCtx} [GLOBAL] slot acquired (${activeSlots}/${this.maxSlots})`);
                return token;
            }

            await sleep(GLOBAL_SLOT_WAIT_POLL_MS);
        }

        return null;
    }

    async renew(token: string): Promise<boolean> {
        if (!this.maxSlots) return true;
        const redis = getRedisConnection();
        const renewed = await redis.eval(
            `
            local key = KEYS[1]
            local token = ARGV[1]
            local now = tonumber(ARGV[2])
            local leaseMs = tonumber(ARGV[3])
            local exists = redis.call('ZSCORE', key, token)
            if exists then
                redis.call('ZADD', key, now + leaseMs, token)
                redis.call('PEXPIRE', key, leaseMs * 2)
                return 1
            end
            return 0
            `,
            1,
            GLOBAL_SEMAPHORE_KEY,
            token,
            Date.now(),
            GLOBAL_SLOT_LEASE_MS
        );
        return Number(renewed) === 1;
    }

    async release(jobCtx: string, token: string | null) {
        if (!this.maxSlots || !token) return;
        try {
            const redis = getRedisConnection();
            await redis.zrem(GLOBAL_SEMAPHORE_KEY, token);
            const activeSlots = await redis.zcard(GLOBAL_SEMAPHORE_KEY);
            console.log(`${jobCtx} [GLOBAL] slot released (${activeSlots}/${this.maxSlots})`);
        } catch (error) {
            console.error(`${jobCtx} [GLOBAL] failed to release slot`, error);
        }
    }
}

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

/**
 * Get available disk space in bytes for a given path
 * Works on both Linux (Railway) and macOS (local dev)
 */
const getAvailableDiskSpace = async (targetPath: string): Promise<number> => {
    try {
        // Ensure the directory exists
        await fs.mkdir(targetPath, { recursive: true });

        if (process.platform === 'win32') {
            // Windows - not supported in production but handle for local dev
            return Number.MAX_SAFE_INTEGER;
        }

        // Unix/Linux/macOS - use df command
        const output = execSync(`df -k "${targetPath}" | tail -1 | awk '{print $4}'`, {
            encoding: 'utf-8',
        }).trim();

        const availableKB = parseInt(output, 10);
        if (Number.isFinite(availableKB)) {
            return availableKB * 1024; // Convert KB to bytes
        }

        console.warn('[DISK] Could not parse disk space, assuming unlimited');
        return Number.MAX_SAFE_INTEGER;
    } catch (error) {
        console.error('[DISK] Error checking disk space:', error);
        // On Railway, /tmp is memory-backed, so check available memory instead
        const memInfo = process.memoryUsage();
        const heapAvailable = memInfo.heapTotal - memInfo.heapUsed;
        return heapAvailable * 2; // Rough estimate
    }
};

/**
 * Check if system has enough memory to process
 * Returns { canProcess: boolean, usagePercent: number, availableMB: number }
 */
const checkMemoryUsage = (): { canProcess: boolean; usagePercent: number; availableMB: number } => {
    const totalMemory = os.totalmem();
    const freeMemory = os.freemem();
    const usedMemory = totalMemory - freeMemory;
    const usagePercent = Math.round((usedMemory / totalMemory) * 100);
    const availableMB = Math.round(freeMemory / (1024 * 1024));

    return {
        canProcess: usagePercent < MEMORY_THRESHOLD_PERCENT,
        usagePercent,
        availableMB,
    };
};

/**
 * Sleep utility for retry delays
 */
const sleep = (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Wrap a promise with a timeout
 */
const withTimeout = async <T>(
    promise: Promise<T>,
    timeoutMs: number,
    operationName: string
): Promise<T> => {
    let timeoutHandle: NodeJS.Timeout | null = null;

    try {
        return await Promise.race([
            promise,
            new Promise<T>((_, reject) => {
                timeoutHandle = setTimeout(() => {
                    reject(new Error(`${operationName} timed out after ${Math.round(timeoutMs / 1000)}s`));
                }, timeoutMs);
            }),
        ]);
    } finally {
        if (timeoutHandle) clearTimeout(timeoutHandle);
    }
};

/**
 * Retry wrapper with exponential backoff
 */
const withRetry = async <T>(
    fn: () => Promise<T>,
    maxRetries: number = 3,
    baseDelayMs: number = 1000,
    operationName: string = 'operation'
): Promise<T> => {
    let lastError: Error | null = null;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            return await fn();
        } catch (error: any) {
            lastError = error;
            console.warn(`[RETRY] ${operationName} failed (attempt ${attempt}/${maxRetries}): ${error.message}`);

            if (attempt < maxRetries) {
                const delay = baseDelayMs * Math.pow(2, attempt - 1);
                console.log(`[RETRY] Waiting ${delay}ms before retry...`);
                await sleep(delay);
            }
        }
    }

    throw lastError;
};

/**
 * Create a throttled progress reporter
 * Only reports progress at most once per interval
 */
const createThrottledProgressReporter = (
    job: Job,
    updateVideoProgress: (videoId: string, progress: number) => Promise<void>,
    videoId: string
) => {
    let lastUpdateTime = 0;
    let lastReportedPercent = -1;

    return async (percent: number) => {
        const now = Date.now();

        // Always report 0 and 100
        const isImportant = percent === 0 || percent === 100 || percent - lastReportedPercent >= 10;
        const throttleExpired = now - lastUpdateTime >= PROGRESS_UPDATE_INTERVAL_MS;

        if (isImportant || throttleExpired) {
            lastUpdateTime = now;
            lastReportedPercent = percent;

            // Update BullMQ job progress (Redis) - fire and forget
            job.updateProgress(percent).catch(() => { });

            // Update MongoDB only for significant changes (every 10%)
            if (percent % 10 === 0 || percent === 100) {
                await updateVideoProgress(videoId, percent);
            }
        }
    };
};

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

// Safe FPS parsing - replaces dangerous eval()
// Handles formats like "30/1", "30000/1001", "30", "29.97"
const parseFps = (fpsString: string | undefined): number => {
    if (!fpsString) return 0;

    // Handle fraction format (e.g., "30/1" or "30000/1001")
    if (fpsString.includes('/')) {
        const parts = fpsString.split('/');
        if (parts.length === 2) {
            const numerator = parseFloat(parts[0]);
            const denominator = parseFloat(parts[1]);
            if (Number.isFinite(numerator) && Number.isFinite(denominator) && denominator !== 0) {
                return numerator / denominator;
            }
        }
        return 0;
    }

    // Handle decimal format (e.g., "29.97")
    const parsed = parseFloat(fpsString);
    return Number.isFinite(parsed) ? parsed : 0;
};

// Track workers for graceful shutdown
let userWorker: Worker<TranscodeJobData> | null = null;
let creatorWorker: Worker<TranscodeJobData> | null = null;
let adWorker: Worker<TranscodeJobData> | null = null;
let globalJobLimiter: GlobalJobLimiter | null = null;
let isShuttingDown = false;
let activeJobs = 0;

const processJob = async (job: Job<TranscodeJobData>) => {
    const { videoId, userId, userType, r2Key, qualities } = job.data;
    const tempDir = path.join(TEMP_BASE_DIR, videoId);
    const cpuSlot = cpuSlotTracker.acquire();
    const cpuLabel = cpuSlot ? `cpu-${cpuSlot}` : 'cpu-unassigned';
    const jobCtx = `[INSTANCE:${INSTANCE_ID}][${cpuLabel}][QUEUE:${job.queueName}][VIDEO:${videoId}]`;
    const ctxLog = (message: string) => console.log(`${jobCtx} ${message}`);
    const ctxWarn = (message: string) => console.warn(`${jobCtx} ${message}`);
    const ctxError = (message: string, error?: unknown) => {
        if (error) {
            console.error(`${jobCtx} ${message}`, error);
        } else {
            console.error(`${jobCtx} ${message}`);
        }
    };
    let globalSlotToken: string | null = null;
    let globalRenewTimer: NodeJS.Timeout | null = null;

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
    kpis.timings.queueWait = Math.max(0, jobStartTime - (job.timestamp || jobStartTime));

    console.log(`\n${'='.repeat(80)}`);
    ctxLog(`🎬 Starting transcoding job ${job.id}`);
    ctxLog(`👤 User: ${userId} (${userType})`);
    ctxLog(`🎯 Target qualities: ${qualities.join(', ')}`);
    ctxLog(`📁 Temp directory: ${tempDir}`);
    console.log(`${'='.repeat(80)}\n`);

    try {
        if (globalJobLimiter?.isEnabled()) {
            globalSlotToken = await globalJobLimiter.acquire(jobCtx);
            if (!globalSlotToken) {
                throw new Error(
                    `Global transcode capacity is full. Waited ${Math.round(GLOBAL_SLOT_WAIT_TIMEOUT_MS / 1000)}s for a slot.`
                );
            }
            const renewEveryMs = Math.max(10000, Math.floor(globalJobLimiter.getLeaseMs() / 3));
            globalRenewTimer = setInterval(() => {
                if (!globalSlotToken || !globalJobLimiter) return;
                globalJobLimiter.renew(globalSlotToken)
                    .then(renewed => {
                        if (!renewed) {
                            ctxWarn('global slot lease was lost; continuing current job and relying on local capacity guard');
                        }
                    })
                    .catch(err => {
                        ctxWarn(`failed to renew global slot lease: ${err?.message || 'unknown error'}`);
                    });
            }, renewEveryMs);
            globalRenewTimer.unref();
        }

        // --- Pre-flight checks ---
        console.log(`[TRANSCODE] 🔍 Running pre-flight checks...`);

        // Check memory availability
        const memCheck = checkMemoryUsage();
        console.log(`[TRANSCODE] 💾 Memory: ${memCheck.usagePercent}% used, ${memCheck.availableMB}MB available`);
        if (!memCheck.canProcess) {
            throw new Error(`Insufficient memory: ${memCheck.usagePercent}% used (threshold: ${MEMORY_THRESHOLD_PERCENT}%). Please try again later.`);
        }

        // --- Setup ---
        console.log(`[TRANSCODE] 📂 Creating temp directory...`);
        await fs.mkdir(tempDir, { recursive: true });
        console.log(`[TRANSCODE] ✅ Temp directory created`);

        // Get file size from R2 metadata BEFORE downloading
        console.log(`[TRANSCODE] 📊 Checking file size from R2...`);
        const r2Metadata = await r2Service.getFileMetadata(R2_BUCKETS.RAW, r2Key);
        if (!r2Metadata) {
            throw new Error(`File not found in R2: ${r2Key}`);
        }
        const expectedFileSize = r2Metadata.size;
        console.log(`[TRANSCODE] 📦 Expected file size: ${(expectedFileSize / (1024 * 1024)).toFixed(2)} MB`);

        // Check disk space (need ~3x file size: 1x original + 2x for transcoded outputs)
        const requiredSpace = expectedFileSize * MIN_REQUIRED_DISK_SPACE_MULTIPLIER;
        const availableSpace = await getAvailableDiskSpace(TEMP_BASE_DIR);
        console.log(`[TRANSCODE] 💿 Disk space: ${(availableSpace / (1024 * 1024)).toFixed(0)}MB available, ${(requiredSpace / (1024 * 1024)).toFixed(0)}MB required`);

        if (availableSpace < requiredSpace || availableSpace < MIN_FREE_DISK_BYTES) {
            // Clean up stale temp directories - but ONLY if video is not actively processing
            console.log(`[TRANSCODE] 🧹 Attempting to clean up stale temp directories...`);
            try {
                const tempContents = await fs.readdir(TEMP_BASE_DIR);
                for (const dir of tempContents) {
                    // Skip our own directory
                    if (dir === videoId) continue;

                    const dirPath = path.join(TEMP_BASE_DIR, dir);
                    const stat = await fs.stat(dirPath);

                    // Only consider directories older than 30 minutes
                    const isOld = Date.now() - stat.mtimeMs > 30 * 60 * 1000;
                    if (!isOld) continue;

                    // CRITICAL: Check MongoDB to ensure this video is not actively processing
                    // The directory name IS the videoId
                    const otherVideo = await Video.findOne({ videoId: dir });

                    // Safe to delete if:
                    // 1. Video doesn't exist in DB (orphaned directory)
                    // 2. Video exists but is NOT in 'processing' status
                    const isActivelyProcessing = otherVideo?.status === 'processing';

                    if (!isActivelyProcessing) {
                        await fs.rm(dirPath, { recursive: true, force: true });
                        console.log(`[TRANSCODE] 🗑️ Cleaned up stale directory: ${dir} (status: ${otherVideo?.status || 'not found'})`);
                    } else {
                        console.log(`[TRANSCODE] ⚠️ Skipping active directory: ${dir} (status: processing)`);
                    }
                }
            } catch (cleanupError) {
                // Ignore cleanup errors
            }

            // Re-check space after cleanup
            const spaceAfterCleanup = await getAvailableDiskSpace(TEMP_BASE_DIR);
            if (spaceAfterCleanup < requiredSpace || spaceAfterCleanup < MIN_FREE_DISK_BYTES) {
                throw new Error(
                    `Insufficient disk space: ${(spaceAfterCleanup / (1024 * 1024)).toFixed(0)}MB available, ` +
                    `${(requiredSpace / (1024 * 1024)).toFixed(0)}MB required. ` +
                    `Video file is ${(expectedFileSize / (1024 * 1024)).toFixed(0)}MB. ` +
                    `Try with a smaller video or wait for other jobs to complete.`
                );
            }
        }

        console.log(`[TRANSCODE] 💾 Updating video status to 'processing'...`);
        if (!job.data.callbackUrl) {
            await Video.findOneAndUpdate(
                { videoId },
                {
                    status: 'processing',
                    'transcoding.startedAt': new Date(),
                    'transcoding.progress': 0,
                }
            );
        }
        console.log(`[TRANSCODE] ✅ Video status updated to 'processing'`);

        // Weighted progress: download=10, each quality=20, thumbnail=5, upload=5
        const totalWeight = 10 + (qualities.length * 20) + 5 + 5;
        let completedWeight = 0;

        // Create throttled progress reporter to prevent Redis overwhelm
        const throttledProgress = createThrottledProgressReporter(job, updateVideoProgress, videoId);

        const reportProgress = async (weight: number) => {
            completedWeight += weight;
            const percent = Math.min(Math.round((completedWeight / totalWeight) * 100), 99);
            console.log(`[TRANSCODE] 📊 Progress: ${percent}% (${completedWeight}/${totalWeight} weight units)`);
            await throttledProgress(percent);
        };

        // --- Phase 1: Download original from R2 ---
        console.log(`\n[TRANSCODE] 📥 PHASE 1: Downloading original video from R2`);
        const downloadStartTime = Date.now();
        const originalExt = path.extname(r2Key) || '.mp4';
        const inputPath = path.join(tempDir, `original${originalExt}`);

        console.log(`[TRANSCODE] ☁️ R2 Key: ${r2Key}`);
        console.log(`[TRANSCODE] 💾 Local path: ${inputPath}`);

        // Download with retry logic
        await withRetry(
            () => r2Service.downloadToFile(R2_BUCKETS.RAW, r2Key, inputPath),
            3,
            2000,
            'R2 download'
        );
        kpis.timings.download = Date.now() - downloadStartTime;

        // Get original file size
        const stats = await fs.stat(inputPath);
        kpis.sizes.original = stats.size;

        await reportProgress(10);
        console.log(`[TRANSCODE] ✅ Download complete (${(kpis.sizes.original / (1024 * 1024)).toFixed(2)} MB in ${(kpis.timings.download / 1000).toFixed(1)}s)`);

        // --- Phase 2: Probe video for metadata ---
        console.log(`\n[TRANSCODE] 🔍 PHASE 2: Probing video metadata`);
        const probeData = await withTimeout(
            ffmpegService.probe(inputPath),
            FFMPEG_TIMEOUT_MS,
            'ffprobe'
        );
        const duration = probeData.format.duration || 0;
        const videoStream = probeData.streams.find(s => s.codec_type === 'video');

        // Validate video stream exists
        if (!videoStream) {
            throw new Error('Invalid video file: No video stream found. This may be an audio-only file or corrupted.');
        }

        const originalWidth = videoStream.width || 0;
        const originalHeight = videoStream.height || 0;

        // Validate video dimensions
        if (originalWidth < 64 || originalHeight < 64) {
            throw new Error(`Invalid video dimensions: ${originalWidth}x${originalHeight}. Minimum supported is 64x64.`);
        }

        // Validate duration
        if (duration <= 0) {
            throw new Error('Invalid video: Could not determine duration. File may be corrupted.');
        }

        const originalCodec = videoStream.codec_name;
        const originalBitrate = parseInt(String(probeData.format.bit_rate || '0'));
        const originalFps = parseFps(videoStream.r_frame_rate);

        console.log(`[TRANSCODE] ⏱️ Video duration: ${duration.toFixed(2)}s`);
        console.log(`[TRANSCODE] 📐 Video dimensions: ${originalWidth}x${originalHeight}`);
        console.log(`[TRANSCODE] 🎞️ Codec: ${originalCodec}, Bitrate: ${(originalBitrate / 1000).toFixed(0)}k, FPS: ${originalFps.toFixed(1)}`);
        console.log(`[TRANSCODE] ✅ Probe complete`);

        // Store original metadata (skip for callback jobs — they don't have a Video record here)
        if (!job.data.callbackUrl) {
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
        }
        const probeResult = { width: originalWidth, height: originalHeight, duration, codec: originalCodec, bitrate: originalBitrate };

        // Smart quality selection - don't upscale, but include source quality
        const smartQualities = qualities.filter(quality => {
            const preset = QUALITY_PRESETS[quality];
            if (!preset) return false;

            // Skip if target resolution is higher than source (no upscaling)
            if (preset.height > originalHeight) {
                console.log(`[TRANSCODE] ⚠️ Skipping ${quality} (${preset.height}p) - source is only ${originalHeight}p`);
                return false;
            }

            // Include source quality - if source is 1080p, create 1080p variant
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

            // Check memory before each quality transcoding
            const memCheckBeforeQuality = checkMemoryUsage();
            if (!memCheckBeforeQuality.canProcess) {
            ctxWarn(`⚠️ Memory high (${memCheckBeforeQuality.usagePercent}%), waiting 10s before ${smartQualities[i]}...`);
                await sleep(10000);
            }

            const quality = smartQualities[i];
            const preset = QUALITY_PRESETS[quality];
            if (!preset) {
                // console.warn(`[TRANSCODE] ⚠️ Unknown quality preset: ${quality}, skipping`);
                continue;
            }

            console.log(`\n[TRANSCODE] 🎞️ Transcoding quality ${i + 1}/${smartQualities.length}: ${quality}`);
            console.log(`[TRANSCODE] 📐 Target resolution: ${preset.width}x${preset.height}`);

            const startTime = Date.now();
            let lastLoggedPercent = 0;
            let lastProgressUpdate = 0;

            const hlsOutput = await ffmpegService.transcodeToHLS(
                inputPath,
                tempDir,
                quality,
                preset,
                (percent) => {
                    const now = Date.now();

                    // Throttle progress updates to prevent Redis overwhelm
                    const shouldUpdate =
                        percent === 0 ||
                        percent === 100 ||
                        now - lastProgressUpdate >= PROGRESS_UPDATE_INTERVAL_MS;

                    if (!shouldUpdate) return;
                    lastProgressUpdate = now;

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

                    // Throttled update to Redis
                    job.updateProgress(overallPercent).catch(() => { });
                },
                FFMPEG_TIMEOUT_MS
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
            await withTimeout(
                ffmpegService.generateThumbnail(inputPath, thumbnailPath, timestamp, FFMPEG_TIMEOUT_MS),
                FFMPEG_TIMEOUT_MS,
                `thumbnail generation ${i + 1}`
            );
            thumbnailPaths.push(thumbnailPath);
        }

        await reportProgress(5);
        console.log(`[TRANSCODE] ✅ Generated ${thumbnailPaths.length} thumbnails`);

        const thumbnailKeys = thumbnailPaths.map((_, i) =>
            `${userType}/${userId}/${videoId}/thumbnail_${i + 1}.jpg`
        );

        // --- Phase 5: Upload HLS files to R2 TRANSCODED bucket ---
        console.log(`\n[TRANSCODE] ☁️ PHASE 5: Uploading HLS files to R2 (with retry)`);
        const uploadedOutputs = [];
        let totalFilesUploaded = 0;

        // Upload each quality's HLS files
        for (let i = 0; i < hlsOutputs.length; i++) {
            const output = hlsOutputs[i];
            console.log(`\n[TRANSCODE] 📤 Uploading quality ${i + 1}/${hlsOutputs.length}: ${output.quality}`);
            console.log(`[TRANSCODE] 📊 Files: 1 playlist + ${output.segmentCount} segments`);

            // Upload playlist (.m3u8) with retry
            const playlistKey = `${userType}/${userId}/${videoId}/${output.quality}.m3u8`;
            console.log(`[TRANSCODE] 📤 Uploading playlist: ${output.quality}.m3u8`);
            await withRetry(
                () => r2Service.uploadFromFile(
                    R2_BUCKETS.TRANSCODED,
                    playlistKey,
                    output.playlistPath,
                    'application/vnd.apple.mpegurl'
                ),
                3, 2000, `upload ${output.quality}.m3u8`
            );
            totalFilesUploaded++;

            // Upload all .ts segments with retry
            let totalSegmentSize = 0;
            for (let j = 0; j < output.segmentFiles.length; j++) {
                const segmentFile = output.segmentFiles[j];
                const segmentFilename = path.basename(segmentFile);
                const segmentKey = `${userType}/${userId}/${videoId}/${segmentFilename}`;

                const { size } = await withRetry(
                    () => r2Service.uploadFromFile(
                        R2_BUCKETS.TRANSCODED,
                        segmentKey,
                        segmentFile,
                        'video/mp2t'
                    ),
                    3, 1000, `upload segment ${segmentFilename}`
                );
                totalSegmentSize += size;
                totalFilesUploaded++;

                // Log progress every 10 segments
                if ((j + 1) % 10 === 0 || j === output.segmentFiles.length - 1) {
                    console.log(`[TRANSCODE] 📤 Uploaded ${j + 1}/${output.segmentFiles.length} segments`);
                }
            }

            console.log(`[TRANSCODE] ✅ ${output.quality} uploaded (${(totalSegmentSize / (1024 * 1024)).toFixed(2)} MB)`);

            // Track size in KPIs
            if (output.quality === '1080p') kpis.sizes['1080p'] = totalSegmentSize;
            else if (output.quality === '720p') kpis.sizes['720p'] = totalSegmentSize;
            else if (output.quality === '480p') kpis.sizes['480p'] = totalSegmentSize;
            else if (output.quality === '360p') kpis.sizes['360p'] = totalSegmentSize;
            else if (output.quality === '240p') kpis.sizes['240p'] = totalSegmentSize;
            kpis.sizes.total = (kpis.sizes.total || 0) + totalSegmentSize;

            uploadedOutputs.push({
                quality: output.quality,
                playlistUrl: playlistKey,  // Store only path, frontend constructs full URL
                segmentCount: output.segmentCount,
                size: totalSegmentSize,
            });
        }

        // Upload master playlist with retry
        console.log(`\n[TRANSCODE] 📤 Uploading master playlist...`);
        const masterPlaylistKey = `${userType}/${userId}/${videoId}/master.m3u8`;
        await withRetry(
            () => r2Service.uploadFromFile(
                R2_BUCKETS.TRANSCODED,
                masterPlaylistKey,
                masterPlaylistPath,
                'application/vnd.apple.mpegurl'
            ),
            3, 2000, 'upload master.m3u8'
        );
        totalFilesUploaded++;
        console.log(`[TRANSCODE] ✅ Master playlist uploaded`);

        // Upload thumbnails with retry
        const uploadStartTime = Date.now();
        console.log(`\n[TRANSCODE] 📤 Uploading ${thumbnailPaths.length} thumbnails...`);
        const thumbnailUrls: string[] = [];

        for (let i = 0; i < thumbnailPaths.length; i++) {
            await withRetry(
                () => r2Service.uploadFromFile(
                    R2_BUCKETS.THUMBNAILS,
                    thumbnailKeys[i],
                    thumbnailPaths[i],
                    'image/jpeg'
                ),
                3, 1000, `upload thumbnail ${i + 1}`
            );
            thumbnailUrls.push(thumbnailKeys[i]);  // Store only path, frontend constructs full URL
            totalFilesUploaded++;
        }
        console.log(`[TRANSCODE] ✅ All thumbnails uploaded`);

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

        // Store only the R2 key (path), frontend constructs full URL using R2 bucket constants
        console.log(`[TRANSCODE] 💾 Saving to DB - masterPlaylistUrl: ${masterPlaylistKey}`);
        console.log(`[TRANSCODE] 💾 Saving to DB - thumbnail: ${thumbnailUrls[0]}`);
        console.log(`[TRANSCODE] 💾 Saving to DB - outputs:`, JSON.stringify(uploadedOutputs, null, 2));

        const { callbackUrl } = job.data;

        if (callbackUrl) {
            // Ad jobs (or any job with callbackUrl): POST results back instead of writing to Video collection
            const callbackOk = await postCallback(callbackUrl, {
                masterPlaylistUrl: masterPlaylistKey,
                outputs: uploadedOutputs,
                thumbnail: thumbnailUrls[0],
                thumbnails: thumbnailUrls,
                originalMetadata: probeResult,
                duration: Math.round(duration),
            }, jobCtx);
            if (!callbackOk) {
                throw new Error(`Callback to ${callbackUrl} failed after retries`);
            }
        } else {
            // Standard video jobs: write directly to Video collection
            await Video.findOneAndUpdate(
                { videoId },
                {
                    status: 'completed',
                    transcodingCompleted: true,
                    masterPlaylistUrl: masterPlaylistKey,
                    outputs: uploadedOutputs,
                    thumbnail: thumbnailUrls[0],
                    thumbnails: thumbnailUrls,
                    duration: Math.round(duration),
                    kpis,
                    'transcoding.progress': 100,
                    'transcoding.completedAt': new Date(),
                }
            );
        }

        console.log(`[TRANSCODE] ✅ Video status updated to 'completed'`);
        ctxLog(`🎉 Job ${job.id} completed successfully!`);

        // Job cleanup policy is managed by queue producer defaults:
        // removeOnComplete/removeOnFail in video-manch-backend/src/services/queueService.ts

        return { success: true, masterPlaylistUrl: masterPlaylistKey, outputs: uploadedOutputs };
    } catch (error: any) {
        // console.error(`\n[TRANSCODE] ❌ Job ${job.id} FAILED`);
        // console.error(`[TRANSCODE] 💥 Error: ${error.message}`);
        // console.error(`[TRANSCODE] 📋 Stack trace:`, error.stack);

        // console.log(`[TRANSCODE] 💾 Updating video status to 'failed'...`);
        try {
            const { callbackUrl } = job.data;
            if (callbackUrl) {
                await postCallback(callbackUrl, {
                    error: error.message || 'Unknown transcoding error',
                }, jobCtx).catch(() => {});
            } else {
                await Video.findOneAndUpdate(
                    { videoId },
                    {
                        status: 'failed',
                        'transcoding.error': error.message || 'Unknown transcoding error',
                    }
                );
            }
        } catch (dbError) {
            ctxError(`⚠️ Failed to update video status to 'failed' in DB`, dbError);
        }
        ctxLog(`❌ Job failed; video status updated to 'failed'`);

        // Job cleanup policy is managed by queue producer defaults:
        // removeOnComplete/removeOnFail in video-manch-backend/src/services/queueService.ts

        throw error;
    } finally {
        activeJobs--;
        cpuSlotTracker.release(cpuSlot);
        if (globalRenewTimer) {
            clearInterval(globalRenewTimer);
            globalRenewTimer = null;
        }
        await globalJobLimiter?.release(jobCtx, globalSlotToken);

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

const postCallback = async (
    callbackUrl: string,
    payload: Record<string, unknown>,
    jobCtx: string,
): Promise<boolean> => {
    const MAX_RETRIES = 3;
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        try {
            const internalSecret = process.env.INTERNAL_SECRET;
            const resp = await fetch(callbackUrl, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    ...(internalSecret ? { 'X-Internal-Secret': internalSecret } : {}),
                },
                body: JSON.stringify(payload),
            });
            if (resp.ok) {
                console.log(`${jobCtx} [CALLBACK] POST to ${callbackUrl} succeeded (attempt ${attempt})`);
                return true;
            }
            console.warn(`${jobCtx} [CALLBACK] POST to ${callbackUrl} returned ${resp.status} (attempt ${attempt})`);
        } catch (err: any) {
            console.warn(`${jobCtx} [CALLBACK] POST to ${callbackUrl} failed (attempt ${attempt}): ${err.message}`);
        }
        if (attempt < MAX_RETRIES) {
            await new Promise(r => setTimeout(r, 2000 * attempt));
        }
    }
    return false;
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
        if (adWorker) {
            await adWorker.pause();
            console.log('[SHUTDOWN] Ad worker paused');
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
        if (adWorker) {
            await adWorker.close();
            console.log('[SHUTDOWN] Ad worker closed');
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

        console.log('[STARTUP] Getting Redis config...');
        const redisConfig = getRedisConfig();

        console.log('[STARTUP] Initializing workers...');
        const enableCreatorWorker = (process.env.ENABLE_CREATOR_WORKER || 'true').toLowerCase() !== 'false';
        const enableAdWorker = (process.env.ENABLE_AD_WORKER || 'true').toLowerCase() !== 'false';
        const concurrencyPlan = computeConcurrencyPlan(enableCreatorWorker, enableAdWorker);
        const replicaCountHint = parsePositiveIntEnv('REPLICA_COUNT') || parsePositiveIntEnv('RAILWAY_REPLICA_COUNT');
        const globalMaxTranscodeJobs = parsePositiveIntEnv('GLOBAL_MAX_TRANSCODE_JOBS');
        globalJobLimiter = new GlobalJobLimiter(globalMaxTranscodeJobs);
        cpuSlotTracker.reset(concurrencyPlan.totalConcurrency);

        if (enableCreatorWorker && concurrencyPlan.autoBudget < 2 && !parsePositiveIntEnv('WORKER_CONCURRENCY') && !parsePositiveIntEnv('USER_WORKER_CONCURRENCY') && !parsePositiveIntEnv('CREATOR_WORKER_CONCURRENCY')) {
            console.warn(`[STARTUP] [INSTANCE:${INSTANCE_ID}] Low CPU budget detected (${concurrencyPlan.autoBudget}). Set WORKER_CONCURRENCY/USER_WORKER_CONCURRENCY/CREATOR_WORKER_CONCURRENCY for manual tuning.`);
        }

        // Worker settings with stalled job recovery enabled
        // Now using Railway Redis (unlimited requests), so we can enable proper job recovery
        const workerSettings = {
            connection: redisConfig,
            // Use blocking wait when no jobs (30 seconds block timeout)
            drainDelay: 30000,
            // Enable stalled check to recover interrupted jobs on redeploy
            skipStalledCheck: false,
            // Skip lock renewal for idle workers
            skipLockRenewal: false,
            // Lock renew interval - only when actively processing
            lockRenewTime: 30000,
            // Stalled job recovery settings (top-level in BullMQ v5+)
            stalledInterval: 120000,  // Check for stalled jobs every 2 minutes
            maxStalledCount: 2,       // Max stalled count before marking as failed
        };

        // Single worker handles both queues to minimize Redis connections
        // User queue worker
        userWorker = new Worker<TranscodeJobData>(
            'user-transcode-queue',
            processJob,
            {
                ...workerSettings,
                concurrency: concurrencyPlan.userConcurrency,
                lockDuration: 600000, // 10 min timeout per job
            }
        );

        // Start creator worker by default; allow opt-out with ENABLE_CREATOR_WORKER=false
        if (enableCreatorWorker) {
            creatorWorker = new Worker<TranscodeJobData>(
                'creator-transcode-queue',
                processJob,
                {
                    ...workerSettings,
                    concurrency: concurrencyPlan.creatorConcurrency,
                    lockDuration: 1800000, // 30 min timeout (larger files)
                }
            );
        } else {
            console.log('[STARTUP] Creator worker disabled (set ENABLE_CREATOR_WORKER=true or unset it to enable)');
        }

        // Start ad worker for ad-transcode-queue
        if (enableAdWorker) {
            adWorker = new Worker<TranscodeJobData>(
                'ad-transcode-queue',
                processJob,
                {
                    ...workerSettings,
                    concurrency: concurrencyPlan.adConcurrency,
                    lockDuration: 300000, // 5 min timeout (ads are short, 15-30s)
                }
            );
        } else {
            console.log('[STARTUP] Ad worker disabled (set ENABLE_AD_WORKER=true or unset it to enable)');
        }

        // Event handlers
        const setupEventHandlers = (worker: Worker, name: string) => {
            worker.on('completed', (job) => {
                console.log(`[${name}] [INSTANCE:${INSTANCE_ID}] ✅ Job ${job.id} completed`);
            });

            worker.on('failed', (job, err) => {
                console.error(`[${name}] [INSTANCE:${INSTANCE_ID}] ❌ Job ${job?.id} failed:`, err.message);
            });

            worker.on('error', (err) => {
                console.error(`[${name}] [INSTANCE:${INSTANCE_ID}] ⚠️ Worker error:`, err);
            });

            worker.on('stalled', (jobId) => {
                // console.warn(`[${name}] ⚠️ Job ${jobId} stalled`);
            });
        };

        setupEventHandlers(userWorker, 'USER');
        if (creatorWorker) {
            setupEventHandlers(creatorWorker, 'CREATOR');
        }
        if (adWorker) {
            setupEventHandlers(adWorker, 'AD');
        }


        console.log(`\n${'='.repeat(80)}`);
        console.log(`✅ TRANSCODING ENGINE READY [INSTANCE:${INSTANCE_ID}]`);
        console.log(`   - Available CPU detected: ${concurrencyPlan.availableCpu}`);
        console.log(`   - CPU target: ${(concurrencyPlan.cpuTarget * 100).toFixed(0)}% | Reserved cores: ${concurrencyPlan.reservedCpu}`);
        console.log(`   - Auto CPU budget: ${concurrencyPlan.autoBudget}`);
        console.log(`   - Total concurrency slots: ${concurrencyPlan.totalConcurrency}`);
        if (replicaCountHint) {
            const estimatedClusterSlots = concurrencyPlan.totalConcurrency * replicaCountHint;
            console.log(`   - Replica hint: ${replicaCountHint} (estimated cluster slots: ${estimatedClusterSlots})`);
        }
        if (globalJobLimiter.isEnabled()) {
            console.log(`   - Global transcode cap (all replicas): ${globalJobLimiter.getConfiguredMaxSlots()} via Redis semaphore`);
        } else {
            console.log('   - Global transcode cap: disabled (set GLOBAL_MAX_TRANSCODE_JOBS to enable)');
        }
        console.log(`   - User queue worker (concurrency: ${concurrencyPlan.userConcurrency}, timeout: 10min)`);
        if (creatorWorker) {
            console.log(`   - Creator queue worker (concurrency: ${concurrencyPlan.creatorConcurrency}, timeout: 30min)`);
        }
        if (adWorker) {
            console.log(`   - Ad queue worker (concurrency: ${concurrencyPlan.adConcurrency}, timeout: 5min)`);
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
