import chokidar, { FSWatcher } from 'chokidar';
import fs from 'fs/promises';
import path from 'path';
import { r2Service } from '../services/r2Service.js';
import { getRedisConnection } from '../config/redis.js';

export interface SegmentWatcherOptions {
  trackAllSegments?: boolean; // Track all segments for VOD conversion
}

export class SegmentWatcher {
  private watchDir: string;
  private streamId: string;
  private userId: string;
  private userType: 'user' | 'creator';
  private watcher?: FSWatcher;
  private uploadedFiles = new Set<string>();
  private segmentCount = 0;
  private isRunning = false;
  private options: SegmentWatcherOptions;
  private allSegments: string[] = []; // Track all segments in order

  constructor(
    watchDir: string,
    streamId: string,
    userId: string,
    userType: 'user' | 'creator',
    options: SegmentWatcherOptions = {}
  ) {
    this.watchDir = watchDir;
    this.streamId = streamId;
    this.userId = userId;
    this.userType = userType;
    this.options = options;
  }

  /**
   * Start watching for new segments
   */
  async start(): Promise<void> {
    if (this.isRunning) {
      return;
    }

    this.isRunning = true;

    this.watcher = chokidar.watch(this.watchDir, {
      ignoreInitial: true,
      awaitWriteFinish: {
        stabilityThreshold: 500, // Wait 500ms for file to stabilize
        pollInterval: 100,
      },
      ignored: /(^|[\/\\])\../, // Ignore hidden files
    });

    this.watcher.on('add', async (filePath) => {
      await this.handleNewFile(filePath);
    });

    this.watcher.on('change', async (filePath) => {
      // Handle playlist updates
      if (filePath.endsWith('.m3u8')) {
        await this.handlePlaylistUpdate(filePath);
      }
    });

    this.watcher.on('error', (error) => {
      console.error(`[SEGMENT_WATCHER] Error:`, error);
    });

    console.log(`[SEGMENT_WATCHER] Started watching ${this.watchDir}`);
  }

  /**
   * Stop watching
   */
  async stop(): Promise<void> {
    if (!this.isRunning) {
      return;
    }

    this.isRunning = false;

    if (this.watcher) {
      await this.watcher.close();
      this.watcher = undefined;
    }

    console.log(`[SEGMENT_WATCHER] Stopped. Uploaded ${this.segmentCount} segments.`);
  }

  /**
   * Handle new file (segment or playlist)
   */
  private async handleNewFile(filePath: string): Promise<void> {
    const fileName = path.basename(filePath);

    // Skip if already uploaded
    if (this.uploadedFiles.has(filePath)) {
      return;
    }

    try {
      if (filePath.endsWith('.ts')) {
        await this.uploadSegment(filePath);
      } else if (filePath.endsWith('.m3u8')) {
        await this.uploadPlaylist(filePath);
      }

      this.uploadedFiles.add(filePath);
    } catch (error: any) {
      console.error(`[SEGMENT_WATCHER] Failed to upload ${fileName}:`, error.message);
    }
  }

  /**
   * Handle playlist update
   */
  private async handlePlaylistUpdate(filePath: string): Promise<void> {
    try {
      await this.uploadPlaylist(filePath);
    } catch (error: any) {
      console.error(`[SEGMENT_WATCHER] Failed to update playlist:`, error.message);
    }
  }

  /**
   * Upload a segment file to R2
   */
  private async uploadSegment(filePath: string): Promise<void> {
    const fileName = path.basename(filePath);
    const r2Key = this.getR2Key(fileName);

    // Read file content
    const content = await fs.readFile(filePath);

    // Upload to R2 with immutable cache
    await r2Service.uploadLiveSegment(r2Key, content, 'video/mp2t');

    this.segmentCount++;

    // Track segment for VOD conversion if enabled
    if (this.options.trackAllSegments) {
      this.allSegments.push(filePath);
    }

    // Publish segment event to Redis
    const redis = getRedisConnection();
    await redis.publish('live:segment:uploaded', JSON.stringify({
      streamId: this.streamId,
      segmentNumber: this.segmentCount,
      fileName,
      r2Key,
    }));

    console.log(`[SEGMENT_WATCHER] Uploaded segment ${fileName} (total: ${this.segmentCount})`);
  }

  /**
   * Upload a playlist file to R2
   */
  private async uploadPlaylist(filePath: string): Promise<void> {
    const fileName = path.basename(filePath);
    const r2Key = this.getR2Key(fileName);

    // Read file content
    const content = await fs.readFile(filePath, 'utf-8');

    // Upload to R2 with no-cache for playlists (must always be fresh)
    await r2Service.uploadLivePlaylist(r2Key, content);

    console.log(`[SEGMENT_WATCHER] Updated playlist ${fileName}`);
  }

  /**
   * Get R2 key for a file
   */
  private getR2Key(fileName: string): string {
    return `live/${this.userType}/${this.userId}/${this.streamId}/${fileName}`;
  }

  /**
   * Get segment count
   */
  getSegmentCount(): number {
    return this.segmentCount;
  }

  /**
   * Get all tracked segments (for VOD conversion)
   */
  getAllSegments(): string[] {
    return [...this.allSegments];
  }
}
