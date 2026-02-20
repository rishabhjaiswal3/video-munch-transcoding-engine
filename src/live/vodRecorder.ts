import ffmpeg from 'fluent-ffmpeg';
import fs from 'fs/promises';
import path from 'path';
import axios from 'axios';
import { r2Service } from '../services/r2Service.js';
import { getRedisConnection } from '../config/redis.js';

export interface VODRecorderOptions {
  streamId: string;
  userId: string;
  userType: 'user' | 'creator';
  title: string;
  description?: string;
  category?: string;
  thumbnail?: string;
  duration: number;
}

/**
 * Service for converting live stream recordings to VOD
 */
export class VODRecorder {
  private options: VODRecorderOptions;
  private outputDir: string;
  private apiUrl: string;

  constructor(options: VODRecorderOptions) {
    this.options = options;
    this.outputDir = path.join(
      process.env.LIVE_SEGMENTS_DIR || '/tmp/live-segments',
      options.streamId
    );
    this.apiUrl = process.env.API_URL || 'http://localhost:3000';
  }

  /**
   * Convert recorded stream to VOD and register it in the backend
   */
  async convertAndRegister(): Promise<{
    success: boolean;
    videoId?: string;
    error?: string;
  }> {
    try {
      console.log(`[VOD_RECORDER] Starting VOD conversion for stream ${this.options.streamId}`);

      // Step 1: Find all segments
      const segments = await this.findSegments();
      if (segments.length === 0) {
        return { success: false, error: 'No segments found' };
      }

      console.log(`[VOD_RECORDER] Found ${segments.length} segments`);

      // Step 2: Concatenate segments to MP4
      const vodPath = await this.concatenateSegments(segments);
      console.log(`[VOD_RECORDER] Created VOD at ${vodPath}`);

      // Step 3: Upload VOD to R2
      const vodKey = await this.uploadVOD(vodPath);
      console.log(`[VOD_RECORDER] Uploaded VOD to R2: ${vodKey}`);

      // Step 4: Generate thumbnail from first frame if not provided
      let thumbnailKey = this.options.thumbnail;
      if (!thumbnailKey) {
        thumbnailKey = await this.generateThumbnail(vodPath);
      }

      // Step 5: Register VOD in backend
      const videoId = await this.registerVOD(vodKey, thumbnailKey);
      console.log(`[VOD_RECORDER] Registered VOD with videoId: ${videoId}`);

      // Step 6: Update LiveStream with recordedVideoId
      await this.linkToLiveStream(videoId);

      // Step 7: Clean up local files
      await this.cleanup();

      // Publish event
      const redis = getRedisConnection();
      await redis.publish('live:vod:created', JSON.stringify({
        streamId: this.options.streamId,
        videoId,
        userId: this.options.userId,
      }));

      return { success: true, videoId };
    } catch (error: any) {
      console.error(`[VOD_RECORDER] Error:`, error);
      return { success: false, error: error.message };
    }
  }

  /**
   * Find all recorded segments
   */
  private async findSegments(): Promise<string[]> {
    const files = await fs.readdir(this.outputDir);

    // Use 720p segments for VOD (good balance)
    const segments = files
      .filter(f => f.startsWith('720p_') && f.endsWith('.ts'))
      .sort((a, b) => {
        // Sort by segment number
        const numA = parseInt(a.match(/\d+/)?.[0] || '0');
        const numB = parseInt(b.match(/\d+/)?.[0] || '0');
        return numA - numB;
      })
      .map(f => path.join(this.outputDir, f));

    return segments;
  }

  /**
   * Concatenate segments into a single MP4 file
   */
  private async concatenateSegments(segments: string[]): Promise<string> {
    const concatFilePath = path.join(this.outputDir, 'concat.txt');
    const vodPath = path.join(this.outputDir, 'vod.mp4');

    // Create concat file
    const concatContent = segments.map(seg => `file '${seg}'`).join('\n');
    await fs.writeFile(concatFilePath, concatContent);

    return new Promise((resolve, reject) => {
      ffmpeg()
        .input(concatFilePath)
        .inputOptions(['-f', 'concat', '-safe', '0'])
        .outputOptions([
          '-c:v', 'libx264',
          '-preset', 'fast',
          '-crf', '22',
          '-c:a', 'aac',
          '-b:a', '128k',
          '-movflags', '+faststart',
        ])
        .output(vodPath)
        .on('progress', (progress) => {
          if (progress.percent) {
            console.log(`[VOD_RECORDER] Encoding progress: ${progress.percent.toFixed(1)}%`);
          }
        })
        .on('end', () => resolve(vodPath))
        .on('error', reject)
        .run();
    });
  }

  /**
   * Upload VOD to R2
   */
  private async uploadVOD(vodPath: string): Promise<string> {
    const content = await fs.readFile(vodPath);
    const r2Key = `${this.options.userType}/${this.options.userId}/vod/${this.options.streamId}/video.mp4`;

    await r2Service.uploadLiveSegment(r2Key, content, 'video/mp4');

    return r2Key;
  }

  /**
   * Generate thumbnail from VOD
   */
  private async generateThumbnail(vodPath: string): Promise<string> {
    const thumbnailPath = path.join(this.outputDir, 'thumbnail.jpg');

    await new Promise<void>((resolve, reject) => {
      ffmpeg(vodPath)
        .screenshots({
          count: 1,
          folder: this.outputDir,
          filename: 'thumbnail.jpg',
          size: '1280x720',
        })
        .on('end', () => resolve())
        .on('error', reject);
    });

    // Upload thumbnail to R2
    const content = await fs.readFile(thumbnailPath);
    const r2Key = `${this.options.userType}/${this.options.userId}/vod/${this.options.streamId}/thumbnail.jpg`;
    await r2Service.uploadLiveSegment(r2Key, content, 'image/jpeg');

    return r2Key;
  }

  /**
   * Register VOD in backend
   */
  private async registerVOD(vodKey: string, thumbnailKey?: string): Promise<string> {
    const response = await axios.post(`${this.apiUrl}/videos/register-vod`, {
      userId: this.options.userId,
      userType: this.options.userType,
      title: `[Recording] ${this.options.title}`,
      description: this.options.description,
      category: this.options.category,
      masterPlaylistUrl: vodKey,
      thumbnail: thumbnailKey,
      duration: this.options.duration,
      contentType: 'vod',
      source: 'live_recording',
      sourceStreamId: this.options.streamId,
    }, {
      headers: {
        'X-Internal-Key': process.env.INTERNAL_API_KEY || 'transcoding-engine-key',
      },
    });

    if (!response.data.success) {
      throw new Error(response.data.error || 'Failed to register VOD');
    }

    return response.data.data.videoId;
  }

  /**
   * Link the VOD to the original LiveStream
   */
  private async linkToLiveStream(videoId: string): Promise<void> {
    try {
      await axios.post(`${this.apiUrl}/live/${this.options.streamId}/link-vod`, {
        videoId,
      }, {
        headers: {
          'X-Internal-Key': process.env.INTERNAL_API_KEY || 'transcoding-engine-key',
        },
      });
    } catch (error) {
      console.error(`[VOD_RECORDER] Failed to link VOD to stream:`, error);
      // Non-fatal error
    }
  }

  /**
   * Clean up local files
   */
  private async cleanup(): Promise<void> {
    const keepRecordings = process.env.KEEP_LIVE_RECORDINGS === 'true';
    if (keepRecordings) {
      console.log(`[VOD_RECORDER] Keeping local recordings for ${this.options.streamId}`);
      return;
    }

    try {
      await fs.rm(this.outputDir, { recursive: true, force: true });
      console.log(`[VOD_RECORDER] Cleaned up local files for ${this.options.streamId}`);
    } catch (error) {
      console.error(`[VOD_RECORDER] Cleanup error:`, error);
    }
  }
}
