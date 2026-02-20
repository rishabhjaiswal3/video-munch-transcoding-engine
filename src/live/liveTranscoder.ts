import ffmpeg from 'fluent-ffmpeg';
import fs from 'fs/promises';
import path from 'path';
import { SegmentWatcher } from './segmentWatcher.js';
import { r2Service } from '../services/r2Service.js';

export interface LiveQualityPreset {
  name: string;
  width: number;
  height: number;
  videoBitrate: string;
  audioBitrate: string;
}

// Live streaming quality presets (optimized for lower latency)
export const LIVE_QUALITY_PRESETS: LiveQualityPreset[] = [
  { name: '1080p', width: 1920, height: 1080, videoBitrate: '4000k', audioBitrate: '128k' },
  { name: '720p', width: 1280, height: 720, videoBitrate: '2000k', audioBitrate: '96k' },
  { name: '480p', width: 854, height: 480, videoBitrate: '1000k', audioBitrate: '64k' },
];

export interface LiveTranscoderOptions {
  enableDVR?: boolean;      // Enable DVR rewind (keeps more segments)
  dvrWindowSeconds?: number; // DVR window in seconds (default 300 = 5 mins)
  enableRecording?: boolean; // Keep all segments for VOD conversion
  enableThumbnails?: boolean; // Capture thumbnails periodically
  thumbnailIntervalSeconds?: number; // Thumbnail capture interval (default 30)
}

export class LiveTranscoder {
  private streamId: string;
  private userId: string;
  private userType: 'user' | 'creator';
  private streamKey: string;
  private outputDir: string;
  private ffmpegProcesses: ffmpeg.FfmpegCommand[] = [];
  private segmentWatcher?: SegmentWatcher;
  private isRunning: boolean = false;
  private options: LiveTranscoderOptions;
  private thumbnailProcess?: ffmpeg.FfmpegCommand;
  private allSegments: string[] = []; // Track all segments for VOD conversion

  constructor(
    streamId: string,
    userId: string,
    userType: 'user' | 'creator',
    streamKey: string,
    options: LiveTranscoderOptions = {}
  ) {
    this.streamId = streamId;
    this.userId = userId;
    this.userType = userType;
    this.streamKey = streamKey;
    this.options = {
      enableDVR: options.enableDVR ?? true,
      dvrWindowSeconds: options.dvrWindowSeconds ?? 300, // 5 minutes default
      enableRecording: options.enableRecording ?? true,
      enableThumbnails: options.enableThumbnails ?? true,
      thumbnailIntervalSeconds: options.thumbnailIntervalSeconds ?? 30,
    };
    this.outputDir = path.join(
      process.env.LIVE_SEGMENTS_DIR || '/tmp/live-segments',
      this.streamId
    );
  }

  /**
   * Start live transcoding
   */
  async start(): Promise<void> {
    if (this.isRunning) {
      console.log(`[LIVE_TRANSCODER] Already running for stream ${this.streamId}`);
      return;
    }

    this.isRunning = true;

    try {
      // Create output directory
      await fs.mkdir(this.outputDir, { recursive: true });

      // Create thumbnails directory
      if (this.options.enableThumbnails) {
        await fs.mkdir(path.join(this.outputDir, 'thumbnails'), { recursive: true });
      }

      // Start segment watcher for uploading to R2
      this.segmentWatcher = new SegmentWatcher(
        this.outputDir,
        this.streamId,
        this.userId,
        this.userType,
        { trackAllSegments: this.options.enableRecording }
      );
      await this.segmentWatcher.start();

      // Start FFmpeg processes for each quality
      const rtmpInput = `rtmp://localhost:1935/live/${this.streamKey}`;

      for (const preset of LIVE_QUALITY_PRESETS) {
        await this.startQualityTranscode(rtmpInput, preset);
      }

      // Start thumbnail capture if enabled
      if (this.options.enableThumbnails) {
        this.startThumbnailCapture(rtmpInput);
      }

      // Generate master playlist
      await this.generateMasterPlaylist();

      console.log(`[LIVE_TRANSCODER] Started for stream ${this.streamId} (DVR: ${this.options.enableDVR}, Recording: ${this.options.enableRecording})`);
    } catch (error) {
      this.isRunning = false;
      throw error;
    }
  }

  /**
   * Start transcoding for a specific quality
   */
  private async startQualityTranscode(
    inputUrl: string,
    preset: LiveQualityPreset
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      const outputPath = path.join(this.outputDir, `${preset.name}.m3u8`);
      const segmentPath = path.join(this.outputDir, `${preset.name}_%05d.ts`);

      // Calculate HLS list size based on DVR settings
      // Each segment is 4 seconds, so for 5 min DVR = 75 segments
      const hlsListSize = this.options.enableDVR
        ? Math.ceil((this.options.dvrWindowSeconds || 300) / 4)
        : 10;

      // For recording mode, don't delete segments
      const hlsFlags = this.options.enableRecording
        ? 'append_list+independent_segments+program_date_time'
        : 'delete_segments+append_list+independent_segments+program_date_time';

      const command = ffmpeg(inputUrl)
        .inputOptions([
          '-re', // Read input at native frame rate
        ])
        .outputOptions([
          // Video encoding
          '-c:v', 'libx264',
          '-preset', 'veryfast',
          '-tune', 'zerolatency',
          '-b:v', preset.videoBitrate,
          '-maxrate', preset.videoBitrate,
          '-bufsize', `${parseInt(preset.videoBitrate) * 2}k`,
          `-vf`, `scale=${preset.width}:${preset.height}:force_original_aspect_ratio=decrease,pad=${preset.width}:${preset.height}:(ow-iw)/2:(oh-ih)/2`,

          // Audio encoding
          '-c:a', 'aac',
          '-ar', '44100',
          '-b:a', preset.audioBitrate,

          // HLS settings for live with DVR support
          '-f', 'hls',
          '-hls_time', '4', // 4 second segments
          '-hls_list_size', hlsListSize.toString(),
          '-hls_flags', hlsFlags,
          '-hls_segment_type', 'mpegts',
          '-hls_segment_filename', segmentPath,

          // Keyframe alignment
          '-g', '48', // GOP size (2 seconds at 24fps)
          '-keyint_min', '48',
          '-sc_threshold', '0',

          // Thread count
          '-threads', '4',
        ])
        .output(outputPath);

      command.on('start', (commandLine) => {
        console.log(`[LIVE_TRANSCODER] Started ${preset.name}: ${commandLine.slice(0, 200)}...`);
        resolve();
      });

      command.on('progress', (progress) => {
        // Progress logging for live streams (optional)
        // console.log(`[LIVE_TRANSCODER] ${preset.name} progress:`, progress.timemark);
      });

      command.on('error', (error: any, stdout, stderr) => {
        if (this.isRunning) {
          console.error(`[LIVE_TRANSCODER] ${preset.name} error:`, error.message);
          // Don't reject here - just log. Stream might have ended normally.
        }
      });

      command.on('end', () => {
        console.log(`[LIVE_TRANSCODER] ${preset.name} ended`);
      });

      this.ffmpegProcesses.push(command);
      command.run();
    });
  }

  /**
   * Generate HLS master playlist
   */
  private async generateMasterPlaylist(): Promise<void> {
    let playlist = '#EXTM3U\n';
    playlist += '#EXT-X-VERSION:3\n';

    for (const preset of LIVE_QUALITY_PRESETS) {
      const bandwidth = parseInt(preset.videoBitrate) * 1000 + parseInt(preset.audioBitrate) * 1000;
      playlist += `#EXT-X-STREAM-INF:BANDWIDTH=${bandwidth},RESOLUTION=${preset.width}x${preset.height},NAME="${preset.name}"\n`;
      playlist += `${preset.name}.m3u8\n`;
    }

    const masterPath = path.join(this.outputDir, 'master.m3u8');
    await fs.writeFile(masterPath, playlist);
    console.log(`[LIVE_TRANSCODER] Master playlist created at ${masterPath}`);
  }

  /**
   * Start capturing thumbnails periodically
   */
  private startThumbnailCapture(inputUrl: string): void {
    const thumbnailDir = path.join(this.outputDir, 'thumbnails');
    const interval = this.options.thumbnailIntervalSeconds || 30;
    let thumbnailCount = 0;

    const captureNextThumbnail = () => {
      if (!this.isRunning) return;

      const thumbnailPath = path.join(thumbnailDir, `thumb_${String(thumbnailCount).padStart(4, '0')}.jpg`);

      const command = ffmpeg(inputUrl)
        .inputOptions(['-re'])
        .outputOptions([
          '-vframes', '1',
          '-q:v', '5',
          '-vf', 'scale=640:360:force_original_aspect_ratio=decrease,pad=640:360:(ow-iw)/2:(oh-ih)/2',
        ])
        .output(thumbnailPath);

      command.on('end', async () => {
        thumbnailCount++;

        // Upload thumbnail to R2
        try {
          const content = await fs.readFile(thumbnailPath);
          const r2Key = `live/${this.userType}/${this.userId}/${this.streamId}/thumbnails/thumb_latest.jpg`;
          await r2Service.uploadLiveSegment(r2Key, content, 'image/jpeg');

          // Also keep numbered versions for VOD thumbnails
          const numberedKey = `live/${this.userType}/${this.userId}/${this.streamId}/thumbnails/thumb_${String(thumbnailCount).padStart(4, '0')}.jpg`;
          await r2Service.uploadLiveSegment(numberedKey, content, 'image/jpeg');

          console.log(`[LIVE_TRANSCODER] Captured and uploaded thumbnail ${thumbnailCount}`);
        } catch (error: any) {
          console.error(`[LIVE_TRANSCODER] Failed to upload thumbnail:`, error.message);
        }

        // Schedule next capture
        if (this.isRunning) {
          setTimeout(captureNextThumbnail, interval * 1000);
        }
      });

      command.on('error', (error: any) => {
        if (this.isRunning) {
          console.error(`[LIVE_TRANSCODER] Thumbnail capture error:`, error.message);
          // Retry after interval
          setTimeout(captureNextThumbnail, interval * 1000);
        }
      });

      command.run();
    };

    // Start first capture after a short delay
    setTimeout(captureNextThumbnail, 5000);
    console.log(`[LIVE_TRANSCODER] Started thumbnail capture (every ${interval}s)`);
  }

  /**
   * Convert recorded segments to VOD
   * Call this after the stream ends
   */
  async convertToVOD(): Promise<{ success: boolean; vodPath?: string; error?: string }> {
    if (!this.options.enableRecording) {
      return { success: false, error: 'Recording was not enabled' };
    }

    try {
      console.log(`[LIVE_TRANSCODER] Converting stream ${this.streamId} to VOD`);

      // Find all 720p segments (good balance of quality and size)
      const segmentPattern = path.join(this.outputDir, '720p_*.ts');
      const segmentsDir = this.outputDir;

      // Read directory and find segments
      const files = await fs.readdir(segmentsDir);
      const segments = files
        .filter(f => f.startsWith('720p_') && f.endsWith('.ts'))
        .sort(); // Sort to maintain order

      if (segments.length === 0) {
        return { success: false, error: 'No segments found' };
      }

      // Create concat file
      const concatFilePath = path.join(this.outputDir, 'concat.txt');
      const concatContent = segments.map(seg => `file '${path.join(segmentsDir, seg)}'`).join('\n');
      await fs.writeFile(concatFilePath, concatContent);

      // Output VOD path
      const vodPath = path.join(this.outputDir, 'vod.mp4');

      // Concatenate and convert to MP4
      await new Promise<void>((resolve, reject) => {
        ffmpeg()
          .input(concatFilePath)
          .inputOptions(['-f', 'concat', '-safe', '0'])
          .outputOptions([
            '-c', 'copy', // Copy streams without re-encoding
            '-movflags', '+faststart', // Optimize for streaming
          ])
          .output(vodPath)
          .on('end', () => {
            console.log(`[LIVE_TRANSCODER] VOD created at ${vodPath}`);
            resolve();
          })
          .on('error', (err: any) => {
            console.error(`[LIVE_TRANSCODER] VOD conversion error:`, err.message);
            reject(err);
          })
          .run();
      });

      return { success: true, vodPath };
    } catch (error: any) {
      return { success: false, error: error.message };
    }
  }

  /**
   * Get all recorded segments for VOD upload
   */
  async getRecordedSegments(): Promise<string[]> {
    if (!this.options.enableRecording) {
      return [];
    }

    try {
      const files = await fs.readdir(this.outputDir);
      return files
        .filter(f => f.endsWith('.ts'))
        .map(f => path.join(this.outputDir, f))
        .sort();
    } catch {
      return [];
    }
  }

  /**
   * Stop live transcoding
   */
  async stop(): Promise<void> {
    if (!this.isRunning) {
      return;
    }

    console.log(`[LIVE_TRANSCODER] Stopping stream ${this.streamId}`);
    this.isRunning = false;

    // Kill all FFmpeg processes
    for (const process of this.ffmpegProcesses) {
      try {
        process.kill('SIGTERM');
      } catch (error) {
        // Process might already be dead
      }
    }
    this.ffmpegProcesses = [];

    // Stop segment watcher
    if (this.segmentWatcher) {
      await this.segmentWatcher.stop();
    }

    // Wait a bit for final segments to upload
    await new Promise(resolve => setTimeout(resolve, 2000));

    // Clean up local files (optional - keep for DVR/recording)
    const keepRecording = process.env.KEEP_LIVE_RECORDINGS === 'true';
    if (!keepRecording) {
      try {
        await fs.rm(this.outputDir, { recursive: true, force: true });
        console.log(`[LIVE_TRANSCODER] Cleaned up local files for stream ${this.streamId}`);
      } catch (error) {
        console.error(`[LIVE_TRANSCODER] Failed to clean up:`, error);
      }
    }

    console.log(`[LIVE_TRANSCODER] Stopped stream ${this.streamId}`);
  }

  /**
   * Check if transcoder is running
   */
  isActive(): boolean {
    return this.isRunning;
  }

  /**
   * Get output directory
   */
  getOutputDir(): string {
    return this.outputDir;
  }
}
