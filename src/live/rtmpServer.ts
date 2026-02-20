import NodeMediaServer from 'node-media-server';
import axios from 'axios';
import { LiveTranscoder } from './liveTranscoder.js';
import { VODRecorder } from './vodRecorder.js';
import { getRedisConnection } from '../config/redis.js';

export interface StreamSession {
  streamId: string;
  userId: string;
  userType: 'user' | 'creator';
  streamKey: string;
  transcoder?: LiveTranscoder;
  startedAt?: Date;
  title?: string;
  description?: string;
  category?: string;
  recordingEnabled?: boolean;
}

// Active stream sessions
const activeSessions = new Map<string, StreamSession>();

// Backend API URL for validation
const BACKEND_URL = process.env.BACKEND_URL || 'http://localhost:3000';
const INTERNAL_WEBHOOK_SECRET = process.env.INTERNAL_WEBHOOK_SECRET || 'internal-secret';

/**
 * Validate stream key against backend API
 */
async function validateStreamKey(streamKey: string): Promise<StreamSession | null> {
  try {
    const response = await axios.post(
      `${BACKEND_URL}/live/ingest/validate`,
      { streamKey },
      {
        headers: {
          'Content-Type': 'application/json',
          'x-internal-secret': INTERNAL_WEBHOOK_SECRET,
        },
        timeout: 5000,
      }
    );

    if (response.data.success) {
      const data = response.data.data;
      return {
        streamId: data.streamId,
        userId: data.userId,
        userType: data.userType,
        streamKey,
        title: data.title,
        description: data.description,
        category: data.category,
        recordingEnabled: data.recordingEnabled ?? true,
      };
    }

    return null;
  } catch (error: any) {
    console.error('[RTMP] Stream key validation failed:', error.message);
    return null;
  }
}

/**
 * Notify backend that stream started
 */
async function notifyStreamStarted(streamId: string, playbackR2Key: string): Promise<void> {
  try {
    await axios.post(
      `${BACKEND_URL}/live/${streamId}/start`,
      { playbackR2Key },
      {
        headers: {
          'Content-Type': 'application/json',
          'x-internal-secret': INTERNAL_WEBHOOK_SECRET,
        },
        timeout: 5000,
      }
    );
    console.log(`[RTMP] Backend notified: stream ${streamId} started`);
  } catch (error: any) {
    console.error('[RTMP] Failed to notify stream start:', error.message);
  }
}

/**
 * Notify backend that stream ended
 */
async function notifyStreamEnded(streamId: string): Promise<void> {
  try {
    await axios.post(
      `${BACKEND_URL}/live/${streamId}/end`,
      {},
      {
        headers: {
          'Content-Type': 'application/json',
          'x-internal-secret': INTERNAL_WEBHOOK_SECRET,
        },
        timeout: 5000,
      }
    );
    console.log(`[RTMP] Backend notified: stream ${streamId} ended`);
  } catch (error: any) {
    console.error('[RTMP] Failed to notify stream end:', error.message);
  }
}

/**
 * Create and start the RTMP server
 */
export function createRtmpServer(): NodeMediaServer {
  const rtmpPort = parseInt(process.env.RTMP_PORT || '1935', 10);
  const httpPort = parseInt(process.env.RTMP_HTTP_PORT || '8000', 10);

  const config = {
    rtmp: {
      port: rtmpPort,
      chunk_size: 60000,
      gop_cache: true,
      ping: 30,
      ping_timeout: 60,
    },
    http: {
      port: httpPort,
      allow_origin: '*',
      mediaroot: process.env.LIVE_SEGMENTS_DIR || '/tmp/live-segments',
    },
    trans: {
      ffmpeg: process.env.FFMPEG_PATH || '/usr/bin/ffmpeg',
    },
  };

  const nms = new NodeMediaServer(config);

  // Handle pre-publish (stream key validation)
  nms.on('prePublish', async (id: string, streamPath: string, args: any) => {
    console.log(`[RTMP] prePublish: id=${id}, streamPath=${streamPath}`);

    // Extract stream key from path (format: /live/streamKey)
    const parts = streamPath.split('/');
    const streamKey = parts[parts.length - 1];

    if (!streamKey) {
      console.log('[RTMP] No stream key provided, rejecting');
      const session = nms.getSession(id);
      session?.reject();
      return;
    }

    // Validate stream key
    const sessionData = await validateStreamKey(streamKey);
    if (!sessionData) {
      console.log('[RTMP] Invalid stream key, rejecting');
      const session = nms.getSession(id);
      session?.reject();
      return;
    }

    console.log(`[RTMP] Stream key validated for stream ${sessionData.streamId}`);
    activeSessions.set(id, sessionData);
  });

  // Handle post-publish (stream started)
  nms.on('postPublish', async (id: string, streamPath: string, args: any) => {
    console.log(`[RTMP] postPublish: id=${id}, streamPath=${streamPath}`);

    const session = activeSessions.get(id);
    if (!session) {
      console.log('[RTMP] No session found for id:', id);
      return;
    }

    session.startedAt = new Date();

    try {
      // Create and start the live transcoder with options
      const transcoder = new LiveTranscoder(
        session.streamId,
        session.userId,
        session.userType,
        session.streamKey,
        {
          enableDVR: true,
          dvrWindowSeconds: 300, // 5 minutes DVR window
          enableRecording: session.recordingEnabled ?? true,
          enableThumbnails: true,
          thumbnailIntervalSeconds: 30,
        }
      );

      session.transcoder = transcoder;

      // Start transcoding (this runs in background)
      transcoder.start().catch((error) => {
        console.error(`[RTMP] Transcoder error for stream ${session.streamId}:`, error);
      });

      // Wait a bit for segments to start appearing, then notify backend
      setTimeout(async () => {
        const playbackR2Key = `live/${session.userType}/${session.userId}/${session.streamId}/master.m3u8`;
        await notifyStreamStarted(session.streamId, playbackR2Key);

        // Publish to Redis for real-time updates
        const redis = getRedisConnection();
        await redis.publish('live:stream:started', JSON.stringify({
          streamId: session.streamId,
          userId: session.userId,
          startedAt: session.startedAt,
        }));
      }, 5000);

    } catch (error: any) {
      console.error(`[RTMP] Failed to start transcoder:`, error);
    }
  });

  // Handle donePublish (stream ended)
  nms.on('donePublish', async (id: string, streamPath: string, args: any) => {
    console.log(`[RTMP] donePublish: id=${id}, streamPath=${streamPath}`);

    const session = activeSessions.get(id);
    if (!session) {
      console.log('[RTMP] No session found for id:', id);
      return;
    }

    const duration = session.startedAt
      ? Math.floor((Date.now() - session.startedAt.getTime()) / 1000)
      : 0;

    try {
      // Stop the transcoder
      if (session.transcoder) {
        await session.transcoder.stop();
      }

      // Notify backend that stream ended
      await notifyStreamEnded(session.streamId);

      // Publish to Redis
      const redis = getRedisConnection();
      await redis.publish('live:stream:ended', JSON.stringify({
        streamId: session.streamId,
        userId: session.userId,
        duration,
      }));

      // Start VOD conversion in background if recording was enabled
      if (session.recordingEnabled && duration > 60) {
        // Only convert streams longer than 1 minute
        console.log(`[RTMP] Starting VOD conversion for stream ${session.streamId}`);

        const vodRecorder = new VODRecorder({
          streamId: session.streamId,
          userId: session.userId,
          userType: session.userType,
          title: session.title || 'Untitled Stream',
          description: session.description,
          category: session.category,
          duration,
        });

        // Run VOD conversion in background (don't await)
        vodRecorder.convertAndRegister()
          .then((result) => {
            if (result.success) {
              console.log(`[RTMP] VOD created for stream ${session.streamId}: ${result.videoId}`);
            } else {
              console.error(`[RTMP] VOD conversion failed:`, result.error);
            }
          })
          .catch((error) => {
            console.error(`[RTMP] VOD conversion error:`, error);
          });
      }

    } catch (error: any) {
      console.error(`[RTMP] Error handling stream end:`, error);
    } finally {
      activeSessions.delete(id);
    }
  });

  // Handle prePlay (viewer connecting)
  nms.on('prePlay', (id: string, streamPath: string, args: any) => {
    console.log(`[RTMP] prePlay: id=${id}, streamPath=${streamPath}`);
  });

  // Handle donePlay (viewer disconnected)
  nms.on('donePlay', (id: string, streamPath: string, args: any) => {
    console.log(`[RTMP] donePlay: id=${id}, streamPath=${streamPath}`);
  });

  return nms;
}

/**
 * Get active sessions count
 */
export function getActiveSessionsCount(): number {
  return activeSessions.size;
}

/**
 * Get active session by stream ID
 */
export function getActiveSession(streamId: string): StreamSession | undefined {
  for (const session of activeSessions.values()) {
    if (session.streamId === streamId) {
      return session;
    }
  }
  return undefined;
}
