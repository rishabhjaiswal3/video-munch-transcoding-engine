import { WebSocketServer, WebSocket } from 'ws';
import { IncomingMessage } from 'http';
import { spawn, ChildProcessWithoutNullStreams } from 'child_process';
import axios from 'axios';
import { LiveTranscoder } from './liveTranscoder.js';

const BACKEND_URL = process.env.BACKEND_URL || 'http://localhost:3000';
const INTERNAL_WEBHOOK_SECRET = process.env.INTERNAL_WEBHOOK_SECRET || 'internal-secret';

interface BrowserStreamSession {
  streamId: string;
  userId: string;
  userType: 'user' | 'creator';
  streamKey: string;
  ws: WebSocket;
  ffmpeg?: ChildProcessWithoutNullStreams;
  transcoder?: LiveTranscoder;
  startedAt?: Date;
  title?: string;
  recordingEnabled?: boolean;
}

const activeSessions = new Map<string, BrowserStreamSession>();

/**
 * Validate stream credentials
 */
async function validateStreamKey(streamId: string, streamKey: string): Promise<{
  valid: boolean;
  userId?: string;
  userType?: 'user' | 'creator';
  title?: string;
  recordingEnabled?: boolean;
}> {
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

    if (response.data.success && response.data.data.streamId === streamId) {
      const data = response.data.data;
      return {
        valid: true,
        userId: data.userId,
        userType: data.userType,
        title: data.title,
        recordingEnabled: data.recordingEnabled ?? true,
      };
    }

    return { valid: false };
  } catch (error: any) {
    console.error('[BROWSER_STREAM] Validation failed:', error.message);
    return { valid: false };
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
    console.log(`[BROWSER_STREAM] Backend notified: stream ${streamId} started`);
  } catch (error: any) {
    console.error('[BROWSER_STREAM] Failed to notify stream start:', error.message);
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
    console.log(`[BROWSER_STREAM] Backend notified: stream ${streamId} ended`);
  } catch (error: any) {
    console.error('[BROWSER_STREAM] Failed to notify stream end:', error.message);
  }
}

/**
 * Start FFmpeg process to convert WebM to RTMP
 * This pipes browser WebM input to local RTMP server
 */
function startFFmpegProcess(session: BrowserStreamSession): void {
  const rtmpUrl = `rtmp://localhost:1935/live/${session.streamKey}`;

  // FFmpeg command to convert WebM (VP8/VP9 + Opus) to RTMP (H.264 + AAC)
  const ffmpegArgs = [
    // Input from stdin (browser WebM chunks)
    '-i', 'pipe:0',

    // Video encoding
    '-c:v', 'libx264',
    '-preset', 'veryfast',
    '-tune', 'zerolatency',
    '-b:v', '2500k',
    '-maxrate', '2500k',
    '-bufsize', '5000k',
    '-pix_fmt', 'yuv420p',
    '-g', '60', // GOP size (2 seconds at 30fps)

    // Audio encoding
    '-c:a', 'aac',
    '-b:a', '128k',
    '-ar', '44100',

    // Output format
    '-f', 'flv',

    // RTMP output
    rtmpUrl,
  ];

  console.log(`[BROWSER_STREAM] Starting FFmpeg for stream ${session.streamId}`);
  console.log(`[BROWSER_STREAM] RTMP URL: ${rtmpUrl}`);

  const ffmpeg = spawn(process.env.FFMPEG_PATH || 'ffmpeg', ffmpegArgs, {
    stdio: ['pipe', 'pipe', 'pipe'],
  });

  session.ffmpeg = ffmpeg;

  ffmpeg.stdout.on('data', (data) => {
    // FFmpeg stdout (usually empty for streaming)
  });

  ffmpeg.stderr.on('data', (data) => {
    const message = data.toString();
    // Only log important messages, not frame progress
    if (message.includes('error') || message.includes('Error') || message.includes('failed')) {
      console.error(`[BROWSER_STREAM] FFmpeg error:`, message);
    }
  });

  ffmpeg.on('close', (code) => {
    console.log(`[BROWSER_STREAM] FFmpeg closed with code ${code} for stream ${session.streamId}`);
    cleanupSession(session.streamId);
  });

  ffmpeg.on('error', (error) => {
    console.error(`[BROWSER_STREAM] FFmpeg error for stream ${session.streamId}:`, error);
    cleanupSession(session.streamId);
  });
}

/**
 * Clean up a session
 */
async function cleanupSession(streamId: string): Promise<void> {
  const session = activeSessions.get(streamId);
  if (!session) return;

  console.log(`[BROWSER_STREAM] Cleaning up session for stream ${streamId}`);

  // Kill FFmpeg process
  if (session.ffmpeg) {
    try {
      session.ffmpeg.stdin.end();
      session.ffmpeg.kill('SIGTERM');
    } catch (e) {
      // Process might already be dead
    }
  }

  // Notify backend
  await notifyStreamEnded(streamId);

  // Remove from active sessions
  activeSessions.delete(streamId);
}

/**
 * Create WebSocket server for browser stream ingestion
 */
export function createBrowserStreamServer(port: number): WebSocketServer {
  const wss = new WebSocketServer({ port });

  console.log(`[BROWSER_STREAM] WebSocket server listening on port ${port}`);

  wss.on('connection', async (ws: WebSocket, req: IncomingMessage) => {
    const url = new URL(req.url || '', `http://localhost:${port}`);
    const pathParts = url.pathname.split('/');

    // Expected path: /live/ingest/{streamId}?key={streamKey}
    if (pathParts.length < 4 || pathParts[1] !== 'live' || pathParts[2] !== 'ingest') {
      console.log('[BROWSER_STREAM] Invalid path:', url.pathname);
      ws.close(4000, 'Invalid path');
      return;
    }

    const streamId = pathParts[3];
    const streamKey = url.searchParams.get('key');

    if (!streamId || !streamKey) {
      console.log('[BROWSER_STREAM] Missing streamId or key');
      ws.close(4001, 'Missing credentials');
      return;
    }

    console.log(`[BROWSER_STREAM] New connection for stream ${streamId}`);

    // Validate credentials
    const validation = await validateStreamKey(streamId, streamKey);
    if (!validation.valid) {
      console.log(`[BROWSER_STREAM] Invalid credentials for stream ${streamId}`);
      ws.close(4002, 'Invalid credentials');
      return;
    }

    // Check if stream already active
    if (activeSessions.has(streamId)) {
      console.log(`[BROWSER_STREAM] Stream ${streamId} already active`);
      ws.close(4003, 'Stream already active');
      return;
    }

    // Create session
    const session: BrowserStreamSession = {
      streamId,
      userId: validation.userId!,
      userType: validation.userType!,
      streamKey,
      ws,
      startedAt: new Date(),
      title: validation.title,
      recordingEnabled: validation.recordingEnabled,
    };

    activeSessions.set(streamId, session);

    // Start FFmpeg process
    startFFmpegProcess(session);

    // Wait a bit for RTMP connection to establish, then notify backend
    setTimeout(async () => {
      const playbackR2Key = `live/${session.userType}/${session.userId}/${session.streamId}/master.m3u8`;
      await notifyStreamStarted(session.streamId, playbackR2Key);
    }, 5000);

    // Handle incoming video data
    ws.on('message', (data: Buffer) => {
      if (session.ffmpeg && session.ffmpeg.stdin.writable) {
        try {
          session.ffmpeg.stdin.write(data);
        } catch (error) {
          console.error(`[BROWSER_STREAM] Error writing to FFmpeg:`, error);
        }
      }
    });

    // Handle close
    ws.on('close', () => {
      console.log(`[BROWSER_STREAM] WebSocket closed for stream ${streamId}`);
      cleanupSession(streamId);
    });

    // Handle errors
    ws.on('error', (error: Error) => {
      console.error(`[BROWSER_STREAM] WebSocket error for stream ${streamId}:`, error);
      cleanupSession(streamId);
    });

    // Send confirmation
    ws.send(JSON.stringify({ type: 'connected', streamId }));
  });

  wss.on('error', (error: Error) => {
    console.error('[BROWSER_STREAM] WebSocket server error:', error);
  });

  return wss;
}

/**
 * Get active browser stream sessions count
 */
export function getActiveBrowserSessionsCount(): number {
  return activeSessions.size;
}
