# Video Manch Transcoding Engine

Standalone video transcoding worker for Video Manch. It pulls jobs from BullMQ, transcodes them to HLS with FFmpeg, stores the playlists and segments in Cloudflare R2, and writes status back to MongoDB.

```
Backend API → BullMQ (Redis) → Transcoding engine → R2 (HLS) + MongoDB
```

The worker runs as its own process. It shares Redis and MongoDB with the backend and can run as more than one replica.

## Features

- HLS adaptive bitrate output (H.264 video, AAC audio)
- Renditions at 1080p, 720p, 480p, 360p, and 240p, without upscaling past the source
- Master playlist plus per-quality `.m3u8` playlists and `.ts` segments
- Three JPEG thumbnails per video
- Separate `user-transcode-queue` and `creator-transcode-queue`
- CPU-based concurrency, with an optional Redis cap across replicas
- Disk, memory, and upload retries before a job is marked failed
- Graceful shutdown on `SIGINT` and `SIGTERM`

## Prerequisites

- Node.js 18 or newer
- FFmpeg with libx264 and AAC
- Redis
- MongoDB
- A Cloudflare R2 account

Install FFmpeg:

```bash
# macOS
brew install ffmpeg

# Ubuntu/Debian
sudo apt update
sudo apt install ffmpeg
```

On Windows, install FFmpeg from [ffmpeg.org](https://ffmpeg.org/download.html) and make sure `ffmpeg` and `ffprobe` are on `PATH`.

## Setup

```bash
npm install
cp .env.example .env
ffmpeg -version
ffprobe -version
```

Fill in `.env`. Bucket names below match `.env.example`:

```env
MONGODB_URI=mongodb+srv://<user>:<password>@<cluster>.mongodb.net/?appName=videoManch
REDIS_URL=redis://localhost:6379

R2_ACCOUNT_ID=your_account_id
R2_ACCESS_KEY_ID=your_access_key_id
R2_SECRET_ACCESS_KEY=your_secret_access_key
R2_BUCKET_RAW=videos-raw-upload
R2_BUCKET_TRANSCODED=videos-transcoded-data
R2_BUCKET_THUMBNAILS=videos-thumbnails
R2_PUBLIC_URL=https://your-public-url.com
```

The worker stores R2 object keys in MongoDB. The frontend builds the public URL from `R2_PUBLIC_URL`.

## Run

```bash
npm run dev
```

Production:

```bash
npm run build
npm start
```

`npm run worker` starts the same process once, without file watching.

## Pipeline

A job looks like `{ videoId, userId, userType, r2Key, qualities }`.

1. **Download.** The original file is pulled from the raw R2 bucket into the OS temp directory, under `videomanch-transcoding/{videoId}/`.
2. **Probe.** FFprobe reads duration, dimensions, codec, bitrate, and frame rate. Audio-only files and videos smaller than 64×64 are rejected.
3. **Transcode to HLS.** Each requested quality at or below the source height is encoded with libx264 and AAC. Segments are about 10 seconds. A `master.m3u8` lists the variants. Qualities above the source are skipped so the worker does not upscale.
4. **Thumbnails.** Three 1280×720 JPEGs are taken near 10%, 30%, and 50% of the duration, capped at 2s, 10s, and 20s.
5. **Upload.** Playlists, `.ts` segments, and thumbnails are uploaded with retries. Segments are cached for one year. Playlists are cached for one hour.
6. **Finalize.** MongoDB is updated to `completed` with the master playlist key, per-quality playlist keys, thumbnail keys, duration, and timing stats. On failure the video status is set to `failed` and the temp directory is removed.

Object keys use `{userType}/{userId}/{videoId}/`, for example `user/abc/vid-1/master.m3u8`.

## Quality presets

| Quality | Resolution | Video bitrate | Audio bitrate |
|---------|------------|---------------|---------------|
| 1080p   | 1920×1080  | 5000k         | 192k          |
| 720p    | 1280×720   | 2500k         | 128k          |
| 480p    | 854×480    | 1000k         | 96k           |
| 360p    | 640×360    | 600k          | 64k           |
| 240p    | 426×240    | 400k          | 48k           |

## Concurrency

Each replica sizes its workers from available CPU (`CPU_UTILIZATION_TARGET`, default `0.8`, minus `RESERVED_CPU_CORES`, default `1`). Override that with:

- `WORKER_CONCURRENCY`
- `USER_WORKER_CONCURRENCY`
- `CREATOR_WORKER_CONCURRENCY`

The creator queue runs unless `ENABLE_CREATOR_WORKER=false`.

`GLOBAL_MAX_TRANSCODE_JOBS` caps in-flight transcodes across all replicas through a Redis semaphore. Leave it unset to limit each replica only by its own CPU budget.

User jobs use a 10 minute BullMQ lock. Creator jobs use a 30 minute lock. FFmpeg itself is allowed up to 60 minutes per quality.

## Scaling

Start more than one replica against the same `REDIS_URL` and `MONGODB_URI`. BullMQ distributes jobs. Set `GLOBAL_MAX_TRANSCODE_JOBS` when several replicas share one machine or one Redis, so they do not all encode at once.

Railway deploys from the `Dockerfile` (`railway.toml`). The image installs FFmpeg, builds TypeScript, then prunes dev dependencies and runs `node dist/index.js`.

## Shutdown

`Ctrl+C`, `SIGINT`, or `SIGTERM` pauses both workers, waits up to 5 minutes for the job already in progress, then closes Redis and MongoDB.

## Project layout

```
video-munch-transcoding-engine/
├── src/
│   ├── config/
│   │   ├── database.ts
│   │   ├── redis.ts
│   │   └── r2.ts
│   ├── models/Video.ts
│   ├── services/
│   │   ├── ffmpegService.ts    # probe, HLS encode, master playlist, thumbnails
│   │   └── r2Service.ts
│   ├── types/index.ts
│   └── index.ts                # queue workers and the VOD pipeline
├── .env.example
├── Dockerfile
├── package.json
├── railway.toml
└── tsconfig.json
```

## Backend contract

The backend enqueues the job. This worker does not expose HTTP.

```typescript
await queueService.addTranscodeJob({
  videoId,
  userId,
  userType, // "user" | "creator"
  r2Key,
  qualities: ["1080p", "720p", "480p", "360p"],
});
```

Queue names must stay `user-transcode-queue` and `creator-transcode-queue`.

## Environment variables

| Variable | Description | Example |
|----------|-------------|---------|
| `MONGODB_URI` | MongoDB connection string | `mongodb://localhost:27017/video` |
| `REDIS_URL` | Redis connection string. Use `rediss://` for TLS. | `redis://localhost:6379` |
| `R2_ACCOUNT_ID` | Cloudflare account ID | `abc123` |
| `R2_ACCESS_KEY_ID` | R2 access key | `your_key` |
| `R2_SECRET_ACCESS_KEY` | R2 secret key | `your_secret` |
| `R2_BUCKET_RAW` | Original uploads | `videos-raw-upload` |
| `R2_BUCKET_TRANSCODED` | HLS playlists and segments | `videos-transcoded-data` |
| `R2_BUCKET_THUMBNAILS` | JPEG thumbnails | `videos-thumbnails` |
| `R2_PUBLIC_URL` | Public base URL for transcoded objects | `https://cdn.example.com` |
| `R2_PUBLIC_URL_THUMBNAILS` | Optional public base URL for thumbnails | `https://thumbs.example.com` |
| `FFMPEG_PATH` | Optional ffmpeg binary | `/usr/bin/ffmpeg` |
| `FFPROBE_PATH` | Optional ffprobe binary | `/usr/bin/ffprobe` |
| `ENABLE_CREATOR_WORKER` | Set to `false` to run only the user queue | `true` |
| `GLOBAL_MAX_TRANSCODE_JOBS` | Optional cluster-wide encode cap | `16` |

See `.env.example` for the CPU and semaphore tunables.

## Troubleshooting

**FFmpeg is missing.** `which ffmpeg` and `which ffprobe` should both resolve. Set `FFMPEG_PATH` and `FFPROBE_PATH` if the binaries are not on `PATH`.

**Redis.** `redis-cli ping` should return `PONG`. BullMQ needs `maxRetriesPerRequest: null`, which this worker already sets.

**MongoDB.** Confirm `MONGODB_URI` and that the Video collection uses the same `videoId` the backend enqueued.

**Jobs sit in the queue.** The backend must push to `user-transcode-queue` or `creator-transcode-queue`, and this process must be running against that same Redis.

**A long video is retried while it is still encoding.** The BullMQ lock (10 minutes on the user queue, 30 minutes on the creator queue) can expire before a 60-minute-per-quality ffmpeg run finishes. Raise `lockDuration` in `src/index.ts` if jobs are marked stalled mid-encode.
