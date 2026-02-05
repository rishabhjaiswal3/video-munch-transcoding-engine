# 🎬 I_TUBE Transcoding Engine

**Standalone video transcoding microservice** for the I_TUBE platform.

This is a dedicated, scalable transcoding worker that processes video files using FFmpeg, BullMQ queues, and Cloudflare R2 storage.

---

## 🏗️ Architecture

```
Backend API → BullMQ Queue → Transcoding Engine → R2 Storage → MongoDB
```

The transcoding engine is **completely independent** from the backend API:
- Runs as a separate process/service
- Can be scaled horizontally (multiple instances)
- Connects to the same Redis queue and MongoDB as the backend
- Processes video transcoding jobs asynchronously

---

## ✨ Features

- ✅ **Multi-quality transcoding** (1080p, 720p, 480p, 360p)
- ✅ **H.264 + AAC encoding** (web-optimized MP4)
- ✅ **Automatic thumbnail generation**
- ✅ **Progress tracking** with detailed logs
- ✅ **Graceful shutdown** (waits for active jobs)
- ✅ **Separate queues** for users vs creators
- ✅ **Cloudflare R2 integration** for storage
- ✅ **Comprehensive logging** with progress bars

---

## 📋 Prerequisites

### Required Software:
1. **Node.js** (v18 or higher)
2. **FFmpeg** (with libx264 and AAC support)
3. **Redis** (for BullMQ queue)
4. **MongoDB** (for video metadata)
5. **Cloudflare R2** account (for storage)

### Install FFmpeg:

**macOS:**
```bash
brew install ffmpeg
```

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install ffmpeg
```

**Windows:**
Download from [ffmpeg.org](https://ffmpeg.org/download.html)

---

## 🚀 Installation

### 1. Install Dependencies
```bash
cd transcoding-engine
yarn install
# or
npm install
```

### 2. Configure Environment
```bash
cp .env.example .env
```

Edit `.env` with your credentials:
```env
# MongoDB
MONGODB_URI=mongodb+srv://<Name>:<Password>@videomanch.f6qdcu1.mongodb.net/?appName=videoManch

# Redis
REDIS_URL=redis://localhost:6379

# Cloudflare R2
R2_ACCOUNT_ID=your_account_id
R2_ACCESS_KEY_ID=your_access_key_id
R2_SECRET_ACCESS_KEY=your_secret_access_key
R2_BUCKET_RAW=itube-raw
R2_BUCKET_TRANSCODED=itube-transcoded
R2_BUCKET_THUMBNAILS=itube-thumbnails
R2_PUBLIC_URL=https://your-public-url.com
```

### 3. Verify FFmpeg Installation
```bash
ffmpeg -version
ffprobe -version
```

---

## 🎯 Usage

### Development Mode
```bash
yarn dev
# or
npm run dev
```

### Production Mode
```bash
# Build TypeScript
yarn build

# Run compiled code
yarn start
```

---

## 📊 How It Works

### 1. **Job Reception**
- Listens to BullMQ queues: `user-transcode-queue` and `creator-transcode-queue`
- Receives job data: `{ videoId, userId, userType, r2Key, qualities }`

### 2. **Processing Pipeline**

```
Phase 1: Download → Phase 2: Probe → Phase 3: Transcode → Phase 4: Thumbnail → Phase 5: Upload → Phase 6: Finalize
```

**Phase 1: Download Original Video**
- Downloads from R2 `itube-raw` bucket
- Saves to temp directory: `/tmp/itube-transcoding/{videoId}/`

**Phase 2: Probe Video Metadata**
- Extracts duration, resolution, codec info using FFprobe

**Phase 3: Transcode to Multiple Qualities**
- For each quality (1080p, 720p, 480p, 360p):
  - Encodes with H.264 video + AAC audio
  - Maintains aspect ratio with padding
  - Optimizes for web streaming (`-movflags +faststart`)
  - Shows progress bar in logs

**Phase 4: Generate Thumbnail**
- Extracts frame at 10% of video duration
- Creates 1280×720 JPEG thumbnail

**Phase 5: Upload to R2**
- Uploads all transcoded videos to `itube-transcoded` bucket
- Uploads thumbnail

**Phase 6: Finalize**
- Updates MongoDB with:
  - Status: `completed`
  - Output URLs for each quality
  - Thumbnail URL
  - Video duration

### 3. **Progress Tracking**
- Real-time progress updates to BullMQ (visible in backend)
- Periodic updates to MongoDB
- Detailed console logs with progress bars

---

## 📈 Console Output Example

```
================================================================================
🎬 I_TUBE TRANSCODING ENGINE
   Standalone Video Processing Worker
================================================================================

[STARTUP] Connecting to MongoDB...
[DATABASE] ✅ Connected to MongoDB
[STARTUP] Connecting to Redis...
[REDIS] ✅ Connected to Redis
[STARTUP] Initializing workers...

================================================================================
✅ TRANSCODING ENGINE READY
   - User queue worker (concurrency: 2, timeout: 10min)
   - Creator queue worker (concurrency: 1, timeout: 30min)
   - Listening for transcoding jobs...
================================================================================

================================================================================
[TRANSCODE] 🎬 Starting transcoding job abc-123
[TRANSCODE] 🎥 Video ID: abc-123-def-456
[TRANSCODE] 👤 User: user123 (creator)
[TRANSCODE] 🎯 Target qualities: 1080p, 720p, 480p, 360p
================================================================================

[TRANSCODE] 📥 PHASE 1: Downloading original video from R2
[R2] 📥 Downloading from R2
[R2] ✅ Download complete
[TRANSCODE] 📊 Progress: 10% (10/85 weight units)

[TRANSCODE] 🔍 PHASE 2: Probing video metadata
[FFMPEG] 🔍 Probing video: /tmp/itube-transcoding/abc-123/original.mp4
[FFMPEG] ✅ Probe successful
[TRANSCODE] ⏱️ Video duration: 120.50s
[TRANSCODE] 📐 Video dimensions: 1920x1080

[TRANSCODE] ⚙️ PHASE 3: Transcoding to multiple qualities

[TRANSCODE] 🎞️ Transcoding quality 1/4: 1080p
[TRANSCODE] 🔄 Starting FFmpeg encoding...
[TRANSCODE] 🔄 1080p: [█░░░░░░░░░░░░░░░░░░░] 5% | Time: 2.3s | Overall: 11%
[TRANSCODE] 🔄 1080p: [██░░░░░░░░░░░░░░░░░░] 10% | Time: 4.8s | Overall: 12%
[TRANSCODE] 🔄 1080p: [████████████████████] 100% | Time: 45.2s | Overall: 30%
[TRANSCODE] ✅ 1080p transcoding complete in 45.2s

...

[TRANSCODE] ☁️ PHASE 5: Uploading transcoded files to R2
[TRANSCODE] 📤 Uploading 1/4: 1080p
[R2] 📤 Uploading to R2
[R2] ✅ Upload complete
[TRANSCODE] ✅ 1080p uploaded (125.50 MB)

[TRANSCODE] 💾 PHASE 6: Finalizing in database
[TRANSCODE] ✅ Video status updated to 'completed'
[TRANSCODE] 🎉 Job abc-123 completed successfully!
[TRANSCODE] 📊 Generated 4 quality variants
================================================================================
```

---

## ⚙️ Configuration

### Worker Concurrency

**User Queue:**
- Concurrency: 2 (processes 2 jobs simultaneously)
- Timeout: 10 minutes per job

**Creator Queue:**
- Concurrency: 1 (processes 1 job at a time)
- Timeout: 30 minutes per job (larger files)

### Quality Presets

| Quality | Resolution | Video Bitrate | Audio Bitrate |
|---------|-----------|---------------|---------------|
| 1080p   | 1920×1080 | 5000k         | 192k          |
| 720p    | 1280×720  | 2500k         | 128k          |
| 480p    | 854×480   | 1000k         | 96k           |
| 360p    | 640×360   | 600k          | 64k           |

---

## 🔧 Scaling

### Horizontal Scaling
Run multiple instances of the transcoding engine:

```bash
# Terminal 1
cd transcoding-engine
yarn dev

# Terminal 2
cd transcoding-engine
yarn dev

# Terminal 3
cd transcoding-engine
yarn dev
```

All instances will:
- Connect to the same Redis queue
- Process jobs in parallel
- Automatically distribute workload

### Vertical Scaling
Increase concurrency in `src/index.ts`:

```typescript
userWorker = new Worker<TranscodeJobData>(
  'user-transcode-queue',
  processJob,
  {
    connection,
    concurrency: 4, // Increase from 2 to 4
    lockDuration: 600000,
  }
);
```

---

## 🛑 Graceful Shutdown

The engine handles shutdown gracefully:

1. **Stop accepting new jobs**
2. **Wait for active jobs to complete** (max 5 minutes)
3. **Close worker connections**
4. **Disconnect from MongoDB and Redis**
5. **Exit cleanly**

Trigger shutdown:
```bash
Ctrl+C  # or send SIGTERM/SIGINT
```

---

## 🐛 Troubleshooting

### FFmpeg not found
```bash
# Check if FFmpeg is installed
which ffmpeg
which ffprobe

# If not found, install it
brew install ffmpeg  # macOS
sudo apt install ffmpeg  # Ubuntu
```

### Redis connection error
```bash
# Check if Redis is running
redis-cli ping
# Should return: PONG

# Start Redis if not running
redis-server
```

### MongoDB connection error
```bash
# Check if MongoDB is running
mongosh --eval "db.runCommand({ ping: 1 })"

# Start MongoDB if not running
mongod
```

### Jobs not being processed
1. Check if backend is adding jobs to the queue
2. Verify Redis connection
3. Check worker logs for errors
4. Ensure queue names match (`user-transcode-queue`, `creator-transcode-queue`)

---

## 📁 Project Structure

```
transcoding-engine/
├── src/
│   ├── config/
│   │   ├── database.ts      # MongoDB connection
│   │   ├── redis.ts         # Redis connection
│   │   └── r2.ts            # Cloudflare R2 config
│   ├── models/
│   │   └── Video.ts         # Video model (MongoDB)
│   ├── services/
│   │   ├── ffmpegService.ts # FFmpeg operations
│   │   └── r2Service.ts     # R2 file operations
│   ├── types/
│   │   └── index.ts         # TypeScript types
│   └── index.ts             # Main worker entry point
├── .env.example             # Environment variables template
├── package.json             # Dependencies and scripts
├── tsconfig.json            # TypeScript configuration
└── README.md                # This file
```

---

## 🔗 Integration with Backend

The backend API adds jobs to the queue:

```typescript
// backend/src/routes/upload.ts
await queueService.addTranscodeJob({
  videoId,
  userId,
  userType,
  r2Key,
  qualities: ['1080p', '720p', '480p']
});
```

The transcoding engine picks up and processes these jobs automatically.

---

## 📝 Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `MONGODB_URI` | MongoDB connection string | `mongodb://localhost:27017/itube` |
| `REDIS_URL` | Redis connection string | `redis://localhost:6379` |
| `R2_ACCOUNT_ID` | Cloudflare R2 account ID | `abc123...` |
| `R2_ACCESS_KEY_ID` | R2 access key | `your_key` |
| `R2_SECRET_ACCESS_KEY` | R2 secret key | `your_secret` |
| `R2_BUCKET_RAW` | Bucket for original videos | `itube-raw` |
| `R2_BUCKET_TRANSCODED` | Bucket for transcoded videos | `itube-transcoded` |
| `R2_BUCKET_THUMBNAILS` | Bucket for generated thumbnails | `itube-thumbnails` |
| `R2_PUBLIC_URL` | Public URL for R2 bucket | `https://cdn.example.com` |
| `FFMPEG_PATH` | Custom FFmpeg binary path (optional) | `/usr/local/bin/ffmpeg` |
| `FFPROBE_PATH` | Custom FFprobe binary path (optional) | `/usr/local/bin/ffprobe` |

---

## 🚀 Deployment

### Docker (Recommended)

```dockerfile
FROM node:18-alpine

# Install FFmpeg
RUN apk add --no-cache ffmpeg

WORKDIR /app

COPY package*.json ./
RUN npm install --production

COPY . .
RUN npm run build

CMD ["node", "dist/index.js"]
```

### PM2 (Process Manager)

```bash
# Install PM2
npm install -g pm2

# Start worker
pm2 start dist/index.js --name transcoding-engine

# Monitor
pm2 logs transcoding-engine
pm2 monit
```

---

## 📊 Monitoring

### Check Queue Status
```bash
# In backend directory
yarn queue:status
```

### View Logs
```bash
# Real-time logs
tail -f logs/transcoding.log

# PM2 logs
pm2 logs transcoding-engine
```

---

## 🎯 Performance Tips

1. **Use SSD storage** for temp directory (`/tmp`)
2. **Allocate sufficient RAM** (4GB+ recommended)
3. **Use multiple workers** for parallel processing
4. **Monitor CPU usage** (FFmpeg is CPU-intensive)
5. **Consider GPU acceleration** for large-scale deployments

---

## 📄 License

Part of the I_TUBE platform.

---

## 🤝 Contributing

This is a standalone microservice. To contribute:
1. Ensure changes don't break backend integration
2. Test with various video formats
3. Update logs and documentation

---

**Built with ❤️ for scalable video processing**
