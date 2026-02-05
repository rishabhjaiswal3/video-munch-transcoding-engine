FROM node:18-alpine

# Install FFmpeg
RUN apk add --no-cache ffmpeg

# Create app directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install production dependencies
RUN npm install --production

# Copy source and build
COPY . .
RUN npm run build

# Create temp directory for video processing
RUN mkdir -p /tmp/videomanch-transcoding

# Start the worker
CMD ["node", "dist/index.js"]
