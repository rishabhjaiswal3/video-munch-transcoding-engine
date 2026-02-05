FROM node:18-alpine

# Install FFmpeg
RUN apk add --no-cache ffmpeg

# Create app directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install ALL dependencies (including devDependencies for build)
RUN npm install

# Copy source and build
COPY . .
RUN npm run build

# Remove devDependencies after build
RUN npm prune --production

# Create temp directory for video processing
RUN mkdir -p /tmp/videomanch-transcoding

# Start the worker
CMD ["node", "dist/index.js"]
