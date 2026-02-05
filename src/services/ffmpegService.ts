import ffmpeg, { FfprobeData } from 'fluent-ffmpeg';
import path from 'path';
import fs from 'fs/promises';

// Allow overriding FFmpeg binary paths via environment variables
if (process.env.FFMPEG_PATH) {
    ffmpeg.setFfmpegPath(process.env.FFMPEG_PATH);
}
if (process.env.FFPROBE_PATH) {
    ffmpeg.setFfprobePath(process.env.FFPROBE_PATH);
}

export interface QualityPreset {
    width: number;
    height: number;
    bitrate: string;
    audioBitrate: string;
}

export interface HLSOutput {
    playlistPath: string;    // .m3u8 file path
    segmentPattern: string;  // .ts files pattern
    segmentCount: number;    // Number of .ts chunks created
    segmentFiles: string[];  // Array of all .ts file paths
}

export const ffmpegService = {
    /**
     * Probe a video file to extract duration and metadata.
     */
    probe(inputPath: string): Promise<FfprobeData> {
        console.log(`[FFMPEG] 🔍 Probing video: ${inputPath}`);
        return new Promise((resolve, reject) => {
            ffmpeg.ffprobe(inputPath, (err, data) => {
                if (err) {
                    console.error(`[FFMPEG] ❌ Probe failed:`, err);
                    return reject(err);
                }
                console.log(`[FFMPEG] ✅ Probe successful`);
                resolve(data);
            });
        });
    },

    /**
     * Transcode a video to HLS format with adaptive bitrate streaming.
     * Generates .m3u8 playlist and .ts segment files.
     */
    transcodeToHLS(
        inputPath: string,
        outputDir: string,
        quality: string,
        preset: QualityPreset,
        onProgress?: (percent: number) => void
    ): Promise<HLSOutput> {
        console.log(`[FFMPEG] ⚙️ Starting HLS transcode`);
        console.log(`[FFMPEG] 💾 Input: ${inputPath}`);
        console.log(`[FFMPEG] 📁 Output dir: ${outputDir}`);
        console.log(`[FFMPEG] 🎯 Quality: ${quality}`);
        console.log(`[FFMPEG] 📐 Resolution: ${preset.width}x${preset.height}`);
        console.log(`[FFMPEG] 🎚️ Bitrate: ${preset.bitrate} (video), ${preset.audioBitrate} (audio)`);

        const playlistFilename = `${quality}.m3u8`;
        const segmentPattern = `${quality}_%03d.ts`;
        const playlistPath = path.join(outputDir, playlistFilename);
        const segmentPath = path.join(outputDir, segmentPattern);

        return new Promise((resolve, reject) => {
            ffmpeg(inputPath)
                .outputOptions([
                    // Video codec
                    '-c:v libx264',
                    '-preset medium',
                    `-b:v ${preset.bitrate}`,
                    '-maxrate ' + preset.bitrate,
                    '-bufsize ' + (parseInt(preset.bitrate) * 2) + 'k',

                    // Audio codec
                    '-c:a aac',
                    `-b:a ${preset.audioBitrate}`,

                    // Scaling with aspect ratio preservation
                    `-vf scale=${preset.width}:${preset.height}:force_original_aspect_ratio=decrease,pad=${preset.width}:${preset.height}:(ow-iw)/2:(oh-ih)/2`,

                    // HLS specific options
                    '-f hls',
                    '-hls_time 6',                    // 6 second segments
                    '-hls_list_size 0',               // Keep all segments in playlist
                    '-hls_segment_type mpegts',       // Use MPEG-TS format
                    '-hls_segment_filename ' + segmentPath,
                    '-hls_playlist_type vod',         // Video on demand

                    // Optimization
                    '-g 48',                          // GOP size (keyframe interval)
                    '-sc_threshold 0',                // Disable scene change detection
                    '-keyint_min 48',                 // Minimum keyframe interval

                    // Force overwrite
                    '-y',
                ])
                .output(playlistPath)
                .on('progress', (progress) => {
                    if (onProgress && progress.percent !== undefined) {
                        onProgress(Math.round(progress.percent));
                    }
                })
                .on('end', async () => {
                    console.log(`[FFMPEG] ✅ HLS transcode complete`);

                    // Count generated segments
                    try {
                        const files = await fs.readdir(outputDir);
                        const segmentFiles = files.filter(f => f.startsWith(quality) && f.endsWith('.ts'));
                        const segmentCount = segmentFiles.length;

                        console.log(`[FFMPEG] 📊 Generated ${segmentCount} segments`);
                        console.log(`[FFMPEG] 📄 Playlist: ${playlistFilename}`);

                        resolve({
                            playlistPath,
                            segmentPattern,
                            segmentCount,
                            segmentFiles: segmentFiles.map(f => path.join(outputDir, f))
                        });
                    } catch (err) {
                        reject(err);
                    }
                })
                .on('error', (err) => {
                    console.error(`[FFMPEG] ❌ HLS transcode failed:`, err.message);
                    reject(err);
                })
                .run();
        });
    },

    /**
     * Generate a master playlist that references all quality variants.
     * This enables adaptive bitrate streaming.
     */
    async generateMasterPlaylist(
        outputDir: string,
        variants: Array<{
            quality: string;
            bandwidth: number;  // bits per second
            resolution: string; // e.g., "1920x1080"
            playlistFilename: string;
        }>
    ): Promise<string> {
        console.log(`[FFMPEG] 📝 Generating master playlist`);
        console.log(`[FFMPEG] 📊 Variants: ${variants.length}`);

        const masterPlaylistPath = path.join(outputDir, 'master.m3u8');

        // Sort variants by bandwidth (highest first for better initial quality)
        const sortedVariants = [...variants].sort((a, b) => b.bandwidth - a.bandwidth);

        // Generate master playlist content
        let content = '#EXTM3U\n';
        content += '#EXT-X-VERSION:3\n\n';

        for (const variant of sortedVariants) {
            content += `#EXT-X-STREAM-INF:BANDWIDTH=${variant.bandwidth},RESOLUTION=${variant.resolution}\n`;
            content += `${variant.playlistFilename}\n`;
        }

        await fs.writeFile(masterPlaylistPath, content, 'utf-8');

        console.log(`[FFMPEG] ✅ Master playlist created: master.m3u8`);
        console.log(`[FFMPEG] 📋 Content:\n${content}`);

        return masterPlaylistPath;
    },

    /**
     * Generate a JPEG thumbnail from a video at a given timestamp.
     */
    generateThumbnail(
        inputPath: string,
        outputPath: string,
        timestampSeconds: number = 2
    ): Promise<void> {
        console.log(`[FFMPEG] 🖼️ Generating thumbnail`);
        console.log(`[FFMPEG] 💾 Input: ${inputPath}`);
        console.log(`[FFMPEG] 💾 Output: ${outputPath}`);
        console.log(`[FFMPEG] ⏱️ Timestamp: ${timestampSeconds}s`);

        return new Promise((resolve, reject) => {
            ffmpeg(inputPath)
                .screenshots({
                    count: 1,
                    folder: path.dirname(outputPath),
                    filename: path.basename(outputPath),
                    timemarks: [timestampSeconds.toString()],
                    size: '1280x720',
                })
                .on('end', () => {
                    console.log(`[FFMPEG] ✅ Thumbnail generated`);
                    resolve();
                })
                .on('error', (err) => {
                    console.error(`[FFMPEG] ❌ Thumbnail generation failed:`, err.message);
                    reject(err);
                });
        });
    },
};
