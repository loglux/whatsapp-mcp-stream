import ffmpeg from "fluent-ffmpeg";
import fs from "fs";
import path from "path";
import os from "os";
import { log } from "./logger.js";

// Attempt to find ffmpeg path automatically or allow setting via environment variable
const FFMPEG_PATH = process.env.FFMPEG_PATH || findFfmpegPath();
if (FFMPEG_PATH) {
  ffmpeg.setFfmpegPath(FFMPEG_PATH);
  log.info(`Using ffmpeg found at: ${FFMPEG_PATH}`);
} else {
  log.warn(
    "ffmpeg path not found or set. Audio conversion will likely fail. Set FFMPEG_PATH environment variable if needed.",
  );
}

function findFfmpegPath(): string | null {
  // Basic check in common locations or PATH (this is simplified)
  // A more robust solution might use 'which' or 'where' commands
  // or check standard installation directories per OS.
  // For now, we rely on it being in the system PATH or set via env var.
  // Returning null signifies it wasn't explicitly found here.
  return null;
}

let cachedAllowedRoots: string[] | null = null;
function getAllowedFfmpegRoots(): string[] {
  if (cachedAllowedRoots) return cachedAllowedRoots;
  const roots: string[] = [];
  if (process.env.MEDIA_DIR) {
    try {
      roots.push(fs.realpathSync(process.env.MEDIA_DIR));
    } catch {
      roots.push(path.resolve(process.env.MEDIA_DIR));
    }
  }
  try {
    roots.push(fs.realpathSync(os.tmpdir()));
  } catch {
    roots.push(path.resolve(os.tmpdir()));
  }
  cachedAllowedRoots = roots;
  return roots;
}

function assertSafeFfmpegInput(inputPath: string): string {
  if (typeof inputPath !== "string" || inputPath.length === 0) {
    throw new Error("ffmpeg input path must be a non-empty string");
  }
  // Reject paths ffmpeg would parse as a CLI option (leading '-' is a flag, not a file).
  if (inputPath.startsWith("-")) {
    throw new Error(`ffmpeg input path must not start with '-': ${inputPath}`);
  }
  if (!path.isAbsolute(inputPath)) {
    throw new Error(`ffmpeg input path must be absolute: ${inputPath}`);
  }
  let resolved: string;
  try {
    resolved = fs.realpathSync(inputPath);
  } catch {
    throw new Error(
      `ffmpeg input file not found or not accessible: ${inputPath}`,
    );
  }
  const roots = getAllowedFfmpegRoots();
  const within = roots.some((root) => {
    const rel = path.relative(root, resolved);
    return rel !== "" && !rel.startsWith("..") && !path.isAbsolute(rel);
  });
  if (!within) {
    throw new Error(
      `ffmpeg input path is outside allowed roots (MEDIA_DIR, tmpdir): ${resolved}`,
    );
  }
  return resolved;
}

export class AudioUtils {
  /**
   * Convert an audio file to Opus format in an Ogg container using ffmpeg.
   * Throws an error if ffmpeg is not found or conversion fails.
   *
   * @param inputPath Path to the input audio file.
   * @param outputPath Optional path for the output file. Defaults to input path with .ogg extension.
   * @param bitrate Target bitrate (e.g., "32k").
   * @param sampleRate Target sample rate (e.g., 24000).
   * @returns Path to the converted file.
   */
  static convertToOpusOgg(
    inputPath: string,
    outputPath?: string,
    bitrate = "32k",
    sampleRate = 24000,
  ): Promise<string> {
    return new Promise((resolve, reject) => {
      if (!FFMPEG_PATH && !process.env.FFMPEG_PATH) {
        // Check again in case it became available after startup
        if (!findFfmpegPath()) {
          return reject(
            new Error(
              "ffmpeg path not configured. Set FFMPEG_PATH or ensure ffmpeg is in system PATH.",
            ),
          );
        }
      }
      let safeInputPath: string;
      try {
        safeInputPath = assertSafeFfmpegInput(inputPath);
      } catch (err) {
        return reject(err instanceof Error ? err : new Error(String(err)));
      }

      const finalOutputPath =
        outputPath || `${path.parse(safeInputPath).name}.ogg`;
      const outputDir = path.dirname(finalOutputPath);
      if (!fs.existsSync(outputDir)) {
        fs.mkdirSync(outputDir, { recursive: true });
      }

      log.debug(
        `Starting ffmpeg conversion: ${safeInputPath} -> ${finalOutputPath}`,
      );
      ffmpeg(safeInputPath)
        .audioCodec("libopus")
        .audioBitrate(bitrate)
        .audioFrequency(sampleRate)
        .outputOptions([
          "-application voip", // Optimize for voice
          "-vbr on", // Variable bitrate
          "-compression_level 10", // Max compression
          "-frame_duration 60", // Good frame duration for voice
        ])
        .output(finalOutputPath)
        .on("end", () => {
          log.debug(`ffmpeg conversion finished: ${finalOutputPath}`);
          resolve(finalOutputPath);
        })
        .on("error", (err: Error) => {
          // Add type Error
          log.error(`ffmpeg conversion error for ${inputPath}:`, err);
          reject(new Error(`ffmpeg conversion failed: ${err.message}`));
        })
        .run();
    });
  }

  /**
   * Converts an audio file to Opus/Ogg and saves it to a temporary file.
   * Useful when you need a temporary .ogg file for sending.
   *
   * @param inputPath Path to the input audio file.
   * @param bitrate Target bitrate.
   * @param sampleRate Target sample rate.
   * @returns Path to the temporary converted file.
   */
  static async convertToOpusOggTemp(
    inputPath: string,
    bitrate = "32k",
    sampleRate = 24000,
  ): Promise<string> {
    const tempFileName = `whatsapp_audio_converted_${Date.now()}.ogg`;
    const tempOutputPath = path.join(os.tmpdir(), tempFileName);
    log.debug(`Converting ${inputPath} to temporary file: ${tempOutputPath}`);
    try {
      const convertedPath = await this.convertToOpusOgg(
        inputPath,
        tempOutputPath,
        bitrate,
        sampleRate,
      );
      return convertedPath;
    } catch (error) {
      // Clean up temp file if conversion failed partway
      if (fs.existsSync(tempOutputPath)) {
        try {
          fs.unlinkSync(tempOutputPath);
        } catch (cleanupError) {
          log.warn(
            `Failed to clean up temporary file ${tempOutputPath}:`,
            cleanupError,
          );
        }
      }
      throw error; // Re-throw the original conversion error
    }
  }
}
