import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { WhatsAppService } from "../services/whatsapp.js";
import { log } from "../utils/logger.js";
import {
  CallToolResult,
  ImageContent,
  AudioContent,
  TextContent,
} from "@modelcontextprotocol/sdk/types.js";
import { AudioUtils } from "../utils/audio.js";
import fs from "fs";
import path from "path";
import os from "os";
import crypto from "crypto";
import axios from "axios";
import { fileTypeFromBuffer } from "file-type";

export function registerMediaTools(
  server: McpServer,
  whatsappService: WhatsAppService,
): void {
  log.info("Registering media tools...");

  server.tool(
    "send_media",
    "Send media (image, video, document, audio) via WhatsApp.",
    {
      recipient_jid: z
        .string()
        .describe(
          "The recipient JID (e.g., 123456789@s.whatsapp.net or 123456789-12345678@g.us)",
        ),
      media_path: z
        .string()
        .optional()
        .describe("Absolute path to the local media file"),
      media_url: z.string().url().optional().describe("URL of the media file"),
      media_content: z
        .string()
        .optional()
        .describe("Base64 encoded media content"),
      mime_type: z
        .string()
        .optional()
        .describe(
          "MIME type of the media_content (required if using media_content)",
        ),
      filename: z
        .string()
        .optional()
        .describe(
          "Filename for the media (recommended if using media_content)",
        ),
      caption: z.string().optional().describe("Optional caption for the media"),
      as_audio_message: z
        .boolean()
        .optional()
        .default(false)
        .describe(
          "Send audio specifically as a voice note (requires ffmpeg for conversion if not opus/ogg)",
        ),
      idempotency_key: z
        .string()
        .min(1)
        .max(200)
        .optional()
        .describe(
          "Optional idempotency key. Repeating the same send_media request with the same key returns the original result instead of sending again.",
        ),
      include_full_data: z
        .boolean()
        .optional()
        .default(false)
        .describe("Whether to include the full base64 data in the response"),
    },
    async ({
      recipient_jid,
      media_path,
      media_url,
      media_content,
      mime_type,
      filename,
      caption,
      as_audio_message,
      idempotency_key,
      include_full_data = false,
    }): Promise<CallToolResult> => {
      let input: string | null = null;
      let inputType: "path" | "url" | "base64" | null = null;

      if (media_path) {
        input = media_path;
        inputType = "path";
      } else if (media_url) {
        input = media_url;
        inputType = "url";
      } else if (media_content) {
        if (!mime_type) {
          return {
            content: [
              {
                type: "text",
                text: "mime_type is required when using media_content",
              },
            ],
            isError: true,
          };
        }
        input = media_content;
        inputType = "base64";
      }

      if (!input || !inputType) {
        return {
          content: [
            {
              type: "text",
              text: "One of media_path, media_url, or media_content must be provided",
            },
          ],
          isError: true,
        };
      }

      try {
        const requestFingerprint = crypto
          .createHash("sha256")
          .update(
            JSON.stringify({
              recipient_jid,
              media_path: media_path || null,
              media_url: media_url || null,
              media_content: media_content || null,
              mime_type: mime_type || null,
              filename: filename || null,
              caption: caption || null,
              as_audio_message,
            }),
          )
          .digest("hex");
        let sentMessage: any;
        let finalMediaPath = media_path;

        if (as_audio_message) {
          let audioPath: string;
          let tempFilePath: string | null = null;
          let needsCleanup = false;

          if (inputType === "path") {
            audioPath = input;
          } else if (inputType === "url") {
            const resp = await axios.get(input, {
              responseType: "arraybuffer",
            });
            const buffer = Buffer.from(resp.data);
            const detected = await fileTypeFromBuffer(buffer);
            const ext = detected?.ext || "bin";
            tempFilePath = path.join(
              os.tmpdir(),
              `whatsapp_audio_${Date.now()}.${ext}`,
            );
            fs.writeFileSync(tempFilePath, buffer);
            audioPath = tempFilePath;
            needsCleanup = true;
          } else {
            const buffer = Buffer.from(input, "base64");
            const detected = await fileTypeFromBuffer(buffer);
            const ext = detected?.ext || "bin";
            tempFilePath = path.join(
              os.tmpdir(),
              `whatsapp_audio_${Date.now()}.${ext}`,
            );
            fs.writeFileSync(tempFilePath, buffer);
            audioPath = tempFilePath;
            needsCleanup = true;
          }

          if (!audioPath.endsWith(".ogg")) {
            const convertedPath =
              await AudioUtils.convertToOpusOggTemp(audioPath);
            if (needsCleanup && tempFilePath && fs.existsSync(tempFilePath)) {
              fs.unlinkSync(tempFilePath);
            }
            tempFilePath = convertedPath;
            needsCleanup = true;
            audioPath = convertedPath;
            finalMediaPath = audioPath;
          }

          sentMessage = await whatsappService.sendMedia(
            recipient_jid,
            audioPath,
            caption,
            true,
            {
              idempotencyKey: idempotency_key,
              requestFingerprint,
            },
          );

          if (needsCleanup && tempFilePath && fs.existsSync(tempFilePath)) {
            fs.unlinkSync(tempFilePath);
          }
        } else {
          if (inputType === "base64") {
            sentMessage = await whatsappService.sendMediaFromBase64(
              recipient_jid,
              input,
              mime_type!,
              filename,
              caption,
              false,
              {
                idempotencyKey: idempotency_key,
                requestFingerprint,
              },
            );
          } else {
            sentMessage = await whatsappService.sendMedia(
              recipient_jid,
              input,
              caption,
              false,
              {
                idempotencyKey: idempotency_key,
                requestFingerprint,
              },
            );
            if (inputType === "path") {
              finalMediaPath = input;
            }
          }
        }

        const messageId =
          sentMessage?.key?.remoteJid && sentMessage?.key?.id
            ? `${sentMessage.key.remoteJid}:${sentMessage.key.id}`
            : undefined;

        const result: any = {
          success: true,
          message: `Media (${as_audio_message ? "audio message" : "file"}) sent successfully.`,
          messageId: messageId || "unknown",
          timestamp: Number(sentMessage?.messageTimestamp || Date.now() / 1000),
          filePathUsed: finalMediaPath,
          deduplicated: Boolean(sentMessage?.__deduplicated),
          idempotencyKey: idempotency_key || null,
        };

        if (include_full_data && inputType === "base64") {
          result.mediaData = media_content;
          result.mimeType = mime_type;
        } else if (
          include_full_data &&
          inputType === "path" &&
          input &&
          fs.existsSync(input)
        ) {
          const buffer = fs.readFileSync(input);
          result.mediaData = buffer.toString("base64");
          const detectedType = await fileTypeFromBuffer(buffer);
          result.mimeType = detectedType?.mime || "application/octet-stream";
        }

        return {
          content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
        };
      } catch (error: any) {
        log.error(`Error in send_media tool to ${recipient_jid}:`, error);
        return {
          content: [
            {
              type: "text",
              text: `Error sending media to ${recipient_jid}: ${error.message}`,
            },
          ],
          isError: true,
        };
      }
    },
  );

  server.tool(
    "download_media",
    "Download media from a WhatsApp message and return its content.",
    {
      message_id: z.string().describe("The message ID in the format jid:id"),
      include_full_data: z
        .boolean()
        .optional()
        .default(false)
        .describe("Whether to include the full base64 data in the response"),
      save_to_disk: z
        .boolean()
        .optional()
        .default(false)
        .describe("Whether to save media to disk and return a local URL"),
    },
    async ({
      message_id,
      include_full_data = false,
      save_to_disk = false,
    }): Promise<CallToolResult> => {
      try {
        const media = await whatsappService.downloadMedia(message_id);
        if (!media) {
          return {
            content: [
              {
                type: "text",
                text: `Media not found or failed to download for message: ${message_id}`,
              },
            ],
            isError: true,
          };
        }

        const metadata: any = {
          filename: media.filename || "unknown",
          mimetype: media.mimetype,
          filesize: media.filesize || "unknown",
        };

        if (save_to_disk) {
          const mediaDir =
            process.env.MEDIA_DIR || path.join(process.cwd(), "media");
          if (!fs.existsSync(mediaDir)) {
            fs.mkdirSync(mediaDir, { recursive: true });
          }
          const baseName = message_id.replace(/[^a-zA-Z0-9_-]/g, "_");
          const originalExt = media.filename
            ? path.extname(media.filename).replace(".", "")
            : "";
          const mimeExtMap: Record<string, string> = {
            "audio/ogg": "ogg",
            "audio/opus": "opus",
            "audio/mpeg": "mp3",
            "audio/mp4": "m4a",
            "video/mp4": "mp4",
            "image/jpeg": "jpg",
            "image/png": "png",
            "image/webp": "webp",
            "application/pdf": "pdf",
          };
          const mimeExt = media.mimetype
            ? mimeExtMap[media.mimetype] ||
              media.mimetype.split("/")[1]?.split(";")[0]
            : "";
          const ext = originalExt || mimeExt || "bin";
          const safeExt = ext.replace(/[^a-zA-Z0-9]/g, "") || "bin";
          const filename = `${baseName}.${safeExt}`;
          const filePath = path.join(mediaDir, filename);
          fs.writeFileSync(filePath, media.data);
          const urlPath = `/media/${filename}`;
          const publicBase = (
            process.env.MEDIA_PUBLIC_BASE_URL ||
            process.env.PUBLIC_BASE_URL ||
            ""
          ).replace(/\/$/, "");
          metadata.savedPath = filePath;
          metadata.savedFilename = filename;
          metadata.url = urlPath;
          if (publicBase) {
            metadata.publicUrl = `${publicBase}${urlPath}`;
          }
          if (media.filename) {
            metadata.originalFilename = media.filename;
          } else {
            metadata.filename = filename;
          }
        }

        const metadataContent: TextContent = {
          type: "text",
          text: JSON.stringify(metadata, null, 2),
        };

        const contentArray: Array<TextContent | ImageContent | AudioContent> = [
          metadataContent,
        ];

        if (include_full_data) {
          const base64 = media.data.toString("base64");
          if (media.mimetype.startsWith("image/")) {
            contentArray.push({
              type: "image",
              data: base64,
              mimeType: media.mimetype,
            });
          } else if (media.mimetype.startsWith("audio/")) {
            contentArray.push({
              type: "audio",
              data: base64,
              mimeType: media.mimetype,
            });
          } else {
            contentArray.push({ type: "text", text: base64 });
          }
        }

        return { content: contentArray };
      } catch (error: any) {
        log.error(
          `Error in download_media tool for message ${message_id}:`,
          error,
        );
        return {
          content: [
            {
              type: "text",
              text: `Error downloading media for message ${message_id}: ${error.message}`,
            },
          ],
          isError: true,
        };
      }
    },
  );

  log.info("Media tools registered.");
}
