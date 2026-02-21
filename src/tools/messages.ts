import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { WhatsAppService } from "../services/whatsapp.js"; // Removed unused SimpleMessage import
import { log } from "../utils/logger.js";
import { CallToolResult } from "@modelcontextprotocol/sdk/types.js";
export function registerMessageTools(
  server: McpServer,
  whatsappService: WhatsAppService,
): void {
  log.info("Registering message tools...");

  server.tool(
    "list_messages",
    "Get WhatsApp messages from a specific chat.",
    {
      chat_id: z
        .string()
        .describe(
          "The JID of the chat to retrieve messages from (e.g., 123456789@s.whatsapp.net or 123456789-12345678@g.us)",
        ),
      limit: z
        .number()
        .int()
        .positive()
        .optional()
        .default(50)
        .describe("Maximum number of messages to return"),
      include_media: z
        .boolean()
        .optional()
        .default(false)
        .describe("Also fetch media payload/metadata for messages with media"),
      include_full_data: z
        .boolean()
        .optional()
        .default(false)
        .describe(
          "When include_media=true, include base64 media data in output",
        ),
      media_limit: z
        .number()
        .int()
        .positive()
        .optional()
        .default(5)
        .describe("Max number of media messages to enrich per call"),
      // Filtering would need to be done after fetching.
    },
    async ({
      chat_id,
      limit,
      include_media = false,
      include_full_data = false,
      media_limit = 5,
    }): Promise<CallToolResult> => {
      try {
        const messages = await whatsappService.getMessages(chat_id, limit);
        if (!include_media) {
          return {
            content: [
              { type: "text", text: JSON.stringify(messages, null, 2) },
            ],
          };
        }

        let remaining = media_limit;
        const enriched = [];
        for (const message of messages) {
          if (!message.hasMedia) {
            enriched.push(message);
            continue;
          }

          if (remaining <= 0) {
            enriched.push({
              ...message,
              media: { status: "skipped", reason: "media_limit_reached" },
            });
            continue;
          }

          remaining -= 1;
          try {
            const media = await whatsappService.downloadMedia(message.id);
            if (!media) {
              enriched.push({
                ...message,
                media: { status: "not_available" },
              });
              continue;
            }
            enriched.push({
              ...message,
              media: {
                status: "ok",
                mimetype: media.mimetype,
                filename: media.filename || null,
                filesize: media.filesize || media.data.length,
                data: include_full_data
                  ? media.data.toString("base64")
                  : undefined,
              },
            });
          } catch (error: any) {
            enriched.push({
              ...message,
              media: {
                status: "error",
                error: error?.message || String(error),
              },
            });
          }
        }

        // Return the simplified message structure
        return {
          content: [{ type: "text", text: JSON.stringify(enriched, null, 2) }],
        };
      } catch (error: any) {
        log.error(`Error in list_messages tool for chat ${chat_id}:`, error);
        return {
          content: [
            {
              type: "text",
              text: `Error listing messages for chat ${chat_id}: ${error.message}`,
            },
          ],
          isError: true,
        };
      }
    },
  );

  server.tool(
    "get_message_by_id",
    "Get a specific WhatsApp message by its ID.",
    {
      message_id: z
        .string()
        .describe(
          "The message ID in the format jid:id (e.g., 123456789@s.whatsapp.net:ABCDEF)",
        ),
    },
    async ({ message_id }): Promise<CallToolResult> => {
      try {
        const message = await whatsappService.getMessageById(message_id);
        if (!message) {
          return {
            content: [
              { type: "text", text: `Message not found for ID: ${message_id}` },
            ],
            isError: true,
          };
        }
        return {
          content: [{ type: "text", text: JSON.stringify(message, null, 2) }],
        };
      } catch (error: any) {
        log.error(
          `Error in get_message_by_id tool for ID ${message_id}:`,
          error,
        );
        return {
          content: [
            {
              type: "text",
              text: `Error getting message ${message_id}: ${error.message}`,
            },
          ],
          isError: true,
        };
      }
    },
  );

  server.tool(
    "search_messages",
    "Search messages by text, optionally within a specific chat.",
    {
      query: z.string().describe("Text to search for"),
      chat_id: z
        .string()
        .optional()
        .describe("Optional chat JID to scope search"),
      limit: z
        .number()
        .int()
        .positive()
        .optional()
        .default(20)
        .describe("Maximum number of results"),
    },
    async ({ query, chat_id, limit }): Promise<CallToolResult> => {
      try {
        const messages = await whatsappService.searchMessages(
          query,
          limit,
          chat_id,
        );
        return {
          content: [{ type: "text", text: JSON.stringify(messages, null, 2) }],
        };
      } catch (error: any) {
        log.error("Error in search_messages tool:", error);
        return {
          content: [
            {
              type: "text",
              text: `Error searching messages: ${error.message}`,
            },
          ],
          isError: true,
        };
      }
    },
  );

  // A simple implementation might fetch N messages before/after based on timestamp, but exact context is hard.
  // This implementation fetches recent messages and identifies the target.
  server.tool(
    "get_message_context",
    "Get recent messages around a specific message ID within its chat (context accuracy depends on fetch limit).",
    {
      message_id: z
        .string()
        .describe("The serialized ID of the target message"),
      limit: z
        .number()
        .int()
        .positive()
        .optional()
        .default(20)
        .describe("Number of recent messages to fetch for context"),
    },
    async ({ message_id, limit }): Promise<CallToolResult> => {
      try {
        // First, get the target message to find its chat ID
        const targetMessage = await whatsappService.getMessageById(message_id);
        if (!targetMessage) {
          return {
            content: [
              { type: "text", text: `Target message not found: ${message_id}` },
            ],
            isError: true,
          };
        }

        // Fetch recent messages from the same chat
        const recentMessages = await whatsappService.getMessages(
          targetMessage.to,
          limit,
        );

        // Find the index of the target message
        const targetIndex = recentMessages.findIndex(
          (msg) => msg.id === message_id,
        );

        // Structure the context (this is approximate)
        const context = {
          targetMessage: targetMessage,
          // Note: 'before' and 'after' here are just messages fetched chronologically,
          // not necessarily the exact messages before/after in the full history.
          contextMessages: recentMessages,
          targetIndex: targetIndex, // Index within the fetched contextMessages array
        };

        return {
          content: [{ type: "text", text: JSON.stringify(context, null, 2) }],
        };
      } catch (error: any) {
        log.error(
          `Error in get_message_context tool for message ${message_id}:`,
          error,
        );
        return {
          content: [
            {
              type: "text",
              text: `Error getting context for message ${message_id}: ${error.message}`,
            },
          ],
          isError: true,
        };
      }
    },
  );

  // get_last_interaction is similar to list_chats with limit 1
  server.tool(
    "get_last_interaction",
    "Get the most recent message involving a specific contact or group JID.",
    {
      jid: z
        .string()
        .describe(
          "The JID of the contact or group (e.g., 123456789@s.whatsapp.net or 123456789-12345678@g.us)",
        ),
    },
    async ({ jid }): Promise<CallToolResult> => {
      try {
        // Fetch the chat to get the last message
        const chat = await whatsappService.getChatById(jid);
        if (!chat || !chat.lastMessage) {
          return {
            content: [
              {
                type: "text",
                text: `No recent interaction found for JID: ${jid}`,
              },
            ],
            isError: true, // Consider if this should be an error or just empty result
          };
        }
        return {
          content: [
            { type: "text", text: JSON.stringify(chat.lastMessage, null, 2) },
          ],
        };
      } catch (error: any) {
        log.error(`Error in get_last_interaction tool for JID ${jid}:`, error);
        return {
          content: [
            {
              type: "text",
              text: `Error getting last interaction for ${jid}: ${error.message}`,
            },
          ],
          isError: true,
        };
      }
    },
  );

  server.tool(
    "send_message",
    "Send a WhatsApp text message to a person or group.",
    {
      recipient_jid: z
        .string()
        .describe(
          "The recipient JID (e.g., 123456789@s.whatsapp.net or 123456789-12345678@g.us)",
        ),
      message: z.string().describe("The message text to send"),
    },
    async ({ recipient_jid, message }): Promise<CallToolResult> => {
      try {
        const sentMessage = await whatsappService.sendMessage(
          recipient_jid,
          message,
        );
        const messageId =
          sentMessage?.key?.remoteJid && sentMessage?.key?.id
            ? `${sentMessage.key.remoteJid}:${sentMessage.key.id}`
            : undefined;
        // Return confirmation or details of the sent message
        const result = {
          success: true,
          message: "Message sent successfully.",
          messageId: messageId || "unknown",
          timestamp: Number(sentMessage?.messageTimestamp || Date.now() / 1000),
        };
        return {
          content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
        };
      } catch (error: any) {
        log.error(`Error in send_message tool to ${recipient_jid}:`, error);
        return {
          content: [
            {
              type: "text",
              text: `Error sending message to ${recipient_jid}: ${error.message}`,
            },
          ],
          isError: true,
        };
      }
    },
  );

  log.info("Message tools registered.");
}
