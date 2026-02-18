import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';
import { WhatsAppService } from '../services/whatsapp.js';
import { log } from '../utils/logger.js';
import { CallToolResult } from '@modelcontextprotocol/sdk/types.js';

export function registerChatTools(
  server: McpServer,
  whatsappService: WhatsAppService,
): void {
  log.info('Registering chat tools...');

  server.tool(
    'list_chats',
    'Get WhatsApp chats, optionally filtered and sorted.',
    {
      limit: z.number().int().positive().optional().default(20).describe('Maximum number of chats to return'),
      // Filtering/pagination depends on the underlying store; we apply limit locally.
      include_last_message: z.boolean().optional().default(true).describe('Whether to include the last message details'),
      // Sorting is handled by the store timestamp if available.
    },
    async ({ limit, include_last_message }): Promise<CallToolResult> => {
      try {
        // Chats are sorted by timestamp in the service.
        const chats = await whatsappService.listChats(limit, include_last_message);
        // Return the simplified chat structure
        return {
          content: [{ type: 'text', text: JSON.stringify(chats, null, 2) }],
        };
      } catch (error: any) {
        const message = error instanceof Error ? error.message : String(error);
        log.error(`Error in list_chats tool: ${message}`);
        return {
          content: [{ type: 'text', text: `Error listing chats: ${message}` }],
          isError: true,
        };
      }
    },
  );

  server.tool(
    'list_groups',
    'List group chats only.',
    {
      limit: z.number().int().positive().optional().default(20).describe('Maximum number of groups to return'),
      include_last_message: z.boolean().optional().default(true).describe('Whether to include the last message details'),
    },
    async ({ limit, include_last_message }): Promise<CallToolResult> => {
      try {
        const groups = await whatsappService.listGroups(limit, include_last_message);
        return {
          content: [{ type: 'text', text: JSON.stringify(groups, null, 2) }],
        };
      } catch (error: any) {
        const message = error instanceof Error ? error.message : String(error);
        log.error(`Error in list_groups tool: ${message}`);
        return {
          content: [{ type: 'text', text: `Error listing groups: ${message}` }],
          isError: true,
        };
      }
    },
  );

  server.tool(
    'get_chat_by_id',
    'Get WhatsApp chat metadata by JID.',
    {
      jid: z.string().describe('The JID of the chat to retrieve (e.g., 123456789@s.whatsapp.net or 123456789-12345678@g.us)'),
      // include_last_message: z.boolean().optional().default(true)
    },
    async ({ jid }): Promise<CallToolResult> => {
      try {
        const chat = await whatsappService.getChatById(jid);
        if (!chat) {
          return {
            content: [{ type: 'text', text: `Chat not found for JID: ${jid}` }],
            isError: true,
          };
        }
        // Return the simplified chat structure
        return {
          content: [{ type: 'text', text: JSON.stringify(chat, null, 2) }],
        };
      } catch (error: any) {
        log.error(`Error in get_chat_by_id tool for JID ${jid}:`, error);
        return {
          content: [{ type: 'text', text: `Error getting chat ${jid}: ${error.message}` }],
          isError: true,
        };
      }
    },
  );

  // Note: get_direct_chat_by_contact and get_contact_chats might require iterating
  // through all chats or contacts, which can be inefficient.
  // Implementing simplified versions or indicating potential performance issues.

  server.tool(
    'get_direct_chat_by_contact_number',
    'Get direct WhatsApp chat JID by contact phone number (less reliable, use get_chat_by_id if JID is known).',
    {
      phone_number: z.string().describe('The phone number of the contact (e.g., 1234567890)'),
    },
    async ({ phone_number }): Promise<CallToolResult> => {
      try {
        // Construct potential JID
        const jid = `${phone_number}@s.whatsapp.net`;
        const chat = await whatsappService.getChatById(jid);
         if (!chat || chat.isGroup) { // Ensure it's a direct chat
          return {
            content: [{ type: 'text', text: `Direct chat not found for number: ${phone_number}` }],
            isError: true,
          };
        }
        return {
          // Return only the JID or the full chat object? Let's return the object.
          content: [{ type: 'text', text: JSON.stringify(chat, null, 2) }],
        };
      } catch (error: any) {
        log.error(`Error in get_direct_chat_by_contact_number tool for number ${phone_number}:`, error);
        // Don't expose detailed errors, just indicate not found
         return {
            content: [{ type: 'text', text: `Could not find direct chat for number: ${phone_number}` }],
            isError: true,
          };
      }
    },
  );

  server.tool(
    'get_chat_by_contact',
    'Resolve a contact by name or phone number and return the chat metadata.',
    {
      query: z.string().describe('Name or phone number to resolve'),
      max_candidates: z.number().int().positive().optional().default(5).describe('Maximum number of matches to consider'),
    },
    async ({ query, max_candidates }): Promise<CallToolResult> => {
      try {
        const candidates = await whatsappService.resolveContacts(query, max_candidates);
        if (!candidates.length) {
          return {
            content: [{ type: 'text', text: `No contacts matched for query: ${query}` }],
            isError: true,
          };
        }

        const top = candidates[0];
        const second = candidates[1];
        const canAutoSelect = top && (!second || top.score >= (second.score + 10)) && top.score >= 90;

        if (!canAutoSelect && candidates.length > 1) {
          return {
            content: [{ type: 'text', text: JSON.stringify({ matches: candidates }, null, 2) }],
          };
        }

        const chat = await whatsappService.getChatById(top.id);
        if (!chat) {
          return {
            content: [{ type: 'text', text: JSON.stringify({ contact: top, chat: null }, null, 2) }],
          };
        }
        return {
          content: [{ type: 'text', text: JSON.stringify(chat, null, 2) }],
        };
      } catch (error: any) {
        log.error(`Error in get_chat_by_contact tool for query ${query}:`, error);
        return {
          content: [{ type: 'text', text: `Error getting chat for ${query}: ${error.message}` }],
          isError: true,
        };
      }
    },
  );

  // get_contact_chats can be expensive without a dedicated index.
  // A possible implementation would list all chats and filter by participants, but this is very inefficient.
  // We might omit this or provide a limited version based on known chats.
  // For now, let's omit it and potentially add later if feasible.

  log.info('Chat tools registered.');
}
