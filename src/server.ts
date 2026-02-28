import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { SSEServerTransport } from '@modelcontextprotocol/sdk/server/sse.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { Implementation } from '@modelcontextprotocol/sdk/types.js';
import { isInitializeRequest, ListToolsRequestSchema } from '@modelcontextprotocol/sdk/types.js';
import express, { Request, Response } from 'express'; // Import Request and Response
import { zodToJsonSchema } from 'zod-to-json-schema';
import { WhatsAppService } from './services/whatsapp.js';
import { log } from './utils/logger.js';
import { BrowserProcessManager } from './utils/browser-process-manager.js';
import { randomUUID } from 'node:crypto';
// Import tool registration functions
import { registerContactTools } from './tools/contacts.js';
import { registerChatTools } from './tools/chats.js';
import { registerMessageTools } from './tools/messages.js';
import { registerMediaTools } from './tools/media.js';
import { registerAuthTools } from './tools/auth.js';
import { registerAdminRoutes } from './http/admin.js';

const SERVER_INFO: Implementation = {
  name: 'whatsapp-mcp-stream',
  version: '1.0.0', // Consider reading from package.json
};

type ExecutionMetadata = {
  readOnlyHint?: boolean;
  destructiveHint?: boolean;
  idempotentHint?: boolean;
  openWorldHint?: boolean;
};

const TOOL_EXECUTION_METADATA: Record<string, ExecutionMetadata> = {
  ping: {
    readOnlyHint: true,
    idempotentHint: true,
    openWorldHint: false,
  },
  get_qr_code: {
    readOnlyHint: true,
    idempotentHint: true,
    openWorldHint: false,
  },
  check_auth_status: {
    readOnlyHint: true,
    idempotentHint: true,
    openWorldHint: false,
  },
  send_message: {
    readOnlyHint: false,
    idempotentHint: true,
    destructiveHint: false,
    openWorldHint: true,
  },
  send_media: {
    readOnlyHint: false,
    idempotentHint: true,
    destructiveHint: false,
    openWorldHint: true,
  },
  logout: {
    readOnlyHint: false,
    idempotentHint: true,
    destructiveHint: true,
    openWorldHint: false,
  },
  search_contacts: {
    readOnlyHint: true,
    idempotentHint: true,
    openWorldHint: true,
  },
  resolve_contact: {
    readOnlyHint: true,
    idempotentHint: true,
    openWorldHint: true,
  },
  get_contact_by_id: {
    readOnlyHint: true,
    idempotentHint: true,
    openWorldHint: false,
  },
  get_profile_pic: {
    readOnlyHint: true,
    idempotentHint: true,
    openWorldHint: true,
  },
  get_group_info: {
    readOnlyHint: true,
    idempotentHint: true,
    openWorldHint: true,
  },
  list_chats: {
    readOnlyHint: true,
    idempotentHint: true,
    openWorldHint: false,
  },
  list_system_chats: {
    readOnlyHint: true,
    idempotentHint: true,
    openWorldHint: false,
  },
  list_groups: {
    readOnlyHint: true,
    idempotentHint: true,
    openWorldHint: false,
  },
  get_chat_by_id: {
    readOnlyHint: true,
    idempotentHint: true,
    openWorldHint: false,
  },
  get_direct_chat_by_contact_number: {
    readOnlyHint: true,
    idempotentHint: true,
    openWorldHint: false,
  },
  get_chat_by_contact: {
    readOnlyHint: true,
    idempotentHint: true,
    openWorldHint: false,
  },
  analyze_group_overlaps: {
    readOnlyHint: true,
    idempotentHint: true,
    openWorldHint: true,
  },
  find_members_without_direct_chat: {
    readOnlyHint: true,
    idempotentHint: true,
    openWorldHint: true,
  },
  find_members_not_in_contacts: {
    readOnlyHint: true,
    idempotentHint: true,
    openWorldHint: true,
  },
  run_group_audit: {
    readOnlyHint: true,
    idempotentHint: true,
    openWorldHint: true,
  },
  list_messages: {
    readOnlyHint: true,
    idempotentHint: true,
    openWorldHint: false,
  },
  get_message_by_id: {
    readOnlyHint: true,
    idempotentHint: true,
    openWorldHint: false,
  },
  search_messages: {
    readOnlyHint: true,
    idempotentHint: true,
    openWorldHint: false,
  },
  get_message_context: {
    readOnlyHint: true,
    idempotentHint: true,
    openWorldHint: false,
  },
  get_last_interaction: {
    readOnlyHint: true,
    idempotentHint: true,
    openWorldHint: false,
  },
  download_media: {
    readOnlyHint: true,
    idempotentHint: true,
    openWorldHint: false,
  },
};

export class WhatsAppMcpServer {
  public readonly server: McpServer;
  private readonly whatsapp: WhatsAppService;
  private sseTransports: { [sessionId: string]: SSEServerTransport } = {};
  private httpTransports: { [sessionId: string]: StreamableHTTPServerTransport } = {};
  private browserProcessManager: BrowserProcessManager;

  constructor() {
    this.browserProcessManager = new BrowserProcessManager();
    this.whatsapp = new WhatsAppService();

    this.server = new McpServer(SERVER_INFO, {
      // Define initial capabilities if needed
      capabilities: {
        // Example: Enable logging capability
        logging: {},
      },
      instructions: 'This server provides tools to interact with WhatsApp.',
    });

    this.registerTools();
  }

  private registerTools() {
    log.info('Registering MCP tools...');
    // Call tool registration functions here
    registerAuthTools(this.server, this.whatsapp);
    registerContactTools(this.server, this.whatsapp);
    registerChatTools(this.server, this.whatsapp);
    registerMessageTools(this.server, this.whatsapp);
    registerMediaTools(this.server, this.whatsapp);

    // Remove example dummy tool if no longer needed, or keep for testing
    // this.server.tool('ping', async () => ({
    //   content: [{ type: 'text', text: 'pong' }],
    // }));
    // Let's keep ping for now for basic testing
    this.server.tool('ping', async () => ({
      content: [{ type: 'text', text: 'pong' }],
    }));

    this.overrideListToolsHandler();

    log.info('MCP tools registered.');
  }

  private overrideListToolsHandler() {
    this.server.server.setRequestHandler(ListToolsRequestSchema, () => {
      const registeredTools = (this.server as any)._registeredTools || {};
      return {
        tools: Object.entries(registeredTools)
          .filter(([, tool]: any) => tool?.enabled)
          .map(([name, tool]: [string, any]) => {
            const execution = TOOL_EXECUTION_METADATA[name];
            return {
              name,
              description: tool.description,
              inputSchema: tool.inputSchema
                ? zodToJsonSchema(tool.inputSchema, {
                    strictUnions: true,
                  })
                : {
                    type: 'object',
                    properties: {},
                  },
              ...(execution
                ? {
                    annotations: execution,
                  }
                : {}),
            };
          }),
      };
    });
  }

  async start(transportType: 'stdio' | 'sse' | 'http' = 'stdio') {
    if (transportType === 'stdio') {
      await this.startStdioTransport();
    } else if (transportType === 'sse') {
      await this.startSseTransport();
    } else {
      await this.startHttpTransport();
    }

    // Start WhatsApp initialization in the background so admin/UI can load
    log.info(`Initializing WhatsApp client...`);
    try {
      // Clean up any orphaned browser processes before starting
      await this.browserProcessManager.cleanupOrphanedProcesses();

      // Initialize the WhatsApp client without blocking server startup
      this.whatsapp.initialize()
        .then(() => {
          log.info('WhatsApp client initialized successfully.');
        })
        .catch((error) => {
          log.error({ err: error }, 'Failed to initialize WhatsApp client');
        });
    } catch (error) {
      log.error({ err: error }, 'Failed to initialize WhatsApp client');
    }
  }

  private async startStdioTransport() {
    log.info('Starting MCP server with stdio transport...');
    const stdioTransport = new StdioServerTransport();
    // Handle transport errors
    stdioTransport.onerror = (error) => {
      log.error('StdioTransport Error:', error);
    };
    await this.server.connect(stdioTransport);
    log.info('MCP server connected via stdio.');
  }

  /**
   * Gracefully shutdown the server and clean up resources
   * @returns A promise that resolves when shutdown is complete
   */
  async shutdown(): Promise<void> {
    log.info('Shutting down WhatsApp MCP Server...');
    
    try {
      // First destroy the WhatsApp client to properly close the Puppeteer browser
      // This will also unregister the browser PID
      log.info('Destroying WhatsApp client...');
      await this.whatsapp.destroy();
      log.info('WhatsApp client destroyed successfully');
      
      // Close all SSE transports if any are active
      const sessionIds = Object.keys(this.sseTransports);
      if (sessionIds.length > 0) {
        log.info(`Closing ${sessionIds.length} active SSE transports...`);
        for (const sessionId of sessionIds) {
          try {
            // Clean up the transport
            delete this.sseTransports[sessionId];
          } catch (error) {
            log.warn({ err: error }, `Error closing SSE transport ${sessionId}`);
          }
        }
      }
      
      // Final check for any orphaned processes that might have been missed
      try {
        await this.browserProcessManager.cleanupOrphanedProcesses();
      } catch (cleanupError) {
        log.warn({ err: cleanupError }, 'Error during final browser process cleanup');
        // Continue with shutdown even if cleanup fails
      }
      
      log.info('Server shutdown completed successfully');
    } catch (error) {
      log.error({ err: error }, 'Error during server shutdown');
      throw error;
    }
  }

  private async startSseTransport(port = 3001) {
    log.info(`Starting MCP server with SSE transport on port ${port}...`);
    const app = express();
    app.use(express.json({ limit: '10mb' }));

    this.registerAdminRoutes(app);

    // Endpoint for establishing SSE connection
    app.get('/sse', async (_req: Request, res: Response) => { // Prefix req with _
      log.info('SSE connection requested');
      const transport = new SSEServerTransport('/messages', res);
      this.sseTransports[transport.sessionId] = transport;

      // Handle transport errors
      transport.onerror = (error) => {
        log.error({ err: error }, `SSE Transport Error (Session ${transport.sessionId})`);
        // Clean up transport on error
        delete this.sseTransports[transport.sessionId];
      };

      res.on('close', () => {
        log.info(`SSE connection closed (Session ${transport.sessionId})`);
        delete this.sseTransports[transport.sessionId];
        // Optionally call transport.close() or server-side cleanup if needed
      });

      try {
        await this.server.connect(transport);
        log.info(`SSE transport connected (Session ${transport.sessionId})`);
      } catch (error) {
        log.error({ err: error }, `Failed to connect SSE transport (Session ${transport.sessionId})`);
        delete this.sseTransports[transport.sessionId];
        if (!res.headersSent) {
          res.status(500).send('Failed to connect MCP server');
        }
      }
    });

    // Endpoint for receiving messages from the client via POST
    app.post('/messages', express.json({ limit: '10mb' }), async (req: Request, res: Response) => { // Add types
      const sessionId = req.query.sessionId as string;
      const transport = this.sseTransports[sessionId];

      if (transport) {
        log.debug(`Received POST message for session ${sessionId}`);
        try {
          // Pass raw body if needed, or parsed body
          await transport.handlePostMessage(req, res, req.body);
          // handlePostMessage sends the response (202 Accepted or error)
        } catch (error) {
          log.error({ err: error }, `Error handling POST message for session ${sessionId}`);
          // Ensure response is sent if handlePostMessage failed before sending
          if (!res.headersSent) {
             res.status(500).send('Error processing message');
          }
        }
      } else {
        log.warn(`No active SSE transport found for sessionId: ${sessionId}`);
        res.status(400).send('No active SSE transport found for this session ID');
      }
    });

    return new Promise<void>((resolve, reject) => {
      const serverInstance = app.listen(port, () => {
        log.info(`SSE server listening on http://localhost:${port}`);
        resolve();
      });

      serverInstance.on('error', (error: Error) => { // Add type
        log.error('SSE server failed to start:', error);
        reject(error);
      });
    });
  }

  private async startHttpTransport(port = 3001) {
    log.info(`Starting MCP server with Streamable HTTP transport on port ${port}...`);
    const app = express();
    app.use(express.json({ limit: '10mb' }));

    this.registerAdminRoutes(app);

    // Handle POST requests for client-to-server communication
    app.post('/mcp', async (req: Request, res: Response) => {
      // Check for existing session ID
      const sessionId = req.headers['mcp-session-id'] as string | undefined;
      let transport: StreamableHTTPServerTransport;

      if (sessionId && this.httpTransports[sessionId]) {
        transport = this.httpTransports[sessionId];
      } else if (sessionId && !this.httpTransports[sessionId]) {
        res.status(400).json({
          jsonrpc: '2.0',
          error: {
            code: -32000,
            message: 'Bad Request: Invalid session ID',
          },
          id: req.body?.id ?? null,
        });
        return;
      } else if (!sessionId && isInitializeRequest(req.body)) {
        transport = new StreamableHTTPServerTransport({
          sessionIdGenerator: () => randomUUID(),
          onsessioninitialized: (newSessionId: string) => {
            this.httpTransports[newSessionId] = transport;
          },
        });

        transport.onclose = () => {
          if (transport.sessionId) {
            delete this.httpTransports[transport.sessionId];
          }
        };

        await this.server.connect(transport);
      } else {
        res.status(400).json({
          jsonrpc: '2.0',
          error: {
            code: -32000,
            message: 'Bad Request: No valid session ID provided',
          },
          id: null,
        });
        return;
      }

      await transport.handleRequest(req, res, req.body);
    });

    const handleSessionRequest = async (req: Request, res: Response): Promise<void> => {
      const sessionId = req.headers['mcp-session-id'] as string | undefined;
      if (!sessionId || !this.httpTransports[sessionId]) {
        res.status(400).send('Invalid or missing session ID');
        return;
      }
      const transport = this.httpTransports[sessionId];
      await transport.handleRequest(req, res);
    };

    // Handle GET requests for server-to-client notifications via SSE
    app.get('/mcp', handleSessionRequest);
    // Handle DELETE requests for session termination
    app.delete('/mcp', handleSessionRequest);

    return new Promise<void>((resolve, reject) => {
      const serverInstance = app.listen(port, () => {
        log.info(`Streamable HTTP server listening on http://localhost:${port}/mcp`);
        resolve();
      });

      serverInstance.on('error', (error: Error) => {
        log.error('Streamable HTTP server failed to start:', error);
        reject(error);
      });
    });
  }

  private registerAdminRoutes(app: express.Express) {
    registerAdminRoutes(app, this.whatsapp);
  }

  // Add methods for registering specific tool groups if needed
}
