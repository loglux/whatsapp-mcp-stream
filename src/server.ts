import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { SSEServerTransport } from '@modelcontextprotocol/sdk/server/sse.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { Implementation } from '@modelcontextprotocol/sdk/types.js';
import { isInitializeRequest } from '@modelcontextprotocol/sdk/types.js';
import express, { Request, Response } from 'express'; // Import Request and Response
import fs from 'fs';
// import { z } from 'zod'; // z is unused currently
import { WhatsAppService } from './services/whatsapp.js';
import { log } from './utils/logger.js';
import { BrowserProcessManager } from './utils/browser-process-manager.js';
import qrcode from 'qrcode';
import path from 'path';
import { fileURLToPath } from 'url';
import { createRequire } from 'module';
import { randomUUID } from 'node:crypto';
import multer from 'multer';
const require = createRequire(import.meta.url);
const archiver = require('archiver');
// Import tool registration functions
import { registerContactTools } from './tools/contacts.js';
import { registerChatTools } from './tools/chats.js';
import { registerMessageTools } from './tools/messages.js';
import { registerMediaTools } from './tools/media.js';
import { registerAuthTools } from './tools/auth.js';

const SERVER_INFO: Implementation = {
  name: 'whatsapp-mcp-stream',
  version: '1.0.0', // Consider reading from package.json
};

const __dirname = path.dirname(fileURLToPath(import.meta.url));

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

    log.info('MCP tools registered.');
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
    const mediaDir = process.env.MEDIA_DIR || path.join(process.cwd(), 'media');
    const settingsPath = process.env.SETTINGS_PATH || path.join(mediaDir, 'settings.json');
    type Settings = {
      media_public_base_url?: string;
      upload_max_mb?: number;
      upload_enabled?: boolean;
      max_files_per_upload?: number;
      require_upload_token?: boolean;
      upload_token?: string;
    };
    try {
      if (!fs.existsSync(mediaDir)) {
        fs.mkdirSync(mediaDir, { recursive: true });
      }
    } catch (error) {
      log.warn({ err: error }, 'Failed to ensure media directory');
    }

    app.use('/media', express.static(mediaDir));

    const loadSettings = (): Settings => {
      try {
        if (fs.existsSync(settingsPath)) {
          const raw = fs.readFileSync(settingsPath, 'utf-8');
          const parsed = JSON.parse(raw);
          if (parsed && typeof parsed === 'object') {
            return parsed;
          }
        }
      } catch (error) {
        log.warn({ err: error }, 'Failed to load settings');
      }
      return {};
    };

    const saveSettings = (settings: Settings) => {
      try {
        fs.writeFileSync(settingsPath, JSON.stringify(settings, null, 2));
      } catch (error) {
        log.warn({ err: error }, 'Failed to save settings');
      }
    };

    const readEnvDefaults = (): Settings => ({
      media_public_base_url: process.env.MEDIA_PUBLIC_BASE_URL || process.env.PUBLIC_BASE_URL || '',
      upload_max_mb: process.env.UPLOAD_MAX_MB ? Number(process.env.UPLOAD_MAX_MB) : undefined,
      upload_enabled: process.env.UPLOAD_ENABLED ? process.env.UPLOAD_ENABLED === 'true' : undefined,
      max_files_per_upload: process.env.UPLOAD_MAX_FILES ? Number(process.env.UPLOAD_MAX_FILES) : undefined,
      require_upload_token: process.env.REQUIRE_UPLOAD_TOKEN
        ? process.env.REQUIRE_UPLOAD_TOKEN === 'true'
        : undefined,
      upload_token: process.env.UPLOAD_TOKEN || undefined,
    });

    const normalizeSettings = (settings: Settings): Settings => {
      const normalized: Settings = {};
      if (typeof settings.media_public_base_url === 'string') {
        normalized.media_public_base_url = settings.media_public_base_url.trim().replace(/\/$/, '');
      }
      if (typeof settings.upload_max_mb === 'number' && Number.isFinite(settings.upload_max_mb)) {
        normalized.upload_max_mb = settings.upload_max_mb;
      }
      if (typeof settings.upload_enabled === 'boolean') {
        normalized.upload_enabled = settings.upload_enabled;
      }
      if (typeof settings.max_files_per_upload === 'number' && Number.isFinite(settings.max_files_per_upload)) {
        normalized.max_files_per_upload = settings.max_files_per_upload;
      }
      if (typeof settings.require_upload_token === 'boolean') {
        normalized.require_upload_token = settings.require_upload_token;
      }
      if (typeof settings.upload_token === 'string') {
        normalized.upload_token = settings.upload_token;
      }
      return normalized;
    };

    let runtimeSettings: Settings = {};

    const applySettings = (settings: Settings) => {
      runtimeSettings = settings;
      if (typeof settings.media_public_base_url === 'string') {
        process.env.MEDIA_PUBLIC_BASE_URL = settings.media_public_base_url;
      }
    };

    const mergedSettings = () => {
      const envDefaults = readEnvDefaults();
      const persisted = loadSettings();
      return normalizeSettings({ ...envDefaults, ...persisted });
    };

    applySettings(mergedSettings());

    // Admin dashboard routes
    app.get('/', (_req: Request, res: Response) => {
      res.redirect('/admin');
    });

    app.get('/admin', (_req: Request, res: Response) => {
      try {
        const adminPath = path.join(__dirname, '..', 'admin.html');
        res.sendFile(adminPath);
      } catch (error) {
        log.error('Error serving admin page:', error);
        res.status(500).send('Error loading admin page');
      }
    });

    // API endpoints for admin panel
    app.get('/api/status', (_req: Request, res: Response) => {
      try {
        const isAuthenticated = this.whatsapp.isAuthenticated();
        const isReady = this.whatsapp.isReady();
        const hasQr = Boolean(this.whatsapp.getLatestQrCode());
        const syncStats = this.whatsapp.getSyncStats();
        const dbStats = this.whatsapp.getMessageStoreStats();
        res.json({
          authenticated: isAuthenticated,
          ready: isReady,
          qrAvailable: hasQr,
          chatCount: syncStats.chatCount,
          messageCount: syncStats.messageCount,
          lastHistorySyncAt: syncStats.lastHistorySyncAt,
          lastChatsSyncAt: syncStats.lastChatsSyncAt,
          lastMessagesSyncAt: syncStats.lastMessagesSyncAt,
          warmupAttempts: syncStats.warmupAttempts,
          warmupInProgress: syncStats.warmupInProgress,
          dbChats: dbStats?.chats ?? 0,
          dbMessages: dbStats?.messages ?? 0,
          dbMedia: dbStats?.media ?? 0,
          timestamp: new Date().toISOString()
        });
      } catch (error) {
        log.error('Error getting status:', error);
        res.status(500).json({ error: 'Failed to get status' });
      }
    });

    app.get('/api/settings', (_req: Request, res: Response) => {
      const current = mergedSettings();
      res.json({
        media_public_base_url: current.media_public_base_url || '',
        upload_max_mb: current.upload_max_mb ?? 50,
        upload_enabled: current.upload_enabled ?? true,
        max_files_per_upload: current.max_files_per_upload ?? 1,
        require_upload_token: current.require_upload_token ?? false,
        upload_token: current.upload_token || '',
      });
    });

    app.post('/api/settings', express.json({ limit: '1mb' }), async (req: Request, res: Response) => {
      const body = req.body || {};
      const updates: Settings = {};

      if (body.media_public_base_url !== undefined && typeof body.media_public_base_url !== 'string') {
        res.status(400).json({ error: 'media_public_base_url must be a string' });
        return;
      }
      if (body.media_public_base_url !== undefined) {
        updates.media_public_base_url = body.media_public_base_url;
      }

      if (body.upload_max_mb !== undefined && typeof body.upload_max_mb !== 'number') {
        res.status(400).json({ error: 'upload_max_mb must be a number' });
        return;
      }
      if (body.upload_max_mb !== undefined) {
        updates.upload_max_mb = body.upload_max_mb;
      }

      if (body.upload_enabled !== undefined && typeof body.upload_enabled !== 'boolean') {
        res.status(400).json({ error: 'upload_enabled must be a boolean' });
        return;
      }
      if (body.upload_enabled !== undefined) {
        updates.upload_enabled = body.upload_enabled;
      }

      if (body.max_files_per_upload !== undefined && typeof body.max_files_per_upload !== 'number') {
        res.status(400).json({ error: 'max_files_per_upload must be a number' });
        return;
      }
      if (body.max_files_per_upload !== undefined) {
        updates.max_files_per_upload = body.max_files_per_upload;
      }

      if (body.require_upload_token !== undefined && typeof body.require_upload_token !== 'boolean') {
        res.status(400).json({ error: 'require_upload_token must be a boolean' });
        return;
      }
      if (body.require_upload_token !== undefined) {
        updates.require_upload_token = body.require_upload_token;
      }

      if (body.upload_token !== undefined && typeof body.upload_token !== 'string') {
        res.status(400).json({ error: 'upload_token must be a string' });
        return;
      }
      if (body.upload_token !== undefined) {
        updates.upload_token = body.upload_token;
      }

      const current = mergedSettings();
      const next = normalizeSettings({ ...current, ...updates });
      saveSettings(next);
      applySettings(next);
      res.json({ success: true, settings: next });
    });

    app.get('/api/qr', async (_req: Request, res: Response) => {
      try {
        const qrString = this.whatsapp.getLatestQrCode();

        if (!qrString) {
          if (this.whatsapp.isAuthenticated()) {
            res.status(204).end();
            return;
          }
          res.status(204).end();
          return;
        }

        // Generate QR code as PNG
        const qrDataUrl = await qrcode.toDataURL(qrString, { type: 'image/png' });
        const base64Data = qrDataUrl.split(',')[1];
        const buffer = Buffer.from(base64Data, 'base64');

        res.setHeader('Content-Type', 'image/png');
        res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
        res.setHeader('Pragma', 'no-cache');
        res.setHeader('Expires', '0');
        res.send(buffer);

      } catch (error) {
        log.error('Error generating QR code image:', error);
        res.status(500).json({ error: 'Failed to generate QR code' });
      }
    });

    app.post('/api/warmup', async (_req: Request, res: Response) => {
      try {
        const result = await this.whatsapp.runWarmup();
        res.json({ success: true, ...result });
      } catch (error) {
        log.error('Error running warmup:', error);
        res.status(500).json({ error: 'Failed to run warmup' });
      }
    });

    app.post('/api/force-resync', async (_req: Request, res: Response) => {
      try {
        await this.whatsapp.forceResync();
        res.json({ success: true });
      } catch (error) {
        log.error('Error forcing resync:', error);
        res.status(500).json({ error: 'Failed to force resync' });
      }
    });

    app.get('/api/export/chat/:jid', async (req: Request, res: Response) => {
      try {
        const jid = String(req.params.jid || '').trim();
        if (!jid) {
          res.status(400).json({ error: 'Missing chat JID' });
          return;
        }
        const includeMedia = String(req.query.include_media || '').toLowerCase() === 'true';
        const { chat, messages, media } = await this.whatsapp.exportChat(jid, includeMedia);
        if (!chat && messages.length === 0) {
          res.status(404).json({ error: 'Chat not found in database' });
          return;
        }

        const safeJid = jid.replace(/[^a-zA-Z0-9._-]/g, '_');
        res.setHeader('Content-Type', 'application/zip');
        res.setHeader('Content-Disposition', `attachment; filename="chat_${safeJid}.zip"`);

        const archive = archiver('zip', { zlib: { level: 9 } });
        archive.on('error', (err: Error) => {
          log.error({ err }, 'Export archive error');
          try {
            res.status(500).end();
          } catch {
            // ignore
          }
        });

        archive.pipe(res);

        const publicBase = (runtimeSettings.media_public_base_url || process.env.MEDIA_PUBLIC_BASE_URL || process.env.PUBLIC_BASE_URL || '').replace(/\/$/, '');
        const mediaEntries = media.map((m) => ({
          message_id: m.message_id,
          filename: m.filename,
          mimetype: m.mimetype,
          size: m.size,
          url: publicBase ? `${publicBase}/media/${m.filename}` : `/media/${m.filename}`,
        }));

        archive.append(JSON.stringify({ chat, messages, media: mediaEntries }, null, 2), { name: 'chat.json' });

        if (includeMedia) {
          for (const m of media) {
            if (fs.existsSync(m.file_path)) {
              archive.file(m.file_path, { name: path.join('media', m.filename) });
            }
          }
        }

        await archive.finalize();
      } catch (error) {
        log.error('Error exporting chat:', error);
        res.status(500).json({ error: 'Failed to export chat' });
      }
    });

    const buildPublicUrl = (urlPath: string) => {
      const publicBase = (runtimeSettings.media_public_base_url || process.env.MEDIA_PUBLIC_BASE_URL || process.env.PUBLIC_BASE_URL || '').replace(/\/$/, '');
      return publicBase ? `${publicBase}${urlPath}` : undefined;
    };

    const getUploadMaxBytes = () => {
      const maxMb = runtimeSettings.upload_max_mb ?? (process.env.UPLOAD_MAX_MB ? Number(process.env.UPLOAD_MAX_MB) : 50);
      return Math.max(1, maxMb || 50) * 1024 * 1024;
    };

    const getMaxFilesPerUpload = () => {
      const maxFiles = runtimeSettings.max_files_per_upload ?? (process.env.UPLOAD_MAX_FILES ? Number(process.env.UPLOAD_MAX_FILES) : 1);
      return Math.max(1, maxFiles || 1);
    };

    const checkUploadAuth = (req: Request): string | null => {
      const uploadEnabled = runtimeSettings.upload_enabled ?? (process.env.UPLOAD_ENABLED ? process.env.UPLOAD_ENABLED === 'true' : true);
      if (!uploadEnabled) {
        return 'Upload is disabled';
      }

      const requireToken = runtimeSettings.require_upload_token ?? (process.env.REQUIRE_UPLOAD_TOKEN === 'true');
      if (!requireToken) return null;

      const configuredToken = runtimeSettings.upload_token || process.env.UPLOAD_TOKEN;
      if (!configuredToken) {
        return 'Upload token not configured';
      }

      const headerToken = Array.isArray(req.headers['x-upload-token'])
        ? req.headers['x-upload-token'][0]
        : (req.headers['x-upload-token'] as string | undefined);
      const auth = req.headers.authorization;
      const bearer = auth && auth.startsWith('Bearer ') ? auth.slice(7) : undefined;
      const provided = bearer || headerToken;

      if (!provided || provided !== configuredToken) {
        return 'Invalid or missing upload token';
      }
      return null;
    };

    app.post('/api/upload', express.json({ limit: '50mb' }), async (req: Request, res: Response) => {
      try {
        const authError = checkUploadAuth(req);
        if (authError) {
          res.status(401).json({ error: authError });
          return;
        }

        const { data, filename, mime_type } = req.body || {};
        if (!data || typeof data !== 'string') {
          res.status(400).json({ error: 'Missing data (base64 string expected)' });
          return;
        }

        let base64 = data;
        let inferredMime = mime_type as string | undefined;

        const dataUrlMatch = data.match(/^data:([^;]+);base64,(.*)$/);
        if (dataUrlMatch) {
          inferredMime = inferredMime || dataUrlMatch[1];
          base64 = dataUrlMatch[2];
        }

        const buffer = Buffer.from(base64, 'base64');
        if (!buffer.length) {
          res.status(400).json({ error: 'Invalid base64 data' });
          return;
        }
        if (buffer.length > getUploadMaxBytes()) {
          res.status(413).json({ error: 'File too large' });
          return;
        }

        const originalExt = filename ? path.extname(filename).replace('.', '') : '';
        const mimeExtMap: Record<string, string> = {
          'audio/ogg': 'ogg',
          'audio/opus': 'opus',
          'audio/mpeg': 'mp3',
          'audio/mp4': 'm4a',
          'video/mp4': 'mp4',
          'image/jpeg': 'jpg',
          'image/png': 'png',
          'image/webp': 'webp',
          'application/pdf': 'pdf',
        };
        const mimeExt = inferredMime ? (mimeExtMap[inferredMime] || inferredMime.split('/')[1]?.split(';')[0]) : '';
        const ext = (originalExt || mimeExt || 'bin').replace(/[^a-zA-Z0-9]/g, '') || 'bin';

        const safeBase = filename
          ? path.basename(filename).replace(/[^a-zA-Z0-9._-]/g, '_').replace(/\.[^/.]+$/, '')
          : `upload_${randomUUID()}`;
        const savedFilename = `${safeBase}_${Date.now()}.${ext}`;
        const filePath = path.join(mediaDir, savedFilename);

        fs.writeFileSync(filePath, buffer);

        const urlPath = `/media/${savedFilename}`;
        const publicUrl = buildPublicUrl(urlPath);

        res.json({
          filename: savedFilename,
          originalFilename: filename,
          mimetype: inferredMime || 'application/octet-stream',
          size: buffer.length,
          savedPath: filePath,
          url: urlPath,
          publicUrl,
        });
      } catch (error) {
        log.error({ err: error }, 'Error uploading media');
        res.status(500).json({ error: 'Failed to upload media' });
      }
    });

    const storage = multer.diskStorage({
      destination: (_req: Request, _file: Express.Multer.File, cb: (error: Error | null, destination: string) => void) => cb(null, mediaDir),
      filename: (_req: Request, file: Express.Multer.File, cb: (error: Error | null, filename: string) => void) => {
        const base = path.basename(file.originalname || 'upload').replace(/[^a-zA-Z0-9._-]/g, '_').replace(/\.[^/.]+$/, '');
        const ext = path.extname(file.originalname || '').replace(/[^a-zA-Z0-9.]/g, '') || '';
        const safeExt = ext.startsWith('.') ? ext : ext ? `.${ext}` : '';
        const savedFilename = `${base}_${Date.now()}_${randomUUID()}${safeExt}`;
        cb(null, savedFilename);
      },
    });

    app.post('/api/upload-multipart', (req: Request, res: Response) => {
      const authError = checkUploadAuth(req);
      if (authError) {
        res.status(401).json({ error: authError });
        return;
      }

      const upload = multer({ storage, limits: { fileSize: getUploadMaxBytes() } }).any();
      upload(req, res, (err: any) => {
        if (err) {
          const status = err?.code === 'LIMIT_FILE_SIZE' ? 413 : 400;
          res.status(status).json({ error: err.message || 'Upload failed' });
          return;
        }
        const files = ((req as any).files as Express.Multer.File[] | undefined) || [];
        if (!files.length) {
          res.status(400).json({ error: 'Missing file field' });
          return;
        }
        const maxFiles = getMaxFilesPerUpload();
        if (files.length > maxFiles) {
          for (const file of files) {
            try {
              fs.unlinkSync(file.path);
            } catch {
              // best-effort cleanup
            }
          }
          res.status(400).json({ error: `Too many files. Max allowed is ${maxFiles}` });
          return;
        }

        const mapped = files.map((file) => {
          const urlPath = `/media/${file.filename}`;
          return {
            filename: file.filename,
            originalFilename: file.originalname,
            mimetype: file.mimetype,
            size: file.size,
            savedPath: file.path,
            url: urlPath,
            publicUrl: buildPublicUrl(urlPath),
          };
        });

        res.json({
          file: mapped[0],
          files: mapped,
        });
      });
    });

    app.post('/api/logout', async (_req: Request, res: Response) => {
      try {
        if (!this.whatsapp.isAuthenticated()) {
          res.status(400).json({ error: 'Not authenticated' });
          return;
        }

        // Logout and re-initialize to force a new QR code
        await this.whatsapp.logout();
        await this.whatsapp.initialize();

        res.json({ success: true, message: 'Logged out successfully. New QR code should be available shortly.' });
      } catch (error) {
        log.error('Error during logout:', error);
        res.status(500).json({ error: 'Failed to logout' });
      }
    });
  }

  // Add methods for registering specific tool groups if needed
}
