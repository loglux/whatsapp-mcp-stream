import { z } from "zod";
import { WhatsAppService } from "../services/whatsapp.js";
import qrcode from "qrcode";
import { log } from "../utils/logger.js";
import { CallToolResult } from "@modelcontextprotocol/sdk/types.js";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import crypto from "crypto";

/**
 * Register authentication-related tools with the MCP server
 * @param server The MCP server instance
 * @param whatsappService The WhatsApp service instance
 */
export function registerAuthTools(
  server: McpServer,
  whatsappService: WhatsAppService,
): void {
  log.info("Registering authentication tools...");

  server.tool(
    "get_qr_code",
    "Get the latest WhatsApp QR code as an image for authentication",
    {},
    async (): Promise<CallToolResult> => {
      return await getQrCodeImage(whatsappService);
    },
  );

  server.tool(
    "check_auth_status",
    "Check if the WhatsApp client is authenticated and ready",
    {},
    async (): Promise<CallToolResult> => {
      return await checkAuthStatus(whatsappService);
    },
  );

  server.tool(
    "logout",
    "Logout from WhatsApp and clear the current session",
    {
      idempotency_key: z
        .string()
        .min(1)
        .max(200)
        .optional()
        .describe(
          "Optional idempotency key. Repeating the same logout request with the same key returns the original result instead of logging out again.",
        ),
    },
    async ({ idempotency_key }): Promise<CallToolResult> => {
      return await logoutFromWhatsApp(whatsappService, idempotency_key);
    },
  );

  server.tool(
    "force_resync",
    "Force a full WhatsApp resync (clears app state and reconnects).",
    {},
    async (): Promise<CallToolResult> => {
      return await forceResync(whatsappService);
    },
  );

  log.info("Authentication tools registered.");
}

async function forceResync(
  whatsappService: WhatsAppService,
): Promise<CallToolResult> {
  try {
    await whatsappService.forceResync();
    return {
      content: [
        {
          type: "text",
          text: "Resync started. WhatsApp will reconnect and replay history.",
        },
      ],
      isError: false,
    };
  } catch (error) {
    log.error("Error forcing resync:", error);
    return {
      content: [
        {
          type: "text",
          text: `Error forcing resync: ${error instanceof Error ? error.message : String(error)}`,
        },
      ],
      isError: true,
    };
  }
}

/**
 * Tool to logout from WhatsApp
 * @param whatsappService The WhatsApp service instance
 * @returns A promise that resolves to the tool result containing the logout status
 */
async function logoutFromWhatsApp(
  whatsappService: WhatsAppService,
  idempotencyKey?: string,
): Promise<CallToolResult> {
  try {
    return await whatsappService.executeIdempotentOperation(
      "logout",
      crypto
        .createHash("sha256")
        .update(
          JSON.stringify({
            authenticated: whatsappService.isAuthenticated(),
          }),
        )
        .digest("hex"),
      async () => {
        if (!whatsappService.isAuthenticated()) {
          log.info("Logout requested but client is not authenticated");
          return {
            content: [
              {
                type: "text",
                text: "You are not currently authenticated with WhatsApp, so there is no need to logout.",
              },
            ],
            isError: false,
          } satisfies CallToolResult;
        }

        await whatsappService.logout();
        await whatsappService.initialize();

        log.info("Successfully logged out and reinitialized WhatsApp client");

        return {
          content: [
            {
              type: "text",
              text: "Successfully logged out of WhatsApp. You can now use the get_qr_code tool to authenticate with a new session.",
            },
          ],
          isError: false,
        } satisfies CallToolResult;
      },
      { idempotencyKey, scopeJid: "auth:logout" },
    );
  } catch (error) {
    log.error("Error logging out from WhatsApp:", error);
    return {
      content: [
        {
          type: "text",
          text: `Error logging out: ${error instanceof Error ? error.message : String(error)}`,
        },
      ],
      isError: true,
    };
  }
}

/**
 * Tool to check the authentication status of the WhatsApp client
 * @param whatsappService The WhatsApp service instance
 * @returns A promise that resolves to the tool result containing the authentication status
 */
async function checkAuthStatus(
  whatsappService: WhatsAppService,
): Promise<CallToolResult> {
  try {
    const isAuthenticated = whatsappService.isAuthenticated();
    const isReady = whatsappService.isReady();

    log.info(
      `Auth status checked: ${isAuthenticated ? "authenticated" : "not authenticated"}, ` +
        `ready: ${isReady ? "yes" : "no"}`,
    );

    return {
      content: [
        {
          type: "text",
          text: !isAuthenticated
            ? "You are not authenticated. Use get_qr_code to authenticate."
            : isReady
              ? "Authenticated and ready."
              : "Authenticated, but not ready yet. Please wait a few seconds and try again.",
        },
      ],
      isError: false,
    };
  } catch (error) {
    log.error("Error checking authentication status:", error);
    return {
      content: [
        {
          type: "text",
          text: `Error checking authentication status: ${error instanceof Error ? error.message : String(error)}`,
        },
      ],
      isError: true,
    };
  }
}

/**
 * Tool to get the latest WhatsApp QR code as an image
 * @param whatsappService The WhatsApp service instance
 * @returns A promise that resolves to the tool result containing the QR code image
 */
async function getQrCodeImage(
  whatsappService: WhatsAppService,
): Promise<CallToolResult> {
  try {
    const qrString = whatsappService.getLatestQrCode();

    // Check if the client is already authenticated
    if (whatsappService.isAuthenticated()) {
      log.info("Client is already authenticated, no QR code needed");
      return {
        content: [
          {
            type: "text",
            text: "You are already authenticated with WhatsApp. No QR code is needed.",
          },
        ],
        isError: false,
      };
    } else if (!qrString) {
      log.info("No QR code available yet, client may be initializing");
      return {
        content: [
          {
            type: "text",
            text: "No QR code is currently available. The WhatsApp client may still be initializing. Please try again in a few seconds.",
          },
        ],
        isError: false,
      };
    }

    // Generate QR code as data URL
    const qrDataUrl = await qrcode.toDataURL(qrString);

    // Extract the base64 data from the data URL
    // Data URL format: data:image/png;base64,BASE64_DATA
    const base64Data = qrDataUrl.split(",")[1];

    log.info("QR code image generated successfully");

    return {
      content: [
        {
          type: "image",
          data: base64Data,
          mimeType: "image/png",
        },
      ],
      isError: false,
    };
  } catch (error) {
    log.error("Error generating QR code image:", error);
    return {
      content: [
        {
          type: "text",
          text: `Error generating QR code: ${error instanceof Error ? error.message : String(error)}`,
        },
      ],
      isError: true,
    };
  }
}
