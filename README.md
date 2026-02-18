# WhatsApp MCP Stream

[![CI](https://github.com/loglux/whatsapp-mcp-stream/actions/workflows/ci.yml/badge.svg)](https://github.com/loglux/whatsapp-mcp-stream/actions/workflows/ci.yml)

A WhatsApp MCP server built around **Streamable HTTP** transport, using **Baileys** for WhatsApp connectivity, with a web admin UI and bidirectional media flow (upload + download).

Key points:

- Transport: Streamable HTTP at `/mcp`
- Engine: Baileys
- Admin UI: QR, status, logout, runtime settings
- Media: upload endpoints + `/media` hosting + MCP download tool

## Quick Start (Docker)

```bash
# build and run

docker compose build

docker compose up -d
```

The server will be available at:

- Admin UI: `http://localhost:3003/admin`
- MCP endpoint: `http://localhost:3003/mcp`
- Media files: `http://localhost:3003/media/<filename>`

## Runtime Settings

Settings can be edited in the admin UI and are persisted to `SETTINGS_PATH` (defaults to `MEDIA_DIR/settings.json`).

Supported settings:

- `media_public_base_url`
- `upload_max_mb`
- `upload_enabled`
- `max_files_per_upload`
- `require_upload_token`
- `upload_token`

## Authentication

Built-in authentication is not implemented yet. In production, use a gateway that enforces auth. This project works well behind `authmcp-gateway`:

```text
https://github.com/loglux/authmcp-gateway
```

## Media Upload API

Base64 JSON:

```bash
curl -X POST http://localhost:3003/api/upload \
  -H "Content-Type: application/json" \
  -d {filename:photo.jpg,mime_type:image/jpeg,data:<base64>}
```

Multipart (recommended for large files):

```bash
curl -X POST http://localhost:3003/api/upload-multipart \
  -F "file=@/path/to/file.jpg"
```

Both return `url` and (if configured) `publicUrl`.

### Upload Auth (Optional)

If `require_upload_token=true`, provide a token with either:

- `x-upload-token: <token>`
- `Authorization: Bearer <token>`

## MCP Transport

The server exposes Streamable HTTP at `/mcp`.

Typical flow:

1. `POST /mcp` with JSON-RPC `initialize`
2. Use the returned `mcp-session-id` header for subsequent requests
3. `POST /mcp` for tool calls

## MCP Tools

### Auth

| Tool | Description |
| --- | --- |
| `get_qr_code` | Get the latest WhatsApp QR code as an image for authentication. |
| `check_auth_status` | Check if the WhatsApp client is authenticated and ready. |
| `logout` | Logout from WhatsApp and clear the current session. |

### Contacts

| Tool | Description |
| --- | --- |
| `search_contacts` | Search contacts by name or phone number. |
| `get_contact_by_id` | Get contact details by JID. |

### Chats

| Tool | Description |
| --- | --- |
| `list_chats` | List chats with metadata and optional last message. |
| `get_chat_by_id` | Get chat metadata by JID. |
| `list_groups` | List group chats only. |
| `get_direct_chat_by_contact_number` | Resolve a direct chat JID by phone number. |

### Messages

| Tool | Description |
| --- | --- |
| `list_messages` | Get messages from a specific chat. |
| `get_message_by_id` | Get a specific message by ID (`jid:id`). |
| `get_message_context` | Get recent messages around a specific message. |
| `get_last_interaction` | Get the most recent message for a JID. |
| `send_message` | Send a text message to a person or group. |

### Media

| Tool | Description |
| --- | --- |
| `send_media` | Send media (image/video/document/audio). |
| `download_media` | Download media from a message. |

### Utility

| Tool | Description |
| --- | --- |
| `ping` | Health check tool. |

## License

MIT
