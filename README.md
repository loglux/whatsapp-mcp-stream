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

## Admin UI

![Admin UI](screenshots/wamcpstream.png)
*Admin console with runtime settings, QR linking, export, and status.*

Supported settings:

- `media_public_base_url`
- `upload_max_mb`
- `upload_enabled`
- `max_files_per_upload`
- `require_upload_token`
- `upload_token`
- `auto_download_media`
- `auto_download_max_mb`

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

Note: clients must send `Accept: application/json, text/event-stream` on `initialize`.

## Smoke Test

Quick regression smoke for MCP tools:

```bash
npm run smoke:mcp
```

Optional custom target:

```bash
MCP_BASE_URL=http://localhost:3003 npm run smoke:mcp
```

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
| `resolve_contact` | Resolve a contact by name or phone number (best matches). |
| `get_contact_by_id` | Get contact details by JID. |
| `get_profile_pic` | Get profile picture URL for a JID. |
| `get_group_info` | Get group metadata and participants by group JID. |

### Chats

| Tool | Description |
| --- | --- |
| `list_chats` | List chats with metadata and optional last message. |
| `get_chat_by_id` | Get chat metadata by JID. |
| `list_groups` | List group chats only. |
| `get_direct_chat_by_contact_number` | Resolve a direct chat JID by phone number. |
| `get_chat_by_contact` | Resolve a contact by name or phone number and return chat metadata. |
| `analyze_group_overlaps` | Find members that appear across multiple groups. |
| `find_members_without_direct_chat` | Find group members with no direct chat. |
| `find_members_not_in_contacts` | Find group members missing from contacts. |
| `run_group_audit` | Run combined group audit as one routine operation. |

### Messages

| Tool | Description |
| --- | --- |
| `list_messages` | Get messages from a specific chat. |
| `search_messages` | Search messages by text (optionally scoped to a chat). |
| `get_message_by_id` | Get a specific message by ID (`jid:id`). |
| `get_message_context` | Get recent messages around a specific message. |
| `get_last_interaction` | Get the most recent message for a JID. |
| `send_message` | Send a text message to a person or group. Supports optional `idempotency_key`. |

### Media

| Tool | Description |
| --- | --- |
| `send_media` | Send media (image/video/document/audio). Supports optional `idempotency_key`. |
| `download_media` | Download media from a message. |

### Utility

| Tool | Description |
| --- | --- |
| `ping` | Health check tool. |

## Recovery Notes

This service contains an intentional recovery workaround for Baileys/WhatsApp session-state corruption.

Why it exists:

- In production we observed cases where the container stayed alive and MCP still answered, but the WhatsApp session was functionally broken.
- The most common indicators were Baileys errors like `failed to find key ... to decode mutation` and `failed to sync state from version`.
- In that state, a manual container restart often restored service.

Current behavior:

- On app-state corruption signals, the service first tries a soft recovery with `forceResync()`.
- If the same class of failure repeats within a time window, it escalates to an internal WhatsApp client restart.
- A dedicated `/healthz` endpoint reports `503` only when the service is genuinely stuck outside the allowed recovery window.
- Docker health checks use `/healthz`, so the container is restarted only after in-process recovery has had a chance to work.

This is a pragmatic workaround, not a protocol-level fix. It reduces operator intervention but does not replace upgrading or replacing Baileys when upstream behavior changes.
This workaround may be removed after migrating to a newer Baileys implementation or after confirming that the underlying session-state corruption issue is resolved.

## License

MIT

## Persistence

Chats and messages are persisted to a local SQLite database stored in the session volume.

Environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `DB_PATH` | `<SESSION_DIR>/store.sqlite` | SQLite database path for chats/messages persistence. |
| `WA_EVENT_LOG` | `0` | Enable detailed WhatsApp event logs. |
| `WA_EVENT_STREAM` | `0` | Write raw Baileys event stream to a file for deep debugging. |
| `WA_EVENT_STREAM_PATH` | `/app/logs/wa-events.log` | File path for the event stream log. |
| `WA_RESYNC_RECONNECT` | `1` | Enable reconnect safety net after force resync. |
| `WA_RESYNC_RECONNECT_DELAY_MS` | `15000` | Delay before reconnect after force resync (ms). |
| `WA_SYNC_RECOVERY_COOLDOWN_MS` | `300000` | Minimum delay between automatic app-state recoveries. |
| `WA_SYNC_RECOVERY_WINDOW_MS` | `900000` | Time window used to count repeated app-state corruption failures. |
| `WA_SYNC_SOFT_RECOVERY_LIMIT` | `2` | Number of soft recoveries before escalating to an internal restart. |
| `WA_READINESS_GRACE_MS` | `180000` | Grace period during recovery/disconnect before `/healthz` turns unhealthy. |
| `WA_SEND_DEDUP_WINDOW_MS` | `45000` | Suppress exact duplicate `send_message` requests to the same JID within this window. |
| `WA_IDEMPOTENCY_TTL_MS` | `86400000` | How long completed `send_message` idempotency records are retained in SQLite for safe retries. |
| `WA_MESSAGE_INDEX_MAX` | `20000` | Max in-memory entries for message index (`jid:id` -> raw message). |
| `WA_MESSAGE_KEY_INDEX_MAX` | `20000` | Max in-memory entries for message key index (`id` -> raw message). |

## Export

Export a chat (JSON + optional downloaded media) via:

`GET /api/export/chat/:jid?include_media=true`

If `include_media=true`, the ZIP includes files already downloaded via `download_media`. It does not fetch missing media from WhatsApp.
