#!/usr/bin/env node

const baseUrl = process.env.MCP_BASE_URL || "http://localhost:3003";
const mcpUrl = `${baseUrl.replace(/\/$/, "")}/mcp`;

function parseSseData(text) {
  const line = text.split("\n").find((l) => l.startsWith("data: "));
  if (!line) return null;
  try {
    return JSON.parse(line.slice(6));
  } catch {
    return null;
  }
}

async function initSession() {
  const payload = {
    jsonrpc: "2.0",
    id: 1,
    method: "initialize",
    params: {
      protocolVersion: "2024-11-05",
      capabilities: {},
      clientInfo: { name: "smoke-mcp", version: "1.0.0" },
    },
  };
  const res = await fetch(mcpUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json, text/event-stream",
    },
    body: JSON.stringify(payload),
  });
  const body = await res.text();
  return {
    status: res.status,
    sessionId: res.headers.get("mcp-session-id"),
    parsed: parseSseData(body),
  };
}

async function callTool(sessionId, id, name, args = {}) {
  const payload = {
    jsonrpc: "2.0",
    id,
    method: "tools/call",
    params: { name, arguments: args },
  };
  const res = await fetch(mcpUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json, text/event-stream",
      "mcp-session-id": sessionId,
    },
    body: JSON.stringify(payload),
  });
  const body = await res.text();
  return {
    status: res.status,
    parsed: parseSseData(body),
  };
}

function textFromResult(parsed) {
  const first = parsed?.result?.content?.[0];
  if (!first) return "";
  return first.type === "text" ? first.text : `[${first.type}]`;
}

function shortText(v, max = 180) {
  if (typeof v !== "string") return "";
  return v.length > max ? `${v.slice(0, max)}...` : v;
}

function isOk(entry) {
  return entry.ok && !entry.isError;
}

async function main() {
  const report = [];
  let toolId = 10;

  const init = await initSession();
  report.push({
    step: "initialize",
    ok: init.status === 200 && Boolean(init.sessionId),
    status: init.status,
    isError: Boolean(init.parsed?.error),
    brief: init.sessionId ? `session=${init.sessionId}` : "no session id",
  });

  if (!init.sessionId) {
    console.log(JSON.stringify({ summary: { ok: false }, report }, null, 2));
    process.exit(1);
  }

  const testMatrix = [
    { name: "ping", args: {}, expectToolError: false },
    { name: "check_auth_status", args: {}, expectToolError: false },
    {
      name: "list_chats",
      args: { limit: 3, include_last_message: true },
      expectToolError: false,
    },
    {
      name: "list_groups",
      args: { limit: 3, include_last_message: true },
      expectToolError: false,
    },
    { name: "list_system_chats", args: { limit: 10 }, expectToolError: false },
    { name: "search_contacts", args: { query: "anna" }, expectToolError: false },
    {
      name: "search_messages",
      args: { query: "спасибо", limit: 3 },
      expectToolError: false,
    },
    {
      name: "get_message_by_id",
      args: {
        message_id: "353876219642@s.whatsapp.net:AE878D86764FD890443AA4165CE69FB3",
      },
      expectToolError: false,
    },
    {
      name: "download_media",
      args: {
        message_id: "nope:nope",
        include_full_data: false,
        save_to_disk: false,
      },
      expectToolError: true,
    },
  ];

  let firstChatId = "";

  for (const test of testMatrix) {
    const res = await callTool(init.sessionId, toolId++, test.name, test.args);
    const toolError = Boolean(res.parsed?.result?.isError);
    const text = textFromResult(res.parsed);
    report.push({
      step: test.name,
      ok: res.status === 200 && !res.parsed?.error,
      status: res.status,
      isError: toolError,
      expectedToolError: test.expectToolError,
      brief: shortText(text),
    });
    if (test.name === "list_chats" && typeof text === "string") {
      try {
        const arr = JSON.parse(text);
        if (Array.isArray(arr) && arr[0]?.id) firstChatId = arr[0].id;
      } catch {
        // ignore
      }
    }
  }

  if (firstChatId) {
    const byId = await callTool(init.sessionId, toolId++, "get_chat_by_id", {
      jid: firstChatId,
    });
    const last = await callTool(
      init.sessionId,
      toolId++,
      "get_last_interaction",
      { jid: firstChatId },
    );
    report.push({
      step: "get_chat_by_id",
      ok: byId.status === 200 && !byId.parsed?.error,
      status: byId.status,
      isError: Boolean(byId.parsed?.result?.isError),
      expectedToolError: false,
      brief: shortText(textFromResult(byId.parsed)),
    });
    report.push({
      step: "get_last_interaction",
      ok: last.status === 200 && !last.parsed?.error,
      status: last.status,
      isError: Boolean(last.parsed?.result?.isError),
      expectedToolError: false,
      brief: shortText(textFromResult(last.parsed)),
    });
  }

  let hardFailCount = 0;
  for (const entry of report) {
    if (!entry.ok) hardFailCount += 1;
    if ("expectedToolError" in entry && entry.isError !== entry.expectedToolError) {
      hardFailCount += 1;
    }
  }

  const summary = {
    baseUrl,
    mcpUrl,
    total: report.length,
    hardFailCount,
    ok: hardFailCount === 0,
  };

  console.log(JSON.stringify({ summary, report }, null, 2));
  process.exit(summary.ok ? 0 : 1);
}

main().catch((error) => {
  console.error("[smoke-mcp] fatal:", error?.stack || String(error));
  process.exit(1);
});
