/**
 * pi-session-bridge — Let Pi sessions talk to each other via Unix sockets.
 *
 * Each session that joins the bridge listens on its own Unix socket at
 * ~/.pi/bridge/<session-name>.sock. Other sessions can send messages
 * by connecting to that socket.
 *
 * Online detection: connect() succeeds = online. connect() fails = offline.
 *
 * Usage:
 *   /bridge-on [name]       — join the bridge (auto-name if omitted)
 *   /bridge-off             — leave the bridge
 *   /bridge-list            — list online sessions
 *   /bridge-send <name>     — send a raw message to a session
 *   /bridge-tell <name>     — AI composes and sends a message
 */

import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
import { Type } from "typebox";
import { Box, Text } from "@mariozechner/pi-tui";
import { createServer, createConnection } from "node:net";
import { spawn, execSync } from "node:child_process";
import { existsSync, mkdirSync, readdirSync, readFileSync, unlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";

// ─── Types ───────────────────────────────────────────────

type BridgeMessage =
  | { type: "hello"; from: string; cwd: string }
  | { type: "text"; from: string; text: string; expectAnswer?: boolean }
  | { type: "bye" };

// ─── State ───────────────────────────────────────────────

const BRIDGE_DIR = join(homedir(), ".pi", "bridge");

let server: ReturnType<typeof createServer> | null = null;
let sessionName: string | null = null;
let inboxWatcher: ReturnType<typeof import("node:fs").watch> | null = null;
let inboxMessages: Array<{ from: string; text: string }> = [];
let pendingTrigger = false;
let inboxMode = false; // bridge-on but deliver to inbox for review
const recentMessages = new Set<string>();
function msgHash(from: string, text: string): string {
  return `${from}|${text.slice(0, 200)}`;
}
function isDuplicate(from: string, text: string): boolean {
  const hash = msgHash(from, text);
  if (recentMessages.has(hash)) return true;
  recentMessages.add(hash);
  setTimeout(() => recentMessages.delete(hash), 5000);
  return false;
}

function socketPath(name: string): string {
  return join(BRIDGE_DIR, `${name}.sock`);
}

function getHostName(): string {
  try {
    return require("node:os").hostname().split(".")[0] || "pi";
  } catch {
    return "pi";
  }
}

// ─── Session file discovery ─────────────────────────────
// Scan all Pi session JSONL files to find one by its /name.

const SESSIONS_DIR = join(homedir(), ".pi", "agent", "sessions");

type SessionLookupResult =
  | { file: string; collision: false }
  | { file: undefined; collision: true }
  | { file: undefined; collision: false };

function findSessionFileByName(name: string): SessionLookupResult {
  if (!existsSync(SESSIONS_DIR)) return { file: undefined, collision: false };
  const matches: string[] = [];

  function walk(dir: string): void {
    let entries;
    try { entries = readdirSync(dir, { withFileTypes: true }); } catch { return; }
    for (const entry of entries) {
      const full = join(dir, entry.name);
      if (entry.isDirectory()) {
        walk(full);
      } else if (entry.name.endsWith(".jsonl")) {
        try {
          const content = readFileSync(full, "utf-8");
          const lines = content.split("\n");
          for (const line of lines) {
            if (!line.trim()) continue;
            try {
              const obj = JSON.parse(line);
              if (obj.type === "session_info" && obj.name === name) {
                matches.push(full);
                break; // one match per file is enough
              }
            } catch { /* skip */ }
          }
        } catch { /* unreadable */ }
      }
    }
  }

  walk(SESSIONS_DIR);
  if (matches.length === 0) return { file: undefined, collision: false };
  if (matches.length > 1) return { file: undefined, collision: true };
  return { file: matches[0], collision: false };
}

// Decode cwd from session directory name
// --home-mlarabi-code-git-github-repo-- → /home/mlarabi/code/git/github/repo
function cwdFromSessionFile(sessionFile: string): string | undefined {
  const parts = sessionFile.split("/");
  const dirIdx = parts.indexOf("sessions");
  if (dirIdx < 0) return undefined;
  const encoded = parts[dirIdx + 1];
  if (!encoded || !encoded.startsWith("--") || !encoded.endsWith("--")) return undefined;
  return "/" + encoded.slice(2, -2).replace(/-/g, "/");
}

// ─── Registry ────────────────────────────────────────────
// Maps session names → session metadata so other sessions
// can discover the JSONL path for cold wake-up.

const REGISTRY_FILE = join(BRIDGE_DIR, "registry.json");

type RegistryEntry = {
  name: string;
  sessionFile: string;
  cwd: string;
  joinedAt: string;
};

function readRegistry(): Record<string, RegistryEntry> {
  if (!existsSync(REGISTRY_FILE)) return {};
  try {
    return JSON.parse(readFileSync(REGISTRY_FILE, "utf-8"));
  } catch {
    return {};
  }
}

function writeRegistry(entries: Record<string, RegistryEntry>): void {
  mkdirSync(BRIDGE_DIR, { recursive: true });
  writeFileSync(REGISTRY_FILE, JSON.stringify(entries, null, 2));
}

function registerSession(name: string, sessionFile: string, cwd: string): void {
  const registry = readRegistry();
  registry[name] = { name, sessionFile, cwd, joinedAt: new Date().toISOString() };
  writeRegistry(registry);
}

function unregisterSession(name: string): void {
  const registry = readRegistry();
  delete registry[name];
  writeRegistry(registry);
}

// ─── Inbox (offline delivery) ────────────────────────────
// When a session is offline, messages are written to its inbox.
// On session_start, the inbox is drained and injected into chat.

function isSessionLocked(sessionFile: string): boolean {
  try {
    const out = execSync(`fuser "${sessionFile}" 2>/dev/null`, {
      encoding: "utf-8",
    }).trim();
    if (!out) return false;
    const pids = out.split(/\s+/).filter((p) => p !== String(process.pid));
    return pids.length > 0;
  } catch {
    return false;
  }
}

function isSessionActive(entry: { sessionFile: string; cwd: string }): boolean {
  // Method 1: fuser — checks open file handles
  if (isSessionLocked(entry.sessionFile)) return true;

  // Method 2: scan /proc for a Pi process in the session's working directory
  try {
    const procDirs = readdirSync("/proc").filter((d) => /^\d+$/.test(d));
    for (const pidDir of procDirs) {
      try {
        const target = require("node:fs").readlinkSync(`/proc/${pidDir}/cwd`);
        if (target !== entry.cwd) continue;
        // Must be a Pi or node process (not bash, ssh, etc.)
        const cmdline = readFileSync(`/proc/${pidDir}/cmdline`, "utf-8").replace(/\0/g, " ");
        if (cmdline.includes("pi") || cmdline.includes("pi-coding-agent")) {
          return true;
        }
      } catch {
        continue;
      }
    }
  } catch {
    // /proc scan failed, rely on fuser only
  }

  return false;
}

function inboxDir(name: string): string {
  return join(BRIDGE_DIR, name, "inbox");
}

function targetExists(name: string): boolean {
  // Socket file = currently or recently on bridge
  if (existsSync(socketPath(name))) return true;
  // Registry = was on bridge at some point
  const registry = readRegistry();
  if (registry[name]) return true;
  // Session JSONL = a Pi session with this /name exists
  const lookup = findSessionFileByName(name);
  if (lookup.file) return true;
  return false;
}

function writeInbox(name: string, from: string, text: string): void {
  const dir = inboxDir(name);
  mkdirSync(dir, { recursive: true });
  const file = join(dir, `${Date.now()}.json`);
  writeFileSync(file, JSON.stringify({ from, text, at: new Date().toISOString() }));
}

function peekInbox(name: string): Array<{ from: string; text: string }> {
  const dir = inboxDir(name);
  if (!existsSync(dir)) return [];
  const files = readdirSync(dir)
    .filter((f) => f.endsWith(".json"))
    .sort();
  const messages: Array<{ from: string; text: string }> = [];
  for (const file of files) {
    try {
      const data = JSON.parse(readFileSync(join(dir, file), "utf-8"));
      messages.push({ from: data.from || "unknown", text: data.text || "" });
    } catch {
      // skip malformed
    }
  }
  return messages;
}

function drainInbox(name: string): Array<{ from: string; text: string }> {
  const messages = peekInbox(name);
  const dir = inboxDir(name);
  if (!existsSync(dir)) return messages;
  const files = readdirSync(dir)
    .filter((f) => f.endsWith(".json"))
    .sort();
  for (const file of files) {
    try { unlinkSync(join(dir, file)); } catch { /* ignore */ }
  }
  return messages;
}

// ─── Online peers ────────────────────────────────────────

function listPeers(): string[] {
  if (!existsSync(BRIDGE_DIR)) return [];
  return readdirSync(BRIDGE_DIR)
    .filter((f) => f.endsWith(".sock"))
    .map((f) => f.replace(/\.sock$/, ""));
}

function probe(name: string): Promise<boolean> {
  if (name === sessionName) return Promise.resolve(true);
  if (!existsSync(socketPath(name))) return Promise.resolve(false);
  return new Promise((resolve) => {
    const sock = createConnection(socketPath(name));
    const done = (val: boolean) => {
      sock.destroy();
      resolve(val);
    };
    sock.on("connect", () => done(true));
    sock.on("error", () => done(false));
    sock.setTimeout(500, () => done(false));
  });
}

async function listOnlinePeers(): Promise<string[]> {
  const peers = listPeers();
  const results = await Promise.all(
    peers.map(async (name) => ((await probe(name)) ? name : null)),
  );
  return results.filter((n): n is string => n !== null);
}

// ─── Socket server ───────────────────────────────────────

function startBridge(pi: ExtensionAPI, name: string): void {
  if (server) stopBridge();

  mkdirSync(BRIDGE_DIR, { recursive: true });
  const path = socketPath(name);
  if (existsSync(path)) unlinkSync(path);

  server = createServer((incoming) => {
    let buffer = "";
    incoming.on("data", (chunk) => {
      buffer += chunk.toString();
      const lines = buffer.split("\n");
      buffer = lines.pop() || "";
      for (const line of lines) {
        if (!line.trim()) continue;
        try {
          const msg: BridgeMessage = JSON.parse(line);
          if (msg.type === "text") {
            if (inboxMode) {
              writeInbox(sessionName!, msg.from, msg.text);
            } else {
              sendBridgeMessage(pi, msg.from, msg.text, "incoming");
              triggerAgentTurn(pi);
              if (msg.expectAnswer) {
                setTimeout(() => {
                  pi.sendUserMessage(
                    `The message above from "${msg.from}" expects a reply. ` +
                      `Compose a brief response and send it back using bridge_send with target="${msg.from}".`,
                  );
                }, 100);
              }
            }
          }
        } catch {
          // ignore malformed
        }
      }
    });
    incoming.on("end", () => incoming.destroy());
  });

  server.listen(path, () => {
    sessionName = name;
  });

  server.on("error", () => {
    stopBridge();
  });
}

function stopBridge(): void {
  if (server) {
    try { server.close(); } catch { /* ignore */ }
    server = null;
  }
  if (inboxWatcher) {
    try { inboxWatcher.close(); } catch { /* ignore */ }
    inboxWatcher = null;
  }
  if (sessionName && existsSync(socketPath(sessionName))) {
    try { unlinkSync(socketPath(sessionName)); } catch { /* ignore */ }
  }
  sessionName = null;
}

// ─── Send ────────────────────────────────────────────────

function send(target: string, msg: BridgeMessage, fromSession?: string): Promise<string> {
  if (fromSession && target === fromSession) {
    return Promise.resolve(""); // silently drop self-messages
  }
  return new Promise((resolve, reject) => {
    const sock = createConnection(socketPath(target));
    let reply = "";
    sock.setTimeout(3000, () => {
      sock.destroy();
      if (reply) resolve(reply);
      else resolve("");
    });
    sock.on("connect", () => {
      sock.write(JSON.stringify(msg) + "\n");
      // We don't expect a reply in v0.1 — just fire and forget.
      sock.end();
    });
    sock.on("data", (chunk) => {
      reply += chunk.toString();
    });
    sock.on("end", () => resolve(reply));
    sock.on("error", () => resolve(""));
  });
}

// ─── Extension ───────────────────────────────────────────

function sendBridgeMessage(pi: ExtensionAPI, from: string, text: string, direction: "incoming" | "outgoing" = "incoming", to?: string): void {
  if (direction === "incoming" && isDuplicate(from, text)) return;
  // LLM sees the bridge context in content; TUI pulls raw text from details
  const content = direction === "incoming"
    ? `📨 [bridge] Message from **${from}**: ${text}`
    : `📨 [bridge] Message to **${to}**: ${text}`;
  pi.sendMessage({
    customType: "bridge-message",
    content,
    display: true,
    details: { from, direction, to, rawText: text },
  });
}

// Trigger agent to respond to bridge messages (custom messages don't auto-start turns)
function triggerAgentTurn(pi: ExtensionAPI): void {
  try {
    pi.sendUserMessage("👆");
    pendingTrigger = false;
  } catch (e: any) {
    if (e?.message?.includes("already processing")) {
      pendingTrigger = true;
    } else {
      throw e;
    }
  }
}

// Inject many messages — each gets its own bubble + triggers agent
function injectMany(pi: ExtensionAPI, messages: Array<{ from: string; text: string }>): void {
  for (const m of messages) {
    sendBridgeMessage(pi, m.from, m.text, "incoming");
  }
  triggerAgentTurn(pi);
}

export default function sessionBridgeExtension(pi: ExtensionAPI) {
  // Custom bubble renderer for bridge messages
  // Renderer for bridge-tell compose instructions
  pi.registerMessageRenderer("bridge-tell", (message, { expanded }, theme) => {
    const details = message.details as { target: string; prompt: string } | undefined;
    const target = details?.target ?? "?";
    const prompt = details?.prompt ?? "";
    const label = theme.fg("muted", `compose → ${target}`);
    const box = new Box(1, 1, (t) => theme.bg("customMessageBg", t));
    // Always show the prompt so user remembers what they asked
    let text = `${label}\n${theme.fg("customMessageText", prompt)}`;
    // Expanded shows the full instructions sent to the agent
    if (expanded && message.content) {
      text += `\n\n${theme.fg("dim", message.content)}`;
    }
    box.addChild(new Text(text, 0, 0));
    return box;
  });

  pi.registerMessageRenderer("bridge-message", (message, { expanded }, theme) => {
    const details = message.details as { from: string; to?: string; direction: "incoming" | "outgoing"; rawText?: string } | undefined;
    const from = details?.from ?? "bridge";
    const to = details?.to;
    const direction = details?.direction ?? "incoming";
    const rawText = details?.rawText ?? message.content;

    const isIncoming = direction === "incoming";
    const labelColor = isIncoming ? "accent" : "success";
    const arrow = isIncoming ? "←" : "→";
    const label = to
      ? theme.fg(labelColor, `${from} ${arrow} ${to}`)
      : theme.fg(labelColor, `${arrow} ${from}`);

    const box = new Box(1, 1, (t) => theme.bg("customMessageBg", t));
    box.addChild(new Text(`${label}\n${theme.fg("customMessageText", rawText)}`, 0, 0));
    return box;
  });

  // Cleanup on shutdown
  pi.on("session_shutdown", () => {
    stopBridge();
    stopInboxWatcher();
  });

  // Always-on inbox watcher (bridge off → notify, bridge on → socket handles it)
  function startInboxWatcher(
    piSessionName: string,
    notify: (msg: string, level: "info" | "success" | "warning" | "error") => void,
  ): void {
    if (inboxWatcher) return;
    const dir = inboxDir(piSessionName);
    mkdirSync(dir, { recursive: true });
    try {
      inboxWatcher = (require("node:fs") as typeof import("node:fs")).watch(
        dir,
        (_event, filename) => {
          if (!filename) return;
          if (server && !inboxMode) {
            // Bridge ON, real-time mode — consume files immediately
            const pending = drainInbox(piSessionName);
            if (pending.length === 0) return;
            injectMany(pi, pending);
          } else {
            // Bridge OFF or inbox mode — peek without deleting
            const pending = peekInbox(piSessionName);
            if (pending.length === 0) return;
            // Only add new messages not already in memory
            const existing = new Set(inboxMessages.map((m) => m.text));
            const newMsgs = pending.filter((m) => !existing.has(m.text));
            if (newMsgs.length === 0) return;
            inboxMessages.push(...newMsgs);
            notify(
              `📨 ${inboxMessages.length} pending bridge message(s). Use /bridge-inbox.`,
              "info",
            );
          }
        },
      );
    } catch {
      // fs.watch not available
    }
  }

  function stopInboxWatcher(): void {
    if (inboxWatcher) {
      try { inboxWatcher.close(); } catch { /* ignore */ }
      inboxWatcher = null;
    }
  }

  // Start watcher on session start if session has a name
  // Also drain existing inbox files that arrived while offline
  pi.on("session_start", async (_event, ctx) => {
    const name = ctx.sessionManager.getSessionName();
    if (!name) return;
    startInboxWatcher(name, (msg, level) => ctx.ui.notify(msg, level));

    // Peek without deleting — only user actions (accept/dismiss/clear) drain files
    const pending = peekInbox(name);
    if (pending.length === 0) return;

    if (server) {
      // Bridge ON: consume files + deliver immediately
      drainInbox(name);
      injectMany(pi, pending);
      return;
    }

    // Deduplicate against already-known messages
    const existing = new Set(inboxMessages.map((m) => m.text));
    const newMsgs = pending.filter((m) => !existing.has(m.text));
    if (newMsgs.length === 0) return;
    inboxMessages.push(...newMsgs);
    // TUI may not be ready at session_start — delay the notification
    setTimeout(() => {
      ctx.ui.notify(
        `📨 ${inboxMessages.length} pending bridge message(s). Use /bridge-inbox.`,
        "info",
      );
    }, 500);
  });

  // Drain pending triggers when agent finishes
  pi.on("agent_end", () => {
    if (pendingTrigger) {
      pendingTrigger = false;
      try {
        pi.sendUserMessage("👆");
      } catch (e: any) {
        if (!e?.message?.includes("already processing")) throw e;
      }
    }
  });

  // Always inject bridge context into system prompt (bridge_send works without /bridge-on)
  pi.on("before_agent_start", async (_event, ctx) => {
    const name = ctx.sessionManager.getSessionName() || getHostName();
    const status = server ? `You are joined to the bridge as "${sessionName}".` : "You are NOT joined to the bridge.";
    const bridgePrompt =
      `${status} Your bridge name is "${name}".\n\n` +
      "Other Pi sessions can send you messages and you can send messages to them.\n\n" +
      "Messages from other sessions arrive in this format:\n" +
      `📨 [bridge] Message from **name**: text\n\n` +
      "To send a message to another session, use the `bridge_send` tool with `target` and `text`. " +
      "Use `/bridge-list` to see who is online.\n" +
      "Use `/bridge-on` to start receiving messages in real time.\n" +
      "IMPORTANT: When asked to send a bridge message, rephrase the text into a natural, " +
      "first-person message in your own words. Do NOT send the instruction text verbatim.\n" +
      "Only send bridge messages when explicitly asked by the user. " +
      "When you receive a bridge message, respond naturally as if that person sent it.\n" +
      "To ask for a reply, set expect_answer=true on bridge_send. " +
      "The recipient's agent will automatically compose and send a response.";
    return {
      messages: [{ role: "system", content: bridgePrompt }],
    };
  });

  // ── /bridge-on ─────────────────────────────────────────
  pi.registerCommand("bridge-on", {
    description: "Join the session bridge. Use --inbox to queue messages for review instead of real-time delivery.",
    handler: async (args, ctx) => {
      const raw = (args || "").trim();
      inboxMode = raw.includes("--inbox");
      const name = raw.replace("--inbox", "").trim() || ctx.sessionManager.getSessionName() || getHostName();
      if (ctx.sessionManager.getSessionName() === name && server) {
        ctx.ui.notify(`Already on bridge as "${name}"`, "info");
        return;
      }

      // Check for bridge name collision — another session already using this name?
      const registry = readRegistry();
      const existing = registry[name];
      const mySessionFile = ctx.sessionManager.getSessionFile();
      if (existing && mySessionFile && existing.sessionFile !== mySessionFile) {
        ctx.ui.notify(
          `Bridge name "${name}" is already used by another session. Use /bridge-on <unique-name>.`,
          "error",
        );
        return;
      }

      // Check for session name collision — another Pi session with same /name?
      if (sessionName) {
        const lookup = findSessionFileByName(sessionName);
        if (lookup.collision) {
          ctx.ui.notify(
            `Session name "${sessionName}" is not unique. Another session has the same /name. Use /name to set a unique name.`,
            "error",
          );
          return;
        }
      }

      // Always close previous connection first — only one bridge at a time
      if (server) stopBridge();
      startBridge(pi, name);

      // Start watcher for this session name
      startInboxWatcher(name, (msg, level) => ctx.ui.notify(msg, level));

      if (mySessionFile) registerSession(name, mySessionFile, ctx.cwd || "");

      // Flush pending inbox messages — now that bridge is on, deliver them
      const pending = drainInbox(name);
      if (pending.length > 0) {
        const existing = new Set(inboxMessages.map((m) => m.text));
        const newMsgs = pending.filter((m) => !existing.has(m.text));
        inboxMessages.push(...newMsgs);
      }
      if (inboxMessages.length > 0) {
        if (inboxMode) {
          ctx.ui.notify(`Joined bridge as "${name}" (inbox mode). ${inboxMessages.length} message(s) queued. Use /bridge-inbox to review.`, "info");
        } else {
          const toInject = [...inboxMessages];
          inboxMessages = [];
          injectMany(pi, toInject);
          ctx.ui.notify(`Joined bridge — delivering ${toInject.length} pending message(s)...`, "success");
        }
      } else {
        ctx.ui.notify(`Joined bridge as "${name}"${inboxMode ? " (inbox mode)" : ""}`, "success");
      }
    },
  });

  // ── /bridge-inbox ──────────────────────────────────────
  pi.registerCommand("bridge-inbox", {
    description: "Check pending bridge messages. Use 'accept <n>' to inject or 'dismiss <n>' to discard.",
    handler: async (args, ctx) => {
      const name = ctx.sessionManager.getSessionName() || getHostName();

      // Peek at inbox files without deleting — only drain on accept/clear
      const fresh = peekInbox(name);
      const existing = new Set(inboxMessages.map((m) => m.text));
      const newMsgs = fresh.filter((m) => !existing.has(m.text));
      if (newMsgs.length > 0) inboxMessages.push(...newMsgs);

      const raw = (args || "").trim();
      const parts = raw.split(/\s+/);
      const action = parts[0]?.toLowerCase();
      const idx = parts[1] ? parseInt(parts[1]) : NaN;
      const index = Number.isNaN(idx) ? -1 : idx - 1; // 1-based → 0-based

      if (action === "clear") {
        drainInbox(name); // actually delete files
        const count = inboxMessages.length;
        inboxMessages = [];
        ctx.ui.notify(`Cleared ${count} pending bridge message(s).`, "info");
        return;
      }

      if (action === "dismiss" && index >= 0 && index < inboxMessages.length) {
        const removed = inboxMessages.splice(index, 1)[0];
        ctx.ui.notify(`Dismissed message from "${removed.from}". ${inboxMessages.length} remaining.`, "info");
        return;
      }

      if (action === "accept" && index >= 0 && index < inboxMessages.length) {
        drainInbox(name); // sync disk with memory
        const [accepted] = inboxMessages.splice(index, 1);
        sendBridgeMessage(pi, accepted.from, accepted.text, "incoming");
        triggerAgentTurn(pi);
        ctx.ui.notify(`Injected message from "${accepted.from}".`, "success");
        return;
      }

      if (action === "accept" && index < 0) {
        drainInbox(name); // actually delete files
        const toInject = [...inboxMessages];
        inboxMessages = [];
        injectMany(pi, toInject);
        ctx.ui.notify(`Injecting ${toInject.length} bridge message(s) one by one...`, "success");
        return;
      }

      if (inboxMessages.length === 0) {
        ctx.ui.notify("No pending bridge messages. 📭", "info");
        return;
      }

      const list = inboxMessages
        .map((m, i) => `  ${i + 1}. **${m.from}**: ${m.text.slice(0, 80)}${m.text.length > 80 ? "…" : ""}`)
        .join("\n");
      ctx.ui.notify(
        `📨 ${inboxMessages.length} pending:\n${list}\n\n/bridge-inbox accept <n>  — inject one\n/bridge-inbox dismiss <n> — discard one\n/bridge-inbox accept     — inject all\n/bridge-inbox clear      — discard all`,
        "info",
      );
    },
  });

  // ── /bridge-inbox-mode ────────────────────────────────
  pi.registerCommand("bridge-inbox-mode", {
    description: "Toggle inbox mode while on the bridge. Messages queue for review instead of real-time delivery.",
    handler: async (args, ctx) => {
      if (!server) {
        ctx.ui.notify("Join the bridge first with /bridge-on", "error");
        return;
      }
      const action = (args || "").trim().toLowerCase();
      if (action === "on" || action === "enable") {
        inboxMode = true;
        ctx.ui.notify("Inbox mode enabled — incoming messages will queue for review.", "info");
      } else if (action === "off" || action === "disable") {
        inboxMode = false;
        ctx.ui.notify("Inbox mode disabled — real-time delivery restored.", "info");
      } else {
        ctx.ui.notify(`Inbox mode is ${inboxMode ? "ON" : "OFF"}. Usage: /bridge-inbox-mode on|off`, "info");
      }
    },
  });

  // ── /bridge-off ────────────────────────────────────────
  pi.registerCommand("bridge-off", {
    description: "Leave the session bridge",
    handler: async (_args, ctx) => {
      if (!server) {
        ctx.ui.notify("Not on the bridge", "info");
        return;
      }
      const name = sessionName;
      if (name) unregisterSession(name);
      stopBridge();
      ctx.ui.notify(`Left bridge (was "${name}")`, "success");
    },
  });

  // ── /bridge-check-lock ─────────────────────────────────
  pi.registerCommand("bridge-check-lock", {
    description: "Check if a Pi session (by /name) has its JSONL locked by another process",
    handler: async (args, ctx) => {
      const name = (args || "").trim();
      if (!name) {
        ctx.ui.notify("Usage: /bridge-check-lock <session-name>", "error");
        return;
      }

      // Try registry first, then filesystem scan
      let sessionFile: string | undefined;
      const registry = readRegistry();
      const entry = Object.values(registry).find((e) => e.sessionName === name);
      if (entry) {
        sessionFile = entry.sessionFile;
      } else {
        const lookup = findSessionFileByName(name);
        if (lookup.collision) {
          ctx.ui.notify(
            `Multiple sessions named "${name}" found. Names must be unique. Use /name to rename one.`,
            "error",
          );
          return;
        }
        sessionFile = lookup.file;
      }

      if (!sessionFile) {
        ctx.ui.notify(
          `No session named "${name}" found. Sessions are named via the /name command.`,
          "error",
        );
        return;
      }

      const active = entry
        ? isSessionActive(entry)
        : isSessionActive({ sessionFile, cwd: cwdFromSessionFile(sessionFile) || "" });
      ctx.ui.notify(
        `"${name}" → ${sessionFile}\n` +
          `Active: ${active ? "YES 🛑" : "NO ✅"}`,
        active ? "warning" : "success",
      );
    },
  });

  // ── /bridge-list ───────────────────────────────────────
  pi.registerCommand("bridge-list", {
    description: "List sessions currently on the bridge",
    handler: async (_args, ctx) => {
      mkdirSync(BRIDGE_DIR, { recursive: true });
      const peers = await listOnlinePeers();
      if (peers.length === 0) {
        ctx.ui.notify("No sessions on the bridge", "info");
        return;
      }
      ctx.ui.notify(
        `Online (${peers.length}): ${peers.map((p) => (p === sessionName ? `${p} (you)` : p)).join(", ")}`,
        "info",
      );
    },
  });

  // ── /bridge-send ───────────────────────────────────────
  pi.registerCommand("bridge-send", {
    description: "Send a raw message to a session (no bridge join required). Use --expect-answer to request an auto-reply.",
    handler: async (args, ctx) => {
      const raw = (args || "").trim();
      const expectAnswer = raw.includes("--expect-answer");
      const rest = raw.replace("--expect-answer", "").trim();
      const parts = rest.split(/\s+/);
      const target = parts[0];
      const text = parts.slice(1).join(" ");
      if (!target || !text) {
        ctx.ui.notify("Usage: /bridge-send <name> <message>", "error");
        return;
      }
      // Fast path: check bridge first (socket/registry), then scan sessions
      const onBridge = existsSync(socketPath(target)) || !!readRegistry()[target];
      if (!onBridge) {
        const lookup = findSessionFileByName(target);
        if (lookup.collision) {
          ctx.ui.notify(
            `Multiple sessions named "${target}" found. Names must be unique. Use /name to rename one.`,
            "error",
          );
          return;
        }
        if (!lookup.file) {
          ctx.ui.notify(
            `No session or bridge node named "${target}" found.`,
            "error",
          );
          return;
        }
      }

      const from = ctx.sessionManager.getSessionName() || getHostName();
      if (target === from) {
        ctx.ui.notify("Can't send a message to yourself.", "error");
        return;
      }
      const online = await probe(target);
      const msg: BridgeMessage = { type: "text", from, text, expectAnswer };
      if (online) {
        await send(target, msg, sessionName ?? undefined);
        ctx.ui.notify(`Sent to "${target}"${expectAnswer ? " (expects answer)" : ""}`, "success");
      } else {
        writeInbox(target, from, text);
        ctx.ui.notify(`"${target}" is offline — message queued to inbox.`, "info");
      }
    },
  });

  // ── bridge_send tool (for the model) ──────────────────
  pi.registerTool({
    name: "bridge_send",
    label: "Bridge Send",
    description:
      "Send a message to another Pi session. " +
      "Use expect_answer=true to ask the recipient's agent to auto-reply.",
    parameters: Type.Object({
      target: Type.String({ description: "Name of the target session on the bridge" }),
      text: Type.String({ description: "The message to send" }),
      expect_answer: Type.Optional(Type.Boolean({ description: "Ask recipient to auto-reply. When true, their agent composes and sends a response back on its own." })),
    }),
    async execute(toolCallId, params, _signal, _onUpdate, ctx) {
      // Fast path: check bridge first (socket/registry), then scan sessions
      const onBridge = existsSync(socketPath(params.target)) || !!readRegistry()[params.target];
      if (!onBridge) {
        const lookup = findSessionFileByName(params.target);
        if (lookup.collision) {
          return {
            content: [
              {
                type: "text",
                text: `Multiple sessions named "${params.target}" found. Names must be unique. Use /name to rename one.`,
              },
            ],
            details: {},
          };
        }
        if (!lookup.file) {
          return {
            content: [{ type: "text", text: `No session or bridge node named "${params.target}" found.` }],
            details: {},
          };
        }
      }

      const from = ctx.sessionManager.getSessionName() || getHostName();
      if (params.target === from) {
        return {
          content: [{ type: "text", text: "Can't send a message to yourself." }],
          details: {},
        };
      }
      const online = await probe(params.target);
      const msg: BridgeMessage = { type: "text", from, text: params.text, expectAnswer: params.expect_answer };
      if (online) {
        await send(params.target, msg, sessionName ?? undefined);
        sendBridgeMessage(pi, from, params.text, "outgoing", params.target);
        return {
          content: [{ type: "text", text: `Message sent to "${params.target}".` }],
          details: {},
        };
      }
      writeInbox(params.target, from, params.text);
      // Note: expectAnswer is lost for offline delivery (inbox doesn't preserve it yet)
      sendBridgeMessage(pi, from, params.text, "outgoing", params.target);
      return {
        content: [
          {
            type: "text",
            text: `"${params.target}" is offline. Message queued and will be delivered on next startup.`,
          },
        ],
        details: {},
      };
    },
  });

  // ── /bridge-tell ───────────────────────────────────────
  pi.registerCommand("bridge-tell", {
    description: "Ask the AI to compose and send a message. No bridge join required.",
    handler: async (args, _ctx) => {
      const raw = (args || "").trim();
      const parts = raw.split(/\s+/);
      const target = parts[0];
      const prompt = parts.slice(1).join(" ");
      if (!target || !prompt) {
        _ctx.ui.notify("Usage: /bridge-tell <session-name> <what to tell them>", "error");
        return;
      }

      // Show styled compose bubble (collapsed = label only, expanded = full task)
      // Content is sent to LLM as user message context. User message is just a trigger.
      pi.sendMessage({
        customType: "bridge-tell",
        content:
          `Send a message to the "${target}" Pi session using the bridge_send tool.\n` +
          `Your task: ${prompt}\n` +
          `IMPORTANT: Rephrase this into a natural message in your own words. ` +
          `Do NOT send the instruction text verbatim. Keep it concise.\n` +
          `If you want the recipient to reply, use expect_answer=true.`,
        display: true,
        details: { target, prompt },
      });
      pi.sendUserMessage("👆");
    },
  });

}

