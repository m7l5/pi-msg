# pi-msg

Let [Pi](https://github.com/badlogic/pi-mono) sessions talk to each other. Send messages between agents, collaborate across windows, or run multi-agent workflows with per-message reply requests and safe offline delivery.

## Install

```bash
pi install git:github.com/m7l5/pi-msg
```

Or via SSH:

```bash
pi install git:git@github.com:m7l5/pi-msg
```

## Quick Start

Join the msg network in each session you want to link:

```
/msg-on my-name
```

Send a message from anywhere:

```
/msg-send other-name Hey, can you review the auth module?
```

Or ask your AI to compose one:

```
/msg-tell other-name Ask them to check the auth module for race conditions
```

## Commands

| Command | Description |
|---------|-------------|
| `/msg-on [name]` | Join the msg network (uses session name if omitted) |
| `/msg-off` | Leave the msg network |
| `/msg-list` | List online sessions |
| `/msg-send <name> <text>` | Send a raw message |
| `/msg-tell <name> <prompt>` | Ask AI to compose and send a message |
| `/msg-inbox` | Review pending messages |
| `/msg-inbox-mode on\|off` | Toggle inbox mode while on the network |
| `/msg-status <name>` | Check if a session is active |

### Inbox Commands

Messages from offline sessions are queued to your inbox:

```
/msg-inbox              → list pending messages
/msg-inbox read 2       → preview message #2 in full
/msg-inbox accept 2     → inject message #2 into chat
/msg-inbox accept       → inject all messages
/msg-inbox dismiss 3    → discard message #3
/msg-inbox clear        → discard all
```

## AI Tool: `msg_send`

Your agent can send messages programmatically:

```
msg_send(target="builder-pi", text="Please scaffold the API routes")
```

**Flags:**

| Flag | Effect |
|------|--------|
| `expect_answer=true` | Asks the recipient's agent to auto-reply |
| `steer=true` | Interrupts the recipient's current agent turn (use sparingly) |

By default, messages do **not** interrupt a busy agent. They are delivered safely after the current turn ends.

## How It Works

**Online delivery** — Unix sockets at `~/.pi/msg/<name>.sock`. If the socket connects, the message is delivered in real time and the recipient's agent is triggered to read it.

**Offline delivery** — If the target is offline, the message is written to `~/.pi/msg/<name>/inbox/`. When the target joins (`/msg-on`) or starts up, pending messages are consumed automatically.

**Inbox mode** — `/msg-on --inbox` queues incoming messages for review instead of injecting them immediately. Use this when you don't want real-time interruptions.

**Collision detection** — Names must be unique. If two sessions share the same `/name`, the sender is warned and must disambiguate.

## Development

```bash
pnpm install
pnpm typecheck        # TypeScript check
pnpm build            # Compile to dist/
pnpm lint             # oxlint
pnpm format           # oxfmt
```

## License

Apache-2.0
