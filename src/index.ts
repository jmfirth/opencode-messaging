/**
 * opencode-messaging — cross-session communication for OpenCode 2.
 *
 * Adds tools so an agent can see, read, message, and start other sessions in
 * the same background service:
 *
 *   peer_list       — enumerate other sessions (id, title, directory, agent, running)
 *   peer_read       — read a peer's recent conversation, no side effects
 *   peer_send       — deliver a message to one peer, waking it
 *   peer_broadcast  — deliver the same message to several peers at once
 *   peer_spawn      — create a session in another directory and give it work
 *
 * peer_read declares the "peer_read" permission action. Every tool that sends
 * — peer_send, peer_broadcast and peer_spawn — shares the single "peer_send"
 * action, mirroring how OpenCode gives edit/write/apply_patch one "edit"
 * action: denying it must deny all outbound messaging, or an agent simply
 * reaches for whichever verb was left enabled.
 *
 * Configuration (plugin options, all optional):
 *
 *   accept  "same-directory" (default) | "any" | "none"
 *           which existing sessions may be messaged
 *   spawn   whether peer_spawn may create sessions (default true)
 *   fanout  maximum peer_broadcast recipients (default 10)
 *   rates   per-session send budgets, e.g. { "broadcast": { "limit": 3, "minutes": 2 } }
 *           defaults: send 20/5m, broadcast 3/2m. peer_spawn is not rate
 *           limited — it creates work rather than trading messages.
 *
 * Delivery rides OpenCode's durable session inbox:
 *   "queue" (default) lands when the peer would otherwise go idle
 *   "steer" lands at the peer's next safe provider-turn boundary, mid-task
 *
 * Replies are ordinary peer_send calls in the other direction; every message
 * carries its sender's session ID so the recipient can answer.
 */
import { readFileSync, statSync } from "node:fs"
import { homedir } from "node:os"
import { isAbsolute, join } from "node:path"
import { envelope, provenance } from "./lib/envelope.ts"
import { ago, basename } from "./lib/format.ts"
import { assertDeliverable, policy } from "./lib/policy.ts"
import { rateLimit } from "./lib/rate.ts"
import { render } from "./lib/transcript.ts"
import type { SenderContext, SessionLike } from "./lib/types.ts"

/**
 * The slice of the plugin context opencode-messaging actually uses.
 *
 * The host's own `Plugin.Context` is far wider and its tool-draft generics are
 * effect-schema derived; declaring the four members this plugin touches keeps
 * our call sites checked without pretending to restate the host's API.
 */
interface ToolDraft {
  add(tool: {
    name: string
    description: string
    options?: { codemode?: boolean; permission?: string }
    input: Record<string, unknown>
    execute: (input: any, context: SenderContext) => Promise<{ content: string }>
  }): void
}

interface PluginContext {
  readonly options?: Record<string, unknown>
  readonly tool: { transform: (transform: (draft: ToolDraft) => void) => Promise<void> | void }
  readonly permission: {
    assert: (input: {
      sessionID: string
      agent?: string
      source: { type: "tool"; messageID: string; id: string }
      action: string
      resources: string[]
      save?: string[]
      metadata?: Record<string, unknown>
    }) => Promise<void>
  }
  readonly session: {
    create: (input: Record<string, unknown>) => Promise<{ id?: string; data?: { id?: string } }>
    synthetic: (input: Record<string, unknown>) => Promise<unknown>
  }
}

const SERVICE = join(
  process.env.XDG_STATE_HOME ?? join(homedir(), ".local", "state"),
  "opencode",
  "service.json",
)

/** A broadcast recipient, or a placeholder for an id that matched no session. */
type BroadcastTarget = SessionLike & { readonly missing?: boolean }

const PEER_LIMIT = 40
const TOTAL_CHARS = 12_000

// The session list and message history are not on the plugin's session domain,
// so those two reads go over HTTP to the same background service this plugin is
// running inside. The registration file is written by `opencode2 serve --service`
// and is re-read per call so a service restart does not strand the plugin.
async function api(path: string) {
  const service = (() => {
    try {
      return JSON.parse(readFileSync(SERVICE, "utf8"))
    } catch {
      throw new Error(`cannot reach the opencode service: ${SERVICE} is missing or unreadable`)
    }
  })()

  const response = await fetch(`${service.url}${path}`, {
    headers: {
      authorization: `Basic ${Buffer.from(`opencode:${service.password}`).toString("base64")}`,
    },
  }).catch(() => {
    throw new Error(`cannot reach the opencode service at ${service.url}`)
  })

  if (response.status === 404) throw new Error("no such session")
  if (!response.ok) throw new Error(`opencode service returned ${response.status} for ${path}`)
  return response.json()
}

const sessions = (): Promise<SessionLike[]> => api("/api/session").then((body) => body.data ?? [])

// `/api/session/active` returns { data: { [sessionID]: { type: "running" } } }.
const running = (): Promise<Record<string, unknown>> =>
  api("/api/session/active")
    .then((body) => body.data ?? {})
    .catch(() => ({}))

/** V2 permission checks must be asserted explicitly; tool options are not enough. */
async function assertPermission(
  ctx: PluginContext,
  context: SenderContext,
  action: "peer_read" | "peer_send",
  resources: string[],
  metadata: Record<string, unknown>,
) {
  await ctx.permission.assert({
    sessionID: context.sessionID,
    agent: context.agent,
    source: {
      type: "tool",
      messageID: context.messageID ?? "",
      id: context.id ?? context.messageID ?? "",
    },
    action,
    resources,
    save: resources,
    metadata,
  })
}

export default {
  id: "opencode-messaging",
  setup: async (ctx: PluginContext) => {
    await ctx.tool.transform((tools: ToolDraft) => {
      tools.add({
        name: "peer_list",
        description:
          "List other OpenCode sessions running in this background service, so you can read from " +
          "or message them. Returns each session's id, working directory, agent, how long since it " +
          "was last active, and whether it is running right now. Titles are auto-generated and are " +
          "often duplicated, so always address a session by id.",
        options: { codemode: false },
        input: {
          type: "object",
          properties: {
            all: {
              type: "boolean",
              description:
                "Include subagent sessions and sessions from other directories. Defaults to false, " +
                "which lists only top-level sessions in this session's directory.",
            },
          },
        },
        execute: async (input: { all?: boolean }, context: SenderContext) => {
          const [all, active] = await Promise.all([sessions(), running()])
          const self = all.find((s: SessionLike) => s.id === context.sessionID)

          const peers = all
            .filter((s: SessionLike) => s.id !== context.sessionID)
            .filter((s: SessionLike) => input.all || !s.parentID)
            .filter(
              (s: SessionLike) => input.all || !self || s.location?.directory === self.location?.directory,
            )
            .sort((a: SessionLike, b: SessionLike) => (b.time?.updated ?? 0) - (a.time?.updated ?? 0))
            .slice(0, PEER_LIMIT)

          if (peers.length === 0)
            return {
              content: input.all
                ? "No other sessions exist in this service."
                : "No other sessions in this directory. Retry with all=true to search every directory.",
            }

          const lines = peers.map((s: SessionLike) => {
            const tags = [
              active[s.id] ? "running" : "",
              s.parentID ? "subagent" : "",
              ago(s.time?.updated),
            ].filter(Boolean)
            const where = `${basename(s.location?.directory ?? "")}/${s.agent ?? "?"}`
            return `${s.id}  [${where}] (${tags.join(", ")})  ${s.title ?? ""}`
          })

          return { content: lines.join("\n") }
        },
      })

      tools.add({
        name: "peer_read",
        description:
          "Read another session's recent conversation without disturbing it. Use this when you only " +
          "need information the peer has already produced — it costs the peer nothing and works even " +
          "while it is busy. Use peer_send instead when you need the peer's judgment on something " +
          "nobody has asked it yet.",
        options: { codemode: false, permission: "peer_read" },
        input: {
          type: "object",
          properties: {
            session: { type: "string", description: "Target session id, from peer_list." },
            limit: {
              type: "number",
              description: "How many recent messages to read. Defaults to 10, max 50.",
            },
          },
          required: ["session"],
        },
        execute: async (input: { session: string; limit?: number }, context: SenderContext) => {
          await assertPermission(ctx, context, "peer_read", [input.session], { tool: "peer_read" })
          if (input.session === context.sessionID)
            throw new Error("peer_read cannot target the current session; you already have its history")

          const limit = Math.min(Math.max(Math.floor(input.limit ?? 10), 1), 50)
          const body = await api(
            `/api/session/${encodeURIComponent(input.session)}/message?limit=${limit}&order=desc`,
          )

          const blocks = (body.data ?? [])
            .map(render)
            .filter((block: string | null): block is string => Boolean(block))
            .reverse()

          if (blocks.length === 0) return { content: "Peer has no readable messages yet." }

          // Keep the most recent messages when the transcript is large.
          const kept: string[] = []
          let total = 0
          for (const block of [...blocks].reverse()) {
            if (total + block.length > TOTAL_CHARS) break
            kept.unshift(block)
            total += block.length
          }
          const dropped = blocks.length - kept.length

          return {
            content: (dropped > 0 ? [`(${dropped} older messages omitted)`, ...kept] : kept).join("\n\n"),
          }
        },
      })

      tools.add({
        name: "peer_send",
        description:
          "Send a message to another OpenCode session. The peer wakes and handles it as part of its " +
          "own conversation, and can answer by calling peer_send back at you. This does not block: " +
          "if you expect a reply, finish your turn and it will arrive as a message here. Use " +
          "peer_read instead when you only need information the peer already produced.",
        options: { codemode: false, permission: "peer_send" },
        input: {
          type: "object",
          properties: {
            to: { type: "string", description: "Target session id, from peer_list." },
            message: {
              type: "string",
              description:
                "What to say. Include enough context to stand alone — the peer cannot see your " +
                "conversation, only this message.",
            },
            delivery: {
              type: "string",
              enum: ["queue", "steer"],
              description:
                "queue (default) waits until the peer finishes what it is doing. steer interrupts it " +
                "at the next safe point — only for genuinely urgent messages.",
            },
          },
          required: ["to", "message"],
        },
        execute: async (
          input: { to: string; message: string; delivery?: "queue" | "steer" },
          context: SenderContext,
        ) => {
          await assertPermission(ctx, context, "peer_send", [input.to], { tool: "peer_send" })
          if (input.to === context.sessionID) throw new Error("peer_send cannot target the current session")
          if (!input.message?.trim()) throw new Error("peer_send requires a non-empty message")

          const delivery = input.delivery === "steer" ? "steer" : "queue"
          const all = await sessions()
          const target = all.find((s: SessionLike) => s.id === input.to)
          if (!target) throw new Error(`no session with id ${input.to} — call peer_list for current ids`)

          const self = all.find((s: SessionLike) => s.id === context.sessionID)
          assertDeliverable(ctx, self, target)
          rateLimit(ctx, "send", context.sessionID)
          const from = basename(self?.location?.directory ?? "")

          await ctx.session.synthetic({
            sessionID: input.to,
            description: `Message from ${from}/${context.agent}`,
            delivery,
            resume: true,
            metadata: provenance(context, self),
            text: envelope(context, from, input.message),
          })

          const where = `${basename(target.location?.directory ?? "")}/${target.agent ?? "?"}`
          return {
            content:
              `Delivered to ${input.to} [${where}] via ${delivery}. ` +
              (delivery === "queue"
                ? "It will be picked up when that session next goes idle. "
                : "It will interrupt that session at its next safe point. ") +
              "Any reply arrives as a message in this session.",
          }
        },
      })

      tools.add({
        name: "peer_broadcast",
        description:
          "Send the same message to several sessions at once — for status sweeps like asking every " +
          "active session what it is working on. Each recipient wakes and handles it independently, " +
          "and any replies arrive here as separate messages. Prefer peer_send for a single target, " +
          "and peer_read when you only need information peers have already produced.",
        options: { codemode: false, permission: "peer_send" },
        input: {
          type: "object",
          properties: {
            message: { type: "string", description: "What to say. It must stand alone for every recipient." },
            to: {
              type: "array",
              items: { type: "string" },
              description: "Session ids to message. Omit to reach every top-level session in this directory.",
            },
            delivery: {
              type: "string",
              enum: ["queue", "steer"],
              description: "queue (default) waits for each peer to finish; steer interrupts them.",
            },
          },
          required: ["message"],
        },
        execute: async (
          input: { message: string; to?: string[]; delivery?: "queue" | "steer" },
          context: SenderContext,
        ) => {
          await assertPermission(ctx, context, "peer_send", input.to?.length ? input.to : ["*"], {
            tool: "peer_broadcast",
          })
          if (!input.message?.trim()) throw new Error("peer_broadcast requires a non-empty message")

          const delivery = input.delivery === "steer" ? "steer" : "queue"
          const all = await sessions()
          const self = all.find((s: SessionLike) => s.id === context.sessionID)

          const targets: BroadcastTarget[] = (
            input.to?.length
              ? input.to.map((id) => all.find((s: SessionLike) => s.id === id) ?? { id, missing: true })
              : all.filter(
                  (s: SessionLike) =>
                    s.id !== context.sessionID &&
                    !s.parentID &&
                    (!self || s.location?.directory === self.location?.directory),
                )
          ).filter((s: BroadcastTarget) => s.id !== context.sessionID)

          if (targets.length === 0) return { content: "No recipients matched — nothing was sent." }

          const limit = policy(ctx).fanout
          if (targets.length > limit)
            throw new Error(
              `peer_broadcast would reach ${targets.length} sessions, above the configured fanout limit of ${limit}`,
            )

          rateLimit(ctx, "broadcast", context.sessionID)
          const from = basename(self?.location?.directory ?? "")
          const results = await Promise.all(
            targets.map(async (target: BroadcastTarget) => {
              if (target.missing) return `${target.id} — no such session`
              try {
                assertDeliverable(ctx, self, target)
                await ctx.session.synthetic({
                  sessionID: target.id,
                  description: `Broadcast from ${from}/${context.agent}`,
                  delivery,
                  resume: true,
                  metadata: { ...provenance(context, self), broadcast: true },
                  text: envelope(context, from, input.message),
                })
                return `${target.id} [${basename(target.location?.directory ?? "")}/${target.agent ?? "?"}] — delivered`
              } catch (error: unknown) {
                return `${target.id} — failed: ${(error as { message?: string } | undefined)?.message ?? "unknown error"}`
              }
            }),
          )

          return {
            content: [`Broadcast via ${delivery} to ${targets.length} session(s):`, ...results].join("\n"),
          }
        },
      })

      tools.add({
        name: "peer_spawn",
        description:
          "Create a new top-level session in a directory and give it an opening instruction. Unlike a " +
          "subagent this is a persistent, user-visible session that runs in that directory's own " +
          "config and outlives this turn — use it to hand work to another repository. It replies with " +
          "peer_send, so finish your turn and watch for its message.",
        options: { codemode: false, permission: "peer_send" },
        input: {
          type: "object",
          properties: {
            directory: {
              type: "string",
              description: "Absolute path the new session should work in. Must already exist.",
            },
            message: { type: "string", description: "The opening instruction for the new session." },
            title: { type: "string", description: "Optional title, so it is recognisable in peer_list." },
            agent: {
              type: "string",
              description: "Optional agent id, e.g. build or plan. Defaults to the host default.",
            },
          },
          required: ["directory", "message"],
        },
        execute: async (
          input: { directory: string; message: string; title?: string; agent?: string },
          context: SenderContext,
        ) => {
          await assertPermission(ctx, context, "peer_send", [input.directory], { tool: "peer_spawn" })
          if (!policy(ctx).spawn) throw new Error("peer_spawn is disabled by configuration")
          if (!input.message?.trim()) throw new Error("peer_spawn requires a non-empty message")
          if (!isAbsolute(input.directory)) throw new Error("directory must be an absolute path")
          if (!statSync(input.directory, { throwIfNoEntry: false })?.isDirectory())
            throw new Error(`no such directory: ${input.directory}`)

          const created = await ctx.session.create({
            location: { directory: input.directory },
            ...(input.title ? { title: input.title } : {}),
            ...(input.agent ? { agent: input.agent } : {}),
          })

          const id = created?.id ?? created?.data?.id
          if (!id) throw new Error("session was created but no id was returned")

          const self = (await sessions()).find((s: SessionLike) => s.id === context.sessionID)
          const from = basename(self?.location?.directory ?? "")

          await ctx.session.synthetic({
            sessionID: id,
            description: `Task from ${from}/${context.agent}`,
            delivery: "queue",
            resume: true,
            metadata: { ...provenance(context, self), spawned: true },
            text: envelope(context, from, input.message),
          })

          return {
            content:
              `Spawned ${id} in ${input.directory}${input.agent ? ` on the ${input.agent} agent` : ""} and sent ` +
              "the opening instruction. It is a normal session — you can peer_read it, and its reply " +
              "will arrive here as a message.",
          }
        },
      })
    })
  },
}
