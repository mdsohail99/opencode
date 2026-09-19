import { Clock, Effect, Exit, Option, Schema } from "effect"
import { Tool } from "./tool"
import { BackgroundJob } from "@/background/job"
import { Session } from "../session/session"
import { Agent } from "@/agent/agent"
import { renderOutput, type TaskPromptOps } from "./task"
import type { SessionID } from "../session/schema"
import { errorMessage } from "../util/error"
import type { SessionV1 } from "@opencode-ai/core/v1/session"
import type { ModelV2 } from "@opencode-ai/core/model"
import type { ProviderV2 } from "@opencode-ai/core/provider"

/** Jobs already handed back by next_agent. Process-local, matching the registry. */
const drained = new Set<string>()

export function clearDrainedCache() {
  drained.clear()
}

const NEXT_AGENT_DESCRIPTION = [
  "Block until the next background subagent of this session finishes, then return its result.",
  "Use it to collect the result of a task launched with background=true instead of re-asking it for status.",
  "Each result is returned exactly once; a later call hands back the next pending result.",
  "If nothing has finished yet it waits (honoring timeoutSeconds) for the first running task to finish.",
].join(" ")

const AGENTS_STATUS_DESCRIPTION = [
  "Hierarchical snapshot of background subagents and swarm status with stall detection.",
  "Renders parent-child agent relationships recursively with box-drawing prefixes and detects stalled workers.",
].join(" ")

const MANAGE_AGENTS_DESCRIPTION = [
  "Manage subagent swarm lifecycle: kill, kill_all, inspect, or restart background workers.",
].join(" ")

const ASK_AGENT_DESCRIPTION = [
  "Query an agent out-of-band without interrupting its running task or causing concurrency collisions.",
].join(" ")

export const Parameters = Schema.Struct({
  timeoutSeconds: Schema.optional(Schema.Number).annotate({
    description:
      "Maximum seconds to wait for a running background task to finish before returning. Omit to wait indefinitely.",
  }),
})

const NoParameters = Schema.Struct({})

export const ManageAgentsParameters = Schema.Struct({
  action: Schema.Literals(["kill", "kill_all", "inspect", "restart"]).annotate({
    description: "Action to perform: 'kill', 'kill_all', 'inspect', or 'restart'.",
  }),
  target_id: Schema.optional(Schema.String).annotate({
    description:
      "Target session ID, job ID, or agent name/role (e.g. 'Backend', 'Frontend Lead'). Required for 'kill', 'inspect', 'restart'. For 'kill_all', defaults to current session's swarm.",
  }),
  prompt: Schema.optional(Schema.String).annotate({
    description: "Optional instruction or guidance to provide when resuming or restarting the agent.",
  }),
})

export const AskAgentParameters = Schema.Struct({
  target_id: Schema.String.annotate({
    description: "Target session ID, job ID, or agent name/role (e.g. 'Backend', 'Frontend Lead') to query.",
  }),
  prompt: Schema.String.annotate({
    description: "Question or prompt for the agent.",
  }),
})

export interface AskAgentMetadata {
  target_id?: string
  ephemeral_id?: string
}

function own(job: BackgroundJob.Info, sessionID: SessionID) {
  return job.metadata?.background === true && job.metadata?.parentSessionId === sessionID
}

function elapsed(ms: number) {
  const seconds = Math.floor(ms / 1000)
  if (seconds < 60) return `${seconds}s`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m ${seconds % 60}s`
  const hours = Math.floor(minutes / 60)
  return `${hours}h ${minutes % 60}m`
}

function preview(text: string) {
  const flat = text.replace(/\s+/g, " ").trim()
  return flat.length > 200 ? `${flat.slice(0, 200)}…` : flat
}

function title(job: BackgroundJob.Info) {
  return job.title ? `Background task ${job.status}: ${job.title}` : `Background task ${job.status}`
}

function render(job: BackgroundJob.Info) {
  return renderOutput({
    sessionID: job.id,
    state: job.status,
    summary: title(job),
    text: job.status === "error" ? (job.error ?? "") : (job.output ?? ""),
  })
}

interface AgentTreeNode {
  id: string
  job?: BackgroundJob.Info
  session?: Session.Info
  children: AgentTreeNode[]
}

function countTreeNodes(nodes: AgentTreeNode[]): number {
  return nodes.reduce((acc, node) => acc + 1 + countTreeNodes(node.children), 0)
}

function renderTree(
  nodes: AgentTreeNode[],
  now: number,
  drainedSet: Set<string>,
  prefix = "",
): string[] {
  const lines: string[] = []
  nodes.forEach((node, index) => {
    const isLast = index === nodes.length - 1
    const branch = isLast ? "└─ " : "├─ "
    const childPrefix = prefix + (isLast ? "   " : "│  ")

    const job = node.job
    const session = node.session
    const status = job?.status ?? (session?.time.archived ? "completed" : "running")
    const isDrained = drainedSet.has(node.id) || session?.metadata?.drained === true
    const consumed = isDrained ? " (drained)" : ""

    const lastActiveAt =
      typeof job?.metadata?.last_active_at === "number"
        ? (job.metadata.last_active_at as number)
        : (session?.time.updated ?? job?.started_at ?? now)
    const isStalled = status === "running" && (now - lastActiveAt > 180_000)
    const stalledTag = isStalled ? " [stalled]" : ""

    const startedAt = job?.started_at ?? session?.time.created ?? now
    const timeStr = elapsed(now - startedAt)
    const jobTitle = job?.title ?? session?.title ?? ""
    const titleStr = jobTitle ? ` ${jobTitle}` : ""

    const head = `${prefix}${branch}${node.id} [${status}]${stalledTag}${consumed}${titleStr} ${timeStr}`
    lines.push(head)

    if (status !== "running") {
      const text = job?.status === "error" ? (job.error ?? "") : (job?.output ?? "")
      if (text) {
        lines.push(`${childPrefix}${preview(text)}`)
      }
    }

    if (node.children.length > 0) {
      lines.push(...renderTree(node.children, now, drainedSet, childPrefix))
    }
  })
  return lines
}

export const NextAgentTool = Tool.define(
  "next_agent",
  Effect.gen(function* () {
    const background = yield* BackgroundJob.Service
    const sessions = yield* Session.Service

    const isDrained = (jobID: string) =>
      Effect.gen(function* () {
        if (drained.has(jobID)) return true
        const s = yield* sessions.get(jobID as SessionID).pipe(Effect.option)
        if (Option.isSome(s) && s.value.metadata?.drained === true) {
          drained.add(jobID)
          return true
        }
        return false
      })

    const markDrained = (jobID: string) =>
      Effect.gen(function* () {
        drained.add(jobID)
        const s = yield* sessions.get(jobID as SessionID).pipe(Effect.option)
        if (Option.isSome(s)) {
          const nextMeta = { ...s.value.metadata, drained: true }
          yield* sessions.update(jobID as SessionID, { metadata: nextMeta }).pipe(Effect.ignore)
        }
      })

    const run = Effect.fn("NextAgentTool.execute")(function* (
      params: Schema.Schema.Type<typeof Parameters>,
      ctx: Tool.Context,
    ) {
      const jobs = (yield* background.list()).filter((job) => own(job, ctx.sessionID))
      const pending: BackgroundJob.Info[] = []
      for (const job of jobs) {
        if (yield* isDrained(job.id)) continue
        pending.push(job)
      }

      const terminal = pending.filter((job) => job.status !== "running")
      if (terminal.length > 0) {
        const job = terminal[0]
        yield* markDrained(job.id)
        return { title: title(job), metadata: {}, output: render(job) }
      }
      const running = pending.filter((job) => job.status === "running")
      if (running.length > 0) {
        const waited = yield* Effect.raceAll(
          running.map((job) =>
            background.wait({
              id: job.id,
              ...(params.timeoutSeconds !== undefined ? { timeout: params.timeoutSeconds * 1000 } : {}),
            }),
          ),
        )
        if (waited.timedOut) {
          return {
            title: "Background tasks still running",
            metadata: {},
            output: `Timed out waiting for background tasks to finish; ${running.length} still running.`,
          }
        }
        const job = waited.info
        if (!job) {
          return { title: "No background tasks", metadata: {}, output: "no background tasks" }
        }
        yield* markDrained(job.id)
        return { title: title(job), metadata: {}, output: render(job) }
      }
      return { title: "No background tasks", metadata: {}, output: "no background tasks" }
    })

    return {
      description: NEXT_AGENT_DESCRIPTION,
      parameters: Parameters,
      execute: (params: Schema.Schema.Type<typeof Parameters>, ctx: Tool.Context) =>
        run(params, ctx).pipe(Effect.orDie),
    }
  }),
)

export const AgentsStatusTool = Tool.define(
  "agents_status",
  Effect.gen(function* () {
    const background = yield* BackgroundJob.Service
    const sessions = yield* Session.Service

    const run = Effect.fn("AgentsStatusTool.execute")(function* (
      _params: Schema.Schema.Type<typeof NoParameters>,
      ctx: Tool.Context,
    ) {
      const allJobs = yield* background.list()

      const buildTree = (
        parentID: string,
        visited: Set<string>,
      ): Effect.Effect<AgentTreeNode[]> =>
        Effect.gen(function* () {
          const directChildJobs = allJobs.filter((job) => job.metadata?.parentSessionId === parentID)
          const directChildSessions = yield* sessions.children(parentID as SessionID)

          const childIds = new Set<string>()
          for (const job of directChildJobs) {
            if (job.metadata?.background === true || job.metadata?.parentSessionId === parentID) {
              childIds.add(job.id)
            }
          }
          for (const s of directChildSessions) {
            if (s.metadata?.background === true || childIds.has(s.id)) {
              childIds.add(s.id)
            }
          }

          const nodes: AgentTreeNode[] = []
          for (const id of childIds) {
            if (visited.has(id)) continue
            visited.add(id)

            const job = allJobs.find((j) => j.id === id)
            const sOpt = yield* sessions.get(id as SessionID).pipe(Effect.option)
            const session = directChildSessions.find((s: Session.Info) => s.id === id) ?? (Option.isSome(sOpt) ? sOpt.value : undefined)

            const children = yield* buildTree(id, visited)
            nodes.push({ id, job, session, children })
          }

          return nodes.sort((a, b) => {
            const aTime = a.job?.started_at ?? a.session?.time.created ?? 0
            const bTime = b.job?.started_at ?? b.session?.time.created ?? 0
            return aTime - bTime
          })
        })

      const roots = yield* buildTree(ctx.sessionID, new Set([ctx.sessionID]))
      if (roots.length === 0) {
        return {
          title: "No background tasks",
          metadata: {},
          output: "No background tasks running for this session.",
        }
      }

      const now = yield* Clock.currentTimeMillis
      const lines = renderTree(roots, now, drained)
      const count = countTreeNodes(roots)

      return {
        title: `Background tasks (${count})`,
        metadata: {},
        output: lines.join("\n"),
      }
    })

    return {
      description: AGENTS_STATUS_DESCRIPTION,
      parameters: NoParameters,
      execute: (params: Schema.Schema.Type<typeof NoParameters>, ctx: Tool.Context) =>
        run(params, ctx).pipe(Effect.orDie),
    }
  }),
)

interface ResolvedTarget {
  id: string
  job?: BackgroundJob.Info
  session?: Session.Info
  agentInfo?: Agent.Info
}

const resolveTarget = (
  target: string,
  allJobs: BackgroundJob.Info[],
  sessions: Session.Interface,
  agents: Agent.Interface,
  parentID?: string,
): Effect.Effect<ResolvedTarget | undefined> =>
  Effect.gen(function* () {
    const directJob = allJobs.find((j) => j.id === target)
    const directSession = yield* sessions.get(target as SessionID).pipe(Effect.option)
    if (directJob || Option.isSome(directSession)) {
      return {
        id: target,
        job: directJob,
        session: Option.isSome(directSession) ? directSession.value : undefined,
      }
    }

    const query = target.toLowerCase().trim()
    const matchingJob =
      allJobs.find(
        (j) =>
          j.status === "running" &&
          (j.title?.toLowerCase().includes(query) || (j.metadata?.agent as string)?.toLowerCase().includes(query)),
      ) ??
      allJobs.find(
        (j) =>
          j.title?.toLowerCase().includes(query) || (j.metadata?.agent as string)?.toLowerCase().includes(query),
      )

    if (matchingJob) {
      const s = yield* sessions.get(matchingJob.id as SessionID).pipe(Effect.option)
      return {
        id: matchingJob.id,
        job: matchingJob,
        session: Option.isSome(s) ? s.value : undefined,
      }
    }

    if (parentID) {
      const children = yield* sessions.children(parentID as SessionID)
      const matchingChild =
        children.find(
          (s: Session.Info) =>
            !s.time.archived &&
            (s.agent?.toLowerCase().includes(query) || s.title?.toLowerCase().includes(query)),
        ) ??
        children.find(
          (s: Session.Info) => s.agent?.toLowerCase().includes(query) || s.title?.toLowerCase().includes(query),
        )
      if (matchingChild) {
        const j = allJobs.find((item) => item.id === matchingChild.id)
        return {
          id: matchingChild.id,
          job: j,
          session: matchingChild,
        }
      }
    }

    const agentList = yield* agents.list().pipe(Effect.orElseSucceed(() => []))
    const matchingAgent =
      agentList.find((a) => a.name.toLowerCase() === query) ??
      agentList.find((a) => a.name.toLowerCase().includes(query))
    if (matchingAgent) {
      return {
        id: matchingAgent.name,
        agentInfo: matchingAgent,
      }
    }

    return undefined
  })

export const ManageAgentsTool = Tool.define(
  "manage_agents",
  Effect.gen(function* () {
    const background = yield* BackgroundJob.Service
    const sessions = yield* Session.Service
    const agents = yield* Agent.Service

    const run = Effect.fn("ManageAgentsTool.execute")(function* (
      params: Schema.Schema.Type<typeof ManageAgentsParameters>,
      ctx: Tool.Context,
    ) {
      const ops = ctx.extra?.promptOps as TaskPromptOps | undefined
      const allJobs = yield* background.list()
      const now = yield* Clock.currentTimeMillis
      const resolved = params.target_id
        ? yield* resolveTarget(params.target_id, allJobs, sessions, agents, ctx.sessionID)
        : undefined
      const targetId = resolved?.id ?? params.target_id

      if (params.action === "kill") {
        if (!targetId) {
          return { title: "manage_agents error", metadata: {}, output: "Error: target_id is required for 'kill'." }
        }
        yield* background.cancel(targetId)
        if (ops?.cancel) {
          yield* ops.cancel(targetId as SessionID).pipe(Effect.ignore)
        }
        return {
          title: `Agent killed: ${targetId}`,
          metadata: {},
          output: `Agent '${targetId}' has been cancelled.`,
        }
      }

      if (params.action === "kill_all") {
        const rootID =
          params.target_id && params.target_id.toLowerCase() !== "all"
            ? (resolved?.id ?? params.target_id)
            : ctx.sessionID
        const descendants: string[] = []
        const queue = [rootID]
        const visited = new Set<string>([rootID])

        while (queue.length > 0) {
          const curr = queue.shift()!
          const childJobs = allJobs.filter((j) => j.metadata?.parentSessionId === curr)
          const childSessions = yield* sessions.children(curr as SessionID)
          for (const j of childJobs) {
            if (!visited.has(j.id)) {
              visited.add(j.id)
              descendants.push(j.id)
              queue.push(j.id)
            }
          }
          for (const s of childSessions) {
            if (!visited.has(s.id)) {
              visited.add(s.id)
              descendants.push(s.id)
              queue.push(s.id)
            }
          }
        }

        const cancelledIds: string[] = []
        for (const id of descendants) {
          const j = allJobs.find((item) => item.id === id)
          if (!j || j.status === "running") {
            yield* background.cancel(id)
            if (ops?.cancel) yield* ops.cancel(id as SessionID).pipe(Effect.ignore)
            cancelledIds.push(id)
          }
        }

        return {
          title: `Kill all: ${cancelledIds.length} agents cancelled`,
          metadata: {},
          output: `Cancelled ${cancelledIds.length} active descendant agent(s): ${cancelledIds.join(", ") || "none"}`,
        }
      }

      if (params.action === "inspect") {
        if (!targetId) {
          return { title: "manage_agents error", metadata: {}, output: "Error: target_id is required for 'inspect'." }
        }
        const job = resolved?.job ?? (yield* background.get(targetId))
        const session =
          resolved?.session ??
          (yield* sessions.get(targetId as SessionID).pipe(Effect.option).pipe(Effect.map(Option.getOrUndefined)))

        if (!job && !session) {
          if (resolved?.agentInfo) {
            const a = resolved.agentInfo
            return {
              title: `Inspect: ${targetId}`,
              metadata: {},
              output: [
                `### Agent Persona: ${a.name}`,
                `- **Description**: ${a.description ?? "none"}`,
                `- **Mode**: ${a.mode}`,
                `- **Model**: ${a.model ? `${a.model.providerID}/${a.model.modelID}` : "default"}`,
                `- **Native**: ${a.native ? "true" : "false"}`,
              ].join("\n"),
            }
          }
          return {
            title: "Agent not found",
            metadata: {},
            output: `Agent or job '${params.target_id}' was not found.`,
          }
        }

        const status = job?.status ?? (session?.time.archived ? "completed" : "running")
        const startedAt = job?.started_at ?? session?.time.created ?? 0
        const completedAt = job?.completed_at ? new Date(job.completed_at).toISOString() : "N/A (running)"
        const errorText = job?.error ?? "None"
        const outputText = job?.output ?? "(none)"
        const parentSessionId = (job?.metadata?.parentSessionId as string) ?? session?.parentID ?? "unknown"
        const isDrained = drained.has(targetId) || session?.metadata?.drained === true

        const infoMarkdown = [
          `### Agent Inspection: ${targetId}`,
          `- **Title**: ${job?.title ?? session?.title ?? "untitled"}`,
          `- **Status**: ${status}`,
          `- **Started At**: ${startedAt ? new Date(startedAt).toISOString() : "unknown"}`,
          `- **Completed At**: ${completedAt}`,
          `- **Elapsed**: ${startedAt ? elapsed(now - startedAt) : "unknown"}`,
          `- **Parent Session**: ${parentSessionId}`,
          `- **Drained**: ${isDrained}`,
          `- **Error**: ${errorText}`,
          `- **Output**:`,
          "```",
          outputText,
          "```",
        ].join("\n")

        return {
          title: `Inspect: ${targetId}`,
          metadata: {},
          output: infoMarkdown,
        }
      }

      if (params.action === "restart") {
        if (!targetId) {
          return { title: "manage_agents error", metadata: {}, output: "Error: target_id is required for 'restart'." }
        }
        yield* background.cancel(targetId)
        if (ops?.cancel) yield* ops.cancel(targetId as SessionID).pipe(Effect.ignore)

        const oldJob = resolved?.job ?? (yield* background.get(targetId))
        const targetSession =
          resolved?.session ??
          (yield* sessions.get(targetId as SessionID).pipe(Effect.option).pipe(Effect.map(Option.getOrUndefined)))

        drained.delete(targetId)
        if (targetSession) {
          yield* sessions.update(targetId as SessionID, {
            metadata: { ...targetSession.metadata, drained: false },
          }).pipe(Effect.ignore)
        }

        const runTask = Effect.gen(function* () {
          if (ops) {
            const msgs = yield* sessions.messages({ sessionID: targetId as SessionID }).pipe(Effect.option)
            const userMsg = Option.isSome(msgs) ? msgs.value.find((m) => m.info.role === "user") : undefined
            const promptText =
              params.prompt ?? userMsg?.parts.find((p) => p.type === "text")?.text ?? oldJob?.title ?? "Restarted task"
            const parts = yield* ops.resolvePromptParts(promptText)
            const res = yield* ops.prompt({
              sessionID: targetId as SessionID,
              agent: (oldJob?.metadata?.agent as string) ?? targetSession?.agent ?? "general",
              parts,
            })
            return res.parts.findLast((p) => p.type === "text")?.text ?? ""
          }
          return "restarted"
        })

        yield* background.start({
          id: targetId,
          type: oldJob?.type ?? "task",
          title: oldJob?.title ?? `Restarted task ${targetId}`,
          metadata: {
            ...oldJob?.metadata,
            background: true,
            restarted_at: yield* Clock.currentTimeMillis,
          },
          run: runTask,
        })

        return {
          title: `Agent restarted: ${params.target_id}`,
          metadata: {},
          output: `Agent '${params.target_id}' restarted successfully.`,
        }
      }

      return {
        title: "manage_agents error",
        metadata: {},
        output: `Unknown action '${params.action}'.`,
      }
    })

    return {
      description: MANAGE_AGENTS_DESCRIPTION,
      parameters: ManageAgentsParameters,
      execute: (params: Schema.Schema.Type<typeof ManageAgentsParameters>, ctx: Tool.Context) =>
        run(params, ctx).pipe(Effect.orDie),
    }
  }),
)

export const AskAgentTool = Tool.define(
  "ask_agent",
  Effect.gen(function* () {
    const background = yield* BackgroundJob.Service
    const sessions = yield* Session.Service
    const agents = yield* Agent.Service

    const run = Effect.fn("AskAgentTool.execute")(function* (
      params: Schema.Schema.Type<typeof AskAgentParameters>,
      ctx: Tool.Context,
    ) {
      const allJobs = yield* background.list()
      const resolved = yield* resolveTarget(params.target_id, allJobs, sessions, agents, ctx.sessionID)
      if (!resolved) {
        return {
          title: "ask_agent error",
          metadata: { target_id: params.target_id },
          output: `Error: Target agent or session '${params.target_id}' not found. Use agents_status to check running agents or specify a known agent persona (e.g. 'general', 'explore').`,
        }
      }

      const targetId = resolved.id
      const targetSession =
        resolved.session ??
        (yield* sessions.get(targetId as SessionID).pipe(Effect.option).pipe(Effect.map(Option.getOrUndefined)))

      const ops = ctx.extra?.promptOps as TaskPromptOps | undefined
      if (!ops) {
        return {
          title: "ask_agent error",
          metadata: { target_id: targetId },
          output: "Error: ask_agent requires promptOps in execution context.",
        }
      }

      const agentName = targetSession?.agent ?? resolved.agentInfo?.name ?? targetId
      const ephemeral = yield* sessions.create({
        parentID: targetSession?.id ?? ctx.sessionID,
        title: `[Ephemeral Side-Query] ${targetSession?.title ?? agentName}`,
        agent: agentName,
        model: targetSession?.model,
        workspaceID: targetSession?.workspaceID,
        permission: [{ permission: "*", pattern: "*", action: "deny" }],
      })

      return yield* Effect.gen(function* () {
        let contextPrefix = ""
        if (targetSession) {
          const msgs = yield* sessions.messages({ sessionID: targetSession.id, limit: 10 }).pipe(Effect.option)
          if (Option.isSome(msgs) && msgs.value.length > 0) {
            const history = msgs.value
              .flatMap((m) => m.parts.filter((p) => p.type === "text").map((p) => `${m.info.role}: ${preview(p.text)}`))
              .slice(-5)
              .join("\n")
            if (history) {
              contextPrefix = `[Context from target agent ${targetSession.agent ?? targetId}]:\n${history}\n\n`
            }
          }
        }

        const sideQuestionReminder = `<system-reminder>
This is a side question from the user. You MUST answer directly in a single response.
Your tools are disabled. You cannot read files, write files, run commands, search, or take any actions.
You must complete your response in a single turn with no follow-up turns.
Only use information already in the conversation context or your own knowledge.
Never promise to take action or say "let me check...". Do not write simulated tool calls or tool output blocks as text.
If answering requires reading files or executing commands, state that it cannot be checked from a side question and suggest asking in the main conversation.
</system-reminder>`

        const fullPrompt = `${sideQuestionReminder}\n\n${contextPrefix}[Side-Question]: ${params.prompt}`
        const parts = yield* ops.resolvePromptParts(fullPrompt)

        let promptModel: { modelID: ModelV2.ID; providerID: ProviderV2.ID } | undefined = targetSession?.model
          ? {
              modelID: targetSession.model.id,
              providerID: targetSession.model.providerID,
            }
          : resolved.agentInfo?.model
            ? {
                modelID: resolved.agentInfo.model.modelID,
                providerID: resolved.agentInfo.model.providerID,
              }
            : undefined

        if (!promptModel && targetSession) {
          const msgs = yield* sessions.messages({ sessionID: targetSession.id, limit: 5 }).pipe(Effect.option)
          if (Option.isSome(msgs)) {
            for (const m of msgs.value) {
              if (m.info.role === "assistant") {
                const asst = m.info as SessionV1.Assistant
                if (asst.modelID && asst.providerID) {
                  promptModel = { modelID: asst.modelID, providerID: asst.providerID }
                  break
                }
              }
            }
          }
        }

        const promptExit = yield* Effect.exit(
          ops.prompt({
            sessionID: ephemeral.id,
            agent: agentName,
            model: promptModel,
            tools: { "*": false },
            parts,
          }),
        )

        const answer = Exit.isSuccess(promptExit)
          ? promptExit.value.parts
              .flatMap((p: SessionV1.Part) => (p.type === "text" ? [p.text] : []))
              .join("\n\n") || "No response received."
          : `[Side-query failed: ${errorMessage(promptExit.cause)}]`

        return {
          title: `Answer from ${agentName}`,
          metadata: { target_id: targetId, ephemeral_id: ephemeral.id },
          output: answer,
        }
      }).pipe(
        Effect.ensuring(sessions.remove(ephemeral.id).pipe(Effect.ignore)),
      )
    })

    return {
      description: ASK_AGENT_DESCRIPTION,
      parameters: AskAgentParameters,
      execute: (params: Schema.Schema.Type<typeof AskAgentParameters>, ctx: Tool.Context) =>
        run(params, ctx).pipe(Effect.orDie),
    }
  }),
)
