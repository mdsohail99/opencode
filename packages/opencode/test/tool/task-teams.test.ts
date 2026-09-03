import { afterEach, describe, expect } from "bun:test"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { Deferred, Effect, Fiber, Layer, Option } from "effect"
import { Agent } from "../../src/agent/agent"
import { Account } from "@/account/account"
import { BackgroundJob } from "@/background/job"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Config } from "@/config/config"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Ripgrep } from "@opencode-ai/core/ripgrep"
import { Session } from "@/session/session"
import type { SessionPrompt } from "../../src/session/prompt"
import { MessageID, PartID, SessionID } from "../../src/session/schema"
import { SessionRunState } from "@/session/run-state"
import { SessionStatus } from "@/session/status"

import {
  NextAgentTool,
  AgentsStatusTool,
  ManageAgentsTool,
  AskAgentTool,
  clearDrainedCache,
} from "../../src/tool/task-teams"
import type { TaskPromptOps } from "../../src/tool/task"
import { Truncate } from "@/tool/truncate"
import { ToolRegistry } from "@/tool/registry"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { InstanceBootstrap } from "../../src/project/bootstrap"
import { InstanceStore } from "../../src/project/instance-store"
import { Vcs } from "../../src/project/vcs"
import { Format } from "../../src/format"
import { LSP } from "../../src/lsp/lsp"
import { Project } from "../../src/project/project"
import { ShareNext } from "../../src/share/share-next"
import { Snapshot } from "../../src/snapshot"
import { disposeAllInstances } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"

afterEach(async () => {
  await disposeAllInstances()
  clearDrainedCache()
})

delete process.env["OPENCODE_CONFIG_DIR"]

const ref = {
  providerID: ProviderV2.ID.make("test"),
  modelID: ModelV2.ID.make("test-model"),
}

const layer = (flags: Partial<RuntimeFlags.Info> = {}) =>
  LayerNode.compile(
    LayerNode.group([
      Agent.node,
      BackgroundJob.node,
      EventV2Bridge.node,
      Config.node,
      CrossSpawnSpawner.node,
      Session.node,
      SessionProjector.node,
      SessionRunState.node,
      SessionStatus.node,
      Truncate.node,
      ToolRegistry.node,
      Database.node,
      RuntimeFlags.node,
      Ripgrep.node,
      Vcs.node,
      Format.node,
      LSP.node,
      Project.node,
      ShareNext.node,
      Snapshot.node,
    ]),
    [
      [RuntimeFlags.node, RuntimeFlags.layer(flags)],
      [InstanceStore.bootstrapNode, InstanceBootstrap.node],
      [Account.node, Layer.mock(Account.Service, { active: () => Effect.succeed(Option.none()) })],
    ],
  )

const it = testEffect(layer())

function stubOps(opts?: {
  onPrompt?: (input: SessionPrompt.PromptInput) => void
  text?: string
  error?: NonNullable<SessionV1.Assistant["error"]>
}): TaskPromptOps {
  return {
    cancel: () => Effect.void,
    resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
    prompt: (input) =>
      Effect.sync(() => {
        opts?.onPrompt?.(input)
        return reply(input, opts?.text ?? "done", opts?.error)
      }),
  }
}

function reply(
  input: SessionPrompt.PromptInput,
  text: string,
  error?: NonNullable<SessionV1.Assistant["error"]>,
): SessionV1.WithParts {
  const id = MessageID.ascending()
  return {
    info: {
      id,
      role: "assistant",
      parentID: input.messageID ?? MessageID.ascending(),
      sessionID: input.sessionID,
      mode: input.agent ?? "general",
      agent: input.agent ?? "general",
      cost: 0,
      path: { cwd: "/tmp", root: "/tmp" },
      tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      modelID: input.model?.modelID ?? ref.modelID,
      providerID: input.model?.providerID ?? ref.providerID,
      time: { created: Date.now() },
      finish: "stop",
      error,
    },
    parts: [
      {
        id: PartID.ascending(),
        messageID: id,
        sessionID: input.sessionID,
        type: "text",
        text,
      },
    ],
  }
}

const seed = Effect.fnUntraced(function* (title = "Orchestrator Session") {
  const sessions = yield* Session.Service
  const chat = yield* sessions.create({ title })
  const user = yield* sessions.updateMessage({
    id: MessageID.ascending(),
    role: "user",
    sessionID: chat.id,
    agent: "build",
    model: ref,
    time: { created: Date.now() },
  })
  const assistant: SessionV1.Assistant = {
    id: MessageID.ascending(),
    role: "assistant",
    parentID: user.id,
    sessionID: chat.id,
    mode: "build",
    agent: "build",
    cost: 0,
    path: { cwd: "/tmp", root: "/tmp" },
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    modelID: ref.modelID,
    providerID: ref.providerID,
    time: { created: Date.now() },
  }
  yield* sessions.updateMessage(assistant)
  return { chat, assistant }
})

describe("tool.task-teams", () => {
  describe("agents_status", () => {
    it.instance("returns empty notice when no background tasks exist", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const tool = yield* AgentsStatusTool
        const def = yield* tool.init()

        const result = yield* def.execute(
          {},
          {
            sessionID: chat.id,
            messageID: assistant.id,
            agent: "general",
            abort: new AbortController().signal,
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )

        expect(result.output).toBe("No background tasks running for this session.")
      }),
    )

    it.instance("renders a hierarchical parent-child tree with box-drawing prefixes", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const background = yield* BackgroundJob.Service
        const sessions = yield* Session.Service

        // Create Department Lead 1
        const lead1 = yield* sessions.create({
          parentID: chat.id,
          title: "Architecture Lead",
          metadata: { background: true },
        })
        yield* background.start({
          id: lead1.id,
          type: "task",
          title: "Architecture Lead",
          metadata: { background: true, parentSessionId: chat.id },
          run: Effect.never,
        })

        // Create Specialist 1 under Lead 1
        const spec1 = yield* sessions.create({
          parentID: lead1.id,
          title: "Database Specialist",
          metadata: { background: true },
        })
        yield* background.start({
          id: spec1.id,
          type: "task",
          title: "Database Specialist",
          metadata: { background: true, parentSessionId: lead1.id },
          run: Effect.never,
        })

        // Create Specialist 2 under Lead 1 (completed)
        const spec2 = yield* sessions.create({
          parentID: lead1.id,
          title: "Security Specialist",
          metadata: { background: true },
        })
        yield* background.start({
          id: spec2.id,
          type: "task",
          title: "Security Specialist",
          metadata: { background: true, parentSessionId: lead1.id },
          run: Effect.succeed("Security audit passed with 0 vulnerabilities"),
        })

        // Create Department Lead 2 (last root child)
        const lead2 = yield* sessions.create({
          parentID: chat.id,
          title: "QA Lead",
          metadata: { background: true },
        })
        yield* background.start({
          id: lead2.id,
          type: "task",
          title: "QA Lead",
          metadata: { background: true, parentSessionId: chat.id },
          run: Effect.never,
        })

        const tool = yield* AgentsStatusTool
        const def = yield* tool.init()

        const result = yield* def.execute(
          {},
          {
            sessionID: chat.id,
            messageID: assistant.id,
            agent: "general",
            abort: new AbortController().signal,
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )

        // Verify title count
        expect(result.title).toContain("Background tasks (4)")

        // Verify box-drawing prefixes
        expect(result.output).toContain("├─")
        expect(result.output).toContain("└─")
        expect(result.output).toContain("│")

        // Lead 1 is first child -> ├─
        expect(result.output).toMatch(new RegExp(`├─ ${lead1.id} \\[running\\] Architecture Lead`))

        // Specialist 1 is child of Lead 1 -> │  ├─
        expect(result.output).toMatch(new RegExp(`│  ├─ ${spec1.id} \\[running\\] Database Specialist`))

        // Specialist 2 is last child of Lead 1 -> │  └─
        expect(result.output).toMatch(new RegExp(`│  └─ ${spec2.id} \\[completed\\] Security Specialist`))

        // Lead 2 is last child of root -> └─
        expect(result.output).toMatch(new RegExp(`└─ ${lead2.id} \\[running\\] QA Lead`))
      }),
    )

    it.instance("detects stalled agents older than 3 minutes with [stalled] tag", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const background = yield* BackgroundJob.Service
        const sessions = yield* Session.Service

        const now = Date.now()

        // 1. Stalled worker: last_active_at was 4 minutes ago (240_000 ms)
        const stalledWorker = yield* sessions.create({
          parentID: chat.id,
          title: "Stalled Worker",
          metadata: { background: true },
        })
        yield* background.start({
          id: stalledWorker.id,
          type: "task",
          title: "Stalled Worker",
          metadata: {
            background: true,
            parentSessionId: chat.id,
            last_active_at: now - 240_000,
          },
          run: Effect.never,
        })

        // 2. Active worker: last_active_at was 10 seconds ago
        const activeWorker = yield* sessions.create({
          parentID: chat.id,
          title: "Active Worker",
          metadata: { background: true },
        })
        yield* background.start({
          id: activeWorker.id,
          type: "task",
          title: "Active Worker",
          metadata: {
            background: true,
            parentSessionId: chat.id,
            last_active_at: now - 10_000,
          },
          run: Effect.never,
        })

        const tool = yield* AgentsStatusTool
        const def = yield* tool.init()

        const result = yield* def.execute(
          {},
          {
            sessionID: chat.id,
            messageID: assistant.id,
            agent: "general",
            abort: new AbortController().signal,
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )

        // Stalled worker line must have [stalled] tag
        expect(result.output).toMatch(new RegExp(`${stalledWorker.id} \\[running\\] \\[stalled\\]`))

        // Active worker line must NOT have [stalled] tag
        expect(result.output).toMatch(new RegExp(`${activeWorker.id} \\[running\\] Active Worker`))
        expect(result.output).not.toMatch(new RegExp(`${activeWorker.id} \\[running\\] \\[stalled\\]`))
      }),
    )

    it.instance("resolves last_active_at fallback to session.time.updated or job.started_at", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const background = yield* BackgroundJob.Service
        const sessions = yield* Session.Service

        const oldTime = Date.now() - 250_000

        // Worker with session updated 250s ago, no explicit job.metadata.last_active_at
        const fallbackSessionWorker = yield* sessions.create({
          parentID: chat.id,
          title: "Fallback Session Worker",
          metadata: { background: true },
        })
        yield* sessions.update(fallbackSessionWorker.id, {
          time: { updated: oldTime },
        })
        yield* background.start({
          id: fallbackSessionWorker.id,
          type: "task",
          title: "Fallback Session Worker",
          metadata: {
            background: true,
            parentSessionId: chat.id,
          },
          run: Effect.never,
        })

        const tool = yield* AgentsStatusTool
        const def = yield* tool.init()

        const result = yield* def.execute(
          {},
          {
            sessionID: chat.id,
            messageID: assistant.id,
            agent: "general",
            abort: new AbortController().signal,
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )

        expect(result.output).toMatch(new RegExp(`${fallbackSessionWorker.id} \\[running\\] \\[stalled\\]`))
      }),
    )
  })

  describe("persistent drained state (next_agent)", () => {
    it.instance("persists drained state into SQLite session metadata and survives simulated restart", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const background = yield* BackgroundJob.Service
        const sessions = yield* Session.Service

        // Create completed background worker
        const subagent = yield* sessions.create({
          parentID: chat.id,
          title: "Database Migration Specialist",
          metadata: { background: true },
        })
        yield* background.start({
          id: subagent.id,
          type: "task",
          title: "Database Migration Specialist",
          metadata: { background: true, parentSessionId: chat.id },
          run: Effect.succeed("Migration applied successfully: 12 tables created"),
        })

        const nextTool = yield* NextAgentTool
        const nextDef = yield* nextTool.init()

        const ctx = {
          sessionID: chat.id,
          messageID: assistant.id,
          agent: "general",
          abort: new AbortController().signal,
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        }

        // 1. Consume job via next_agent
        const res1 = yield* nextDef.execute({}, ctx)
        expect(res1.output).toContain("Migration applied successfully: 12 tables created")

        // 2. Verify drained state is persisted in SQLite session metadata via sessions.get
        const sessionAfter = yield* sessions.get(subagent.id)
        expect(sessionAfter.metadata?.drained).toBe(true)

        // 3. Clear in-memory cache to simulate engine restart
        clearDrainedCache()

        // 4. Call next_agent again after restart; it should detect drained: true in SQLite and not re-deliver
        const res2 = yield* nextDef.execute({}, ctx)
        expect(res2.output).toBe("no background tasks")

        // 5. Verify agents_status reflects (drained) from SQLite metadata
        const statusTool = yield* AgentsStatusTool
        const statusDef = yield* statusTool.init()
        const statusRes = yield* statusDef.execute({}, ctx)
        expect(statusRes.output).toContain("(drained)")
      }),
    )
  })

  describe("manage_agents", () => {
    it.instance("action 'kill' cancels target background job and runner", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const background = yield* BackgroundJob.Service
        const sessions = yield* Session.Service

        const worker = yield* sessions.create({
          parentID: chat.id,
          title: "Long Running Task",
          metadata: { background: true },
        })
        yield* background.start({
          id: worker.id,
          type: "task",
          title: "Long Running Task",
          metadata: { background: true, parentSessionId: chat.id },
          run: Effect.never,
        })

        const tool = yield* ManageAgentsTool
        const def = yield* tool.init()

        let cancelledRunnerID = ""
        const result = yield* def.execute(
          { action: "kill", target_id: worker.id },
          {
            sessionID: chat.id,
            messageID: assistant.id,
            agent: "general",
            abort: new AbortController().signal,
            extra: {
              promptOps: {
                ...stubOps(),
                cancel: (id: SessionID) =>
                  Effect.sync(() => {
                    cancelledRunnerID = id
                  }),
              },
            },
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )

        expect(result.output).toContain(`Agent '${worker.id}' has been cancelled.`)
        const job = yield* background.get(worker.id)
        expect(job?.status).toBe("cancelled")
        expect(cancelledRunnerID).toBe(worker.id)
      }),
    )

    it.instance("action 'kill_all' cancels all active descendant jobs recursively", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const background = yield* BackgroundJob.Service
        const sessions = yield* Session.Service

        // Create Lead
        const lead = yield* sessions.create({
          parentID: chat.id,
          title: "Lead Worker",
          metadata: { background: true },
        })
        yield* background.start({
          id: lead.id,
          type: "task",
          title: "Lead Worker",
          metadata: { background: true, parentSessionId: chat.id },
          run: Effect.never,
        })

        // Create 2 Specialists under Lead
        const spec1 = yield* sessions.create({
          parentID: lead.id,
          title: "Child Worker 1",
          metadata: { background: true },
        })
        yield* background.start({
          id: spec1.id,
          type: "task",
          title: "Child Worker 1",
          metadata: { background: true, parentSessionId: lead.id },
          run: Effect.never,
        })

        const spec2 = yield* sessions.create({
          parentID: lead.id,
          title: "Child Worker 2",
          metadata: { background: true },
        })
        yield* background.start({
          id: spec2.id,
          type: "task",
          title: "Child Worker 2",
          metadata: { background: true, parentSessionId: lead.id },
          run: Effect.never,
        })

        const tool = yield* ManageAgentsTool
        const def = yield* tool.init()

        const result = yield* def.execute(
          { action: "kill_all" },
          {
            sessionID: chat.id,
            messageID: assistant.id,
            agent: "general",
            abort: new AbortController().signal,
            extra: { promptOps: stubOps() },
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )

        expect(result.output).toContain("Cancelled 3 active descendant agent(s)")

        const jobLead = yield* background.get(lead.id)
        const jobSpec1 = yield* background.get(spec1.id)
        const jobSpec2 = yield* background.get(spec2.id)

        expect(jobLead?.status).toBe("cancelled")
        expect(jobSpec1?.status).toBe("cancelled")
        expect(jobSpec2?.status).toBe("cancelled")
      }),
    )

    it.instance("action 'inspect' returns comprehensive diagnostic details", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const background = yield* BackgroundJob.Service
        const sessions = yield* Session.Service

        const worker = yield* sessions.create({
          parentID: chat.id,
          title: "Inspector Target Agent",
          metadata: { background: true },
        })
        yield* background.start({
          id: worker.id,
          type: "task",
          title: "Inspector Target Agent",
          metadata: { background: true, parentSessionId: chat.id },
          run: Effect.succeed("Detailed completion output here"),
        })

        const tool = yield* ManageAgentsTool
        const def = yield* tool.init()

        const result = yield* def.execute(
          { action: "inspect", target_id: worker.id },
          {
            sessionID: chat.id,
            messageID: assistant.id,
            agent: "general",
            abort: new AbortController().signal,
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )

        expect(result.output).toContain(`### Agent Inspection: ${worker.id}`)
        expect(result.output).toContain("- **Title**: Inspector Target Agent")
        expect(result.output).toContain("- **Status**: completed")
        expect(result.output).toContain(`- **Parent Session**: ${chat.id}`)
        expect(result.output).toContain("Detailed completion output here")
      }),
    )

    it.instance("action 'restart' cancels previous job and re-starts background task", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const background = yield* BackgroundJob.Service
        const sessions = yield* Session.Service

        const worker = yield* sessions.create({
          parentID: chat.id,
          title: "Worker To Restart",
          metadata: { background: true, drained: true },
        })
        yield* background.start({
          id: worker.id,
          type: "task",
          title: "Worker To Restart",
          metadata: { background: true, parentSessionId: chat.id },
          run: Effect.succeed("first run done"),
        })

        const tool = yield* ManageAgentsTool
        const def = yield* tool.init()

        const restartGate = yield* Deferred.make<void>()
        const promptOps: TaskPromptOps = {
          ...stubOps(),
          prompt: () =>
            Deferred.await(restartGate).pipe(
              Effect.as(reply({ sessionID: worker.id, parts: [] }, "restarted output")),
            ),
        }

        const result = yield* def.execute(
          { action: "restart", target_id: worker.id },
          {
            sessionID: chat.id,
            messageID: assistant.id,
            agent: "general",
            abort: new AbortController().signal,
            extra: { promptOps },
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )

        expect(result.output).toContain(`Agent '${worker.id}' restarted successfully.`)

        const job = yield* background.get(worker.id)
        expect(job?.status).toBe("running")

        const session = yield* sessions.get(worker.id)
        expect(session.metadata?.drained).toBe(false)

        yield* Deferred.succeed(restartGate, void 0)
      }),
    )
  })

  describe("ask_agent", () => {
    it.instance("performs out-of-band side-query without raising Session.BusyError while target session is active", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const sessions = yield* Session.Service
        const background = yield* BackgroundJob.Service
        const runState = yield* SessionRunState.Service

        // Create target worker session
        const workerSession = yield* sessions.create({
          parentID: chat.id,
          title: "Active DB Worker",
          agent: "build",
        })

        // Start running background job
        yield* background.start({
          id: workerSession.id,
          type: "task",
          title: "Active DB Worker",
          metadata: { background: true, parentSessionId: chat.id },
          run: Effect.never,
        })

        // Simulate active runner in SessionRunState
        const started = yield* Deferred.make<void>()
        const releaseRunner = yield* Deferred.make<void>()
        const promptInput = {
          sessionID: workerSession.id,
          parts: [{ type: "text" as const, text: "work" }],
        }
        const work = Deferred.succeed(started, void 0).pipe(
          Effect.andThen(Deferred.await(releaseRunner)),
          Effect.as(reply(promptInput, "done")),
        )

        const fiber = yield* runState
          .ensureRunning(
            workerSession.id,
            Effect.succeed(reply(promptInput, "interrupted")),
            work,
          )
          .pipe(Effect.forkChild)

        // Wait until runner work has actually started and is suspended
        yield* Deferred.await(started)

        // Verify that target session runner is indeed busy: assertNotBusy throws Session.BusyError
        const busyCheck = yield* runState.assertNotBusy(workerSession.id).pipe(
          Effect.match({
            onFailure: (err) => err,
            onSuccess: () => undefined,
          }),
        )
        expect(busyCheck).toBeInstanceOf(Session.BusyError)

        // Now invoke ask_agent against this active, busy worker
        const askTool = yield* AskAgentTool
        const def = yield* askTool.init()

        let queriedSessionID: SessionID | undefined
        let queriedAgent: string | undefined

        const promptOps = stubOps({
          text: "The schema uses snake_case column names.",
          onPrompt: (input) => {
            queriedSessionID = input.sessionID
            queriedAgent = input.agent
          },
        })

        const result = yield* def.execute(
          { target_id: workerSession.id, prompt: "What column naming convention is used?" },
          {
            sessionID: chat.id,
            messageID: assistant.id,
            agent: "general",
            abort: new AbortController().signal,
            extra: { promptOps },
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )

        // 1. Returns out-of-band answer
        expect(result.output).toBe("The schema uses snake_case column names.")

        // 2. Ephemeral session was queried, NOT the active workerSession.id
        expect(queriedSessionID).toBeDefined()
        expect(queriedSessionID).not.toBe(workerSession.id)
        expect(queriedAgent).toBe("build")

        // 3. Ephemeral session was cleanly removed from SQLite
        if (queriedSessionID) {
          const checkEphemeral = yield* sessions.get(queriedSessionID).pipe(Effect.option)
          expect(Option.isNone(checkEphemeral)).toBe(true)
        }

        // 4. Target worker session runner is still active and busy
        const job = yield* background.get(workerSession.id)
        expect(job?.status).toBe("running")

        // Clean up runner fiber
        yield* Deferred.succeed(releaseRunner, void 0)
        yield* Fiber.join(fiber).pipe(Effect.ignore)
      }),
    )

    it.instance("resolves target by agent name or role without needing raw session ID", () =>
      Effect.gen(function* () {
        const background = yield* BackgroundJob.Service
        const sessions = yield* Session.Service
        const { chat, assistant } = yield* seed()

        const workerSession = yield* sessions.create({
          parentID: chat.id,
          title: "Backend Lead Worker",
          agent: "orchestrator-backend",
        })

        yield* background.start({
          id: workerSession.id,
          type: "task",
          title: "Audit database schema",
          metadata: {
            parentSessionId: chat.id,
            background: true,
            agent: "orchestrator-backend",
          },
          run: Effect.succeed("Audit completed"),
        })

        // Test inspect via agent name "Backend"
        const manageTool = yield* ManageAgentsTool
        const manageDef = yield* manageTool.init()
        const inspectRes = yield* manageDef.execute(
          { action: "inspect", target_id: "Backend" },
          {
            sessionID: chat.id,
            messageID: assistant.id,
            agent: "general",
            abort: new AbortController().signal,
            extra: {},
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )
        expect(inspectRes.output).toContain(`Agent Inspection: ${workerSession.id}`)
        expect(inspectRes.output).toContain("Audit database schema")

        // Test ask_agent via agent name "Backend Lead"
        const askTool = yield* AskAgentTool
        const askDef = yield* askTool.init()
        const promptOps = stubOps({ text: "Resolved via agent name." })
        const askRes = yield* askDef.execute(
          { target_id: "Backend Lead", prompt: "Status check?" },
          {
            sessionID: chat.id,
            messageID: assistant.id,
            agent: "general",
            abort: new AbortController().signal,
            extra: { promptOps },
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )
        expect(askRes.output).toBe("Resolved via agent name.")
        expect(askRes.metadata.target_id).toBe(workerSession.id)
      }),
    )
  })

  describe("tool registry", () => {
    it.instance("registers manage_agents and ask_agent in ToolRegistry", () =>
      Effect.gen(function* () {
        const registry = yield* ToolRegistry.Service
        const ids = yield* registry.ids()

        expect(ids).toContain("manage_agents")
        expect(ids).toContain("ask_agent")
        expect(ids).toContain("next_agent")
        expect(ids).toContain("agents_status")
      }),
    )
  })
})
