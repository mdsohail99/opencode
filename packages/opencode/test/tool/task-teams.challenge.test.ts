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

describe("Adversarial M2 Empirical Challenge Suite", () => {
  describe("manage_agents: adversarial stress tests", () => {
    it.instance("kill cancels running fiber via scope interruption and calls runner ops.cancel", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const background = yield* BackgroundJob.Service
        const sessions = yield* Session.Service

        let fiberInterrupted = false
        const worker = yield* sessions.create({
          parentID: chat.id,
          title: "Interruptible Fiber Worker",
          metadata: { background: true },
        })

        yield* background.start({
          id: worker.id,
          type: "task",
          title: "Interruptible Fiber Worker",
          metadata: { background: true, parentSessionId: chat.id },
          run: Effect.never.pipe(
            Effect.onInterrupt(() =>
              Effect.sync(() => {
                fiberInterrupted = true
              }),
            ),
          ),
        })

        let runnerCancelledID = ""
        const tool = yield* ManageAgentsTool
        const def = yield* tool.init()

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
                    runnerCancelledID = id
                  }),
              },
            },
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )

        expect(result.output).toContain(`Agent '${worker.id}' has been cancelled.`)
        expect(runnerCancelledID).toBe(worker.id)
        expect(fiberInterrupted).toBe(true)

        const job = yield* background.get(worker.id)
        expect(job?.status).toBe("cancelled")
      }),
    )

    it.instance("kill with missing target_id returns clean error message", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const tool = yield* ManageAgentsTool
        const def = yield* tool.init()

        const result = yield* def.execute(
          { action: "kill" },
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

        expect(result.output).toBe("Error: target_id is required for 'kill'.")
      }),
    )

    it.instance("kill_all traverses deep 4-level hierarchy and cancels all active descendants while preserving external tasks", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const background = yield* BackgroundJob.Service
        const sessions = yield* Session.Service

        // Root Orchestrator (chat.id)
        // -> Lead A
        //    -> Spec A1
        //       -> SubSpec A1_1 (running)
        //       -> SubSpec A1_2 (already completed)
        //    -> Spec A2 (running)
        // -> Lead B (running)
        // Unrelated Worker C (external session, running)

        const leadA = yield* sessions.create({ parentID: chat.id, title: "Lead A", metadata: { background: true } })
        yield* background.start({
          id: leadA.id,
          type: "task",
          title: "Lead A",
          metadata: { background: true, parentSessionId: chat.id },
          run: Effect.never,
        })

        const specA1 = yield* sessions.create({ parentID: leadA.id, title: "Spec A1", metadata: { background: true } })
        yield* background.start({
          id: specA1.id,
          type: "task",
          title: "Spec A1",
          metadata: { background: true, parentSessionId: leadA.id },
          run: Effect.never,
        })

        const subSpecA1_1 = yield* sessions.create({ parentID: specA1.id, title: "SubSpec A1_1", metadata: { background: true } })
        yield* background.start({
          id: subSpecA1_1.id,
          type: "task",
          title: "SubSpec A1_1",
          metadata: { background: true, parentSessionId: specA1.id },
          run: Effect.never,
        })

        const subSpecA1_2 = yield* sessions.create({ parentID: specA1.id, title: "SubSpec A1_2", metadata: { background: true } })
        yield* background.start({
          id: subSpecA1_2.id,
          type: "task",
          title: "SubSpec A1_2",
          metadata: { background: true, parentSessionId: specA1.id },
          run: Effect.succeed("already finished"),
        })

        const specA2 = yield* sessions.create({ parentID: leadA.id, title: "Spec A2", metadata: { background: true } })
        yield* background.start({
          id: specA2.id,
          type: "task",
          title: "Spec A2",
          metadata: { background: true, parentSessionId: leadA.id },
          run: Effect.never,
        })

        const leadB = yield* sessions.create({ parentID: chat.id, title: "Lead B", metadata: { background: true } })
        yield* background.start({
          id: leadB.id,
          type: "task",
          title: "Lead B",
          metadata: { background: true, parentSessionId: chat.id },
          run: Effect.never,
        })

        // Unrelated external session and job
        const externalChat = yield* sessions.create({ title: "Unrelated Session" })
        const externalWorker = yield* sessions.create({ parentID: externalChat.id, title: "External Worker", metadata: { background: true } })
        yield* background.start({
          id: externalWorker.id,
          type: "task",
          title: "External Worker",
          metadata: { background: true, parentSessionId: externalChat.id },
          run: Effect.never,
        })

        const tool = yield* ManageAgentsTool
        const def = yield* tool.init()

        const cancelledRunnerIds: string[] = []
        const result = yield* def.execute(
          { action: "kill_all" },
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
                    cancelledRunnerIds.push(id)
                  }),
              },
            },
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )

        // Total active descendants in chat tree: Lead A, Spec A1, SubSpec A1_1, Spec A2, Lead B = 5 active workers
        expect(result.output).toContain("Cancelled 5 active descendant agent(s)")

        const jobLeadA = yield* background.get(leadA.id)
        const jobSpecA1 = yield* background.get(specA1.id)
        const jobSubSpecA1_1 = yield* background.get(subSpecA1_1.id)
        const jobSubSpecA1_2 = yield* background.get(subSpecA1_2.id)
        const jobSpecA2 = yield* background.get(specA2.id)
        const jobLeadB = yield* background.get(leadB.id)
        const jobExternal = yield* background.get(externalWorker.id)

        expect(jobLeadA?.status).toBe("cancelled")
        expect(jobSpecA1?.status).toBe("cancelled")
        expect(jobSubSpecA1_1?.status).toBe("cancelled")
        expect(jobSubSpecA1_2?.status).toBe("completed") // Was completed, not altered
        expect(jobSpecA2?.status).toBe("cancelled")
        expect(jobLeadB?.status).toBe("cancelled")

        // External worker MUST NOT be touched
        expect(jobExternal?.status).toBe("running")
        expect(cancelledRunnerIds).not.toContain(externalWorker.id)
      }),
    )

    it.instance("inspect handles non-existent jobs cleanly without throwing", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const tool = yield* ManageAgentsTool
        const def = yield* tool.init()

        const result = yield* def.execute(
          { action: "inspect", target_id: "non-existent-agent-id-9999" },
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

        expect(result.title).toBe("Agent not found")
        expect(result.output).toBe("Agent or job 'non-existent-agent-id-9999' was not found.")
      }),
    )

    it.instance("inspect handles failed error jobs cleanly formatting error details", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const background = yield* BackgroundJob.Service
        const sessions = yield* Session.Service

        const failedWorker = yield* sessions.create({
          parentID: chat.id,
          title: "Failed Worker Job",
          metadata: { background: true },
        })
        yield* background.start({
          id: failedWorker.id,
          type: "task",
          title: "Failed Worker Job",
          metadata: { background: true, parentSessionId: chat.id },
          run: Effect.fail(new Error("Fatal memory allocation error in worker")),
        })

        const tool = yield* ManageAgentsTool
        const def = yield* tool.init()

        const result = yield* def.execute(
          { action: "inspect", target_id: failedWorker.id },
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

        expect(result.output).toContain(`### Agent Inspection: ${failedWorker.id}`)
        expect(result.output).toContain("- **Status**: error")
        expect(result.output).toContain("- **Error**: Fatal memory allocation error in worker")
      }),
    )

    it.instance("restart revives task, resets SQLite drained state, and produces newly collectable output", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const background = yield* BackgroundJob.Service
        const sessions = yield* Session.Service

        const worker = yield* sessions.create({
          parentID: chat.id,
          title: "Drained Task To Revive",
          metadata: { background: true },
        })
        yield* background.start({
          id: worker.id,
          type: "task",
          title: "Drained Task To Revive",
          metadata: { background: true, parentSessionId: chat.id },
          run: Effect.succeed("initial result v1"),
        })

        const ctx = {
          sessionID: chat.id,
          messageID: assistant.id,
          agent: "general",
          abort: new AbortController().signal,
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        }

        // Drain it via next_agent
        const nextTool = yield* NextAgentTool
        const nextDef = yield* nextTool.init()
        const drainRes = yield* nextDef.execute({}, ctx)
        expect(drainRes.output).toContain("initial result v1")

        // Verify drained: true in SQLite
        const sessionBefore = yield* sessions.get(worker.id)
        expect(sessionBefore.metadata?.drained).toBe(true)

        // Now restart via manage_agents
        const manageTool = yield* ManageAgentsTool
        const manageDef = yield* manageTool.init()

        const restartGate = yield* Deferred.make<void>()
        const promptOps: TaskPromptOps = {
          ...stubOps(),
          prompt: () =>
            Deferred.await(restartGate).pipe(
              Effect.as(reply({ sessionID: worker.id, parts: [] }, "restarted result v2")),
            ),
        }

        const restartRes = yield* manageDef.execute(
          { action: "restart", target_id: worker.id },
          { ...ctx, extra: { promptOps } },
        )

        expect(restartRes.output).toContain(`Agent '${worker.id}' restarted successfully.`)

        // Verify SQLite drained state is reset to false
        const sessionAfterRestart = yield* sessions.get(worker.id)
        expect(sessionAfterRestart.metadata?.drained).toBe(false)

        // Release the restarted task prompt
        yield* Deferred.succeed(restartGate, void 0)

        // Collect new result via next_agent
        const nextRes2 = yield* nextDef.execute({}, ctx)
        expect(nextRes2.output).toContain("restarted result v2")

        // And drained is true again after collection
        const sessionFinal = yield* sessions.get(worker.id)
        expect(sessionFinal.metadata?.drained).toBe(true)
      }),
    )
  })

  describe("ask_agent: adversarial stress tests", () => {
    it.instance("cleans up ephemeral session from SQLite even if the prompt produces an error", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const sessions = yield* Session.Service

        const target = yield* sessions.create({
          parentID: chat.id,
          title: "Target Worker For Failure Test",
          agent: "build",
        })

        const askTool = yield* AskAgentTool
        const def = yield* askTool.init()

        let ephemeralSessionID: SessionID | undefined

        // Custom ops where resolvePromptParts or prompt captures the ephemeral session ID and fails
        const failingOps: TaskPromptOps = {
          cancel: () => Effect.void,
          resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
          prompt: (input) =>
            Effect.sync(() => {
              ephemeralSessionID = input.sessionID
            }).pipe(
              Effect.andThen(Effect.die(new Error("Simulated LLM network timeout during side-query"))),
            ),
        }

        // Execute ask_agent expecting failure due to prompt failure
        const resultExit = yield* def
          .execute(
            { target_id: target.id, prompt: "Can you help?" },
            {
              sessionID: chat.id,
              messageID: assistant.id,
              agent: "general",
              abort: new AbortController().signal,
              extra: { promptOps: failingOps },
              messages: [],
              metadata: () => Effect.void,
              ask: () => Effect.void,
            },
          )
          .pipe(Effect.exit)

        // Because execute pipes run(params, ctx).pipe(Effect.orDie), a failure becomes a die exit
        expect(resultExit._tag).toBe("Failure")

        // Most importantly: EMPIRICALLY VERIFY THE EPHEMERAL SESSION WAS REMOVED FROM SQLITE!
        expect(ephemeralSessionID).toBeDefined()
        if (ephemeralSessionID) {
          const checkSession = yield* sessions.get(ephemeralSessionID).pipe(Effect.option)
          expect(Option.isNone(checkSession)).toBe(true)
        }

        // Verify target session has 0 lingering children
        const children = yield* sessions.children(target.id)
        expect(children.length).toBe(0)
      }),
    )

    it.instance("fails cleanly when target agent session does not exist", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const askTool = yield* AskAgentTool
        const def = yield* askTool.init()

        const exit = yield* def
          .execute(
            { target_id: "non-existent-target-session", prompt: "Hello" },
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
          .pipe(Effect.exit)

        expect(exit._tag).toBe("Failure")
      }),
    )

    it.instance("fails cleanly when promptOps is missing from context", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const sessions = yield* Session.Service

        const target = yield* sessions.create({
          parentID: chat.id,
          title: "Target Worker",
          agent: "build",
        })

        const askTool = yield* AskAgentTool
        const def = yield* askTool.init()

        const exit = yield* def
          .execute(
            { target_id: target.id, prompt: "Hello" },
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
          .pipe(Effect.exit)

        expect(exit._tag).toBe("Failure")
      }),
    )
  })

  describe("agents_status: adversarial tree rendering & stall detection", () => {
    it.instance("renders empty swarm notice cleanly", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const tool = yield* AgentsStatusTool
        const def = yield* tool.init()

        const res = yield* def.execute(
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

        expect(res.title).toBe("No background tasks")
        expect(res.output).toBe("No background tasks running for this session.")
      }),
    )

    it.instance("renders deep 4-level hierarchy (Lead -> Specialist -> Tool -> Subtool) with correct prefixes", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const background = yield* BackgroundJob.Service
        const sessions = yield* Session.Service

        // Level 1: Lead
        const lead = yield* sessions.create({
          parentID: chat.id,
          title: "Engineering Lead",
          metadata: { background: true },
        })
        yield* background.start({
          id: lead.id,
          type: "task",
          title: "Engineering Lead",
          metadata: { background: true, parentSessionId: chat.id },
          run: Effect.never,
        })

        // Level 2: Specialist under Lead
        const spec = yield* sessions.create({
          parentID: lead.id,
          title: "Compiler Specialist",
          metadata: { background: true },
        })
        yield* background.start({
          id: spec.id,
          type: "task",
          title: "Compiler Specialist",
          metadata: { background: true, parentSessionId: lead.id },
          run: Effect.never,
        })

        // Level 3: Tool worker under Specialist
        const toolWorker = yield* sessions.create({
          parentID: spec.id,
          title: "AST Transformer",
          metadata: { background: true },
        })
        yield* background.start({
          id: toolWorker.id,
          type: "task",
          title: "AST Transformer",
          metadata: { background: true, parentSessionId: spec.id },
          run: Effect.never,
        })

        // Level 4: Sub-tool under Tool worker
        const subtool = yield* sessions.create({
          parentID: toolWorker.id,
          title: "Bytecode Optimizer",
          metadata: { background: true },
        })
        yield* background.start({
          id: subtool.id,
          type: "task",
          title: "Bytecode Optimizer",
          metadata: { background: true, parentSessionId: toolWorker.id },
          run: Effect.never,
        })

        const tool = yield* AgentsStatusTool
        const def = yield* tool.init()

        const res = yield* def.execute(
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

        expect(res.title).toContain("Background tasks (4)")
        const lines = res.output.split("\n")

        // Level 1 should start with "└─ " (single child of root)
        expect(lines[0]).toMatch(new RegExp(`^└─ ${lead.id} \\[running\\] Engineering Lead`))
        // Level 2 child prefix from "└─ " becomes "   └─ "
        expect(lines[1]).toMatch(new RegExp(`^   └─ ${spec.id} \\[running\\] Compiler Specialist`))
        // Level 3 child prefix becomes "      └─ "
        expect(lines[2]).toMatch(new RegExp(`^      └─ ${toolWorker.id} \\[running\\] AST Transformer`))
        // Level 4 child prefix becomes "         └─ "
        expect(lines[3]).toMatch(new RegExp(`^         └─ ${subtool.id} \\[running\\] Bytecode Optimizer`))
      }),
    )

    it.instance("renders multi-child branching with continuing vertical pipes across 3 levels", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const background = yield* BackgroundJob.Service
        const sessions = yield* Session.Service

        // Root has 2 Leads: Lead A, Lead B
        const leadA = yield* sessions.create({
          parentID: chat.id,
          title: "Lead A",
          metadata: { background: true },
        })
        yield* background.start({
          id: leadA.id,
          type: "task",
          title: "Lead A",
          metadata: { background: true, parentSessionId: chat.id },
          run: Effect.never,
        })

        const leadB = yield* sessions.create({
          parentID: chat.id,
          title: "Lead B",
          metadata: { background: true },
        })
        yield* background.start({
          id: leadB.id,
          type: "task",
          title: "Lead B",
          metadata: { background: true, parentSessionId: chat.id },
          run: Effect.never,
        })

        // Lead A has 2 Specialists: Spec A1, Spec A2
        const specA1 = yield* sessions.create({
          parentID: leadA.id,
          title: "Spec A1",
          metadata: { background: true },
        })
        yield* background.start({
          id: specA1.id,
          type: "task",
          title: "Spec A1",
          metadata: { background: true, parentSessionId: leadA.id },
          run: Effect.never,
        })

        const specA2 = yield* sessions.create({
          parentID: leadA.id,
          title: "Spec A2",
          metadata: { background: true },
        })
        yield* background.start({
          id: specA2.id,
          type: "task",
          title: "Spec A2",
          metadata: { background: true, parentSessionId: leadA.id },
          run: Effect.never,
        })

        // Spec A1 has Tool A1.1
        const toolA1 = yield* sessions.create({
          parentID: specA1.id,
          title: "Tool A1.1",
          metadata: { background: true },
        })
        yield* background.start({
          id: toolA1.id,
          type: "task",
          title: "Tool A1.1",
          metadata: { background: true, parentSessionId: specA1.id },
          run: Effect.never,
        })

        const tool = yield* AgentsStatusTool
        const def = yield* tool.init()

        const res = yield* def.execute(
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

        expect(res.title).toContain("Background tasks (5)")
        const lines = res.output.split("\n")

        // Lead A is first child of root -> ├─
        expect(lines[0]).toContain(`├─ ${leadA.id} [running] Lead A`)
        // Spec A1 is first child of Lead A -> │  ├─
        expect(lines[1]).toContain(`│  ├─ ${specA1.id} [running] Spec A1`)
        // Tool A1.1 is child of Spec A1 -> │  │  └─
        expect(lines[2]).toContain(`│  │  └─ ${toolA1.id} [running] Tool A1.1`)
        // Spec A2 is second child of Lead A -> │  └─
        expect(lines[3]).toContain(`│  └─ ${specA2.id} [running] Spec A2`)
        // Lead B is second child of root -> └─
        expect(lines[4]).toContain(`└─ ${leadB.id} [running] Lead B`)
      }),
    )

    it.instance("strictly tests 180,000ms stall boundary: 179s vs 181s", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const background = yield* BackgroundJob.Service
        const sessions = yield* Session.Service

        const sub179 = yield* sessions.create({
          parentID: chat.id,
          title: "179s Worker",
          metadata: { background: true },
        })

        const sub181 = yield* sessions.create({
          parentID: chat.id,
          title: "181s Worker",
          metadata: { background: true },
        })

        const now = Date.now()

        // 179,000 ms ago -> elapsed time strictly < 180_000ms -> NOT stalled
        yield* background.start({
          id: sub179.id,
          type: "task",
          title: "179s Worker",
          metadata: {
            background: true,
            parentSessionId: chat.id,
            last_active_at: now - 179_000,
          },
          run: Effect.never,
        })

        // 181,000 ms ago -> elapsed time strictly > 180_000ms -> MUST be stalled
        yield* background.start({
          id: sub181.id,
          type: "task",
          title: "181s Worker",
          metadata: {
            background: true,
            parentSessionId: chat.id,
            last_active_at: now - 181_000,
          },
          run: Effect.never,
        })

        const tool = yield* AgentsStatusTool
        const def = yield* tool.init()

        const res = yield* def.execute(
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

        const lines = res.output.split("\n")
        const line179 = lines.find((l) => l.includes(sub179.id))!
        const line181 = lines.find((l) => l.includes(sub181.id))!

        expect(line179).not.toContain("[stalled]")
        expect(line181).toContain("[stalled]")
      }),
    )

    it.instance("verifies complete fallback hierarchy for last_active_at resolution", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const background = yield* BackgroundJob.Service
        const sessions = yield* Session.Service

        const now = Date.now()
        const oldTime = now - 200_000
        const recentTime = now - 10_000

        // Case A: Priority 1 (job.metadata.last_active_at) takes precedence over session.time.updated
        const workerA = yield* sessions.create({
          parentID: chat.id,
          title: "Worker A",
          metadata: { background: true },
        })
        yield* sessions.update(workerA.id, { time: { created: oldTime, updated: oldTime } })
        yield* background.start({
          id: workerA.id,
          type: "task",
          title: "Worker A",
          metadata: {
            background: true,
            parentSessionId: chat.id,
            last_active_at: recentTime,
          },
          run: Effect.never,
        })

        // Case B: Priority 2 (session.time.updated) takes precedence when last_active_at is absent
        // Fresh updated time (10s ago) -> NOT stalled
        const workerB = yield* sessions.create({
          parentID: chat.id,
          title: "Worker B",
          metadata: { background: true },
        })
        yield* sessions.update(workerB.id, { time: { created: oldTime, updated: recentTime } })
        yield* background.start({
          id: workerB.id,
          type: "task",
          title: "Worker B",
          metadata: {
            background: true,
            parentSessionId: chat.id,
          },
          run: Effect.never,
        })

        // Case C: Priority 2 (session.time.updated) is used when last_active_at is absent
        // Old updated time (200s ago) -> STALLED
        const workerC = yield* sessions.create({
          parentID: chat.id,
          title: "Worker C",
          metadata: { background: true },
        })
        yield* sessions.update(workerC.id, { time: { created: oldTime, updated: oldTime } })
        yield* background.start({
          id: workerC.id,
          type: "task",
          title: "Worker C",
          metadata: {
            background: true,
            parentSessionId: chat.id,
          },
          run: Effect.never,
        })

        const tool = yield* AgentsStatusTool
        const def = yield* tool.init()

        const res = yield* def.execute(
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

        const lines = res.output.split("\n")
        const lineA = lines.find((l) => l.includes(workerA.id))!
        const lineB = lines.find((l) => l.includes(workerB.id))!
        const lineC = lines.find((l) => l.includes(workerC.id))!

        expect(lineA).not.toContain("[stalled]")
        expect(lineB).not.toContain("[stalled]")
        expect(lineC).toContain("[stalled]")
      }),
    )
  })

  describe("SQLite drained persistence: multi-task restart resilience", () => {
    it.instance("persists multiple completed tasks and never re-delivers across multiple restarts", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const background = yield* BackgroundJob.Service
        const sessions = yield* Session.Service

        // Create Task 1 (completed)
        const task1 = yield* sessions.create({
          parentID: chat.id,
          title: "Task One",
          metadata: { background: true },
        })
        yield* background.start({
          id: task1.id,
          type: "task",
          title: "Task One",
          metadata: { background: true, parentSessionId: chat.id },
          run: Effect.succeed("result of task one"),
        })

        // Create Task 2 (completed)
        const task2 = yield* sessions.create({
          parentID: chat.id,
          title: "Task Two",
          metadata: { background: true },
        })
        yield* background.start({
          id: task2.id,
          type: "task",
          title: "Task Two",
          metadata: { background: true, parentSessionId: chat.id },
          run: Effect.succeed("result of task two"),
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

        // 1. First drain: retrieves Task 1
        const res1 = yield* nextDef.execute({}, ctx)
        expect(res1.output).toContain("result of task one")

        // 2. Simulate process crash / engine restart
        clearDrainedCache()

        // 3. Second drain: retrieves Task 2 (Task 1 must NOT be re-delivered)
        const res2 = yield* nextDef.execute({}, ctx)
        expect(res2.output).toContain("result of task two")

        // 4. Simulate second crash / engine restart
        clearDrainedCache()

        // 5. Third drain: both Task 1 and Task 2 are drained in SQLite -> returns "no background tasks"
        const res3 = yield* nextDef.execute({}, ctx)
        expect(res3.output).toBe("no background tasks")

        // 6. Verify SQLite persisted metadata directly
        const s1 = yield* sessions.get(task1.id)
        const s2 = yield* sessions.get(task2.id)
        expect(s1.metadata?.drained).toBe(true)
        expect(s2.metadata?.drained).toBe(true)

        // 7. Test restart un-draining: restart Task 1 via manage_agents
        const manageTool = yield* ManageAgentsTool
        const manageDef = yield* manageTool.init()
        yield* manageDef.execute({ action: "restart", target_id: task1.id }, ctx)

        // Verify task1 in SQLite metadata now has drained = false
        const s1AfterRestart = yield* sessions.get(task1.id)
        expect(s1AfterRestart.metadata?.drained).toBe(false)
      }),
    )
  })
})
