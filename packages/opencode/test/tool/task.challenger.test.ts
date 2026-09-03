import { afterEach, describe, expect } from "bun:test"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { Global } from "@opencode-ai/core/global"
import { Cause, Effect, Exit, Layer, Option } from "effect"
import * as fsPromises from "fs/promises"
import path from "path"
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
import { TaskTool, type TaskPromptOps, isReadOnlyAgent, renderOutput } from "../../src/tool/task"
import { Truncate } from "@/tool/truncate"
import { ToolRegistry } from "@/tool/registry"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { InstanceState } from "@/effect/instance-state"
import { InstanceRef } from "@/effect/instance-ref"
import { Worktree } from "../../src/worktree"
import { Git } from "../../src/git"
import { FSUtil } from "@opencode-ai/core/fs-util"
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
import { Permission } from "@/permission"

afterEach(async () => {
  await disposeAllInstances()
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
      Worktree.node,
      FSUtil.node,
      Git.node,
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
const background = testEffect(layer({ experimentalBackgroundSubagents: true }))

const seed = Effect.fn("Challenger.seed")(function* (title = "Challenger") {
  const session = yield* Session.Service
  const chat = yield* session.create({ title })
  const user = yield* session.updateMessage({
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
    variant: "xhigh",
    time: { created: Date.now() },
  }
  yield* session.updateMessage(assistant)
  return { chat, assistant }
})

function stubOps(opts?: {
  onPrompt?: (input: SessionPrompt.PromptInput) => void
  text?: string
}): TaskPromptOps {
  return {
    cancel: () => Effect.void,
    resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
    prompt: (input) =>
      Effect.sync(() => {
        opts?.onPrompt?.(input)
        return reply(input, opts?.text ?? "done")
      }),
  }
}

function reply(input: SessionPrompt.PromptInput, text: string): SessionV1.WithParts {
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

function taskCtx(sessionID: SessionID, messageID: MessageID, promptOps: TaskPromptOps) {
  return {
    sessionID,
    messageID,
    agent: "build",
    abort: new AbortController().signal,
    extra: { promptOps },
    messages: [],
    metadata: () => Effect.void,
    ask: () => Effect.void,
  }
}

const exists = (p: string) =>
  Effect.promise(() =>
    fsPromises
      .access(p)
      .then(() => true)
      .catch(() => false),
  )

describe("empirical challenger: concurrency & worktree guardrails", () => {
  it.instance(
    "boundary: exactly 19 running workers allows 20th, 20 running workers rejects 21st",
    () =>
      Effect.gen(function* () {
        const jobs = yield* BackgroundJob.Service
        const { chat, assistant } = yield* seed()
        const tool = yield* TaskTool
        const def = yield* tool.init()

        // 1. Start 19 background jobs
        for (let i = 0; i < 19; i++) {
          yield* jobs.start({
            id: `worker-exact-${i}`,
            type: "task",
            metadata: { parentSessionId: chat.id, sessionId: `worker-exact-${i}` },
            run: Effect.never,
          })
        }

        // 2. 20th worker MUST succeed
        const res20 = yield* def.execute(
          {
            description: "task 20",
            prompt: "execute task 20",
            subagent_type: "general",
          },
          taskCtx(chat.id, assistant.id, stubOps({ text: "task 20 ok" })),
        )
        expect(res20.metadata.sessionId).toBeDefined()

        // Add 20th background job to saturate active ceiling
        yield* jobs.start({
          id: "worker-exact-19",
          type: "task",
          metadata: { parentSessionId: chat.id, sessionId: "worker-exact-19" },
          run: Effect.never,
        })

        // 3. 21st worker MUST fail
        const exit21 = yield* def
          .execute(
            {
              description: "task 21",
              prompt: "execute task 21",
              subagent_type: "general",
            },
            taskCtx(chat.id, assistant.id, stubOps({ text: "task 21 should fail" })),
          )
          .pipe(Effect.exit)

        expect(Exit.isFailure(exit21)).toBe(true)
        if (Exit.isFailure(exit21)) {
          const err = Cause.squash(exit21.cause)
          expect(String(err)).toContain("Concurrency safety ceiling reached: 20/20 active agents running")
        }
      }),
  )

  it.instance(
    "boundary: resuming existing task (params.task_id) succeeds when exactly 20 workers are running",
    () =>
      Effect.gen(function* () {
        const jobs = yield* BackgroundJob.Service
        const sessions = yield* Session.Service
        const { chat, assistant } = yield* seed()
        const child = yield* sessions.create({ parentID: chat.id, title: "existing child" })
        const tool = yield* TaskTool
        const def = yield* tool.init()

        // Saturate ceiling with 20 running workers
        for (let i = 0; i < 20; i++) {
          yield* jobs.start({
            id: `worker-res-full-${i}`,
            type: "task",
            metadata: { parentSessionId: chat.id, sessionId: `worker-res-full-${i}` },
            run: Effect.never,
          })
        }

        // Resuming with task_id must succeed
        const result = yield* def.execute(
          {
            description: "resuming existing",
            prompt: "keep going",
            subagent_type: "general",
            task_id: child.id,
          },
          taskCtx(chat.id, assistant.id, stubOps({ text: "resumed successfully" })),
        )

        expect(result.metadata.sessionId).toBe(child.id)
      }),
  )

  it.instance(
    "boundary: resuming existing task succeeds even when running workers exceed ceiling (e.g. 15 workers)",
    () =>
      Effect.gen(function* () {
        const jobs = yield* BackgroundJob.Service
        const sessions = yield* Session.Service
        const { chat, assistant } = yield* seed()
        const child = yield* sessions.create({ parentID: chat.id, title: "existing child 2" })
        const tool = yield* TaskTool
        const def = yield* tool.init()

        for (let i = 0; i < 15; i++) {
          yield* jobs.start({
            id: `worker-res-overflow-${i}`,
            type: "task",
            metadata: { parentSessionId: chat.id, sessionId: `worker-res-overflow-${i}` },
            run: Effect.never,
          })
        }

        const result = yield* def.execute(
          {
            description: "resuming on overflow",
            prompt: "keep going",
            subagent_type: "general",
            task_id: child.id,
          },
          taskCtx(chat.id, assistant.id, stubOps({ text: "resumed on overflow" })),
        )

        expect(result.metadata.sessionId).toBe(child.id)
      }),
  )

  it.instance(
    "boundary: non-existent task_id creates child session when ceiling is saturated",
    () =>
      Effect.gen(function* () {
        const jobs = yield* BackgroundJob.Service
        const { chat, assistant } = yield* seed()
        const tool = yield* TaskTool
        const def = yield* tool.init()

        for (let i = 0; i < 10; i++) {
          yield* jobs.start({
            id: `worker-nonexistent-${i}`,
            type: "task",
            metadata: { parentSessionId: chat.id, sessionId: `worker-nonexistent-${i}` },
            run: Effect.never,
          })
        }

        // Non-existent task_id (valid SessionID format, but not in DB)
        const nonExistentId = "ses_missing_task_id"
        const result = yield* def.execute(
          {
            description: "new from missing task_id",
            prompt: "fallback create",
            subagent_type: "general",
            task_id: nonExistentId,
          },
          taskCtx(chat.id, assistant.id, stubOps({ text: "created from missing" })),
        )

        expect(result.metadata.sessionId).toBeDefined()
        expect(result.metadata.sessionId).not.toBe(nonExistentId)
      }),
  )

  it.instance(
    "status filtering: only running background jobs count towards ceiling, completed/failed do not",
    () =>
      Effect.gen(function* () {
        const jobs = yield* BackgroundJob.Service
        const { chat, assistant } = yield* seed()
        const tool = yield* TaskTool
        const def = yield* tool.init()

        // 8 completed jobs
        for (let i = 0; i < 8; i++) {
          const j = yield* jobs.start({
            id: `job-completed-${i}`,
            type: "task",
            metadata: { parentSessionId: chat.id, sessionId: `job-completed-${i}` },
            run: Effect.succeed("done"),
          })
          yield* jobs.wait({ id: j.id, timeout: 500 })
        }

        // 1 running job
        yield* jobs.start({
          id: "job-running-1",
          type: "task",
          metadata: { parentSessionId: chat.id, sessionId: "job-running-1" },
          run: Effect.never,
        })

        // New worker must succeed because active running count is 1 (< 10)
        const res = yield* def.execute(
          {
            description: "task with completed jobs",
            prompt: "run",
            subagent_type: "general",
          },
          taskCtx(chat.id, assistant.id, stubOps({ text: "ok" })),
        )

        expect(res.metadata.sessionId).toBeDefined()
      }),
  )

  it.instance(
    "config override: max_concurrent_agents = 2 restricts to 2 workers",
    () =>
      Effect.gen(function* () {
        const jobs = yield* BackgroundJob.Service
        const { chat, assistant } = yield* seed()
        const tool = yield* TaskTool
        const def = yield* tool.init()

        yield* jobs.start({
          id: "worker-ovr2-0",
          type: "task",
          metadata: { parentSessionId: chat.id, sessionId: "worker-ovr2-0" },
          run: Effect.never,
        })

        const res2 = yield* def.execute(
          {
            description: "task 2",
            prompt: "run 2",
            subagent_type: "general",
          },
          taskCtx(chat.id, assistant.id, stubOps({ text: "2 ok" })),
        )
        expect(res2.metadata.sessionId).toBeDefined()

        yield* jobs.start({
          id: "worker-ovr2-1",
          type: "task",
          metadata: { parentSessionId: chat.id, sessionId: "worker-ovr2-1" },
          run: Effect.never,
        })

        const exit3 = yield* def
          .execute(
            {
              description: "task 3",
              prompt: "run 3",
              subagent_type: "general",
            },
            taskCtx(chat.id, assistant.id, stubOps({ text: "3 fail" })),
          )
          .pipe(Effect.exit)

        expect(Exit.isFailure(exit3)).toBe(true)
        if (Exit.isFailure(exit3)) {
          const err = Cause.squash(exit3.cause)
          expect(String(err)).toContain("Concurrency safety ceiling reached: 2/2 active agents running")
        }
      }),
    { config: { experimental: { max_concurrent_agents: 2 } } as any },
  )

  it.instance(
    "config override: max_concurrent_agents = 15 allows 15 workers",
    () =>
      Effect.gen(function* () {
        const jobs = yield* BackgroundJob.Service
        const { chat, assistant } = yield* seed()
        const tool = yield* TaskTool
        const def = yield* tool.init()

        for (let i = 0; i < 14; i++) {
          yield* jobs.start({
            id: `worker-ovr15-${i}`,
            type: "task",
            metadata: { parentSessionId: chat.id, sessionId: `worker-ovr15-${i}` },
            run: Effect.never,
          })
        }

        const res15 = yield* def.execute(
          {
            description: "task 15",
            prompt: "run 15",
            subagent_type: "general",
          },
          taskCtx(chat.id, assistant.id, stubOps({ text: "15 ok" })),
        )
        expect(res15.metadata.sessionId).toBeDefined()

        yield* jobs.start({
          id: "worker-ovr15-14",
          type: "task",
          metadata: { parentSessionId: chat.id, sessionId: "worker-ovr15-14" },
          run: Effect.never,
        })

        const exit16 = yield* def
          .execute(
            {
              description: "task 16",
              prompt: "run 16",
              subagent_type: "general",
            },
            taskCtx(chat.id, assistant.id, stubOps({ text: "16 fail" })),
          )
          .pipe(Effect.exit)

        expect(Exit.isFailure(exit16)).toBe(true)
        if (Exit.isFailure(exit16)) {
          const err = Cause.squash(exit16.cause)
          expect(String(err)).toContain("Concurrency safety ceiling reached: 15/15 active agents running")
        }
      }),
    { config: { experimental: { max_concurrent_agents: 15 } } as any },
  )

  it.instance(
    "isReadOnlyAgent detection oracle: built-in agents",
    () =>
      Effect.gen(function* () {
        const agentService = yield* Agent.Service
        const exploreAgent = yield* agentService.get("explore")
        const generalAgent = yield* agentService.get("general")
        const buildAgent = yield* agentService.get("build")

        expect(exploreAgent).toBeDefined()
        expect(generalAgent).toBeDefined()
        expect(buildAgent).toBeDefined()

        expect(isReadOnlyAgent(exploreAgent!)).toBe(true)
        expect(isReadOnlyAgent(generalAgent!)).toBe(false)
        expect(isReadOnlyAgent(buildAgent!)).toBe(false)
      }),
  )

  it.instance(
    "isReadOnlyAgent detection oracle: custom permission rulesets",
    () =>
      Effect.gen(function* () {
        // Read-only custom agent (*: deny, read: allow)
        const customReadOnly: Agent.Info = {
          name: "custom-ro",
          description: "read only",
          permission: [
            { permission: "*", pattern: "*", action: "deny" },
            { permission: "read", pattern: "*", action: "allow" },
          ],
          prompt: "ro",
          options: {},
          mode: "subagent",
          native: false,
        }
        expect(isReadOnlyAgent(customReadOnly)).toBe(true)

        // Writable agent with wildcard allow
        const customWildcardAllow: Agent.Info = {
          name: "custom-wild",
          description: "wildcard allow",
          permission: [{ permission: "*", pattern: "*", action: "allow" }],
          prompt: "wild",
          options: {},
          mode: "subagent",
          native: false,
        }
        expect(isReadOnlyAgent(customWildcardAllow)).toBe(false)

        // Writable agent with specific write permission
        const customWriteOnly: Agent.Info = {
          name: "custom-write",
          description: "write allow",
          permission: [
            { permission: "*", pattern: "*", action: "deny" },
            { permission: "write_to_file", pattern: "*", action: "allow" },
          ],
          prompt: "write",
          options: {},
          mode: "subagent",
          native: false,
        }
        expect(isReadOnlyAgent(customWriteOnly)).toBe(false)

        // Agent with empty permissions (defaults to 'ask', thus not read-only)
        const customEmpty: Agent.Info = {
          name: "custom-empty",
          description: "empty permissions",
          permission: [],
          prompt: "empty",
          options: {},
          mode: "subagent",
          native: false,
        }
        expect(isReadOnlyAgent(customEmpty)).toBe(false)
      }),
  )

  background.instance(
    "explore subagent with background: true launches in-place without creating worktree",
    () =>
      Effect.gen(function* () {
        const jobs = yield* BackgroundJob.Service
        const { chat, assistant } = yield* seed()
        const ctx = yield* InstanceState.context
        const root = path.join(Global.Path.data, "worktree", ctx.project.id)
        const tool = yield* TaskTool
        const def = yield* tool.init()

        const result = yield* def.execute(
          {
            description: "explore read-only",
            prompt: "scan code",
            subagent_type: "explore",
            background: true,
          },
          taskCtx(chat.id, assistant.id, stubOps({ text: "read-only completed" })),
        )

        const waited = yield* jobs.wait({ id: result.metadata.sessionId, timeout: 2000 })
        expect(waited.timedOut).toBe(false)
        expect(waited.info?.status).toBe("completed")
        expect(waited.info?.output).toBe("read-only completed")

        expect(yield* exists(root)).toBe(false)
      }),
    { git: true },
  )

  background.instance(
    "general subagent with worktree: false and background: true runs in-place without worktree",
    () =>
      Effect.gen(function* () {
        const jobs = yield* BackgroundJob.Service
        const { chat, assistant } = yield* seed()
        const ctx = yield* InstanceState.context
        const root = path.join(Global.Path.data, "worktree", ctx.project.id)
        const tool = yield* TaskTool
        const def = yield* tool.init()

        const result = yield* def.execute(
          {
            description: "general in-place",
            prompt: "run in-place",
            subagent_type: "general",
            background: true,
            worktree: false,
          },
          taskCtx(chat.id, assistant.id, stubOps({ text: "general in-place completed" })),
        )

        const waited = yield* jobs.wait({ id: result.metadata.sessionId, timeout: 2000 })
        expect(waited.timedOut).toBe(false)
        expect(waited.info?.status).toBe("completed")
        expect(waited.info?.output).toBe("general in-place completed")

        expect(yield* exists(root)).toBe(false)
      }),
    { git: true },
  )

  it.instance(
    "output compression oracle: >1500 chars compressed to 200 chars abstract with full log saved",
    () =>
      Effect.gen(function* () {
        const sessionID = "challenger-session-long"
        const largeText = "A".repeat(2000)
        const output = renderOutput({
          sessionID,
          state: "completed",
          text: largeText,
          merge: { status: "clean", stat: "1 file changed" },
        })

        const expectedLogPath = path.join(Global.Path.log, "tasks", `${sessionID}.log`)
        expect(yield* exists(expectedLogPath)).toBe(true)

        const savedContent = yield* Effect.promise(() => fsPromises.readFile(expectedLogPath, "utf-8"))
        expect(savedContent).toBe(largeText)

        expect(output).toContain(`[Full log: ${expectedLogPath}]`)
        expect(output).toContain("1 file changed")
        expect(output).toContain("A".repeat(200) + "...")
        expect(output.length).toBeLessThan(largeText.length)

        yield* Effect.promise(() => fsPromises.unlink(expectedLogPath).catch(() => {}))
      }),
  )

  it.instance(
    "output compression oracle: under 1500 chars is not compressed",
    () =>
      Effect.gen(function* () {
        const sessionID = "challenger-session-short"
        const normalText = "Hello world short result"
        const output = renderOutput({
          sessionID,
          state: "completed",
          text: normalText,
        })

        const logPath = path.join(Global.Path.log, "tasks", `${sessionID}.log`)
        expect(yield* exists(logPath)).toBe(false)
        expect(output).toContain(normalText)
        expect(output).not.toContain("[Full log:")
      }),
  )

  it.instance(
    "soft ESC cancellation decoupling: background job with daemon: true survives session cancel",
    () =>
      Effect.gen(function* () {
        const jobs = yield* BackgroundJob.Service
        const runState = yield* SessionRunState.Service
        const { chat, assistant } = yield* seed()

        const daemonJob = yield* jobs.start({
          id: "daemon-worker-1",
          type: "task",
          metadata: { parentSessionId: chat.id, sessionId: "daemon-worker-1", daemon: true },
          run: Effect.never,
        })

        const normalJob = yield* jobs.start({
          id: "normal-worker-1",
          type: "task",
          metadata: { parentSessionId: chat.id, sessionId: "normal-worker-1" },
          run: Effect.never,
        })

        yield* runState.cancel(chat.id)

        const normalStatus = (yield* jobs.get(normalJob.id))?.status
        const daemonStatus = (yield* jobs.get(daemonJob.id))?.status

        expect(normalStatus).toBe("cancelled")
        expect(daemonStatus).toBe("running")
      }),
  )
})
