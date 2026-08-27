import { afterEach, describe, expect } from "bun:test"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { Global } from "@opencode-ai/core/global"
import { Effect, Exit } from "effect"
import * as fs from "fs/promises"
import path from "path"
import { Agent } from "../../src/agent/agent"
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
import { TaskTool, type TaskPromptOps } from "../../src/tool/task"
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

afterEach(async () => {
  await disposeAllInstances()
})

const ref = {
  providerID: ProviderV2.ID.make("test"),
  modelID: ModelV2.ID.make("test-model"),
}

// Spec F.11: this file mirrors test/tool/task.test.ts's layer, PLUS Worktree.node
// (the landed task.ts requires Worktree.Service at definition time). The existing
// task.test.ts stays unmodified; the missing-service regression is tracked as a
// finding against src/tool/task.ts (unconditional yield at definition time).
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
      // InstanceBootstrap (the replacement for InstanceStore.bootstrapNode)
      // declares Vcs, Format, LSP, Project, ShareNext, Snapshot as deps.
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
    ],
  )

const it = testEffect(layer())
const background = testEffect(layer({ experimentalBackgroundSubagents: true }))

const baseTaskParams = {
  description: "worktree task",
  prompt: "execute in the worktree",
  subagent_type: "general",
}

function taskCtx(sessionID: SessionID, messageID: MessageID, promptOps: TaskPromptOps, directory?: string) {
  return {
    sessionID,
    messageID,
    agent: "build",
    ...(directory ? { directory } : {}),
    abort: new AbortController().signal,
    extra: { promptOps },
    messages: [],
    metadata: () => Effect.void,
    ask: () => Effect.void,
  }
}

function stubOps(opts?: { onPrompt?: (input: SessionPrompt.PromptInput) => void; text?: string }): TaskPromptOps {
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

const seed = Effect.fn("WorktreeTaskTest.seed")(function* () {
  const session = yield* Session.Service
  const chat = yield* session.create({ title: "Worktree" })
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

const exists = (target: string) =>
  Effect.promise(() =>
    fs
      .stat(target)
      .then(() => true)
      .catch(() => false),
  )

describe("tool.task worktree hookup (spec B/E)", () => {
  // Spec F.11 — non-worktree regression control. Foreground (non-background)
  // tasks do NOT imply worktree mode, so no allocation must happen under
  // Global.Path.data/worktree/<projectID>. Background tasks DO imply worktree
  // once spec B lands, so this control must stay foreground.
  it.instance(
    "foreground task without worktree allocates nothing under the worktree root",
    () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const ctx = yield* InstanceState.context
        const root = path.join(Global.Path.data, "worktree", ctx.project.id)

        const tool = yield* TaskTool
        const def = yield* tool.init()
        const result = yield* def.execute(
          { ...baseTaskParams },
          taskCtx(chat.id, assistant.id, stubOps({ text: "done" })),
        )
        expect(result.metadata.sessionId).toBeDefined()

        expect(yield* exists(root)).toBe(false)
      }),
  )

  // Spec B: a background task implies worktree isolation. The child runs inside
  // a worktree; on success the tool mergeAndCleanup removes it on completion
  // (cleanup happens via the background job, so the worktree root ends up with
  // only dot entries — the worktree dir itself is removed).
  background.instance(
    "background task allocates an isolated worktree and removes it on completion",
    () =>
      Effect.gen(function* () {
        const jobs = yield* BackgroundJob.Service
        const { chat, assistant } = yield* seed()
        const ctx = yield* InstanceState.context
        const root = path.join(Global.Path.data, "worktree", ctx.project.id)
        const tool = yield* TaskTool
        const def = yield* tool.init()

        const result = yield* def.execute(
          { ...baseTaskParams, background: true },
          taskCtx(chat.id, assistant.id, stubOps({ text: "worktree done" })),
        )

        const waited = yield* jobs.wait({ id: result.metadata.sessionId, timeout: 2_000 })
        expect(waited.timedOut).toBe(false)
        expect(waited.info?.status).toBe("completed")
        expect(waited.info?.output).toBe("worktree done")

        // On completion the worktree is removed: only dot entries remain under
        // the project root (the .journal/.alive housekeeping dirs).
        const entries = (yield* Effect.promise(() => fs.readdir(root).catch(() => []))).filter(
          (entry) => !entry.startsWith("."),
        )
        expect(entries).toHaveLength(0)
      }),
    { git: true },
  )

  // Spec F.13: a worktree task with no instance context must fail cleanly. The
  // landed implementation: explicit `worktree: true` (foreground) requires an
  // instance; with none provided the tool must fail fast — never hang.
  it.instance(
    "worktree: true with no instance context fails cleanly instead of hanging",
    () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const tool = yield* TaskTool
        const def = yield* tool.init()

        const exit = yield* def
          .execute(
            { ...baseTaskParams, worktree: true },
            taskCtx(chat.id, assistant.id, stubOps()),
          )
          .pipe(
            // Clear the ambient instance reference for the execute only —
            // the seed above needs an instance, the tool under test must not.
            Effect.provideService(InstanceRef, undefined),
            Effect.timeoutOrElse({
              duration: "10 seconds",
              orElse: () => Effect.fail(new Error("worktree task did not fail cleanly without an instance")),
            }),
            Effect.exit,
          )
        expect(Exit.isFailure(exit)).toBe(true)
      }),
  )
})