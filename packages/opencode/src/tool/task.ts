import * as Tool from "./tool"
import DESCRIPTION from "./task.txt"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { BackgroundJob } from "@/background/job"
import { Session } from "@/session/session"
import { SessionID, MessageID } from "../session/schema"
import { MessageV2 } from "../session/message-v2"
import { Agent } from "../agent/agent"
import { deriveSubagentSessionPermission } from "../agent/subagent-permissions"
import type { SessionPrompt } from "../session/prompt"
import { Config } from "@/config/config"
import { Effect, Exit, Option, Schema, Scope } from "effect"
import { EffectBridge } from "@/effect/bridge"
import { Database } from "@opencode-ai/core/database/database"
import { Worktree } from "@/worktree"
import { InstanceState } from "@/effect/instance-state"
import { InstanceRef } from "@/effect/instance-ref"
import type { InstanceContext } from "@/project/instance-context"
import { errorMessage } from "../util/error"
import { Global } from "@opencode-ai/core/global"
import path from "path"
import fs from "fs"
import { Permission } from "@/permission"
import { GlobalBus } from "@/bus/global"

export interface TaskPromptOps {
  cancel(sessionID: SessionID): Effect.Effect<void>
  resolvePromptParts(template: string): Effect.Effect<SessionPrompt.PromptInput["parts"]>
  prompt(input: SessionPrompt.PromptInput): Effect.Effect<SessionV1.WithParts>
}

const id = "task"
const BACKGROUND_DESCRIPTION = [
  "Background mode: background=true launches the subagent asynchronously and returns immediately.",
  "Foreground is the default; use it when you need the result before continuing.",
  "Use background only for independent work that can run while you continue elsewhere.",
  "You will be notified automatically when it finishes.",
].join(" ")
const BACKGROUND_STARTED = [
  "The task is working in the background. You will be notified automatically when it finishes.",
  "DO NOT sleep, poll for progress, ask the task for status, or duplicate this task's work — avoid working with the same files or topics it is using.",
  "Work on non-overlapping tasks, or briefly tell the user what you launched and end your response.",
].join("\n")
const BACKGROUND_UPDATED = [
  "Additional context sent to the running background task.",
  "The task is still working in the background. You will be notified automatically when it finishes.",
  "DO NOT sleep, poll for progress, ask the task for status, or duplicate this task's work — avoid working with the same files or topics it is using.",
  "Work on non-overlapping tasks, or briefly tell the user what you sent and end your response.",
].join("\n")

const BaseParameterFields = {
  description: Schema.String.annotate({ description: "A short (3-5 words) description of the task" }),
  prompt: Schema.String.annotate({ description: "The task for the agent to perform" }),
  subagent_type: Schema.String.annotate({ description: "The type of specialized agent to use for this task" }),
  task_id: Schema.optional(Schema.String).annotate({
    description:
      "This should only be set if you mean to resume a previous task (you can pass a prior task_id and the task will continue the same subagent session as before instead of creating a fresh one)",
  }),
  command: Schema.optional(Schema.String).annotate({ description: "The command that triggered this task" }),
}

export const Parameters = Schema.Struct({
  ...BaseParameterFields,
  background: Schema.optional(Schema.Boolean).annotate({
    description:
      "Run the agent in the background. You will be notified when it completes. DO NOT sleep, poll, or proactively check on its progress",
  }),
  worktree: Schema.optional(Schema.Boolean).annotate({
    description:
      "Run the subagent in an isolated git worktree under the app data directory. Its edits are auto-committed and, with approval, merged back into the parent branch on completion. Implicit for background tasks when the project is a git repo",
  }),
  daemon: Schema.optional(Schema.Boolean).annotate({
    description: "Run the background task as a daemon decoupled from terminal chat ESC cancellation.",
  }),
  autoApprove: Schema.optional(Schema.Boolean).annotate({
    description: "Automatically approve merging the worktree on completion without prompting for user confirmation.",
  }),
})

export const WRITE_PERMISSIONS = [
  "edit",
  "write",
  "write_to_file",
  "replace_file_content",
  "apply_patch",
] as const

export function isReadOnlyAgent(subagent: Agent.Info): boolean {
  const hasAllowedWrite = subagent.permission.some(
    (rule) =>
      WRITE_PERMISSIONS.includes(rule.permission as (typeof WRITE_PERMISSIONS)[number]) &&
      rule.action !== "deny",
  )
  if (hasAllowedWrite) return false

  return WRITE_PERMISSIONS.every(
    (perm) => Permission.evaluate(perm, "*", subagent.permission).action === "deny",
  )
}

export function renderOutput(input: {
  sessionID: string
  state: "running" | "completed" | "error" | "cancelled"
  summary?: string
  text: string
  merge?: { status: string; stat?: string; detail?: string }
}) {
  let text = input.text
  if (text.length > 1500) {
    const tasksDir = path.join(Global.Path.log, "tasks")
    try {
      fs.mkdirSync(tasksDir, { recursive: true })
      const logPath = path.join(tasksDir, `${input.sessionID}.log`)
      fs.writeFileSync(logPath, text, "utf-8")
      const abstract = text.slice(0, 200).trim()
      const parts = [
        abstract + "...",
        ...(input.merge?.stat ? [input.merge.stat] : []),
        `[Full log: ${logPath}]`,
      ]
      text = parts.join("\n\n")
    } catch {
      // Fallback if log directory write fails
    }
  }
  const tag = input.state === "error" ? "task_error" : "task_result"
  const merge = input.merge
    ? [
        `<merge status="${input.merge.status}">`,
        ...(input.merge.stat ? [input.merge.stat] : []),
        ...(input.merge.detail ? [input.merge.detail] : []),
        "</merge>",
      ]
    : []
  return [
    `<task id="${input.sessionID}" state="${input.state}">`,
    ...(input.summary ? [`<summary>${input.summary}</summary>`] : []),
    `<${tag}>`,
    text,
    `</${tag}>`,
    ...merge,
    "</task>",
  ].join("\n")
}

/** Active worktree per child session, shared across resume/extend tool calls. */
interface WorktreeEntry {
  readonly info: Worktree.Info
  readonly parentCtx: InstanceContext
  readonly childCtx: InstanceContext
  consumed: boolean
}

const worktrees = new Map<SessionID, WorktreeEntry>()

export const TaskTool = Tool.define(
  id,
  Effect.gen(function* () {
    const agent = yield* Agent.Service
    const background = yield* BackgroundJob.Service
    const config = yield* Config.Service
    const sessions = yield* Session.Service
    const scope = yield* Scope.Scope
    const database = yield* Database.Service
    // Optional backend: the tool must be definable without the worktree
    // service installed (embedded runtimes, lean test layers). Explicit
    // isolation fails with a clear error; implied isolation (background)
    // degrades to running in place with a warning.
    const worktreeOption = yield* Effect.serviceOption(Worktree.Service)

    const run = Effect.fn("TaskTool.execute")(function* (
      params: Schema.Schema.Type<typeof Parameters>,
      ctx: Tool.Context,
    ) {
      // Foreground explicit worktree: fail cleanly BEFORE any instance-bound
      // service call (e.g. config.get would defect without an instance).
      if (params.worktree === true && Option.isNone(Option.fromNullishOr(yield* InstanceRef))) {
        return yield* Effect.fail(new Error("Worktree mode requires a project instance context"))
      }
      const cfg = yield* config.get()
      const runInBackground = params.background === true

      const parent = yield* sessions.get(ctx.sessionID)
      let current = parent
      let depth = 0
      while (current.parentID) {
        depth++
        current = yield* sessions.get(current.parentID)
      }
      const rootSession = current

      if (depth >= (cfg.subagent_depth ?? 1)) {
        return yield* Effect.fail(
          new Error(
            `Subagent depth limit reached (${cfg.subagent_depth ?? 1}). Increase "subagent_depth" to allow nested subagents.`,
          ),
        )
      }

      if (!params.task_id) {
        const runningJobs = (yield* background.list()).filter((j) => j.status === "running")
        const maxConcurrent = cfg.max_concurrent_agents ?? cfg.experimental?.max_concurrent_agents ?? 20
        if (runningJobs.length >= maxConcurrent) {
          return yield* Effect.fail(
            new Error(
              `Concurrency safety ceiling reached: ${runningJobs.length}/${maxConcurrent} active agents running. Wait for existing tasks to complete or adjust max_concurrent_agents.`,
            ),
          )
        }
      }

      if (!ctx.extra?.bypassAgentCheck) {
        yield* ctx.ask({
          permission: id,
          patterns: [params.subagent_type],
          always: ["*"],
          metadata: {
            description: params.description,
            subagent_type: params.subagent_type,
          },
        })
      }

      const next = yield* agent.get(params.subagent_type)
      if (!next) {
        return yield* Effect.fail(new Error(`Unknown agent type: ${params.subagent_type} is not a valid agent type`))
      }

      const isReadOnly = isReadOnlyAgent(next)
      const useWorktree = !isReadOnly && params.worktree !== false && (params.worktree === true || runInBackground)

      const session = params.task_id
        ? yield* sessions.get(SessionID.make(params.task_id)).pipe(Effect.catchCause(() => Effect.succeed(undefined)))
        : undefined
      const childPermission = deriveSubagentSessionPermission({
        parentSessionPermission: parent.permission ?? [],
        subagent: next,
      })
      const childToolDenies = [
        ...(next.permission.some((rule) => rule.permission === "todowrite")
          ? []
          : [{ permission: "todowrite" as const, pattern: "*" as const, action: "deny" as const }]),
        ...(next.permission.some((rule) => rule.permission === id)
          ? []
          : [{ permission: id, pattern: "*" as const, action: "deny" as const }]),
        ...(cfg.experimental?.primary_tools?.map((permission) => ({
          permission,
          pattern: "*" as const,
          action: "deny" as const,
        })) ?? []),
      ]
      const nextSession =
        session ??
        (yield* sessions.create({
          parentID: ctx.sessionID,
          title: params.description + ` (@${next.name} subagent)`,
          agent: next.name,
          permission: [
            ...childPermission,
            ...childToolDenies.filter(
              (deny) =>
                !childPermission.some(
                  (rule) =>
                    rule.permission === deny.permission && rule.pattern === deny.pattern && rule.action === deny.action,
                ),
            ),
          ],
        }))

      const msg = yield* MessageV2.get({ sessionID: ctx.sessionID, messageID: ctx.messageID }).pipe(
        Effect.provideService(Database.Service, database),
        Effect.orDie,
      )
      if (msg.info.role !== "assistant") return yield* Effect.fail(new Error("Not an assistant message"))
      const variant = msg.info.variant

      const model = next.model ?? {
        modelID: msg.info.modelID,
        providerID: msg.info.providerID,
      }
      const metadata = {
        parentSessionId: ctx.sessionID,
        sessionId: nextSession.id,
        model,
        ...(runInBackground ? { background: true } : {}),
        daemon: params.daemon === true,
      }

      yield* ctx.metadata({
        title: params.description,
        metadata,
      })

      const ops = ctx.extra?.promptOps as TaskPromptOps
      if (!ops) return yield* Effect.fail(new Error("TaskTool requires promptOps in ctx.extra"))

      // Worktree isolation (spec B): allocate an isolated git worktree before
      // scheduling whenever requested explicitly or implied by background mode.
      const worktreeSvc = useWorktree ? Option.getOrNull(worktreeOption) ?? undefined : undefined
      if (useWorktree && !worktreeSvc) {
        if (params.worktree === true)
          return yield* Effect.fail(new Error("Worktree mode requires the worktree service"))
        yield* Effect.logWarning("Worktree mode unavailable: worktree service not installed; running without isolation")
      }
      let worktreeEntry: WorktreeEntry | undefined
      let mergeResult: Worktree.MergeResult | undefined
      if (worktreeSvc) {
        const existing = worktrees.get(nextSession.id)
        if (existing) {
          worktreeEntry = existing
        } else {
          const parentCtx = Option.fromNullishOr(yield* InstanceRef)
          if (Option.isNone(parentCtx)) {
            if (params.worktree === true)
              return yield* Effect.fail(new Error("Worktree mode requires a project instance context"))
            yield* Effect.logWarning("Worktree mode unavailable: no project instance; running without isolation")
          } else {
            const created = yield* worktreeSvc.create({ name: `ocd-${params.subagent_type}` }).pipe(
              Effect.match({
                onSuccess: (info) => ({ ok: true as const, info }),
                onFailure: (error: unknown) => ({ ok: false as const, error }),
              }),
            )
            if (created.ok) {
              const childCtx: InstanceContext = {
                directory: created.info.directory,
                worktree: created.info.directory,
                project: parentCtx.value.project,
              }
              worktreeEntry = { info: created.info, parentCtx: parentCtx.value, childCtx, consumed: false }
              worktrees.set(nextSession.id, worktreeEntry)
            } else if (params.worktree === true) {
              return yield* Effect.fail(new Error(`Failed to create a subagent worktree:\n${errorMessage(created.error)}`))
            } else {
              yield* Effect.logWarning("Subagent worktree unavailable; running without isolation", {
                error: errorMessage(created.error),
              })
            }
          }
        }
      }

      const provideChild = <A, E, R>(entry: WorktreeEntry, effect: Effect.Effect<A, E, R>) =>
        Effect.provideService(InstanceRef, entry.childCtx)(effect)
      const provideParent = <A, E, R>(entry: WorktreeEntry, effect: Effect.Effect<A, E, R>) =>
        Effect.provideService(InstanceRef, entry.parentCtx)(effect)

      // Security gate R3: merge NEVER proceeds without human approval. The diff
      // preview (stat + changed files) is shown in the permission prompt; deny,
      // rejection, or a non-interactive environment defaults to no merge.
      const requestMergeApproval = Effect.fn("TaskTool.requestWorktreeMergeApproval")(function* (preview: {
        stat: string
        files: string[]
      }) {
        const decision = yield* ctx
          .ask({
            permission: "worktree-merge",
            patterns: ["*"],
            always: [],
            metadata: { stat: preview.stat, files: preview.files },
          })
          .pipe(
            Effect.match({ onFailure: () => false, onSuccess: () => true }),
            Effect.timeoutOption("10 minutes"),
          )
        return Option.isSome(decision) ? decision.value : false
      })

      // Exactly-once completion: the first exit claims the entry — success
      // merges (inside the parent InstanceRef so the lifecycle targets the
      // parent checkout), failure/cancel removes the worktree. Later exits
      // (resume/extend) no-op. Runs under the child InstanceRef so the child's
      // tools target the worktree checkout.
      const cleanupWorktree = Effect.fnUntraced(function* (
        entry: WorktreeEntry,
        exit: Exit.Exit<string, unknown>,
      ) {
        if (entry.consumed) return
        entry.consumed = true
        worktrees.delete(nextSession.id)
        if (!worktreeSvc) return
        if (!Exit.isSuccess(exit)) {
          yield* provideParent(entry, worktreeSvc.remove({ directory: entry.info.directory })).pipe(Effect.ignore)
          return
        }
        mergeResult = yield* provideParent(
          entry,
          worktreeSvc.mergeAndCleanup(entry.info, {
            commit: `ocd ${params.subagent_type} #${nextSession.id}`,
            email: `ocd-subagent-${nextSession.id}@ocd.local`,
            approve:
              depth > 0 || params.autoApprove === true
                ? () => Effect.succeed(true)
                : (preview) => requestMergeApproval(preview),
          }),
        ).pipe(
          Effect.match({
            onSuccess: (result) => result,
            onFailure: (error: unknown) => ({ status: "merge_failed" as const, detail: errorMessage(error) }),
          }),
        )
      })

      const wrappedRun = (entry: WorktreeEntry | undefined, run: Effect.Effect<string, unknown>) =>
        (entry ? provideChild(entry, run) : run).pipe(
          Effect.onInterrupt(() => ops.cancel(nextSession.id)),
          Effect.onExit((exit) => (entry ? cleanupWorktree(entry, exit) : Effect.void)),
        )

      const mergeBlock = () =>
        mergeResult ? { status: mergeResult.status, stat: mergeResult.stat, detail: mergeResult.detail } : undefined

      const runTask = Effect.fn("TaskTool.runTask")(function* () {
        const parts = yield* ops.resolvePromptParts(params.prompt)
        const result = yield* ops.prompt({
          messageID: MessageID.ascending(),
          sessionID: nextSession.id,
          model: {
            modelID: model.modelID,
            providerID: model.providerID,
          },
          variant: next.model ? undefined : variant,
          agent: next.name,
          parts,
        })
        if (result.info.role === "assistant" && result.info.error) {
          const message =
            "message" in result.info.error.data && typeof result.info.error.data.message === "string"
              ? result.info.error.data.message
              : result.info.error.name
          return yield* Effect.fail(new Error(`Subagent failed (task_id: ${nextSession.id}): ${message}`))
        }
        const failed = result.parts.findLast((item) => item.type === "tool" && item.state.status === "error")
        if (failed?.type === "tool" && failed.state.status === "error") {
          return yield* Effect.fail(new Error(`Subagent failed (task_id: ${nextSession.id}): ${failed.state.error}`))
        }
        return result.parts.findLast((item) => item.type === "text")?.text ?? ""
      })

      const emitMilestone = (state: "started" | "running" | "completed" | "error") => {
        GlobalBus.emit("event", {
          payload: {
            type: "task.milestone",
            properties: {
              sessionID: nextSession.id,
              parentSessionID: ctx.sessionID,
              status: state,
              state,
              agent: next.name,
              description: params.description,
            },
          },
        })
      }

      const bubbleMilestoneToRoot = (state: "completed" | "error") => {
        if (depth >= 1 && rootSession.id !== ctx.sessionID) {
          const milestoneLine = `[Milestone] @${next.name}: ${params.description} (${state})`
          return ops
            .prompt({
              sessionID: rootSession.id,
              agent: rootSession.agent ?? ctx.agent,
              variant,
              parts: [
                {
                  type: "text",
                  synthetic: true,
                  text: milestoneLine,
                },
              ],
            })
            .pipe(Effect.ignore, Effect.forkIn(scope, { startImmediately: true }))
        }
        return Effect.void
      }

      const inject = Effect.fn("TaskTool.injectBackgroundResult")(function* (
        state: "completed" | "error",
        text: string,
      ) {
        emitMilestone(state)
        yield* bubbleMilestoneToRoot(state)
        const currentParent = yield* sessions.get(ctx.sessionID)
        yield* ops
          .prompt({
            sessionID: ctx.sessionID,
            agent: currentParent.agent ?? ctx.agent,
            variant,
            parts: [
              {
                type: "text",
                synthetic: true,
                text: renderOutput({
                  sessionID: nextSession.id,
                  state,
                  summary:
                    state === "completed"
                      ? `Background task completed: ${params.description}`
                      : `Background task failed: ${params.description}`,
                  text,
                  merge: mergeBlock(),
                }),
              },
            ],
          })
          .pipe(Effect.ignore, Effect.forkIn(scope, { startImmediately: true }))
      })

      const notify = Effect.fn("TaskTool.notifyBackgroundResult")(function* (jobID: string) {
        yield* background.wait({ id: jobID }).pipe(
          Effect.flatMap((result) => {
            if (result.info?.status === "completed") return inject("completed", result.info.output ?? "")
            if (result.info?.status === "error") return inject("error", result.info.error ?? "")
            return Effect.void
          }),
          Effect.forkIn(scope, { startImmediately: true }),
        )
      })

      if (yield* background.extend({ id: nextSession.id, run: wrappedRun(worktreeEntry, runTask()) })) {
        emitMilestone("running")
        return {
          title: params.description,
          metadata: {
            ...metadata,
            background: true,
            jobId: nextSession.id,
          },
          output: renderOutput({
            sessionID: nextSession.id,
            state: "running",
            summary: "Background task updated",
            text: BACKGROUND_UPDATED,
          }),
        }
      }

      const info = yield* background.start({
        id: nextSession.id,
        type: id,
        title: params.description,
        metadata,
        onPromote: Effect.all([
          ctx.metadata({
            title: params.description,
            metadata: { ...metadata, background: true, jobId: nextSession.id },
          }),
          notify(nextSession.id),
        ]),
        run: wrappedRun(worktreeEntry, runTask()),
      })
      emitMilestone("started")

      function backgroundResult() {
        return {
          title: params.description,
          metadata: {
            ...metadata,
            background: true,
            jobId: info.id,
          },
          output: renderOutput({
            sessionID: nextSession.id,
            state: "running",
            summary: "Background task started",
            text: BACKGROUND_STARTED,
          }),
        }
      }

      if (runInBackground) {
        yield* notify(info.id)
        return backgroundResult()
      }

      const runCancel = yield* EffectBridge.make()
      const cancel = ops.cancel(nextSession.id)

      function onAbort() {
        runCancel.fork(cancel)
      }

      return yield* Effect.acquireUseRelease(
        Effect.sync(() => {
          ctx.abort.addEventListener("abort", onAbort)
        }),
        () =>
          Effect.gen(function* () {
            const result = yield* Effect.raceFirst(
              background.wait({ id: nextSession.id }).pipe(Effect.map((waited) => waited.info)),
              background.waitForPromotion(nextSession.id),
            )
            if (result?.metadata?.background === true) return backgroundResult()
            if (result?.status === "error") {
              emitMilestone("error")
              yield* bubbleMilestoneToRoot("error")
              return yield* Effect.fail(new Error(result.error ?? "Task failed"))
            }
            if (result?.status === "cancelled") {
              emitMilestone("error")
              yield* bubbleMilestoneToRoot("error")
              return yield* Effect.fail(new Error("Task cancelled"))
            }
            emitMilestone("completed")
            yield* bubbleMilestoneToRoot("completed")
            return {
              title: params.description,
              metadata,
              output: renderOutput({
                sessionID: nextSession.id,
                state: "completed",
                text: result?.output ?? "",
                merge: mergeBlock(),
              }),
            }
          }),
        (_, exit) =>
          Effect.gen(function* () {
            if (Exit.hasInterrupts(exit))
              yield* Effect.all([cancel, background.cancel(nextSession.id)], { discard: true })
            // Foreground caller abandoned the session: drop the worktree
            // instead of merging. The worktree entry is claimed here so the
            // child's own onExit cleanup becomes a no-op.
            const svc = worktreeSvc
            if (worktreeEntry && !worktreeEntry.consumed && svc) {
              worktreeEntry.consumed = true
              worktrees.delete(nextSession.id)
              yield* provideParent(worktreeEntry, svc.remove({ directory: worktreeEntry.info.directory })).pipe(
                Effect.ignore,
              )
            }
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                ctx.abort.removeEventListener("abort", onAbort)
              }),
            ),
          ),
      )
    })

    return {
      description: [DESCRIPTION, BACKGROUND_DESCRIPTION].join("\n\n"),
      parameters: Parameters,
      execute: (params: Schema.Schema.Type<typeof Parameters>, ctx: Tool.Context) =>
        run(params, ctx).pipe(Effect.orDie),
    }
  }),
)
