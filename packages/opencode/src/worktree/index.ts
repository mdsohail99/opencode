import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { path } from "@opencode-ai/core/effect/app-node-platform"
import { Global } from "@opencode-ai/core/global"
import { InstanceStore } from "@/project/instance-store"
import { Project } from "@/project/project"
import { Database } from "@opencode-ai/core/database/database"
import { eq } from "drizzle-orm"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import type { ProjectV2 } from "@opencode-ai/core/project"
import { Slug } from "@opencode-ai/core/util/slug"
import { errorMessage } from "../util/error"
import { GlobalBus } from "@/bus/global"
import { Git } from "@/git"
import { Effect, Layer, Option, Path, Schema, Scope, Context, Stream } from "effect"
import { ChildProcess } from "effect/unstable/process"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { AppProcess } from "@opencode-ai/core/process"
import { InstanceState } from "@/effect/instance-state"
import { WorktreeEvent } from "@opencode-ai/schema/worktree-event"
import { Env } from "@/worktree/env"
import { EffectFlock } from "@opencode-ai/core/util/effect-flock"

export const Event = WorktreeEvent

export const Info = Schema.Struct({
  name: Schema.String,
  branch: Schema.optional(Schema.String),
  directory: Schema.String,
  parentBranch: Schema.optional(Schema.String),
  parentHeadSHA: Schema.optional(Schema.String),
}).annotate({ identifier: "Worktree" })
export type Info = Schema.Schema.Type<typeof Info>

export const MergeStatuses = [
  "merged",
  "no_changes",
  "parent_dirty",
  "parent_stash_conflict",
  "parent_merge_in_progress",
  "parent_rebase_in_progress",
  "parent_stash_foreign",
  "parent_detached",
  "merge_conflict",
  "crash_recovered",
  "commit_failed",
  "parent_moved",
  "merge_blocked",
  "merge_rejected",
  "merge_integrity_failure",
  "merge_failed",
] as const
export type MergeStatus = (typeof MergeStatuses)[number]

export interface MergeResult {
  readonly status: MergeStatus
  readonly stat?: string
  readonly files?: string[]
  readonly conflicts?: string[]
  readonly stash?: string
  readonly detail?: string
  readonly recovery?: string
}

export interface MergeOptions {
  readonly approve?: (preview: { stat: string; files: string[] }) => Effect.Effect<boolean>
  readonly stash?: boolean
  readonly commit?: string
  readonly email?: string
}

export const CreateInput = Schema.Struct({
  name: Schema.optional(Schema.String),
  startCommand: Schema.optional(
    Schema.String.annotate({ description: "Additional startup script to run after the project's start command" }),
  ),
}).annotate({ identifier: "WorktreeCreateInput" })
export type CreateInput = Schema.Schema.Type<typeof CreateInput>

export const RemoveInput = Schema.Struct({
  directory: Schema.String,
}).annotate({ identifier: "WorktreeRemoveInput" })
export type RemoveInput = Schema.Schema.Type<typeof RemoveInput>

export const ResetInput = Schema.Struct({
  directory: Schema.String,
}).annotate({ identifier: "WorktreeResetInput" })
export type ResetInput = Schema.Schema.Type<typeof ResetInput>

export class NotGitError extends Schema.TaggedErrorClass<NotGitError>()("WorktreeNotGitError", {
  message: Schema.String,
}) {}

export class NameGenerationFailedError extends Schema.TaggedErrorClass<NameGenerationFailedError>()(
  "WorktreeNameGenerationFailedError",
  {
    message: Schema.String,
  },
) {}

export class CreateFailedError extends Schema.TaggedErrorClass<CreateFailedError>()("WorktreeCreateFailedError", {
  message: Schema.String,
}) {}

export class StartCommandFailedError extends Schema.TaggedErrorClass<StartCommandFailedError>()(
  "WorktreeStartCommandFailedError",
  {
    message: Schema.String,
  },
) {}

export class RemoveFailedError extends Schema.TaggedErrorClass<RemoveFailedError>()("WorktreeRemoveFailedError", {
  message: Schema.String,
}) {}

export class ResetFailedError extends Schema.TaggedErrorClass<ResetFailedError>()("WorktreeResetFailedError", {
  message: Schema.String,
}) {}

export class ListFailedError extends Schema.TaggedErrorClass<ListFailedError>()("WorktreeListFailedError", {
  message: Schema.String,
}) {}

export type Error =
  | NotGitError
  | NameGenerationFailedError
  | CreateFailedError
  | StartCommandFailedError
  | RemoveFailedError
  | ResetFailedError
  | ListFailedError

function slugify(input: string) {
  return input
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+/, "")
    .replace(/-+$/, "")
}

function failedRemoves(...chunks: string[]) {
  return chunks.filter(Boolean).flatMap((chunk) =>
    chunk
      .split("\n")
      .map((line) => line.trim())
      .flatMap((line) => {
        const match = line.match(/^warning:\s+failed to remove\s+(.+):\s+/i)
        if (!match) return []
        const value = match[1]?.trim().replace(/^['"]|['"]$/g, "")
        if (!value) return []
        return [value]
      }),
  )
}

// ---------------------------------------------------------------------------
// Effect service
// ---------------------------------------------------------------------------

export interface Interface {
  readonly makeWorktreeInfo: (options?: { name?: string; detached?: boolean }) => Effect.Effect<Info, Error>
  readonly createFromInfo: (info: Info, startCommand?: string) => Effect.Effect<void, Error>
  readonly create: (input?: CreateInput) => Effect.Effect<Info, Error>
  readonly list: () => Effect.Effect<(Omit<Info, "branch"> & { branch?: string })[], Error>
  readonly remove: (input: RemoveInput) => Effect.Effect<boolean, Error>
  readonly reset: (input: ResetInput) => Effect.Effect<boolean, Error>
  readonly mergeAndCleanup: (info: Info, options?: MergeOptions) => Effect.Effect<MergeResult, Error>
  readonly pruneOrphans: () => Effect.Effect<number, Error>
  readonly recover: () => Effect.Effect<{ restored: string[]; kept: string[] }, Error>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Worktree") {}

type GitResult = { code: number; text: string; stderr: string }

const layer: Layer.Layer<
  Service,
  never,
  | FSUtil.Service
  | Path.Path
  | AppProcess.Service
  | Git.Service
  | Project.Service
  | InstanceStore.Service
  | Database.Service
  | EffectFlock.Service
> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const scope = yield* Scope.Scope
    const fs = yield* FSUtil.Service
    const pathSvc = yield* Path.Path
    const appProcess = yield* AppProcess.Service
    const { db } = yield* Database.Service
    const gitSvc = yield* Git.Service
    const project = yield* Project.Service
    const store = yield* InstanceStore.Service
    const flock = yield* EffectFlock.Service

    const git = Effect.fnUntraced(function* (args: string[], opts?: { cwd?: string }) {
      const scoped = opts?.cwd ? Env.isWorktreeDirectory(opts.cwd) : false
      if (scoped && Env.isDeniedGitSubcommand(args)) {
        return {
          code: 128,
          text: "",
          stderr: "network git is disabled inside subagent worktrees",
        } satisfies GitResult
      }
      return yield* appProcess
        .run(
          ChildProcess.make("git", args, {
            cwd: opts?.cwd,
            env: scoped ? Env.scrubGitEnv() : undefined,
            extendEnv: !scoped,
            stdin: "ignore",
          }),
        )
        .pipe(
          Effect.match({
            onSuccess: (result) =>
              ({
                code: result.exitCode,
                text: result.stdout.toString("utf8"),
                stderr: result.stderr.toString("utf8"),
              }) satisfies GitResult,
            onFailure: (e: unknown) =>
              ({
                code: 1,
                text: "",
                stderr: e instanceof Error ? e.message : String(e),
              }) satisfies GitResult,
          }),
        )
    })

    // -- lifecycle support (journal, alive pids, kill tree, validation) --

    const projectRoot = (projectID: string) => pathSvc.join(Global.Path.data, "worktree", projectID)
    const journalDir = (root: string) => pathSvc.join(root, ".journal")
    const aliveDir = (root: string) => pathSvc.join(root, ".alive")
    const journalPath = (root: string, name: string) => pathSvc.join(journalDir(root), `${name}.json`)
    const alivePath = (root: string, name: string) => pathSvc.join(aliveDir(root), `${name}.json`)

    interface JournalEntry {
      readonly name: string
      readonly directory: string
      readonly branch?: string
      readonly parentBranch?: string
      readonly parentHeadSHA?: string
      readonly stage: string
      readonly pid: number
      readonly mergedSHA?: string
    }

    const readJournal = (root: string, name: string) =>
      fs
        .readFileString(journalPath(root, name))
        .pipe(
          Effect.flatMap((text) =>
            Effect.try({
              try: () => JSON.parse(text) as JournalEntry,
              catch: () => new Error("invalid journal"),
            }),
          ),
          Effect.option,
        )

    const writeJournal = (root: string, entry: JournalEntry) =>
      fs
        .writeFileString(journalPath(root, entry.name), JSON.stringify(entry))
        .pipe(Effect.orDie)

    const deleteJournal = (root: string, name: string) => fs.remove(journalPath(root, name)).pipe(Effect.ignore)

    const journalNames = (root: string) =>
      fs
        .readDirectory(journalDir(root))
        .pipe(
          Effect.map((files) => files.filter((f) => f.endsWith(".json")).map((f) => f.slice(0, -".json".length))),
          Effect.catch(() => Effect.succeed([])),
          Effect.orDie,
        )

    const readAlive = (root: string, name: string) =>
      fs
        .readFileString(alivePath(root, name))
        .pipe(
          Effect.flatMap((text) =>
            Effect.try({
              try: () => JSON.parse(text) as { pids: number[] },
              catch: () => new Error("invalid alive"),
            }),
          ),
          Effect.match({
            onFailure: (): { pids: number[] } => ({ pids: [] }),
            onSuccess: (entry) => entry,
          }),
          Effect.orDie,
        )

    const writeAlive = (root: string, name: string, pids: number[]) =>
      fs.writeFileString(alivePath(root, name), JSON.stringify({ pids })).pipe(Effect.orDie)

    const deleteAlive = (root: string, name: string) => fs.remove(alivePath(root, name)).pipe(Effect.ignore)

    const addAlivePid = Effect.fnUntraced(function* (root: string, name: string, pid: number) {
      const existing = yield* readAlive(root, name)
      const pids = existing.pids.includes(pid) ? existing.pids : [...existing.pids, pid]
      yield* writeAlive(root, name, pids)
    })

    const killPids = Effect.fnUntraced(function* (pids: number[]) {
      if (!pids.length) return
      if (process.platform === "win32") {
        yield* Effect.forEach(
          pids,
          (pid) =>
            appProcess
              .run(
                ChildProcess.make("taskkill", ["/F", "/T", "/PID", String(pid)], {
                  extendEnv: true,
                  stdin: "ignore",
                }),
              )
              .pipe(Effect.ignore),
          { concurrency: "unbounded" },
        )
        return
      }
      const descendants = yield* Effect.forEach(pids, descendantsOf, { concurrency: "unbounded" }).pipe(
        Effect.map((groups) => groups.flat()),
      )
      for (const pid of [...descendants, ...pids]) {
        try {
          process.kill(pid, "SIGKILL")
        } catch {
          // already gone
        }
      }
    })

    const descendantsOf: (pid: number) => Effect.Effect<number[], never, never> = Effect.fnUntraced(function* (
      pid: number,
    ) {
      const result = yield* appProcess
        .run(ChildProcess.make("pgrep", ["-P", String(pid)], { extendEnv: true, stdin: "ignore" }))
        .pipe(
          Effect.catch(() =>
            Effect.succeed({
              command: "pgrep",
              exitCode: 1,
              stdout: Buffer.alloc(0),
              stderr: Buffer.alloc(0),
              stdoutTruncated: false,
              stderrTruncated: false,
            }),
          ),
        )
      const children = result.stdout
        .toString("utf8")
        .split("\n")
        .map((line) => Number.parseInt(line.trim(), 10))
        .filter((n) => Number.isFinite(n))
      const nested: number[] = yield* Effect.forEach(children, (child) => descendantsOf(child), {
        concurrency: "unbounded",
      }).pipe(Effect.map((groups) => groups.flat()))
      return [...children, ...nested]
    })

    const killAlive = Effect.fnUntraced(function* (root: string, name: string) {
      const entry = yield* readAlive(root, name)
      yield* killPids(entry.pids)
      yield* deleteAlive(root, name)
    })

    // R7 input boundary: directory must stay under the project root; branch regex.
    const BRANCH_REGEX = /^[A-Za-z0-9._/-]+$/

    const requireValidBranch = (branch: string) => {
      if (!branch) return false
      if (branch.startsWith("-")) return false
      if (branch.includes("..") || branch.includes("@{")) return false
      if (branch.endsWith("/") || branch.includes("\\")) return false
      return BRANCH_REGEX.test(branch)
    }

    const validateDirectory = Effect.fnUntraced(function* (root: string, directory: string) {
      const base = yield* canonical(root)
      const dir = yield* canonical(directory)
      if (dir === base || !dir.startsWith(`${base}${pathSvc.sep}`)) {
        return false
      }
      return true
    })

    const MAX_NAME_ATTEMPTS = 26
    const candidate = Effect.fn("Worktree.candidate")(function* (input: {
      root: string
      name?: string
      detached?: boolean
    }) {
      const ctx = yield* InstanceState.context
      for (const attempt of Array.from({ length: MAX_NAME_ATTEMPTS }, (_, i) => i)) {
        const name = input.name ? (attempt === 0 ? input.name : `${input.name}-${Slug.create()}`) : Slug.create()
        const branch = input.detached ? undefined : `opencode/${name}`
        const directory = pathSvc.join(input.root, name)

        if (yield* fs.exists(directory).pipe(Effect.orDie)) continue

        if (branch) {
          const ref = `refs/heads/${branch}`
          const branchCheck = yield* git(["show-ref", "--verify", "--quiet", ref], { cwd: ctx.worktree })
          if (branchCheck.code === 0) continue
        }

        return { name, directory, ...(branch ? { branch } : {}) }
      }
      return yield* new NameGenerationFailedError({ message: "Failed to generate a unique worktree name" })
    })

    const makeWorktreeInfo = Effect.fn("Worktree.makeWorktreeInfo")(function* (input?: {
      name?: string
      detached?: boolean
    }) {
      const ctx = yield* InstanceState.context
      if (ctx.project.vcs !== "git") {
        return yield* new NotGitError({ message: "Worktrees are only supported for git projects" })
      }

      const root = pathSvc.join(Global.Path.data, "worktree", ctx.project.id)
      yield* fs.makeDirectory(root, { recursive: true }).pipe(Effect.orDie)

      return yield* candidate({ root, name: input?.name ? slugify(input.name) : "", detached: input?.detached })
    })

    const setup = Effect.fnUntraced(function* (info: Info) {
      const ctx = yield* InstanceState.context
      const created = yield* git(
        info.branch
          ? ["worktree", "add", "--no-checkout", "-b", info.branch, info.directory]
          : ["worktree", "add", "--no-checkout", "--detach", info.directory, "HEAD"],
        { cwd: ctx.worktree },
      )
      if (created.code !== 0) {
        return yield* new CreateFailedError({
          message: created.stderr || created.text || "Failed to create git worktree",
        })
      }

      yield* project.addSandbox(ctx.project.id, info.directory).pipe(Effect.catch(() => Effect.void))
    })

    const boot = Effect.fnUntraced(function* (info: Info, startCommand?: string) {
      const ctx = yield* InstanceState.context
      const workspaceID = yield* InstanceState.workspaceID
      const projectID = ctx.project.id
      const extra = startCommand?.trim()

      const populated = yield* git(["reset", "--hard"], { cwd: info.directory })
      if (populated.code !== 0) {
        const message = populated.stderr || populated.text || "Failed to populate worktree"
        yield* Effect.logError("worktree checkout failed", { directory: info.directory, message })
        GlobalBus.emit("event", {
          directory: info.directory,
          project: ctx.project.id,
          workspace: workspaceID,
          payload: { type: Event.Failed.type, properties: { message } },
        })
        return
      }

      const booted = yield* store.load({ directory: info.directory }).pipe(
        Effect.as(true),
        Effect.catch((error) =>
          Effect.gen(function* () {
            const message = errorMessage(error)
            yield* Effect.logError("worktree bootstrap failed", { directory: info.directory, message })
            GlobalBus.emit("event", {
              directory: info.directory,
              project: ctx.project.id,
              workspace: workspaceID,
              payload: { type: Event.Failed.type, properties: { message } },
            })
            return false
          }),
        ),
      )
      if (!booted) return

      GlobalBus.emit("event", {
        directory: info.directory,
        project: ctx.project.id,
        workspace: workspaceID,
        payload: {
          type: Event.Ready.type,
          properties: { name: info.name, ...(info.branch ? { branch: info.branch } : {}) },
        },
      })

      yield* runStartScripts(info.directory, { projectID, extra })
    })

    const createFromInfo = Effect.fn("Worktree.createFromInfo")(function* (info: Info, startCommand?: string) {
      const ctx = yield* InstanceState.context
      const root = projectRoot(ctx.project.id)

      // R7 input boundary: directory under the project root, branch shaped safe.
      if (!(yield* validateDirectory(root, info.directory))) {
        return yield* new CreateFailedError({
          message: `Worktree directory must be under ${root}`,
        })
      }
      if (info.branch && !requireValidBranch(info.branch)) {
        return yield* new CreateFailedError({
          message: `Invalid branch name: ${info.branch}`,
        })
      }

      yield* setup(info)
      yield* boot(info, startCommand).pipe(
        Effect.catchCause((cause) => Effect.logError("worktree bootstrap failed", { cause })),
        Effect.forkIn(scope),
      )
    })

    const create = Effect.fn("Worktree.create")(function* (input?: CreateInput) {
      const ctx = yield* InstanceState.context
      const info = yield* makeWorktreeInfo({ name: input?.name })
      const root = projectRoot(ctx.project.id)

      // A.1 inputs, captured at create time and mirrored into the crash journal.
      const parentBranch = yield* gitSvc.branch(ctx.worktree)
      if (!parentBranch) {
        return yield* new CreateFailedError({
          message: "Parent branch is detached; worktrees require a named parent branch",
        })
      }
      const head = yield* gitSvc.run(["rev-parse", "HEAD"], { cwd: ctx.worktree })
      const parentHeadSHA = head.exitCode === 0 ? head.stdout.toString("utf8").trim() : undefined
      const enriched: Info = {
        ...info,
        ...(parentBranch ? { parentBranch } : {}),
        ...(parentHeadSHA ? { parentHeadSHA } : {}),
      }

      yield* fs.makeDirectory(journalDir(root), { recursive: true }).pipe(Effect.orDie)
      yield* fs.makeDirectory(aliveDir(root), { recursive: true }).pipe(Effect.orDie)
      yield* writeJournal(root, {
        name: enriched.name,
        directory: enriched.directory,
        branch: enriched.branch,
        parentBranch,
        parentHeadSHA,
        stage: "created",
        pid: process.pid,
      })
      yield* writeAlive(root, enriched.name, [])

      yield* createFromInfo(enriched, input?.startCommand)
      return enriched
    })

    const canonical = Effect.fnUntraced(function* (input: string) {
      const abs = pathSvc.resolve(input)
      const real = yield* fs.realPath(abs).pipe(Effect.catch(() => Effect.succeed(abs)))
      const normalized = pathSvc.normalize(real)
      return process.platform === "win32" ? normalized.toLowerCase() : normalized
    })

    function parseWorktreeList(text: string) {
      return text
        .split("\n")
        .map((line) => line.trim())
        .reduce<{ path?: string; branch?: string }[]>((acc, line) => {
          if (!line) return acc
          if (line.startsWith("worktree ")) {
            acc.push({ path: line.slice("worktree ".length).trim() })
            return acc
          }
          const current = acc[acc.length - 1]
          if (!current) return acc
          if (line.startsWith("branch ")) {
            current.branch = line.slice("branch ".length).trim()
          }
          return acc
        }, [])
    }

    const locateWorktree = Effect.fnUntraced(function* (
      entries: { path?: string; branch?: string }[],
      directory: string,
    ) {
      for (const item of entries) {
        if (!item.path) continue
        const key = yield* canonical(item.path)
        if (key === directory) return item
      }
      return undefined
    })

    const list = Effect.fn("Worktree.list")(function* () {
      const ctx = yield* InstanceState.context
      if (ctx.project.vcs !== "git") {
        return []
      }

      const result = yield* git(["worktree", "list", "--porcelain"], { cwd: ctx.worktree })
      if (result.code !== 0) {
        return yield* new ListFailedError({ message: result.stderr || result.text || "Failed to read git worktrees" })
      }

      const primary = yield* canonical(ctx.project.worktree)
      const primaryName = pathSvc.basename(primary).toLowerCase()
      return yield* Effect.forEach(parseWorktreeList(result.text), (entry) =>
        Effect.gen(function* () {
          if (!entry.path) return undefined
          const directory = yield* canonical(entry.path)
          if (directory === primary) return undefined
          const name = pathSvc.basename(directory).toLowerCase()
          return {
            name: name === primaryName ? pathSvc.basename(pathSvc.dirname(directory)) : name,
            directory,
            ...(entry.branch ? { branch: entry.branch.replace(/^refs\/heads\//, "") } : {}),
          }
        }),
      ).pipe(Effect.map((items) => items.filter((item) => item !== undefined)))
    })

    function stopFsmonitor(target: string) {
      return fs.exists(target).pipe(
        Effect.orDie,
        Effect.flatMap((exists) => (exists ? git(["fsmonitor--daemon", "stop"], { cwd: target }) : Effect.void)),
      )
    }

    function cleanDirectory(target: string) {
      return Effect.tryPromise({
        try: async () => {
          const fsp = await import("fs/promises")
          const attempts = process.platform === "win32" ? 50 : 5
          for (const attempt of Array.from({ length: attempts }, (_, i) => i)) {
            try {
              await fsp.rm(target, { recursive: true, force: true })
              return
            } catch (error) {
              if (attempt === attempts - 1) throw error
              await new Promise((resolve) => setTimeout(resolve, 100))
            }
          }
        },
        catch: (error) =>
          new RemoveFailedError({ message: errorMessage(error) || "Failed to remove git worktree directory" }),
      })
    }

    const remove = Effect.fn("Worktree.remove")(function* (input: RemoveInput) {
      const ctx = yield* InstanceState.context
      if (ctx.project.vcs !== "git") {
        return yield* new NotGitError({ message: "Worktrees are only supported for git projects" })
      }

      const root = projectRoot(ctx.project.id)
      const directory = yield* canonical(input.directory)

      // R7: never cleanDirectory an arbitrary directory.
      if (!(yield* validateDirectory(root, input.directory))) {
        return yield* new RemoveFailedError({ message: `Worktree directory must be under ${root}` })
      }

      // Guillotine: kill the recorded dev-server pids before touching the checkout.
      const name = pathSvc.basename(input.directory)
      yield* killAlive(root, name)
      yield* deleteJournal(root, name)

      // Preserve the loaded path casing for the store cache; `directory` is lowercased on Windows.
      if (directory !== (yield* canonical(ctx.worktree))) yield* store.disposeDirectory(input.directory)

      const list = yield* git(["worktree", "list", "--porcelain"], { cwd: ctx.worktree })
      if (list.code !== 0) {
        return yield* new RemoveFailedError({ message: list.stderr || list.text || "Failed to read git worktrees" })
      }

      const entries = parseWorktreeList(list.text)
      const entry = yield* locateWorktree(entries, directory)

      if (!entry?.path) {
        const directoryExists = yield* fs.exists(directory).pipe(Effect.orDie)
        if (directoryExists) {
          yield* stopFsmonitor(directory)
          yield* cleanDirectory(directory)
        }
        return true
      }

      // Git may return the original casing when a caller supplied a normalized Windows path.
      yield* store.disposeDirectory(entry.path)
      yield* stopFsmonitor(entry.path)
      const removed = yield* git(["worktree", "remove", "--force", entry.path], { cwd: ctx.worktree })
      if (removed.code !== 0) {
        const next = yield* git(["worktree", "list", "--porcelain"], { cwd: ctx.worktree })
        if (next.code !== 0) {
          return yield* new RemoveFailedError({
            message: removed.stderr || removed.text || next.stderr || next.text || "Failed to remove git worktree",
          })
        }

        const stale = yield* locateWorktree(parseWorktreeList(next.text), directory)
        if (stale?.path) {
          return yield* new RemoveFailedError({
            message: removed.stderr || removed.text || "Failed to remove git worktree",
          })
        }
      }

      yield* cleanDirectory(entry.path)

      const branch = entry.branch?.replace(/^refs\/heads\//, "")
      if (branch) {
        const deleted = yield* git(["branch", "-D", branch], { cwd: ctx.worktree })
        if (deleted.code !== 0) {
          return yield* new RemoveFailedError({
            message: deleted.stderr || deleted.text || "Failed to delete worktree branch",
          })
        }
      }

      return true
    })

    const gitExpect = Effect.fnUntraced(function* (
      args: string[],
      opts: { cwd: string },
      error: (r: GitResult) => Error,
    ) {
      const result = yield* git(args, opts)
      if (result.code !== 0) return yield* error(result)
      return result
    })

    const runStartCommand = Effect.fnUntraced(
      function* (directory: string, cmd: string, root: string) {
        const [shell, args] = process.platform === "win32" ? ["cmd", ["/c", cmd]] : ["bash", ["-lc", cmd]]
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const handle = yield* appProcess.spawn(
              ChildProcess.make(shell, args as string[], {
                cwd: directory,
                env: Env.scrubWorktreeEnv(process.env),
                extendEnv: false,
                stdin: "ignore",
              }),
            )
            yield* addAlivePid(root, pathSvc.basename(directory), Number(handle.pid))
            const [stdout, stderr] = yield* Effect.all(
              [
                Stream.runForEach(Stream.decodeText(handle.stdout), () => Effect.void),
                Stream.runForEach(Stream.decodeText(handle.stderr), () => Effect.void),
              ],
              { concurrency: "unbounded" },
            )
            const code = yield* handle.exitCode
            return { code, stderr: "" }
          }),
        )
        return result
      },
      Effect.catch(() => Effect.succeed({ code: 1, stderr: "" })),
    )

    const runStartScript = Effect.fnUntraced(function* (directory: string, cmd: string, kind: string, root: string) {
      const text = cmd.trim()
      if (!text) return true
      const result = yield* runStartCommand(directory, text, root)
      if (result.code === 0) return true
      yield* Effect.logError("worktree start command failed", { kind, directory, message: result.stderr })
      return false
    })

    const runStartScripts = Effect.fnUntraced(function* (
      directory: string,
      input: { projectID: ProjectV2.ID; extra?: string },
    ) {
      const row = yield* db
        .select()
        .from(ProjectTable)
        .where(eq(ProjectTable.id, input.projectID))
        .get()
        .pipe(Effect.orDie)
      const project = row ? Project.fromRow(row) : undefined
      const startup = project?.commands?.start?.trim() ?? ""
      const root = projectRoot(input.projectID)
      const ok = yield* runStartScript(directory, startup, "project", root)
      if (!ok) return false
      yield* runStartScript(directory, input.extra ?? "", "worktree", root)
      return true
    })

    const prune = Effect.fnUntraced(function* (root: string, entries: string[]) {
      const base = yield* canonical(root)
      yield* Effect.forEach(
        entries,
        (entry) =>
          Effect.gen(function* () {
            const target = yield* canonical(pathSvc.resolve(root, entry))
            if (target === base) return
            if (!target.startsWith(`${base}${pathSvc.sep}`)) return
            yield* fs.remove(target, { recursive: true }).pipe(Effect.ignore)
          }),
        { concurrency: "unbounded" },
      )
    })

    const sweep = Effect.fnUntraced(function* (root: string) {
      const first = yield* git(["clean", "-ffdx"], { cwd: root })
      if (first.code === 0) return first

      const entries = failedRemoves(first.stderr, first.text)
      if (!entries.length) return first

      yield* prune(root, entries)
      return yield* git(["clean", "-ffdx"], { cwd: root })
    })

    const reset = Effect.fn("Worktree.reset")(function* (input: ResetInput) {
      const ctx = yield* InstanceState.context
      if (ctx.project.vcs !== "git") {
        return yield* new NotGitError({ message: "Worktrees are only supported for git projects" })
      }

      const directory = yield* canonical(input.directory)
      const primary = yield* canonical(ctx.worktree)
      if (directory === primary) {
        return yield* new ResetFailedError({ message: "Cannot reset the primary workspace" })
      }

      const list = yield* git(["worktree", "list", "--porcelain"], { cwd: ctx.worktree })
      if (list.code !== 0) {
        return yield* new ResetFailedError({ message: list.stderr || list.text || "Failed to read git worktrees" })
      }

      const entry = yield* locateWorktree(parseWorktreeList(list.text), directory)
      if (!entry?.path) {
        return yield* new ResetFailedError({ message: "Worktree not found" })
      }

      const worktreePath = entry.path

      const base = yield* gitSvc.defaultBranch(ctx.worktree)
      if (!base) {
        return yield* new ResetFailedError({ message: "Default branch not found" })
      }

      const sep = base.ref.indexOf("/")
      if (base.ref !== base.name && sep > 0) {
        const remote = base.ref.slice(0, sep)
        const branch = base.ref.slice(sep + 1)
        yield* gitExpect(
          ["fetch", remote, branch],
          { cwd: ctx.worktree },
          (r) => new ResetFailedError({ message: r.stderr || r.text || `Failed to fetch ${base.ref}` }),
        )
      }

      yield* gitExpect(
        ["reset", "--hard", base.ref],
        { cwd: worktreePath },
        (r) => new ResetFailedError({ message: r.stderr || r.text || "Failed to reset worktree to target" }),
      )

      const cleanResult = yield* sweep(worktreePath)
      if (cleanResult.code !== 0) {
        return yield* new ResetFailedError({
          message: cleanResult.stderr || cleanResult.text || "Failed to clean worktree",
        })
      }

      yield* gitExpect(
        ["submodule", "update", "--init", "--recursive", "--force"],
        { cwd: worktreePath },
        (r) => new ResetFailedError({ message: r.stderr || r.text || "Failed to update submodules" }),
      )

      yield* gitExpect(
        ["submodule", "foreach", "--recursive", "git", "reset", "--hard"],
        { cwd: worktreePath },
        (r) => new ResetFailedError({ message: r.stderr || r.text || "Failed to reset submodules" }),
      )

      yield* gitExpect(
        ["submodule", "foreach", "--recursive", "git", "clean", "-fdx"],
        { cwd: worktreePath },
        (r) => new ResetFailedError({ message: r.stderr || r.text || "Failed to clean submodules" }),
      )

      const status = yield* git(["-c", "core.fsmonitor=false", "status", "--porcelain=v1"], { cwd: worktreePath })
      if (status.code !== 0) {
        return yield* new ResetFailedError({ message: status.stderr || status.text || "Failed to read git status" })
      }

      if (status.text.trim()) {
        return yield* new ResetFailedError({ message: `Worktree reset left local changes:\n${status.text.trim()}` })
      }

      yield* runStartScripts(worktreePath, { projectID: ctx.project.id }).pipe(
        Effect.catchCause((cause) => Effect.logError("worktree start task failed", { cause })),
        Effect.forkIn(scope),
      )

      return true
    })

    // ---------------------------------------------------------------------------
    // Merge + cleanup lifecycle (spec A, D, E)
    // ---------------------------------------------------------------------------

    const resultText = (result: { stdout: Buffer; stderr: Buffer }) => ({
      text: result.stdout.toString("utf8").trim(),
      err: result.stderr.toString("utf8").trim(),
    })

    const stderrOf = (result: { stderr: Buffer; stdout: Buffer }) =>
      resultText(result).err || resultText(result).text

    const isPidAlive = (pid: number) => {
      try {
        process.kill(pid, 0)
        return true
      } catch {
        return false
      }
    }

    const journalEntry = (
      info: Info,
      parentBranch: string | undefined,
      parentHeadSHA: string | undefined,
      stage: string,
      extra?: Partial<JournalEntry>,
    ): JournalEntry => ({
      name: info.name,
      directory: info.directory,
      branch: info.branch,
      parentBranch,
      parentHeadSHA,
      stage,
      pid: process.pid,
      ...extra,
    })

    /** Fast-forward merge with a "cannot lock ref"/index.lock retry. */
    const ffMerge = Effect.fn("Worktree.ffMerge")(function* (parent: string, branch: string) {
      for (let attempt = 0; attempt < 3; attempt++) {
        const merge = yield* gitSvc.run(["merge", "--ff-only", `refs/heads/${branch}`], { cwd: parent })
        if (merge.exitCode === 0) return { ok: true as const, detail: "" }
        const detail = stderrOf(merge)
        if (/cannot lock ref|index\.lock/i.test(detail) && attempt < 2) {
          yield* Effect.sleep("300 millis")
          continue
        }
        return { ok: false as const, detail }
      }
      return { ok: false as const, detail: "merge retries exhausted" }
    })

    const diffStat = Effect.fnUntraced(function* (parent: string, base: string | undefined, head: string | undefined) {
      if (!base || !head) return undefined
      const stat = yield* gitSvc.run(["diff", "--stat", `${base}..${head}`], { cwd: parent })
      return stat.exitCode === 0 ? resultText(stat).text || undefined : undefined
    })

    const mergeFiles = Effect.fnUntraced(function* (parent: string, base: string | undefined, head: string | undefined) {
      if (!base || !head) return []
      const result = yield* gitSvc.run(["diff", "--name-status", "--break-rewrites", `${base}..${head}`], { cwd: parent })
      return result.exitCode === 0
        ? result.stdout
            .toString("utf8")
            .split("\n")
            .filter(Boolean)
            .map((line) => line.replace(/^[^\t]+\t/, ""))
        : []
    })

    /** Pre-flight parent guard: returns a blocking status or undefined. */
    const preflightParent = Effect.fn("Worktree.preflightParent")(function* (parent: string, parentBranch?: string) {
      const gitDir = yield* gitSvc.run(["rev-parse", "--git-dir"], { cwd: parent })
      const gitDirPath = pathSvc.resolve(parent, gitDir.stdout.toString("utf8").trim())

      const markerExists = (name: string) => fs.exists(pathSvc.join(gitDirPath, name)).pipe(Effect.orDie)

      if (yield* markerExists("MERGE_HEAD")) {
        return { status: "parent_merge_in_progress" as const }
      }
      if ((yield* markerExists("rebase-merge")) || (yield* markerExists("rebase-apply"))) {
        return { status: "parent_rebase_in_progress" as const }
      }

      const stash = yield* gitSvc.run(["rev-parse", "--verify", "--quiet", "refs/stash"], { cwd: parent })
      if (stash.exitCode === 0) {
        const list = yield* gitSvc.run(["stash", "list"], { cwd: parent })
        return {
          status: "parent_stash_foreign" as const,
          detail: list.exitCode === 0 ? resultText(list).text : undefined,
        }
      }

      const head = yield* gitSvc.run(["rev-parse", "--verify", "--quiet", "HEAD"], { cwd: parent })
      if (head.exitCode !== 0) return { status: "parent_detached" as const }
      if (parentBranch) {
        const ref = yield* gitSvc.run(["rev-parse", "--verify", "--quiet", `refs/heads/${parentBranch}`], { cwd: parent })
        if (ref.exitCode !== 0) return { status: "parent_detached" as const }
      }
      return undefined
    })

    /** The parent-touching window (runs under the cross-process flock). */
    const mergeParent = Effect.fn("Worktree.mergeParent")(function* (
      parent: string,
      root: string,
      info: Info,
      parentBranch: string | undefined,
      parentHeadSHA: string | undefined,
      branch: string,
      options: MergeOptions | undefined,
    ) {
      // Rebase the child branch onto the CURRENT parent tip while holding the
      // lock: a rebase performed earlier could target a stale tip and produce
      // divergent history the ff-merge below refuses (concurrent merges).
      if (info.branch) {
        const rebase = yield* gitSvc.run(["rebase", parentBranch ?? "HEAD"], { cwd: info.directory })
        if (rebase.exitCode !== 0) {
          const unmerged = yield* gitSvc.run(["diff", "--name-only", "--diff-filter=U"], { cwd: info.directory })
          const conflicts = unmerged.exitCode === 0 ? resultText(unmerged).text.split("\n").filter(Boolean) : []
          yield* gitSvc.run(["rebase", "--abort"], { cwd: info.directory }).pipe(Effect.ignore)
          if (conflicts.length) return { status: "merge_conflict" as const, conflicts, detail: resultText(rebase).text }
          return { status: "merge_failed" as const, detail: stderrOf(rebase) }
        }
      }

      const statusResult = yield* gitSvc.run(["status", "--porcelain=v1"], { cwd: parent })
      const dirty = statusResult.exitCode !== 0 || statusResult.stdout.toString("utf8").trim().length > 0

      // Clean parent -> plain ff merge.
      if (!dirty) {
        const merged = yield* ffMerge(parent, branch)
        if (!merged.ok) {
          if (/untracked working tree files would be overwritten/i.test(merged.detail)) {
            return { status: "parent_dirty" as const, detail: merged.detail }
          }
          return { status: "merge_failed" as const, detail: merged.detail }
        }
        const head = yield* gitSvc.run(["rev-parse", "HEAD"], { cwd: parent })
        const mergedSHA = head.exitCode === 0 ? head.stdout.toString("utf8").trim() : undefined
        const stat = yield* diffStat(parent, parentHeadSHA, mergedSHA)
        yield* remove({ directory: info.directory }).pipe(Effect.ignore)
        yield* deleteJournal(root, info.name)
        return { status: "merged" as const, stat }
      }

      // Dirty parent: default = zero parent writes; guarded stash path only on flag.
      if (!options?.stash) return { status: "parent_dirty" as const }

      yield* writeJournal(root, journalEntry(info, parentBranch, parentHeadSHA, "stash_pending"))
      // Tracked dirty state BEFORE the stash; the post-pop verification below
      // expects the byte-identical restore (including the staged column).
      const trackedBefore = yield* gitSvc.run(["status", "--porcelain", "--untracked-files=no"], { cwd: parent })
      const stash = yield* gitSvc.run(
        ["stash", "push", "--include-untracked", "-m", `opencode-worktree:${info.name}`],
        { cwd: parent },
      )
      if (stash.exitCode !== 0) {
        yield* writeJournal(root, journalEntry(info, parentBranch, parentHeadSHA, "stash_failed"))
        return { status: "parent_dirty" as const, detail: stderrOf(stash) }
      }
      yield* writeJournal(root, journalEntry(info, parentBranch, parentHeadSHA, "stashed"))

      const merged = yield* ffMerge(parent, branch)
      if (!merged.ok) {
        return { status: "merge_failed" as const, detail: merged.detail, stash: "stash preserved" }
      }
      const head = yield* gitSvc.run(["rev-parse", "HEAD"], { cwd: parent })
      const mergedSHA = head.exitCode === 0 ? head.stdout.toString("utf8").trim() : undefined
      yield* writeJournal(root, journalEntry(info, parentBranch, parentHeadSHA, "merged", { mergedSHA }))

      const pop = yield* gitSvc.run(["stash", "pop", "--index"], { cwd: parent })
      if (pop.exitCode !== 0) {
        const list = yield* gitSvc.run(["stash", "list"], { cwd: parent })
        yield* writeJournal(root, journalEntry(info, parentBranch, parentHeadSHA, "pop_conflict"))
        return {
          status: "parent_stash_conflict" as const,
          stash: list.exitCode === 0 ? resultText(list).text : undefined,
          recovery: "git stash pop (resolve markers, then git stash drop)",
          detail: stderrOf(pop),
        }
      }

      const verify = yield* gitSvc.run(["status", "--porcelain", "--untracked-files=no"], { cwd: parent })
      if (verify.exitCode !== 0 || verify.stdout.toString("utf8") !== trackedBefore.stdout.toString("utf8")) {
        return { status: "merge_integrity_failure" as const, detail: resultText(verify).text }
      }

      const stat = yield* diffStat(parent, parentHeadSHA, mergedSHA)
      yield* remove({ directory: info.directory }).pipe(Effect.ignore)
      yield* deleteJournal(root, info.name)
      return { status: "merged" as const, stat }
    })

    const mergeAndCleanup = Effect.fn("Worktree.mergeAndCleanup")(function* (info: Info, options?: MergeOptions) {
      const ctx = yield* InstanceState.context
      if (!info.branch) {
        return { status: "commit_failed" as const, detail: "worktree has no branch to merge" }
      }
      const parent = ctx.worktree
      const root = projectRoot(ctx.project.id)
      const branch = info.branch
      const parentBranch = info.parentBranch ?? (yield* gitSvc.branch(parent))
      if (!parentBranch) {
        return { status: "merge_failed" as const, detail: "no parent branch to merge against" }
      }
      const parentHeadSHA = info.parentHeadSHA

      // Step 1: commit the worktree state (cwd = worktree). Empty -> no_changes.
      const stage = yield* gitSvc.run(["add", "-A"], { cwd: info.directory })
      if (stage.exitCode !== 0) return { status: "commit_failed" as const, detail: stderrOf(stage) }
      const pending = yield* gitSvc.run(["diff", "--cached", "--quiet"], { cwd: info.directory })
      if (pending.exitCode === 0) {
        yield* remove({ directory: info.directory }).pipe(Effect.ignore)
        yield* deleteJournal(root, info.name)
        return { status: "no_changes" as const }
      }
      const commit = yield* gitSvc.run(
        [
          "-c",
          "user.name=ocd-subagent",
          "-c",
          `user.email=${options?.email ?? "ocd-subagent@ocd.local"}`,
          "commit",
          "--no-verify",
          "-m",
          options?.commit ?? "ocd subagent worktree",
        ],
        { cwd: info.directory },
      )
      if (commit.exitCode !== 0) {
        const stillDirty = yield* gitSvc.run(["status", "--porcelain", "--untracked-files=no"], { cwd: info.directory })
        if (stillDirty.exitCode !== 0 || stillDirty.stdout.toString("utf8").trim().length > 0) {
          return { status: "commit_failed" as const, detail: stderrOf(commit) }
        }
      }

      // Step 2: diff preview for the approval gate below.
      const stat = yield* diffStat(parent, parentHeadSHA, "HEAD")
      const files = yield* mergeFiles(parent, parentHeadSHA, "HEAD")
      const authors = yield* gitSvc.run(["log", `${parentBranch}..HEAD`, "--format=%an <%ae>"], { cwd: info.directory })
      const foreignAuthors = authors.exitCode === 0
        ? resultText(authors).text
            .split("\n")
            .filter(Boolean)
            .filter((line) => !/ocd-subagent <ocd-subagent-.*@ocd\.local>/.test(line))
        : []
      const blocked = files.some(Env.matchesBlockedPath)

      if (blocked && foreignAuthors.length) {
        // R6 veto: blocked paths authored by a non-ocd identity — silent, no preview.
        yield* remove({ directory: info.directory }).pipe(Effect.ignore)
        return { status: "merge_blocked" as const, stat, files, detail: `non-ocd authors: ${foreignAuthors.join("; ")}` }
      }
      if (blocked) {
        GlobalBus.emit("event", {
          directory: info.directory,
          project: ctx.project.id,
          workspace: yield* InstanceState.workspaceID,
          payload: {
            type: "worktree.blocked",
            properties: { files, stat },
          },
        })
        return { status: "merge_blocked" as const, stat, files }
      }
      const approved = options?.approve ? yield* options.approve({ stat: stat ?? "", files }) : false
      if (!approved) return { status: "merge_rejected" as const, stat, files }

      // Step 4: parent pre-flight (read-only).
      const preflight = yield* preflightParent(parent, parentBranch)
      if (preflight) return preflight

      // Step 5: cross-process lock around the parent-touching window.
      return yield* flock
        .withLock(mergeParent(parent, root, info, parentBranch, parentHeadSHA, branch, options), `worktree:merge:${ctx.project.id}`)
        .pipe(
          Effect.catchTag("LockTimeoutError", () => Effect.succeed({ status: "merge_failed" as const, detail: "lock timeout" })),
          Effect.catchTag("LockCompromisedError", () => Effect.succeed({ status: "merge_integrity_failure" as const, detail: "lock compromised" })),
        )
    })

    // -----------------------------------------------------------------------
    // GC + crash recovery
    // -----------------------------------------------------------------------

    const pruneOrphans = Effect.fn("Worktree.pruneOrphans")(function* () {
      const ctx = yield* InstanceState.context
      if (ctx.project.vcs !== "git") return 0
      const root = projectRoot(ctx.project.id)
      const entries = yield* fs.readDirectory(root).pipe(Effect.catch(() => Effect.succeed([])), Effect.orDie)

      const list = yield* git(["worktree", "list", "--porcelain"], { cwd: ctx.worktree })
      const parsed = parseWorktreeList(list.text)
      const live = new Set<string>()
      for (const entry of parsed) {
        if (entry.path) live.add((yield* canonical(entry.path)).toLowerCase())
      }

      let removed = 0
      for (const entry of entries) {
        if (entry.startsWith(".")) continue
        const dir = pathSvc.join(root, entry)
        if (live.has((yield* canonical(dir)).toLowerCase())) continue
        const alive = yield* readAlive(root, entry)
        if (alive.pids.some(isPidAlive)) continue
        yield* stopFsmonitor(dir).pipe(Effect.ignore)
        yield* cleanDirectory(dir).pipe(Effect.ignore)
        removed++
      }

      if (removed) {
        yield* git(["worktree", "prune"], { cwd: ctx.worktree }).pipe(Effect.ignore)
        const refs = yield* git(["for-each-ref", "--format=%(refname:short)", "refs/heads/opencode/*"], { cwd: ctx.worktree })
        const liveBranches = new Set(
          parsed.map((entry) => entry.branch?.replace(/^refs\/heads\//, "")).filter((b): b is string => !!b),
        )
        for (const ref of refs.text.split("\n").map((line) => line.trim()).filter(Boolean)) {
          if (!liveBranches.has(ref)) yield* git(["branch", "-D", ref], { cwd: ctx.worktree }).pipe(Effect.ignore)
        }
      }

      // Prune stale journals (dead pid, older than 48h).
      const now = Date.now()
      for (const name of yield* journalNames(root)) {
        const entry = Option.getOrElse(yield* readJournal(root, name), () => undefined)
        if (!entry) continue
        if (isPidAlive(entry.pid)) continue
        const info = yield* fs.stat(journalPath(root, name)).pipe(Effect.option)
        if (Option.isSome(info) && now - Option.getOrElse(info.value.mtime, () => new Date(0)).getTime() > 48 * 60 * 60 * 1000) {
          yield* deleteJournal(root, name)
        }
      }
      return removed
    })

    const recover = Effect.fn("Worktree.recover")(function* () {
      const ctx = yield* InstanceState.context
      const root = projectRoot(ctx.project.id)
      const parent = ctx.worktree
      const restored: string[] = []
      const kept: string[] = []

      for (const name of yield* journalNames(root)) {
        const entry = Option.getOrElse(yield* readJournal(root, name), () => undefined)
        if (!entry) continue
        if (isPidAlive(entry.pid)) {
          kept.push(name)
          continue
        }

        if (entry.stage === "stashed") {
          // Dead creator + stashed parent: replay merge + pop only when parent is clean.
          const statusResult = yield* gitSvc.run(["status", "--porcelain=v1"], { cwd: parent })
          const dirty = statusResult.exitCode !== 0 || statusResult.stdout.toString("utf8").trim().length > 0
          if (dirty) {
            kept.push(name)
            continue
          }
          const merged = entry.branch ? yield* ffMerge(parent, entry.branch) : { ok: false as const, detail: "no branch" }
          if (merged.ok) {
            const pop = yield* gitSvc.run(["stash", "pop"], { cwd: parent })
            if (pop.exitCode === 0) {
              yield* deleteJournal(root, name)
              restored.push(name)
            } else {
              yield* writeJournal(root, { ...entry, stage: "pop_conflict" })
              kept.push(name)
            }
          } else {
            kept.push(name)
          }
        } else if (entry.stage === "merged") {
          const head = yield* gitSvc.run(["rev-parse", "HEAD"], { cwd: parent })
          const headSHA = head.exitCode === 0 ? head.stdout.toString("utf8").trim() : undefined
          if (entry.mergedSHA && headSHA === entry.mergedSHA) {
            yield* deleteJournal(root, name)
            restored.push(name)
          } else {
            kept.push(name)
          }
        } else {
          // created / committed / rebased / stash_failed / pop_conflict: no parent action.
          kept.push(name)
        }
      }
      return { restored, kept }
    })

    return Service.of({
      makeWorktreeInfo,
      createFromInfo,
      create,
      list,
      remove,
      reset,
      mergeAndCleanup,
      pruneOrphans,
      recover,
    })
  }),
)

export const node = LayerNode.make({
  service: Service,
  layer: layer,
  deps: [FSUtil.node, path, AppProcess.node, Git.node, Project.node, InstanceStore.node, Database.node, EffectFlock.node],
})

export * as Worktree from "."
