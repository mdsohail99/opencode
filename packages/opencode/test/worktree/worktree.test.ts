import { afterEach, describe, expect } from "bun:test"
import { createHash } from "crypto"
import { Effect, Exit, Cause, Deferred, Fiber } from "effect"
import * as fs from "fs/promises"
import os from "os"
import path from "path"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Global } from "@opencode-ai/core/global"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Git } from "../../src/git"
import { InstanceBootstrap } from "../../src/project/bootstrap"
import { InstanceStore } from "../../src/project/instance-store"
import { InstanceState } from "../../src/effect/instance-state"
import { InstanceRef } from "../../src/effect/instance-ref"
import type { InstanceContext } from "../../src/project/instance-context"
import { Worktree } from "../../src/worktree"
import { GlobalBus, type GlobalEvent } from "../../src/bus/global"
import { disposeAllInstances, TestInstance } from "../fixture/fixture"
import { testEffect, pollWithTimeout } from "../lib/effect"

// Layer identical to test/project/worktree.test.ts — Worktree + FSUtil + Git with
// the InstanceStore bootstrap swapped for the real InstanceBootstrap.
const it = testEffect(
  LayerNode.compile(LayerNode.group([Worktree.node, FSUtil.node, Git.node]), [
    [InstanceStore.bootstrapNode, InstanceBootstrap.node],
  ]),
)
const wintest = process.platform === "win32" ? it.instance : it.instance.skip

function normalize(input: string) {
  return input.replace(/\\/g, "/").toLowerCase()
}

const exists = (target: string) =>
  Effect.promise(() =>
    fs
      .stat(target)
      .then(() => true)
      .catch(() => false),
  )

const readText = (target: string) => Effect.promise(() => fs.readFile(target, "utf8"))

const writeText = (target: string, text: string) => Effect.promise(() => Bun.write(target, text))

// ---------------------------------------------------------------------------
// git helpers (GitService carries core.longpaths + fsmonitor=false config)
// ---------------------------------------------------------------------------

const git = Effect.fn("WorktreeTest.git")(function* (cwd: string, args: string[]) {
  const service = yield* Git.Service
  const result = yield* service.run(args, { cwd })
  if (result.exitCode !== 0) throw new Error(`git ${args.join(" ")} failed: ${result.stderr.toString("utf8")}`)
  return result.text()
})

const gitResult = Effect.fn("WorktreeTest.gitResult")(function* (cwd: string, args: string[]) {
  const service = yield* Git.Service
  return yield* service.run(args, { cwd })
})

const statusPorcelain = Effect.fn("WorktreeTest.statusPorcelain")(function* (cwd: string) {
  const result = yield* gitResult(cwd, ["status", "--porcelain=v1"])
  expect(result.exitCode).toBe(0)
  return result.stdout.toString("utf8")
})

const fileHash = Effect.fn("WorktreeTest.fileHash")(function* (file: string) {
  const content = yield* Effect.promise(() => fs.readFile(file))
  return createHash("sha256").update(content).digest("hex")
})

// ---------------------------------------------------------------------------
// worktree lifecycle helpers
// ---------------------------------------------------------------------------

const waitReady = Effect.fn("WorktreeTest.waitReady")(function* () {
  const ready = yield* Deferred.make<{ name: string; branch?: string }>()
  const on = (evt: GlobalEvent) => {
    if (evt.payload.type !== Worktree.Event.Ready.type) return
    Deferred.doneUnsafe(ready, Effect.succeed(evt.payload.properties))
  }

  GlobalBus.on("event", on)
  yield* Effect.addFinalizer(() => Effect.sync(() => GlobalBus.off("event", on)))

  return yield* Deferred.await(ready).pipe(
    Effect.timeoutOrElse({
      duration: "10 seconds",
      orElse: () => Effect.fail(new Error("timed out waiting for worktree.ready")),
    }),
  )
})

/** create() + wait for the checkout to be populated (Event.Ready). */
const createWorktree = Effect.fn("WorktreeTest.createWorktree")(function* (input?: Worktree.CreateInput) {
  const svc = yield* Worktree.Service
  const ready = yield* waitReady().pipe(Effect.forkScoped)
  const info = yield* svc.create(input)
  yield* Fiber.join(ready)
  return info
})

const cleanupWorktree = (directory: string) =>
  Effect.gen(function* () {
    const svc = yield* Worktree.Service
    yield* svc.remove({ directory }).pipe(Effect.ignore)
  })

const approve = () => Effect.succeed(true)

const projectRoot = Effect.fn("WorktreeTest.projectRoot")(function* () {
  const ctx = yield* InstanceState.context
  return path.join(Global.Path.data, "worktree", ctx.project.id)
})

afterEach(() => disposeAllInstances())

describe("Worktree lifecycle (spec A mergeAndCleanup)", () => {
  it.instance(
    "happy path: ff-merges child work into the parent branch and cleans up",
    () =>
      Effect.gen(function* () {
        const parent = (yield* TestInstance).directory
        const svc = yield* Worktree.Service

        yield* writeText(path.join(parent, "seed.txt"), "seed\n")
        yield* git(parent, ["add", "seed.txt"])
        yield* git(parent, ["commit", "-m", "seed"])

        const info = yield* createWorktree()
        yield* writeText(path.join(info.directory, "child-a.txt"), "child work\n")

        const result = yield* svc.mergeAndCleanup(info, { approve })

        expect(result.status).toBe("merged")
        expect(result.stat).toBeDefined()

        // Parent has both files.
        expect(yield* readText(path.join(parent, "seed.txt"))).toBe("seed\n")
        expect(yield* readText(path.join(parent, "child-a.txt"))).toBe("child work\n")

        // Worktree dir gone.
        expect(yield* exists(info.directory)).toBe(false)
        // opencode/* branch gone.
        expect((yield* gitResult(parent, ["show-ref", "--verify", "--quiet", `refs/heads/${info.branch}`])).exitCode).not.toBe(0)
        // git worktree list --porcelain back to only the primary checkout.
        const list = yield* git(parent, ["worktree", "list", "--porcelain"])
        expect(normalize(list)).not.toContain("opencode/")
        expect(normalize(list)).toContain(normalize(`worktree ${parent}`))
        // Journal gone.
        const root = yield* projectRoot()
        expect(yield* exists(path.join(root, ".journal", `${info.name}.json`))).toBe(false)
      }),
    { git: true },
  )

  it.instance(
    "concurrent merges into the same parent leave no index.lock residue",
    () =>
      Effect.gen(function* () {
        const parent = (yield* TestInstance).directory
        const svc = yield* Worktree.Service

        const infos = yield* Effect.forEach(
          [1, 2, 3],
          (i) =>
            Effect.gen(function* () {
              const info = yield* createWorktree({ name: `par-${i}` })
              yield* writeText(path.join(info.directory, `child-${i}.txt`), `child ${i}\n`)
              return info
            }),
          { concurrency: 3 },
        )

        const results = yield* Effect.forEach(
          infos,
          (info) => svc.mergeAndCleanup(info, { approve }),
          { concurrency: "unbounded" },
        )
        for (const result of results) {
          if (result.status !== "merged") throw new Error(`merge returned ${result.status}: ${result.detail ?? "(no detail)"}`)
          expect(result.status).toBe("merged")
        }

        // No lock residue in the parent repo.
        expect(yield* exists(path.join(parent, ".git", "index.lock"))).toBe(false)
        expect((yield* statusPorcelain(parent)).trim()).toBe("")

        // All three child files landed in the parent, in a consistent state.
        for (const i of [1, 2, 3]) {
          expect(yield* readText(path.join(parent, `child-${i}.txt`))).toBe(`child ${i}\n`)
        }
        const list = yield* git(parent, ["worktree", "list", "--porcelain"])
        expect(normalize(list)).not.toContain("opencode/")
      }),
    { git: true },
  )

  it.instance(
    "stash option restores a dirty parent byte-identically and consumes the stash",
    () =>
      Effect.gen(function* () {
        const parent = (yield* TestInstance).directory
        const svc = yield* Worktree.Service

        for (const name of ["modified.txt", "staged.txt", "delete-me.txt"]) {
          yield* writeText(path.join(parent, name), `${name}\n`)
        }
        yield* git(parent, ["add", "-A"])
        yield* git(parent, ["commit", "-m", "seed"])

        // Parent dirty: modified + staged + untracked + deleted.
        yield* writeText(path.join(parent, "modified.txt"), "modified.txt\nparent-edit\n")
        yield* writeText(path.join(parent, "staged.txt"), "staged.txt\nparent-edit\n")
        yield* git(parent, ["add", "staged.txt"])
        yield* writeText(path.join(parent, "untracked.txt"), "untracked\n")
        yield* Effect.promise(() => fs.rm(path.join(parent, "delete-me.txt")))

        const before = yield* statusPorcelain(parent)
        expect(before).toContain(" M modified.txt")
        expect(before).toContain("M  staged.txt")
        expect(before).toContain("?? untracked.txt")
        expect(before).toContain(" D delete-me.txt")

        const info = yield* createWorktree()
        yield* writeText(path.join(info.directory, "stash-child.txt"), "child stash work\n")

        // NOTE: the byte-restore contract is asserted BEFORE the status code so a
        // status regression still reports it. The spec (F.3) expects this flow to
        // report "merged"; the current implementation returns merge_integrity_failure
        // because the post-pop verification (--untracked-files=no) sees the restored
        // tracked modifications. Tracked in the test report as discrepancy #1.
        const result = yield* svc.mergeAndCleanup(info, { stash: true, approve })

        // Byte-identical restore of the parent's dirty state.
        const after = yield* statusPorcelain(parent)
        expect(after).toBe(before)

        // The child commit landed (the file only exists via the child's commit).
        expect(yield* readText(path.join(parent, "stash-child.txt"))).toBe("child stash work\n")

        // Stash consumed by the successful pop.
        expect((yield* git(parent, ["stash", "list"])).trim()).toBe("")

        expect(result.status).toBe("merged")
        yield* cleanupWorktree(info.directory)
      }),
    { git: true },
  )

  it.instance(
    "stash flow survives an untracked-file collision only via --include-untracked",
    () =>
      Effect.gen(function* () {
        const parent = (yield* TestInstance).directory
        const svc = yield* Worktree.Service

        // Parent has an UNTRACKED file at the exact path the child commits. Without
        // --include-untracked the stash push leaves it behind and the ff-merge fails
        // with "untracked working tree files would be overwritten" -> parent_dirty.
        // This test fails if the implementation regresses away from -u.
        yield* writeText(path.join(parent, "same-name.txt"), "parent-untracked\n")

        const info = yield* createWorktree()
        yield* writeText(path.join(info.directory, "same-name.txt"), "child-committed\n")

        const result = yield* svc.mergeAndCleanup(info, { stash: true, approve })

        // The merge DID land (impossible without -u): parent HEAD is the child commit.
        expect(result.status).toBe("parent_stash_conflict")
        const head = yield* git(parent, ["rev-parse", "HEAD"])
        const childSHA = yield* git(info.directory, ["rev-parse", "HEAD"])
        expect(head).toBe(childSHA)
        expect(yield* git(parent, ["show", "HEAD:same-name.txt"])).toBe("child-committed\n")

        // The stash entry is retained and reported.
        expect(yield* git(parent, ["stash", "list"])).toContain("opencode-worktree:")
        expect(result.stash).toBeDefined()
        expect(result.recovery).toContain("git stash pop")

        yield* cleanupWorktree(info.directory)
      }),
    { git: true },
  )

  it.instance(
    "default refusal: dirty parent without the stash option is untouched",
    () =>
      Effect.gen(function* () {
        const parent = (yield* TestInstance).directory
        const svc = yield* Worktree.Service

        yield* writeText(path.join(parent, "a.txt"), "a\n")
        yield* git(parent, ["add", "a.txt"])
        yield* git(parent, ["commit", "-m", "seed"])

        yield* writeText(path.join(parent, "a.txt"), "a\nparent-dirty\n")
        const before = yield* statusPorcelain(parent)
        const beforeHash = yield* fileHash(path.join(parent, "a.txt"))
        const headBefore = yield* git(parent, ["rev-parse", "HEAD"])

        const info = yield* createWorktree()
        yield* writeText(path.join(info.directory, "child-a.txt"), "child\n")

        const result = yield* svc.mergeAndCleanup(info, { approve })

        expect(result.status).toBe("parent_dirty")

        // Parent tree untouched: same porcelain, same file bytes, same HEAD.
        expect(yield* statusPorcelain(parent)).toBe(before)
        expect(yield* fileHash(path.join(parent, "a.txt"))).toBe(beforeHash)
        expect(yield* git(parent, ["rev-parse", "HEAD"])).toBe(headBefore)
        expect(yield* exists(path.join(parent, "child-a.txt"))).toBe(false)

        // Decision (spec F.4 "decide + test"): the worktree is RETAINED for inspection.
        expect(yield* exists(info.directory)).toBe(true)
        expect((yield* gitResult(parent, ["show-ref", "--verify", "--quiet", `refs/heads/${info.branch}`])).exitCode).toBe(0)

        yield* cleanupWorktree(info.directory)
      }),
    { git: true },
  )

  it.instance(
    "rebase conflict reports merge_conflict, keeps the worktree, and aborts cleanly",
    () =>
      Effect.gen(function* () {
        const parent = (yield* TestInstance).directory
        const svc = yield* Worktree.Service

        yield* writeText(path.join(parent, "file.txt"), "line1\nline2\nline3\n")
        yield* git(parent, ["add", "file.txt"])
        yield* git(parent, ["commit", "-m", "seed"])

        const info = yield* createWorktree()

        // Parent moves the same line AFTER the worktree was created.
        yield* writeText(path.join(parent, "file.txt"), "line1\nparent-edit\nline3\n")
        yield* git(parent, ["add", "file.txt"])
        yield* git(parent, ["commit", "-m", "parent edit"])
        const parentHead = yield* git(parent, ["rev-parse", "HEAD"])

        // Child edits the same line in the worktree.
        yield* writeText(path.join(info.directory, "file.txt"), "line1\nchild-edit\nline3\n")

        const result = yield* svc.mergeAndCleanup(info, { approve })

        expect(result.status).toBe("merge_conflict")
        expect(result.conflicts).toContain("file.txt")
        expect(result.detail ?? "").not.toBe("")

        // Worktree retained.
        expect(yield* exists(info.directory)).toBe(true)
        const list = yield* git(parent, ["worktree", "list", "--porcelain"])
        expect(normalize(list)).toContain(normalize(info.directory))

        // Rebase state cleaned after abort.
        const gitDir = path.join(parent, ".git")
        expect(yield* exists(path.join(gitDir, "worktrees", info.name, "rebase-merge"))).toBe(false)
        expect(yield* exists(path.join(gitDir, "worktrees", info.name, "rebase-apply"))).toBe(false)

        // Parent untouched and clean.
        expect(yield* git(parent, ["rev-parse", "HEAD"])).toBe(parentHead)
        expect((yield* statusPorcelain(parent)).trim()).toBe("")

        yield* cleanupWorktree(info.directory)
      }),
    { git: true },
  )

  it.instance(
    "stash pop conflict retains the stash entry and reports parent_stash_conflict",
    () =>
      Effect.gen(function* () {
        const parent = (yield* TestInstance).directory
        const svc = yield* Worktree.Service

        yield* writeText(path.join(parent, "file.txt"), "line1\nline2\nline3\n")
        yield* git(parent, ["add", "file.txt"])
        yield* git(parent, ["commit", "-m", "seed"])

        const info = yield* createWorktree()

        // Parent dirty on the same line the child will change.
        yield* writeText(path.join(parent, "file.txt"), "line1\nparent-edit\nline3\n")
        const before = yield* statusPorcelain(parent)
        expect(before).toContain(" M file.txt")

        yield* writeText(path.join(info.directory, "file.txt"), "line1\nchild-edit\nline3\n")

        const result = yield* svc.mergeAndCleanup(info, { stash: true, approve })

        expect(result.status).toBe("parent_stash_conflict")
        expect(result.stash).toContain("opencode-worktree:")
        expect(result.recovery).toContain("git stash pop")

        // Stash entry retained in the repo — never auto-dropped.
        expect(yield* git(parent, ["stash", "list"])).toContain("opencode-worktree:")

        // No committed marker residue: HEAD:file.txt is the child's clean content.
        const committed = yield* git(parent, ["show", "HEAD:file.txt"])
        expect(committed).toContain("child-edit")
        expect(committed).not.toContain("<<<<<<<")

        // Git's own pop behavior leaves markers in the working tree only.
        const working = yield* readText(path.join(parent, "file.txt"))
        expect(working).toContain("<<<<<<<")

        // No extra untracked residue appeared.
        const after = yield* statusPorcelain(parent)
        expect(after.split("\n").filter((line) => line.startsWith("??"))).toHaveLength(0)

        // Worktree retained for inspection.
        expect(yield* exists(info.directory)).toBe(true)

        yield* cleanupWorktree(info.directory)
      }),
    { git: true },
  )
})

describe("Worktree crash recovery + GC (spec D/E)", () => {
  it.instance(
    "recover() replays a stashed merge from a dead journal",
    () =>
      Effect.gen(function* () {
        const parent = (yield* TestInstance).directory
        const svc = yield* Worktree.Service
        const root = yield* projectRoot()

        yield* writeText(path.join(parent, "seed.txt"), "seed\n")
        yield* git(parent, ["add", "seed.txt"])
        yield* git(parent, ["commit", "-m", "seed"])

        const info = yield* createWorktree()

        // The "crashed" child committed its work before dying.
        yield* writeText(path.join(info.directory, "child-recovery.txt"), "crash child\n")
        yield* git(info.directory, ["add", "-A"])
        yield* git(info.directory, ["commit", "-m", "crash child work"])
        const childSHA = yield* git(info.directory, ["rev-parse", "HEAD"])

        // The "crashed" process had stashed a dirty parent.
        yield* writeText(path.join(parent, "seed.txt"), "seed\nparent-dirty\n")
        yield* writeText(path.join(parent, "parent-untracked.txt"), "untracked\n")
        yield* git(parent, ["stash", "push", "--include-untracked", "-m", `opencode-worktree:${info.name}`])

        // Replace the live journal (created by create()) with a dead-pid stashed journal.
        const journalPath = path.join(root, ".journal", `${info.name}.json`)
        yield* Effect.promise(() => fs.rm(journalPath))
        yield* writeText(
          journalPath,
          JSON.stringify({
            name: info.name,
            directory: info.directory,
            branch: info.branch,
            parentBranch: info.parentBranch,
            parentHeadSHA: info.parentHeadSHA,
            stage: "stashed",
            pid: 999999999,
          }),
        )

        const result = yield* svc.recover()

        expect(result.restored).toContain(info.name)
        expect(result.kept).not.toContain(info.name)

        // Parent end state: child commit landed + dirty state restored.
        expect(yield* git(parent, ["rev-parse", "HEAD"])).toBe(childSHA)
        expect(yield* readText(path.join(parent, "child-recovery.txt"))).toBe("crash child\n")
        expect(yield* readText(path.join(parent, "seed.txt"))).toBe("seed\nparent-dirty\n")
        expect(yield* exists(path.join(parent, "parent-untracked.txt"))).toBe(true)

        // Journal consumed, stash consumed.
        expect(yield* exists(journalPath)).toBe(false)
        expect((yield* git(parent, ["stash", "list"])).trim()).toBe("")

        yield* cleanupWorktree(info.directory)
      }),
    { git: true },
  )

  it.instance(
    "pruneOrphans removes dead worktrees and branches, skips live and dot entries",
    () =>
      Effect.gen(function* () {
        const parent = (yield* TestInstance).directory
        const svc = yield* Worktree.Service
        const root = yield* projectRoot()

        // A REGISTERED worktree — must be skipped (live).
        const info = yield* createWorktree()

        // Orphaned dir with a dead pid — must be removed.
        const orphanDead = path.join(root, "orphan-dead")
        yield* Effect.promise(() => fs.mkdir(orphanDead, { recursive: true }))
        yield* writeText(path.join(orphanDead, "leftover.txt"), "stale\n")
        yield* writeText(path.join(root, ".alive", "orphan-dead.json"), JSON.stringify({ pids: [999999999] }))

        // Unregistered dir with a LIVE pid — must be skipped.
        const orphanLive = path.join(root, "orphan-live")
        yield* Effect.promise(() => fs.mkdir(orphanLive, { recursive: true }))
        yield* writeText(path.join(root, ".alive", "orphan-live.json"), JSON.stringify({ pids: [process.pid] }))

        // Dot entries (.journal/.locks/.stubs...) — must be ignored.
        yield* Effect.promise(() => fs.mkdir(path.join(root, ".dot-dir"), { recursive: true }))
        yield* writeText(path.join(root, ".dot-dir", "keep.txt"), "keep\n")
        yield* writeText(path.join(root, ".journal", "dot-entry.json"), JSON.stringify({ pid: 999999999 }))

        // An orphan branch with no live worktree — must be deleted.
        yield* git(parent, ["branch", "opencode/orphan-dead-test"])

        const removed = yield* svc.pruneOrphans()

        expect(removed).toBe(1)
        expect(yield* exists(orphanDead)).toBe(false)
        expect(yield* exists(orphanLive)).toBe(true)
        expect(yield* exists(path.join(root, ".dot-dir", "keep.txt"))).toBe(true)
        expect(yield* exists(path.join(root, ".journal", "dot-entry.json"))).toBe(true)

        // Orphan branch deleted; live worktree branch kept.
        expect((yield* gitResult(parent, ["show-ref", "--verify", "--quiet", "refs/heads/opencode/orphan-dead-test"])).exitCode).not.toBe(0)
        expect((yield* gitResult(parent, ["show-ref", "--verify", "--quiet", `refs/heads/${info.branch}`])).exitCode).toBe(0)
        const list = yield* git(parent, ["worktree", "list", "--porcelain"])
        expect(normalize(list)).toContain(normalize(info.directory))

        yield* cleanupWorktree(info.directory)
      }),
    { git: true },
  )

  it.instance(
    "worktree child context resolves InstanceState to the worktree root",
    () =>
      Effect.gen(function* () {
        const parent = (yield* TestInstance).directory
        const svc = yield* Worktree.Service
        const parentCtx = yield* InstanceState.context

        yield* writeText(path.join(parent, "seed.txt"), "seed\n")
        yield* git(parent, ["add", "seed.txt"])
        yield* git(parent, ["commit", "-m", "seed"])

        const before = yield* statusPorcelain(parent)
        const headBefore = yield* git(parent, ["rev-parse", "HEAD"])

        const info = yield* createWorktree()

        // Spec B wiring: childCtx.worktree MUST be the worktree checkout root.
        const childCtx: InstanceContext = {
          directory: info.directory,
          worktree: info.directory,
          project: parentCtx.project,
        }
        const resolved = yield* Effect.provideService(InstanceRef, childCtx)(
          Effect.gen(function* () {
            const directory = yield* InstanceState.directory
            expect(directory).toBe(info.directory)
            yield* writeText(path.join(directory, "child-only.txt"), "in worktree\n")
            return directory
          }),
        )

        expect(resolved).toBe(info.directory)
        expect(yield* readText(path.join(info.directory, "child-only.txt"))).toBe("in worktree\n")

        // Parent untouched: clean, same HEAD, no child file.
        expect(yield* statusPorcelain(parent)).toBe(before)
        expect(yield* git(parent, ["rev-parse", "HEAD"])).toBe(headBefore)
        expect(yield* exists(path.join(parent, "child-only.txt"))).toBe(false)

        yield* cleanupWorktree(info.directory)
      }),
    { git: true },
  )
})

describe("Worktree environment + validation (spec C/R7/F.12-F.13)", () => {
  wintest(
    "creates a node_modules junction at boot when the parent has node_modules",
    () =>
      Effect.gen(function* () {
        const parent = (yield* TestInstance).directory
        const svc = yield* Worktree.Service

        yield* Effect.promise(() => fs.mkdir(path.join(parent, "node_modules", "pkg"), { recursive: true }))
        yield* writeText(path.join(parent, "node_modules", "pkg", "index.js"), "hi\n")
        yield* git(parent, ["add", "-A"])
        yield* git(parent, ["commit", "-m", "seed node_modules"])

        // EXPECTED-FAIL: R9 junction linking is not implemented in src/worktree/index.ts
        // yet — no fs.symlink(junction) call exists. The test documents the contract;
        // boot must still complete (asserted by createWorktree) even without the junction.
        const info = yield* createWorktree()

        const stat = yield* Effect.promise(() =>
          fs
            .lstat(path.join(info.directory, "node_modules"))
            .then((s) => s)
            .catch(() => undefined),
        )
        expect(stat?.isSymbolicLink()).toBe(true)

        yield* cleanupWorktree(info.directory)
      }),
    { git: true },
  )

  it.instance(
    "remove kills recorded dev-server pids before cleanup (guillotine)",
    () =>
      Effect.gen(function* () {
        const svc = yield* Worktree.Service
        const root = yield* projectRoot()
        const info = yield* createWorktree()

        const child = Bun.spawn({
          cmd: [process.execPath, "-e", "setTimeout(() => {}, 60000)"],
          stdout: "ignore",
          stderr: "ignore",
        })
        yield* writeText(path.join(root, ".alive", `${info.name}.json`), JSON.stringify({ pids: [child.pid] }))

        try {
          const ok = yield* svc.remove({ directory: info.directory })
          expect(ok).toBe(true)

          const dead = yield* pollWithTimeout(
            Effect.sync(() => {
              try {
                process.kill(child.pid, 0)
                return undefined
              } catch {
                return true as const
              }
            }),
            "recorded pid still alive after remove",
          )
          expect(dead).toBe(true)
        } finally {
          child.kill()
        }
      }),
    { git: true },
  )

  it.instance(
    "rejects removal of a directory outside the managed root without deleting it",
    () =>
      Effect.gen(function* () {
        const svc = yield* Worktree.Service
        const outside = path.join(os.tmpdir(), `opencode-outside-${Date.now().toString(36)}`)
        yield* Effect.promise(() => fs.mkdir(outside, { recursive: true }))
        yield* writeText(path.join(outside, "keep.txt"), "keep\n")

        try {
          const exit = yield* Effect.exit(svc.remove({ directory: outside }))
          expect(Exit.isFailure(exit)).toBe(true)
          if (Exit.isFailure(exit)) {
            const error = Cause.squash(exit.cause)
            expect(error).toBeInstanceOf(Worktree.RemoveFailedError)
            if (error instanceof Worktree.RemoveFailedError) {
              expect(error.message).toContain("must be under")
            }
          }
          // Nothing was deleted outside the managed root.
          expect(yield* readText(path.join(outside, "keep.txt"))).toBe("keep\n")
        } finally {
          yield* Effect.promise(() => fs.rm(outside, { recursive: true, force: true }))
        }
      }),
    { git: true },
  )

  it.instance(
    "rejects branch injection and out-of-root directories on createFromInfo",
    () =>
      Effect.gen(function* () {
        const svc = yield* Worktree.Service
        const info = yield* svc.makeWorktreeInfo({ name: "injection" })

        // Branch shape gate (R7): no "..", no "@{", no trailing "/", no leading "-", no backslash.
        for (const branch of ["opencode/ab..cd", "opencode/@{upstream}", "opencode/x/", "-evil", "opencode/x\\y"]) {
          const exit = yield* Effect.exit(svc.createFromInfo({ ...info, branch }))
          expect(Exit.isFailure(exit)).toBe(true)
          if (Exit.isFailure(exit)) {
            const error = Cause.squash(exit.cause)
            expect(error).toBeInstanceOf(Worktree.CreateFailedError)
            if (error instanceof Worktree.CreateFailedError) expect(error.message).toContain("Invalid branch name")
          }
        }

        // Directory boundary gate (R7): createFromInfo must refuse out-of-root dirs.
        const outside = path.join(os.tmpdir(), `opencode-outside-${Date.now().toString(36)}`)
        yield* Effect.promise(() => fs.mkdir(outside, { recursive: true }))
        try {
          const exit = yield* Effect.exit(svc.createFromInfo({ ...info, directory: outside }))
          expect(Exit.isFailure(exit)).toBe(true)
          if (Exit.isFailure(exit)) {
            const error = Cause.squash(exit.cause)
            expect(error).toBeInstanceOf(Worktree.CreateFailedError)
            if (error instanceof Worktree.CreateFailedError) expect(error.message).toContain("must be under")
          }
        } finally {
          yield* Effect.promise(() => fs.rm(outside, { recursive: true, force: true }))
        }
      }),
    { git: true },
  )
})
