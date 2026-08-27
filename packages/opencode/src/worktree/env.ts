import path from "path"
import fs from "fs"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Global } from "@opencode-ai/core/global"

/**
 * Security boundary for subagent worktrees (spec C/R1-R9).
 *
 * Credential-less environment + network-git deny-wall applied ONLY inside
 * worktree-scoped processes. The parent/orchestrator scope is untouched:
 * `Project.git` (packages/opencode/src/project/project.ts) keeps extendEnv.
 */

export const worktreeRoot = () => path.join(Global.Path.data, "worktree")

/** True when a directory lives under the managed worktree root (win32 case-insensitive). */
export function isWorktreeDirectory(directory: string) {
  const root = normalize(worktreeRoot())
  const dir = normalize(FSUtil.resolve(directory))
  if (!dir) return false
  const relative = path.relative(root, dir)
  return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative))
}

function normalize(p: string) {
  const resolved = path.normalize(p)
  return process.platform === "win32" ? resolved.toLowerCase() : resolved
}

// ---------------------------------------------------------------------------
// Deny list
// ---------------------------------------------------------------------------

const DENY_NAMES = new Set([
  "GH_TOKEN",
  "GITHUB_TOKEN",
  "GITHUB_ACTIONS_TOKEN",
  "GITLAB_TOKEN",
  "OPENAI_API_KEY",
  "ANTHROPIC_API_KEY",
  "GEMINI_API_KEY",
  "AWS_ACCESS_KEY_ID",
  "AWS_SECRET_ACCESS_KEY",
  "AWS_SESSION_TOKEN",
  "GOOGLE_API_KEY",
  "GOOGLE_APPLICATION_CREDENTIALS",
  "AZURE_OPENAI_API_KEY",
  "AZURE_OPENAI_ENDPOINT",
  "AZURE_OPENAI_API_INSTANCE_NAME",
  "AZURE_OPENAI_API_DEPLOYMENT_NAME",
  "NPM_TOKEN",
  "NODE_AUTH_TOKEN",
  "HUGGING_FACE_HUB_TOKEN",
  "HF_TOKEN",
  "REPLICATE_API_TOKEN",
  "SSH_AUTH_SOCK",
  "SSH_AGENT_PID",
  "GIT_ASKPASS",
  "GIT_CONFIG",
  "GIT_TERMINAL_PROMPT",
  "GIT_ALLOW_PROTOCOL",
])

// Any env var whose name (case-insensitive) smells like a credential.
const DENY_NAME = /(^|_|\b)(API[_-]?KEY|TOKEN|SECRET|PASSWORD|PRIVATE[_-]?KEY|CREDENTIAL|OTP|SIGNING[_-]?KEY|SESSION[_-]?KEY)(\b|_)/i

const ALLOWED_PREFIXES = [
  "XDG_",
  "LC_",
  "LANG",
  "TERM",
  "COLORTERM",
  "FORCE_COLOR",
  "NO_COLOR",
  "PROCESSOR_",
  "ProgramFiles",
  "ProgramData",
  "APPDATA",
  "USERPROFILE",
  "HOMEDRIVE",
  "HOMEPATH",
  "HOME",
  "SHELL",
  "COMSPEC",
  "SYSTEMROOT",
  "WINDIR",
  "PATHEXT",
  "TEMP",
  "TMP",
  "TMPDIR",
  "OPENCODE_CONFIG_DIR",
  "OPENCODE_TEST_HOME",
  "USERNAME",
  "USERDOMAIN",
  "PATH",
  "HTTP_PROXY",
  "HTTPS_PROXY",
  "ALL_PROXY",
  "NO_PROXY",
  "http_proxy",
  "https_proxy",
  "all_proxy",
  "no_proxy",
]

/**
 * Returns a scrubbed copy of `env` safe to pass to a subagent-scoped process.
 * Allowlist-prefixed vars pass through; anything matching a deny name or the
 * generic credential pattern is dropped. Win32 env keys are case-insensitive,
 * so both casings are matched.
 */
export function scrubWorktreeEnv(env: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const next: NodeJS.ProcessEnv = {}
  const keys = process.platform === "win32" ? keysCaseInsensitive(env) : Object.keys(env)
  for (const key of keys) {
    if (isAllowedEnvKey(key)) next[key] = env[key]
  }
  return next
}

function keysCaseInsensitive(env: NodeJS.ProcessEnv) {
  const seen = new Set<string>()
  const keys: string[] = []
  for (const key of Object.keys(env)) {
    const lower = key.toLowerCase()
    if (seen.has(lower)) continue
    seen.add(lower)
    keys.push(key)
  }
  return keys
}

export function isAllowedEnvKey(key: string) {
  const upper = key.toUpperCase()
  if (DENY_NAMES.has(upper)) return false
  if (DENY_NAME.test(upper)) return false
  for (const prefix of ALLOWED_PREFIXES) {
    if (upper.startsWith(prefix.toUpperCase())) return true
  }
  return false
}

// ---------------------------------------------------------------------------
// Git hardening for subagent-scoped git processes
// ---------------------------------------------------------------------------

/** Git subcommands that touch the network — denied inside worktrees. */
export const NETWORK_SUBCOMMANDS = [
  "fetch",
  "pull",
  "clone",
  "push",
  "ls-remote",
  "remote",
  "upload-pack",
  "receive-pack",
] as const

export function isDeniedGitSubcommand(args: string[]) {
  const first = args.find((arg) => !arg.startsWith("-"))
  if (!first) return false
  if (first === "submodule") {
    return args.includes("update") || args.includes("add") || args.includes("sync")
  }
  return (NETWORK_SUBCOMMANDS as readonly string[]).includes(first)
}

let stubWritten = false

/** Credential-less askpass: a stub script that always exits 1. */
export function ensureAskpassStub() {
  if (stubWritten) return stubPath()
  const dir = path.join(worktreeRoot(), ".stubs")
  fs.mkdirSync(dir, { recursive: true })
  const stub = stubPath()
  if (process.platform === "win32") {
    fs.writeFileSync(stub, "@echo off\r\nexit /b 1\r\n")
  } else {
    fs.writeFileSync(stub, "#!/bin/sh\nexit 1\n", { mode: 0o755 })
  }
  stubWritten = true
  return stub
}

function stubPath() {
  return path.join(worktreeRoot(), ".stubs", process.platform === "win32" ? "askpass-stub.cmd" : "askpass-stub")
}

/** Credential-less git environment for subagent git invocations. */
export function scrubGitEnv(base: NodeJS.ProcessEnv = process.env): NodeJS.ProcessEnv {
  const stub = ensureAskpassStub()
  const nul = process.platform === "win32" ? "NUL" : "/dev/null"
  return {
    ...scrubWorktreeEnv(base),
    GIT_TERMINAL_PROMPT: "0",
    GIT_ASKPASS: stub,
    GIT_CONFIG_NOSYSTEM: "1",
    GIT_CONFIG_SYSTEM: "",
    GIT_CONFIG_GLOBAL: "",
    GIT_ALLOW_PROTOCOL: "",
  }
}

// ---------------------------------------------------------------------------
// Merge approval gate (R3) — human-mandatory review paths
// ---------------------------------------------------------------------------

/** Paths that require a human review; NEVER merged without approval. */
export const BLOCKED_MERGE_PATTERNS = [
  ".husky/**",
  ".gitattributes",
  ".github/workflows/**",
  "package.json",
  "bun.lock",
  "pnpm-lock.*",
  "deno.lock",
  ".env*",
  ".npmrc",
  ".gitmodules",
  ".gitignore",
]

export function matchesBlockedPath(file: string) {
  const normalized = file.replace(/\\/g, "/")
  for (const pattern of BLOCKED_MERGE_PATTERNS) {
    if (pattern.includes("/")) {
      if (matchGlob(pattern, normalized)) return true
    } else {
      if (normalized === pattern || normalized.startsWith(`${pattern}/`)) return true
    }
  }
  return false
}

function matchGlob(pattern: string, file: string) {
  const regex = pattern
    .replace(/[.+^${}()|[\]\\]/g, "\\$&")
    .replace(/\*\*/g, "____")
    .replace(/\*/g, "[^/]*")
    .replace(/____/g, ".*")
  return new RegExp(`^${regex}$`).test(file)
}

export * as Env from "./env"