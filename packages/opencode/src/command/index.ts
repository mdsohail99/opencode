import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import path from "path"
import { InstanceState } from "@/effect/instance-state"
import { EffectBridge } from "@/effect/bridge"
import type { InstanceContext } from "@/project/instance-context"
import { Effect, Layer, Context, Schema } from "effect"
import { Config } from "@/config/config"
import { MCP } from "../mcp"
import { Skill } from "../skill"
import PROMPT_INITIALIZE from "./template/initialize.txt"
import PROMPT_REVIEW from "./template/review.txt"
import { LegacyEvent } from "@opencode-ai/schema/legacy-event"

type State = {
  commands: Record<string, Info>
}

export const Event = {
  Executed: LegacyEvent.CommandExecuted,
}

export const Info = Schema.Struct({
  name: Schema.String,
  description: Schema.optional(Schema.String),
  agent: Schema.optional(Schema.String),
  model: Schema.optional(Schema.String),
  source: Schema.optional(Schema.Literals(["command", "mcp", "skill"])),
  // Some command templates are lazy promises from MCP prompt resolution.
  template: Schema.Unknown,
  subtask: Schema.optional(Schema.Boolean),
  hints: Schema.Array(Schema.String),
}).annotate({ identifier: "Command" })

export type Info = Omit<Schema.Schema.Type<typeof Info>, "template"> & { template: Promise<string> | string }

export function hints(template: string) {
  const result: string[] = []
  const numbered = template.match(/\$\d+/g)
  if (numbered) {
    for (const match of [...new Set(numbered)].sort()) result.push(match)
  }
  if (template.includes("$ARGUMENTS")) result.push("$ARGUMENTS")
  return result
}

export const Default = {
  INIT: "init",
  REVIEW: "review",
  ASK: "ask",
  TREE: "tree",
  REPORT: "report",
  ERRORS: "errors",
  RESUME: "resume",
  STOP: "stop",
} as const

export interface Interface {
  readonly get: (name: string) => Effect.Effect<Info | undefined>
  readonly list: () => Effect.Effect<Info[]>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Command") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const config = yield* Config.Service
    const mcp = yield* MCP.Service
    const skill = yield* Skill.Service

    const init = Effect.fn("Command.state")(function* (ctx: InstanceContext) {
      const cfg = yield* config.get()
      const bridge = yield* EffectBridge.make()
      const commands: Record<string, Info> = {}

      commands[Default.INIT] = {
        name: Default.INIT,
        description: "guided AGENTS.md setup",
        source: "command",
        get template() {
          return PROMPT_INITIALIZE.replace("${path}", ctx.worktree)
        },
        hints: hints(PROMPT_INITIALIZE),
      }
      commands[Default.REVIEW] = {
        name: Default.REVIEW,
        description: "review changes [commit|branch|pr], defaults to uncommitted",
        source: "command",
        get template() {
          return PROMPT_REVIEW.replace("${path}", ctx.worktree)
        },
        subtask: true,
        hints: hints(PROMPT_REVIEW),
      }
      commands[Default.ASK] = {
        name: Default.ASK,
        description: "query any running lead or specialist agent out-of-band: /ask <agent> <question>",
        source: "command",
        template: [
          "Use the ask_agent tool to query a running or recent background agent out-of-band.",
          "Input: $ARGUMENTS",
          "Identify the target agent name or role from the input (e.g. Backend, Frontend, Security Architect, etc.) and call ask_agent with that target_id and the question prompt.",
          "If no specific agent is named in the input, call agents_status to check running agents first and select the most relevant active lead, or ask the user which agent they want to query.",
          "Relay the answer directly to the user.",
        ].join("\n"),
        hints: ["$ARGUMENTS"],
      }
      commands[Default.TREE] = {
        name: Default.TREE,
        description: "display the live hierarchical swarm tree of all leads and specialists: /tree [filter]",
        source: "command",
        template: [
          "Inspect and display the live agent swarm hierarchy.",
          "Filter: $ARGUMENTS",
          "1. Call the agents_status tool to inspect the active agent hierarchy tree.",
          "2. If a department or filter was requested in $ARGUMENTS (e.g. backend, frontend), highlight that slice of the swarm.",
          "3. Output the formatted tree with elapsed times and stall warnings.",
        ].join("\n"),
        hints: ["$ARGUMENTS"],
      }
      commands[Default.REPORT] = {
        name: Default.REPORT,
        description: "synthesize current progress and work done across all leads and specialists: /report [filter]",
        source: "command",
        template: [
          "Collect, synthesize, and report all progress and work done across all leads and specialists in the swarm.",
          "Filter/Focus: $ARGUMENTS",
          "1. Call agents_status to retrieve the current swarm hierarchy and identify all active and completed agents.",
          "2. For each completed and running agent, use manage_agents with action='inspect' to review their latest outputs and accomplishments.",
          "3. Provide a consolidated executive status briefing:",
          "   - **Completed Accomplishments**: Specific features, audits, or code changes produced by finished specialists.",
          "   - **Currently In Progress**: What active workers are actively executing and their elapsed time.",
          "   - **Files & Components Touched**: Key files created or modified.",
          "   - **Remaining Work**: What is left to complete the overall mission.",
        ].join("\n"),
        hints: ["$ARGUMENTS"],
      }
      commands[Default.ERRORS] = {
        name: Default.ERRORS,
        description: "scan swarm for failed, errored, or stalled agents and show diagnostics: /errors",
        source: "command",
        template: [
          "Inspect the agent swarm specifically for failures, errors, or stalled workers.",
          "1. Call agents_status to check the swarm hierarchy.",
          "2. For any agent with status 'error', any agent tagged '[stalled]', or any agent reporting failures:",
          "   - Call manage_agents with action='inspect' to get its full error trace and recent output.",
          "   - Print an executive diagnostic: Agent Name, Role/ID, exact failure reason, and the step where it failed.",
          "   - Show how to resume it using: /resume <agent-name>",
          "3. If all agents are healthy with 0 errors, report that the swarm is running cleanly with no failures.",
        ].join("\n"),
        hints: [],
      }
      commands[Default.RESUME] = {
        name: Default.RESUME,
        description: "resume or restart a stalled, failed, or paused agent: /resume <agent>",
        source: "command",
        template: [
          "Resume or restart a background agent using manage_agents.",
          "Target: $ARGUMENTS",
          "Identify the target agent name or ID from $ARGUMENTS and call manage_agents with action='restart' and target_id=<agent>.",
          "Report that the agent has been resumed in the background.",
        ].join("\n"),
        hints: ["$ARGUMENTS"],
      }

      const stopTemplate = [
        "Halt or cancel running background agents using manage_agents.",
        "Target: $ARGUMENTS",
        "If $ARGUMENTS is 'all' or empty, or mentions 'all agents':",
        "  Execute manage_agents with action='kill_all'.",
        "  Confirm to the user that all active background agents in the swarm have been halted.",
        "Else:",
        "  Identify the target agent name or ID from $ARGUMENTS and execute manage_agents with action='kill' and target_id=<agent>.",
        "  Confirm to the user that the specified agent has been halted.",
      ].join("\n")

      commands[Default.STOP] = {
        name: Default.STOP,
        description: "halt a specific agent or all running agents in the swarm: /stop [agent|all]",
        source: "command",
        template: stopTemplate,
        hints: ["$ARGUMENTS"],
      }

      for (const [name, command] of Object.entries(cfg.command ?? {})) {
        commands[name] = {
          name,
          agent: command.agent,
          model: command.model,
          description: command.description,
          source: "command",
          get template() {
            return command.template
          },
          subtask: command.subtask,
          hints: hints(command.template),
        }
      }

      for (const [name, prompt] of Object.entries(yield* mcp.prompts())) {
        commands[name] = {
          name,
          source: "mcp",
          description: prompt.description,
          get template() {
            return bridge.promise(
              mcp
                .getPrompt(
                  prompt.client,
                  prompt.name,
                  prompt.arguments
                    ? Object.fromEntries(prompt.arguments.map((argument, i) => [argument.name, `$${i + 1}`]))
                    : {},
                )
                .pipe(
                  Effect.map(
                    (template) =>
                      template?.messages
                        .map((message) => (message.content.type === "text" ? message.content.text : ""))
                        .join("\n") || "",
                  ),
                ),
            )
          },
          hints: prompt.arguments?.map((_, i) => `$${i + 1}`) ?? [],
        }
      }

      for (const item of yield* skill.all()) {
        if (commands[item.name]) continue
        const dir = item.location === "<built-in>" ? undefined : path.dirname(item.location)
        commands[item.name] = {
          name: item.name,
          description: item.description,
          source: "skill",
          get template() {
            if (!dir) return item.content
            return [
              item.content,
              "",
              `Base directory for this skill: ${dir}`,
              "Relative paths in this skill (e.g., scripts/, references/) are relative to this base directory.",
            ].join("\n")
          },
          hints: [],
        }
      }

      return {
        commands,
      }
    })

    const state = yield* InstanceState.make<State>((ctx) => init(ctx))

    const get = Effect.fn("Command.get")(function* (name: string) {
      const s = yield* InstanceState.get(state)
      return s.commands[name]
    })

    const list = Effect.fn("Command.list")(function* () {
      const s = yield* InstanceState.get(state)
      return Object.values(s.commands)
    })

    return Service.of({ get, list })
  }),
)

export const node = LayerNode.make({ service: Service, layer: layer, deps: [Config.node, MCP.node, Skill.node] })

export * as Command from "."
