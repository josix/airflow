<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# AIP-XXX: Delegated Agent Capabilities for Common AI

> **Draft working copy.** The canonical AIP lives on the Apache Confluence wiki
> once the `[DISCUSS]` thread opens; this file is the source text being refined
> against the actual `apache-airflow-providers-common-ai` codebase.

## Status

| Field | Value |
|---|---|
| State | Draft |
| Discussion Thread | TBD — `[DISCUSS][AIP-XXX] Delegated Agent Capabilities for Common AI` |
| Vote Thread | TBD |
| Vote Result Thread | TBD |
| Progress Tracking | TBD |
| Date Created | 2026-08-15 |
| Authors | TBD |
| Target | `apache-airflow-providers-common-ai` (incremental release; provider is pre-1.0, `lifecycle: incubation`) |
| Reference Implementation | AgentFlow (a multi-agent Claude Code plugin) driven headlessly via the Claude Agent SDK |

---

## Abstract

This AIP proposes a standardized **delegated-agent capability** for
`apache-airflow-providers-common-ai` ("Common AI").

The goal is to let a Common AI `AgentOperator` delegate a specialized task to
another autonomous agent runtime — a coding agent, a research agent, a remote
or vendor-managed agent service, a multi-agent system — while Common AI remains
the Airflow-side agent framework that owns the outer loop.

The delegated runtime is exposed to the outer agent as a normal pydantic-ai
`AbstractToolset`, the same extension point every existing Common AI toolset
already uses (`HookToolset`, `SQLToolset`, `MCPToolset`, `AgentSkillsToolset`,
`DataFusionToolset`, plus third-party toolsets, which
`AgentOperator` explicitly accepts today). This preserves, without new
machinery: Airflow-managed connections, tool-call logging, structured output,
usage limits, durable execution, Human-in-the-Loop (HITL) review, message
history, and OpenTelemetry instrumentation.

What Common AI standardizes is deliberately small: a **runtime protocol**
(start / resume), a **bounded request/result contract** with an explicit
lifecycle (`completed` / `failed` / `needs_input` / `needs_approval`), and a
**toolset** that surfaces the contract as two tools (`delegate_agent`,
`resume_agent`). The internal architecture of the delegated runtime is
intentionally opaque to Airflow.

The initial motivating implementation is **AgentFlow**, an existing
verification-gated multi-agent Claude Code plugin for software-engineering
tasks, driven headlessly through the Claude Agent SDK. The contract itself is
vendor-neutral, and this AIP is not complete until at least a second,
non-Claude runtime validates that.

---

## Motivation

Common AI already provides a multi-turn agent loop through `AgentOperator`
(`providers/common/ai/src/airflow/providers/common/ai/operators/agent.py`).
The outer model dynamically invokes Airflow-backed tools, and the provider
ships toolsets for Hooks, SQL, MCP servers, Agent Skills, and DataFusion,
along with logging, durability, and HITL infrastructure.

This works well for tools whose invocation looks like:

```text
agent → tool → operation → result
```

Some tasks, however, are better handled by a specialized autonomous runtime
whose *internal* shape is:

```text
agent → specialized capability → explore → plan → implement → review → verify → result
```

Software engineering is the canonical example. An autonomous coding system
needs to inspect a repository, reason about architecture, build a plan, edit
several files, run tests, review its own changes, correct failures, maintain
internal session state, and coordinate multiple specialized subagents. The
AgentFlow reference runtime does exactly this with six role-restricted agents
(explorer, planner, executor, reviewer, verifier, and an explainer), each with
an explicit tool allowlist, connected by verification gates that demand
command-output evidence before any completion claim is accepted.

Representing each internal step as an Airflow task, or as a separate Common AI
tool, is undesirable: it would force Airflow to own an orchestration protocol
(iteration loops, review verdicts, retry-on-`NEEDS_CHANGES`) that the
specialized runtime already owns and enforces. Instead, Common AI should treat
the specialized runtime as **one higher-level capability** with a standardized
lifecycle.

### Why now, and why in Common AI

Airflow already has vendor operators for managed agent runtimes —
`apache-airflow-providers-amazon` ships Bedrock AgentCore operators
(`BedrockCreateAgentRuntimeOperator` / `BedrockInvokeAgentRuntimeOperator`),
and `apache-airflow-providers-anthropic` covers Managed Agents sessions. The
Common AI docs (`providers/common/ai/docs/index.rst`) currently draw the
boundary as: *"if Airflow should run the AI step, use common.ai; if the Dag
submits work to a vendor-managed service and waits, use that vendor's
provider."*

This AIP addresses the case both sides of that rule miss: an **Airflow-run
outer agent that decides, mid-reasoning, to submit work to a specialized
runtime and then continues reasoning over the result** — gathering context
with Airflow-native tools first, answering the runtime's mid-run questions,
and synthesizing structured output afterwards. Task-level vendor operators
cannot compose inside the agent loop; per-vendor ad-hoc toolsets would each
reinvent lifecycle, sessions, and security. The generic contract belongs in
Common AI; the runtime adapters belong in vendor providers or third-party
packages — the same split the existing boundary rule already prescribes.

---

## Current State

Facts below reference the provider as of version **0.7.0**
(`state: ready`, `lifecycle: incubation`, pre-1.0).

`AgentOperator` is built on pydantic-ai and accepts arbitrary
`AbstractToolset` implementations via `toolsets=`. This is already the
documented extension point — `docs/toolsets.rst` states that PydanticAI's own
and third-party toolsets work, not just the Airflow-native ones. This is the
single most important hook this proposal relies on, and it requires **no
change** to `AgentOperator`.

Relevant existing infrastructure:

- **Toolsets:** `HookToolset` (introspects explicitly allow-listed hook
  methods into tools), `SQLToolset`, `MCPToolset` (connection-resolved MCP
  servers, with `token_provider` / `env_provider` callables for short-lived
  credentials), `AgentSkillsToolset` (per-run isolation via `for_run()`, async
  context-manager lifecycle, worker-side source resolution), `LoggingToolset`
  (a `WrapperToolset` that `AgentOperator` applies automatically when
  `enable_tool_logging=True`), `DataFusionToolset`.
- **Durable execution** (`durable/`): opt-in via `durable=True`. Step
  boundaries are exactly **one model request** and **one tool call**, keyed by
  a shared monotonic `DurableStepCounter` and fingerprinted
  (model: prepared request; tool: `(name, args, tool_call_id)`). Successful
  steps are cached in the **AIP-103 task state store** on Airflow ≥ 3.3
  (`TaskStateStoreDurableStorage`, `retention=NEVER_EXPIRE`), with an
  ObjectStorage fallback for older cores, and replayed on task retry.
  Tool implementations are opaque to the cache — a natural boundary for a
  delegated agent. Notably, only tools supplied via `toolsets=` participate;
  tools arriving through capabilities (`MCP`, callable-factory `Toolset`,
  `spec_file`) are documented as *not* cached.
- **HITL review** (`HITLReviewMixin`): a post-run review loop with actions
  `approve` / `reject` / `changes_requested`, driven by a UI plugin over XCom;
  `changes_requested` feeds reviewer text back through
  `regenerate_with_feedback()` with the accumulated message history.
- **Message history:** `message_history=` accepts a prior transcript and the
  operator republishes the full transcript to XCom, enabling sessions that
  span task runs (the docs prescribe a load → run → store pattern keyed by a
  session ID).
- **Usage limits, structured output, retry policies, OTel instrumentation** —
  all at the operator level, all inherited by anything registered as a toolset.

Two constraints of the current implementation matter for this design and are
called out honestly rather than assumed away:

1. **`durable=True` and `enable_hitl_review=True` are mutually exclusive
   today** (raised in `AgentOperator.__init__`), as are `durable=True` and
   `code_mode=True`. Examples in this AIP therefore never combine them; the
   interaction is listed under open questions.
2. **`AgentOperator` is not deferrable.** `execute()` runs `agent.run_sync()`
   and holds the worker slot; the provider ships no triggers. A delegated
   runtime that runs for minutes-to-hours makes this cost visible, which
   shapes the design below (bounded invocations, `needs_input` yields) and
   motivates a follow-up on deferral.

Also relevant: a first-class `capabilities=` kwarg on `AgentOperator` is a
documented roadmap item; today capabilities pass through `agent_params`, which
is a *templated* field whose values are not round-trip-safe through Dag
serialization. This AIP consequently routes delegated runtimes exclusively
through `toolsets=` (not templated, durable-cache-covered) and treats a
possible `capabilities=`-based packaging as follow-up.

---

## Problem

There is no standardized Common AI contract for a tool whose implementation is
itself a stateful autonomous agent. A repo-wide search of the provider finds
no delegation, sub-agent, hand-off, or A2A concept today.

A third party can build one — `AgentOperator` accepts any `AbstractToolset` —
but every integration must independently solve:

- start vs. resume semantics and session identity;
- a structured lifecycle (`completed` / `failed` / needs more input / needs
  approval);
- bounded, serializable request and result schemas;
- interaction with durable replay (what is safe to cache, what is a side
  effect);
- interaction with HITL review and reviewer feedback;
- worker-blocking behavior and cancellation;
- nested observability;
- credential separation between the outer LLM and the delegated runtime;
- workspace isolation for runtimes that mutate files.

Without a common abstraction, vendor integrations will expose incompatible
`run_agent()` / `start_session()` / `invoke_coding_agent()` APIs with
incompatible state models, and each will re-discover the durable/HITL/security
interactions above — or silently get them wrong.

The provider's own design principles (its `AGENTS.md`) set the bar an
abstraction here must clear: *"no premature abstraction — wait until there are
3+ concrete use cases."* This AIP identifies at least four:
a Claude Agent SDK runtime (AgentFlow), Bedrock AgentCore, Anthropic Managed
Agents, and a plain remote HTTP/A2A-style agent service — the first is the
reference implementation and two others already have vendor-provider
operators whose functionality a toolset adapter can reuse.

---

## Goals

1. Keep `AgentOperator` as the outer Airflow/Common AI agent, unchanged.
2. Represent delegated agents through the existing `toolsets=` abstraction.
3. Reuse existing Common AI infrastructure (logging, durability, HITL, usage
   limits, structured output, message history) rather than duplicating it.
4. Provide standardized start / resume / result semantics with an explicit
   lifecycle.
5. Make successful delegated invocations participate in Common AI durable
   tool replay, and document precisely what replay does and does not cover.
6. Allow provider-specific runtimes without coupling Common AI to any AI
   vendor or SDK.
7. Let the outer agent gather context with existing Airflow toolsets before
   delegating, and pass it explicitly.
8. Let specialized runtimes maintain their own internal sessions, referenced
   (not embedded) in the outer transcript.
9. Follow the provider's established security rules — explicit allow-listing,
   worker-side (never parse-time) credential resolution, and **no dynamic
   imports from connection extras** (a runtime class is constructed in Dag
   code, never resolved from a connection field).

## Non-Goals

This AIP does not propose:

- replacing pydantic-ai inside Common AI;
- making Claude Code, the Claude Agent SDK, or AgentFlow part of Airflow;
- standardizing the internal architecture of delegated agents;
- representing every delegated subagent as an Airflow task;
- replaying every internal delegated-agent step through Common AI durable
  execution;
- adding shell or filesystem access to `AgentOperator` itself;
- replacing `HookToolset`, `SQLToolset`, `MCPToolset`, or the vendor
  providers' task-level agent-runtime operators;
- defining authentication semantics for individual AI vendors (those live in
  the corresponding runtime adapter and its Airflow connection type);
- exporting the outer agent's toolsets into the delegated runtime (explicitly
  deferred — see *Nested Tool Access*);
- introducing a second agent scheduler in Airflow;
- making `AgentOperator` deferrable (a natural follow-up this AIP motivates,
  but out of scope).

---

## Proposed Design

### 1. Delegated agent as a toolset

Introduce a generic abstraction, working name `DelegatedAgentToolset`, as a
normal pydantic-ai `AbstractToolset`. From `AgentOperator`'s perspective there
is nothing special about it:

```text
AgentOperator
     ├── SQLToolset
     ├── HookToolset
     ├── MCPToolset
     └── DelegatedAgentToolset   ← this AIP
```

Because it enters through `toolsets=`:

- `LoggingToolset` wraps it automatically (`enable_tool_logging=True`), so
  every delegation is visible in task logs with timing;
- `CachingToolset` wraps it under `durable=True`, so a completed delegation is
  replayed, not re-run, on task retry;
- `usage_limits` bounds how many times the outer model may invoke it;
- it is **not** part of the templated `agent_params` field, so it avoids the
  documented Dag-serialization pitfall for capability instances.

Its lifecycle should follow the shape `AgentSkillsToolset` already
established: per-run isolation via `for_run()`, async context-manager entry
that performs worker-side setup (open/attach a runtime session, prepare a
workspace) and teardown, and no work at Dag-parse time. Its configuration
should follow the `MCPToolset` pattern: an Airflow connection ID resolved
lazily on the worker, with optional `token_provider`-style callables for
short-lived credentials.

### 2. Delegated agent runtime protocol

A lightweight protocol separates Common AI from vendor SDKs:

```python
class DelegatedAgentRuntime(Protocol):
    async def start(self, request: DelegatedAgentRequest) -> DelegatedAgentResult: ...
    async def resume(self, request: DelegatedAgentResumeRequest) -> DelegatedAgentResult: ...
```

Common AI does not prescribe how the runtime implements its agent:
Claude Agent SDK, a nested pydantic-ai agent, LangChain/DeepAgents, a remote
REST/A2A service, a managed vendor service, or a custom internal system.

Cancellation: `start`/`resume` are coroutines executed inside the operator's
run; when the Airflow task is killed, the coroutine is cancelled. Runtimes
*should* translate cancellation into a best-effort remote abort; whether an
explicit `cancel(session_id)` method is mandatory in the protocol is an open
question.

Each concrete runtime is its own class in its own package ("one backend per
toolset" — a new backend is a new class, not a mode flag on a shared one).

### 3. Request contract

The initial request is bounded and serializable:

```python
class DelegatedAgentRequest(BaseModel):
    task: str
    context: dict[str, Any] | None = None
    workspace: str | None = None
    metadata: dict[str, str] = {}
```

The outer agent gathers context first with Airflow-native tools, then
delegates:

```text
AgentOperator
     ├── jira_get_issue("AIR-123")
     ├── github_get_pull_request(...)
     └── delegate_agent(task="Implement AIR-123", context={...})
```

`context` is data handed *into* the runtime — the runtime does not get to call
the outer agent's tools (see *Nested Tool Access*).

### 4. Result contract

Runtimes return a normalized result:

```python
class DelegatedAgentResult(BaseModel):
    status: Literal["completed", "failed", "needs_input", "needs_approval"]
    summary: str
    session_id: str | None = None
    output: dict[str, Any] | None = None
    question: str | None = None       # set when status == "needs_input"
    approval_request: str | None = None  # set when status == "needs_approval"
    error: str | None = None          # set when status == "failed"
    usage: dict[str, int] | None = None  # runtime-reported cost telemetry, if available
```

Provider-specific payloads go under `output` without expanding the generic
API. `failed` is a *bounded, in-band* outcome the outer model can reason
about (retry differently, report, escalate); runtime *transport* exceptions
are translated by the toolset into pydantic-ai tool errors so they never
corrupt the outer agent lifecycle. New exception classes go in the provider's
`exceptions.py` (e.g. `DelegatedAgentRuntimeError`), per the project rule
against broad `AirflowException` usage.

Results must remain **small**. Everything in a tool result stays in the outer
model's message history for the rest of the run and is re-paid on every
subsequent model request — the same rationale that gave `SQLToolset` its
bounded columnar results. Runtimes summarize; they do not dump transcripts.

### 5. Start and resume tools

The toolset exposes two tools:

```text
delegate_agent(task, context?, workspace?)          → DelegatedAgentResult
resume_agent(session_id, response, context?)        → DelegatedAgentResult
```

Example interaction:

```text
delegate_agent(...)
   → {status: "needs_input", session_id: "abc123",
      question: "Should API-key authentication remain supported?"}

# outer agent resolves the question with its own tools (Jira, GitHub, docs)…

resume_agent(session_id="abc123",
             response="Keep existing API-key authentication.")
   → {status: "completed", session_id: "abc123", summary: "...", output: {...}}
```

Control stays with the Common AI agent throughout. Two tools (rather than one
tool with an optional session ID) keeps the schema of each tool simple for
the model and makes the start/resume asymmetry explicit; this naming is an
open question.

### 6. Context gathering remains native Common AI

Existing Airflow integrations remain the integration point for external
systems:

```text
preferred                                 avoided
─────────                                 ───────
Common AI                                 AgentFlow
  ├── HookToolset(JiraHook, ...)            ├── custom Jira integration
  ├── HookToolset(GithubHook, ...)          ├── custom GitHub integration
  ├── SQLToolset(...)                       └── custom SQL integration
  └── delegate_agent(...)
```

This retains Airflow Connections, secret backends, tool logging, allow-list
security (`allowed_methods`, `allowed_tables`), and Common AI visibility.
`HookToolset` already converts explicitly allow-listed hook methods into
pydantic-ai tools via signature/docstring introspection, making it the natural
adapter for Jira, GitHub, and other provider hooks.

### 7. Reference implementation: AgentFlow

AgentFlow today is a declarative Claude Code plugin: six markdown-defined
agents with explicit tool allowlists (Riko — exploration; Senku — planning;
Loid — implementation, the only agent with write access; Lawliet —
static-analysis review; Alphonse — test/build verification; Speedwagon —
explanation authoring), orchestrated through a phased pipeline with
verification gates, host-enforced completion hooks, file-based resumable
state, and deliberately rate-limited human-escalation points.

Two AgentFlow properties map directly onto this AIP's contract:

- **Escalation gates → `needs_input`.** AgentFlow's assumption-escalation and
  plan-confirmation gates are exactly the moments a delegated run must yield
  instead of blocking a worker on interactive input. AgentFlow already
  defines an *unanswered default* for its review-divergence gate; the
  reference implementation generalizes that: every interactive gate either
  surfaces as `needs_input` or resolves by its documented default.
- **File-based state → resume.** Re-running AgentFlow's orchestration resumes
  from persisted phase state rather than restarting; `resume_agent` binds to
  the same mechanism via `session_id`.

What does **not** exist yet, stated plainly: AgentFlow has no headless entry
point — it is invoked by a human in an interactive Claude Code session and
does not use the Claude Agent SDK today. Phase 1 therefore *builds* the
headless driver (Claude Agent SDK programmatic session running the
orchestration with the plugin loaded, `AskUserQuestion` intercepted and
translated to `needs_input`) as part of the reference implementation:

```text
AgentFlowToolset (AbstractToolset, third-party package)
      → Claude Agent SDK (headless session, plugin loaded)
      → agent-flow orchestration (explore → plan → implement → review → verify)
      → DelegatedAgentResult
```

From Common AI's perspective this whole stack is `delegate_agent(...)`.
AgentFlow's internal orchestration stays opaque, and AgentFlow remains a
third-party project — no AgentFlow-specific API enters Common AI.

Where practical, AgentFlow's reusable *guidance* (task classification,
exploration strategy, verification strategy) can additionally be offered to
the **outer** agent as ordinary Agent Skills through the existing
`AgentSkillsToolset` — orthogonal to delegation, requiring no new machinery,
and subject to the existing documented warning that skill bundles are code
running on the worker.

### 8. Convenience operators stay thin

A runtime package may ship a convenience operator, but it must be a thin
wrapper:

```python
class AgentFlowOperator(AgentOperator):
    def __init__(self, *, agent_flow_config, toolsets=None, **kwargs):
        super().__init__(
            toolsets=[*(toolsets or []), AgentFlowToolset(**agent_flow_config)],
            **kwargs,
        )
```

It must not reimplement `execute()`, durability, HITL, message history, usage
limits, tool logging, or structured output unless a requirement cannot be
expressed through Common AI's public extension interfaces.

---

## Durable Execution

The durable boundary is the Common AI tool invocation:

```text
model response        durable step (one model request)
jira_get_issue()      durable step (one tool call)
github_get_pr()       durable step
delegate_agent()      durable step  ← the entire delegated run
model response        durable step
```

Mechanically this needs no new code: under `durable=True`, `CachingToolset`
wraps every toolset supplied via `toolsets=`, caches each successful
`call_tool` return value keyed by step index and fingerprinted on
`(tool name, args, tool_call_id)`, and replays it on task retry when the
request still matches. The internal delegated sequence (explore → plan →
implement → review → verify) never becomes five Common AI durable steps; the
runtime owns its internal recovery. This is an intentional abstraction
boundary, and it is the same one tool implementations already sit behind.

Three consequences must be documented, not hand-waved:

1. **Replay restores return values, not side effects.** A replayed
   `delegate_agent` step does not re-execute the remote run — which is the
   point — but it also means a retry after a *failed* delegation re-invokes
   the runtime. Runtimes whose work has side effects (commits, deployments)
   must make `start` idempotent-or-resumable per session, which the
   `session_id` + persisted-state design provides.
2. **Only `toolsets=` participates.** A delegated runtime handed in through
   capabilities/`agent_params` would be invisible to the durable cache (the
   operator documents this for `MCP` and factory `Toolset` capabilities
   today). This AIP therefore specifies `toolsets=` as the sole supported
   registration path.
3. **A cached step is coarse.** If finer-grained checkpointing of a delegated
   run under Airflow retry semantics is ever wanted, it is the runtime's
   session state (potentially in the AIP-103 task state store — open
   question), not new Common AI durable-step vocabulary.

## Worker Occupancy and Long-Running Delegations

`AgentOperator` is synchronous; a delegated run holds a worker slot for its
duration. This AIP takes three positions:

- **Bound each invocation.** Runtimes should accept a per-invocation time
  budget and return `needs_input`/`failed` rather than run unbounded; the
  outer `execution_timeout` remains the hard stop.
- **`needs_input` is also a scheduling valve.** Yielding control back to the
  outer agent keeps interactive stalls (a question only a human or another
  system can answer) from pinning a worker inside the runtime.
- **Deferral is follow-up work, not a dependency.** A deferrable
  delegate-and-poll path (trigger + resumable session) composes naturally
  with this contract precisely because `session_id`/`resume` exist, but
  making `AgentOperator` deferrable is out of scope here.

## Session State

Delegated runtime sessions and Common AI message history serve different
purposes and stay separate:

- **Common AI `message_history`** — the outer conversation, spanning task runs
  via the documented load → run → store XCom pattern.
- **Runtime `session_id`** — opaque handle to the runtime's own state
  (AgentFlow: its persisted orchestration state; a managed service: its
  server-side session).

The outer transcript references the delegated session (`session_id`, summary,
status) but never embeds the runtime's full transcript — both for context-size
economy and because the transcript may contain repository content the outer
context should not carry.

## Human-in-the-Loop

Common AI's existing post-run review loop is retained unchanged: with
`enable_hitl_review=True`, the agent's synthesized result goes to a reviewer
who can **approve**, **reject**, or **request changes**; reviewer feedback
re-enters the agent with accumulated message history, and the agent may then
call `resume_agent(session_id, response=<feedback>)` to push the revision into
the still-live delegated session rather than starting over.

Constraint acknowledged: `enable_hitl_review=True` is currently mutually
exclusive with `durable=True`, so deployments choose review *or* replay per
task today. Lifting that restriction is an existing provider concern, not
introduced by this AIP.

`needs_approval` in the result contract is intentionally minimal in phase 1:
it surfaces to the outer agent, which either satisfies it from policy/context
given in the prompt or ends the run reporting what approval is required
(letting a Dag-level HITL task or the AIP-90 core mechanism handle it). Direct
integration of mid-delegation approvals with Airflow's `AWAITING_INPUT` /
HITL machinery is deliberately deferred (open question), since it interacts
with deferral.

## Nested Tool Access

**Phase 1: no inheritance.** The delegated runtime does not automatically see
the outer agent's toolsets. External context is collected by the outer agent
and passed via `context=`. This is the simplest security and observability
model: the Dag author can enumerate exactly what the runtime can reach.

**Future extension (explicitly out of scope):** a standard bridge exporting
Common AI `AbstractToolset`s into a delegated runtime's own tool namespace.
The provider already contains a one-directional precedent
(`airflow_toolset_to_langchain_tools`), and its documented caveats — per-call
session teardown breaking stateful backends, no live `RunContext` — preview
exactly why a cross-runtime bridge needs its own design round (tool schemas,
lifecycle, cancellation, and security assumptions differ per runtime). If
pursued, it is a follow-up AIP.

---

## Security Considerations

Common AI's documented security model is: *an agent's reach is exactly the set
of tools you register on it*. A delegated autonomous runtime is a registered
tool whose own reach — filesystem, git, process execution, network, package
managers, test runners — is **not** auditable from the Dag. The provider
already documents this class of risk twice: `MCPToolset` ("does not constrain
what those tools do; run only trusted servers") and Agent Skills
(`run_skill_script` executes repository-provided code on the worker, outside
Dag review). A delegated runtime is the strongest instance of the same class
and gets the same treatment: an explicit entry in the security documentation's
defense-layers table, and the requirements below on implementations.

**Tool and capability isolation.** A runtime receives only explicitly
configured capabilities. No Airflow connection is ever automatically visible
to it; anything it needs is passed deliberately (its own connection, an
explicit workspace, explicit context).

**Credential isolation.** The outer LLM connection and the delegated runtime's
connection are distinct Airflow connections. Common AI defines no
vendor-specific credential semantics — those belong to the runtime adapter's
connection type. Credentials resolve lazily on the worker (never at parse
time, never serialized into the Dag), following `MCPToolset`/`GitSkills`
precedent, including callable providers for short-lived minted tokens. Per
the provider's standing rule, no code path may dynamically import or resolve
a runtime implementation class from connection extras.

**Workspace isolation.** Code-mutating runtimes execute in isolated
workspaces: an ephemeral container, a per-task git worktree, or a dedicated
volume. Two concurrent delegated runs must not share a working tree. The
toolset's `for_run()` per-run instance is where workspace acquisition/release
hooks live.

**Plugin and skill trust.** Runtime plugins (e.g. the AgentFlow plugin
content) and Agent Skills are executable code and must come from trusted,
pinned sources — the same warning the provider already attaches to remote
skill repositories.

**Prompt-injection surface.** The delegated runtime's *output*
(`summary`, `question`, `output`) is model-generated text entering the outer
agent's context, exactly like any tool result; and conversely, `context`
passed into a runtime may carry untrusted upstream data. The existing
production-checklist guidance on untrusted input applies on both edges.

## Observability

Two levels, cleanly separated:

- **Common AI level (uniform, automatic):** `LoggingToolset` records each
  `delegate_agent`/`resume_agent` call with timing (args at DEBUG); the
  post-run summary logs request/tool-call/token counts and the tool-call
  sequence; the operator's OTel instrumentation (`[common.ai]
  otel_export_enabled`, GenAI semconv) covers the outer run. Result `status`
  and `session_id` land in logs and, via structured output, in XCom.
- **Runtime level (adapter-specific, optional):** subagent execution, internal
  tool calls, test runs, internal retries, token usage. Adapters may emit
  these into task logs or OTel (AgentFlow already has a transcript-to-SQLite
  analyzer with pluggable exporters, including MLflow). The result's `usage`
  field gives a minimal standardized cost signal. Runtime events are *not*
  part of Common AI's durable step model.

---

## Relationship to Other AIPs

- **AIP-101 (Airflow AI Assistant, Draft)** — an embedded conversational UI
  for troubleshooting Airflow, grounded read-only via an MCP server, strictly
  bounded by the requesting user's permissions. Different layer: AIP-101 is a
  human-facing operational assistant in the UI; this AIP is a Dag-facing
  capability inside `AgentOperator`. Complementary; a future assistant could
  theoretically invoke delegated capabilities, but that is not proposed here.
- **AIP-90 (Human in the loop)** — provides the core approval machinery this
  AIP's `needs_approval` may later bind to (see open questions).
- **AIP-91 (Airflow MCP Server)** — the inverse direction: exposing Airflow
  *to* agents. This AIP consumes external runtimes *from* Airflow.
- **AIP-103 (Task/Asset state store, shipped in 3.3)** — already backs Common
  AI durable storage on Airflow ≥ 3.3; a candidate home for runtime session
  handles (open question).

## Public Interface

Proposed generic public surface in `apache-airflow-providers-common-ai`
(naming subject to discussion):

```text
DelegatedAgentRuntime         (Protocol: start / resume)
DelegatedAgentRequest
DelegatedAgentResumeRequest
DelegatedAgentResult
DelegatedAgentToolset
DelegatedAgentRuntimeError    (+ any narrower exceptions, in exceptions.py)
```

After these exist, a provider-specific runtime is implementable with **no
modification to `AgentOperator`**, and the runtime dependency is an optional
extra of its own package — Common AI's core dependency set is unchanged.

## Example

```python
from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.toolsets.hook import HookToolset
from pydantic_ai.usage import UsageLimits

AgentOperator(
    task_id="implement_ticket",
    prompt="""
    Implement JIRA-123.
    Gather the information necessary to understand the change.
    Delegate repository modification to the software-engineering agent
    when implementation is required.
    """,
    llm_conn_id="pydanticai_default",
    toolsets=[
        HookToolset(
            JiraHook(jira_conn_id="jira_default"),
            allowed_methods=["get_issue", "search_issues"],
            tool_name_prefix="jira_",
        ),
        HookToolset(
            GithubHook(github_conn_id="github_default"),
            allowed_methods=["get_pull_request", "search_code"],
            tool_name_prefix="github_",
        ),
        AgentFlowToolset(          # third-party runtime adapter
            claude_conn_id="claude_code_default",
            plugin_path="/opt/agent-flow",
            workspace="/workspace/repo",
        ),
    ],
    output_type=EngineeringTaskResult,
    durable=True,                  # delegate_agent becomes one replayable step
    usage_limits=UsageLimits(request_limit=30),
)
```

(`durable=True` and `enable_hitl_review=True` are mutually exclusive in the
current provider; a review-gated variant of this Dag sets
`enable_hitl_review=True` and drops `durable=True`.)

Logical execution:

```text
AgentOperator
  ├── jira_get_issue("JIRA-123")
  ├── github_get_pull_request(...)
  └── delegate_agent(...)
        └── AgentFlow: explore → plan → implement → review → verify
              → DelegatedAgentResult(status="completed", ...)
  └── model synthesizes → structured EngineeringTaskResult
```

## Compatibility and Migration Effort

The change is **additive**. Existing `AgentOperator` Dags and all existing
toolsets behave unchanged; the capability activates only when a delegated
toolset is registered in `toolsets=`.

- **Existing Airflow users:** none.
- **Common AI users:** none unless they adopt delegated agents.
- **Existing third-party agent integrations:** optional adoption of the
  lifecycle/result interfaces; a compatibility wrapper around an existing
  runtime should be hours of work, since the runtime's internals are
  untouched. Vendor providers with task-level agent-runtime operators
  (e.g. Bedrock AgentCore) may optionally add a toolset adapter reusing their
  existing hooks; nothing forces them to.

The provider is pre-1.0 (`lifecycle: incubation`), so the new interfaces can
be marked experimental for at least one release and iterated before any
stability promise attaches.

## Downsides

- **Additional abstraction.** Common AI gains lifecycle concepts for agents
  nested inside another agent. Mitigated by keeping the surface to five small
  types and by the phase-1-external validation step.
- **Opaque nested execution.** Airflow cannot inspect or replay the runtime's
  internal actions; observability inside the boundary is adapter-quality-
  dependent.
- **Worker occupancy.** Delegated runs hold worker slots far longer than
  API-call tools until a deferral follow-up lands.
- **Security surface.** Repository mutation and process execution are
  categorically riskier than the provider's read-only-by-default toolsets;
  the security section makes this a documented, opt-in trust decision.
- **Two LLM loops.** Outer routing agent and delegated agent both spend
  tokens; delegation should be reserved for genuine specialization, and
  `usage_limits` plus the result `usage` field keep it measurable.

## Considered Alternatives

1. **Dedicated `ClaudeCodeAgentOperator` (task-level vendor operator).**
   Rejected *as the Common AI integration model*: it duplicates HITL,
   durability, structured output, logging, and limits, and it cannot compose
   inside the outer agent loop (no context-gathering-then-delegate,
   no mid-run `needs_input` resolution). Task-level vendor operators already
   exist (Bedrock AgentCore) and remain valid for submit-and-wait workloads —
   the documented `common.ai`-vs-vendor rule of thumb continues to cover that
   case.
2. **Rewrite AgentFlow as Common AI agents / Airflow tasks per phase.**
   Rejected: re-implements a functioning verification-gated runtime and
   promotes its internal topology into workflow topology. Airflow
   orchestrates the job, not every internal reasoning phase.
3. **Expose each AgentFlow agent as an individual tool** (`riko()`,
   `senku()`, `loid()`, …). Rejected: the outer model would have to drive an
   orchestration protocol (iteration caps, review verdicts, gate ordering)
   that AgentFlow's state machine already owns and enforces.
4. **Fetch all context in upstream tasks before the agent runs.** Rejected as
   the default: the agent is best positioned to decide which context is
   needed; dynamic tool selection is the point of `AgentOperator`. Upstream
   fetching remains available where determinism matters.
5. **Give the delegated runtime the outer agent's tools directly.**
   Deferred: maximum autonomy, but requires cross-framework tool-schema
   bridges and erodes the phase-1 security model (see *Nested Tool Access*).
6. **Add AgentFlow/Claude-specific APIs to Common AI.** Rejected: Common AI
   exposes generic contracts only; AgentFlow stays a third-party reference
   implementation, and Claude-specific authentication (subscription/OAuth)
   stays entirely inside the runtime adapter's connection type.

## Testing Strategy

The generic implementation ships with tests for:

- **Lifecycle:** `start → completed`; `start → failed`;
  `start → needs_input → resume → completed`; `start → needs_approval`;
  resume with an unknown/expired `session_id`.
- **Durable replay:** with `durable=True`, a completed `delegate_agent` call
  is replayed (not re-invoked) after task retry; a changed request
  fingerprint invalidates replay; a failed delegation is re-invoked.
- **Serialization:** request/result models round-trip through Dag
  serialization and XCom; the toolset is constructible at parse time without
  touching connections or the network.
- **Cancellation:** task termination cancels the in-flight coroutine and
  invokes the runtime's abort path when implemented.
- **Error handling:** runtime exceptions surface as bounded tool errors; the
  outer agent run continues or fails cleanly, never corrupts.
- **Security:** credentials not explicitly supplied to the runtime are
  unreachable from it; parse-time never resolves secrets.
- **Logging/limits:** delegation calls appear in tool logs and count against
  `usage_limits`.

Per project standards: pytest, `spec`/`autospec` mocks, parametrized
lifecycle cases, a fake in-memory `DelegatedAgentRuntime` as the primary test
double.

## Implementation Plan

- **Phase 1 — external reference implementation.** Build `AgentFlowToolset` as
  a third-party `AbstractToolset` with no Common AI changes: headless Claude
  Agent SDK driver for the AgentFlow plugin, start/resume over persisted
  session state, escalation-gate → `needs_input` translation, isolated
  workspace. This phase exists to validate the abstraction against a real
  runtime *before* upstreaming anything — satisfying the provider's
  no-premature-abstraction rule.
- **Phase 2 — generic interfaces.** Upstream
  `DelegatedAgentRuntime` / `DelegatedAgentRequest` /
  `DelegatedAgentResumeRequest` / `DelegatedAgentResult` /
  `DelegatedAgentToolset` (+ exceptions) into
  `apache-airflow-providers-common-ai`, marked experimental; port the
  reference implementation onto them.
- **Phase 3 — durable, HITL, limits validation.** Integration-test the
  contract against `durable=True` replay, `enable_hitl_review=True` feedback →
  `resume_agent`, `usage_limits`, message history, and tool logging; write the
  security-documentation entry (defense-layers table row) and an operator
  how-to page.
- **Phase 4 — second runtime.** Implement at least one non-Claude adapter to
  prove vendor neutrality — candidates: a remote HTTP/A2A agent service, a
  Bedrock AgentCore adapter reusing the Amazon provider's hooks, or a nested
  pydantic-ai agent (also the cheapest permanent test fixture).
- **Phase 5 — optional follow-ups (separate proposals).** Toolset export
  bridge into delegated runtimes; deferrable delegate-and-poll execution;
  `needs_approval` ↔ core HITL binding.

## What Defines This AIP as Done?

1. A generic delegated-agent runtime contract exists in Common AI.
2. A delegated agent registers as a normal `AgentOperator` toolset via
   `toolsets=`, with no `AgentOperator` changes.
3. Existing Common AI toolsets compose normally alongside delegated-agent
   toolsets in one operator.
4. Delegated execution returns the structured lifecycle result, including
   `needs_input` → `resume` round-trips.
5. Successful delegated invocations participate in durable tool replay, with
   the replay semantics (values, not side effects) documented.
6. Existing `AgentOperator` behavior is fully backward-compatible.
7. Security and isolation requirements are documented, including the
   defense-layers entry for delegated runtimes.
8. At least two runtime implementations — one Claude-based, one not —
   validate that the API is vendor-neutral.
9. Reference documentation demonstrates the context-gather-then-delegate
   pattern end to end.

## Decision Summary

**Decision:** use `AgentOperator` + `DelegatedAgentToolset` as the integration
boundary for specialized autonomous agent runtimes.

**Rationale:** maximizes reuse of Common AI (logging, durability, HITL,
limits, output, observability) and minimizes runtime-specific code in Airflow;
respects the existing common.ai-vs-vendor boundary by keeping runtime adapters
out of Common AI core.

**Responsibility boundary:**

| Layer | Owns |
|---|---|
| Airflow | scheduling, retries, worker execution, connections/secrets |
| Common AI | outer agent lifecycle, toolsets, durability, HITL, observability |
| `DelegatedAgentToolset` | standardized delegation contract (start/resume/result) |
| Runtime adapter (third-party/vendor) | SDK, authentication, telemetry, workspace mechanics |
| Specialized agent (e.g. AgentFlow) | domain-specific autonomous execution and its internal verification |

For the reference implementation:

```text
Airflow → Common AI AgentOperator → AgentFlowToolset → Claude Agent SDK → agent-flow
```

AgentFlow extends Common AI instead of establishing a parallel Airflow agent
framework.

## Open Questions

1. Naming: `DelegatedAgentToolset` vs `AgentRuntimeToolset` vs
   `SubAgentToolset` vs `AgentCapabilityToolset`.
2. Should `resume_agent` stay a separate tool, or fold into `delegate_agent`
   with an optional `session_id`?
3. Should the runtime protocol mandate `cancel(session_id)`, or keep abort
   best-effort via coroutine cancellation?
4. Should Common AI define a standard progress-event interface for runtimes
   (heartbeat/log streaming into task logs), or leave it adapter-specific?
5. Should `needs_input`/`needs_approval` later bind directly to core HITL
   (AIP-90 `AWAITING_INPUT`), and does that wait for a deferrable
   `AgentOperator`?
6. Should runtime `session_id`s (and small session metadata) be persisted in
   the AIP-103 task state store, alongside the durable cache that already
   lives there, so a task retry can resume rather than restart a delegated
   session?
7. When the roadmap `capabilities=` kwarg lands with proper serializer hooks,
   should a delegated runtime also be packageable as a pydantic-ai
   *capability* — and if so, how does it join durable caching, which today
   covers only `toolsets=`?
8. Should the `durable` × `enable_hitl_review` mutual exclusion be lifted for
   the delegated-agent case (replaying delegated steps across HITL
   iterations), or is that an independent provider improvement?
9. Should exporting Common AI toolsets into delegated runtimes be
   standardized (follow-up AIP), and over which bridge protocol (MCP being
   the obvious candidate, since runtimes commonly speak it already)?

## Proposed Discussion Subject

`[DISCUSS][AIP-XXX] Delegated Agent Capabilities for Common AI`

**Suggested summary:** Common AI already provides a strong generic agent
runtime through `AgentOperator` and composable toolsets. This proposal asks
whether Common AI should standardize a small, vendor-neutral interface —
start/resume semantics and a structured lifecycle result — for delegating
specialized autonomous tasks to external or nested agent runtimes, while
retaining Common AI durability, HITL, tool management, usage limits, and
observability. A software-engineering runtime built on the Claude Agent SDK
around an existing verification-gated multi-agent Claude Code plugin serves as
the reference implementation and will be validated externally before any
interface is upstreamed; a second, non-Claude adapter is part of the
done-criteria. Vendor authentication details (including any
subscription/OAuth handling) stay entirely inside runtime adapters and their
Airflow connection types — Common AI sees only a connection ID and the generic
contract.
