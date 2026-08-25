# KEP-1026: Kubeflow Agentic Platform — Unified Agent Architecture for Cloud Native AI

**Authors:**
- Abhijeet Dhumal (Red Hat) - [@abhijeet-dhumal](https://github.com/abhijeet-dhumal)

**Tracking Issue:** [kubeflow/community#1026](https://github.com/kubeflow/community/issues/1026)

**Status:** Draft

**Related KEPs:**
- [KEP-936: Kubeflow MCP Server](../936-kubeflow-mcp-server/README.md)
- [KEP-872: Spark History Server MCP](../872-spark-history-server-mcp/README.md)
- [KEP-867: Kubeflow Documentation AI](../867-kubeflow-documentation-ai/README.md)

---

## Summary

Kubeflow has organically adopted agentic interfaces across multiple subprojects — training (MCP Server), data engineering (Spark History Server MCP), documentation (Docs Agent), and upcoming components (Katib, Pipelines, Model Registry). These exist as independent islands today with no shared discovery, coordination, or lifecycle management.

This KEP proposes the **Kubeflow Agentic Platform**: an architecture that would unify these MCP servers under open standards — [Model Context Protocol](https://modelcontextprotocol.io/) for agent-to-tool access, [Agent2Agent Protocol](https://a2a-protocol.org/) for agent-to-agent delegation, and [Agent Skills](https://agentskills.io/) for portable workflow instructions — enabling any AI agent to discover, learn, execute, and delegate across the entire Kubeflow lifecycle through a single federated endpoint.

---

## Motivation

### The Problem

The 2026 agentic ecosystem has converged on three standard layers:

1. **Agent Skills** — portable instructions teaching agents _what_ to do (SKILL.md files)
2. **MCP** — standardized tool connectivity teaching agents _how_ to connect
3. **A2A** — agent-to-agent delegation for _collaborative_ multi-agent workflows

Kubeflow currently participates only in layer 2, and does so as disconnected servers. Users must manually configure separate MCP connections per component, learn each server's tool surface independently, and cannot leverage orchestrator agents (LangGraph, ADK, Strands) to delegate complex cross-component tasks.

Meanwhile:
- The [Agentgateway](https://agentgateway.dev/) project (Linux Foundation) has matured as the standard for MCP federation on Kubernetes with CEL-based tool policies
- The [Skills over MCP](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/main/seps/2640-skills-extension.md) working group (SEP-2640) defines how skills travel via `skill://` URI resources
- Enterprise registries (skills.sh, AWS Agent Registry) provide discovery, security scanning, and governance for agent skills at scale
- A2A Agent Cards (`/.well-known/agent-card.json`) enable zero-configuration agent discovery

Kubeflow is uniquely positioned to own this stack for Cloud Native AI — no other CNCF project covers training + optimization + data processing + model management + pipelines as agent-accessible services. But without an explicit architecture, these components will remain islands.

### Goals

1. Define a reference architecture for Kubeflow as a unified agentic platform
2. Establish conventions for how Kubeflow MCP servers interoperate (discovery, naming, coordination)
3. Adopt A2A protocol so external agents can delegate complex ML tasks to Kubeflow
4. Publish Kubeflow Agent Skills to standard registries for discoverability
5. Integrate with Agentgateway for federated access, auth, and tool policy
6. Provide a single-endpoint experience: one connection, all Kubeflow agent capabilities

### Non-Goals

- Replacing individual MCP server repos (each component retains its own lifecycle)
- Building a new agent runtime (Phase 3 of KEP-936 already addresses this)
- Competing with HuggingFace, Antigravity, or Codex as agent builders — we remain infrastructure
- Mandating a specific LLM provider or agent framework
- Implementing a skills registry from scratch (we publish to existing registries)

---

## Proposal

### Architecture Layers

The proposed MCP server would follow the **unified SDK model**: one process, multiple client modules, each wrapping the corresponding Kubeflow SDK client. This mirrors how the Kubeflow SDK itself ships all clients in one package (`pip install kubeflow`).

```
┌─────────────────────────────────────────────────────────────┐
│                       Agent Clients                         │
│   Claude Code · Cursor · Codex · LangGraph · ADK · Strands  │
└──────────────┬──────────────────┬───────────────┬───────────┘
               │ A2A (delegate)   │ MCP (tools)   │ Skills (learn)
               v                  v               v
┌─────────────────────────────────────────────────────────────┐
│            Agentgateway (optional, enterprise)              │
│         CEL policies · OAuth · Routing · Audit              │
└────────────────────────────┬────────────────────────────────┘
                             │
                             v
┌─────────────────────────────────────────────────────────────┐
│              kubeflow-mcp serve (single process)            │
│                                                             │
│  /.well-known/mcp.json        (MCP Server Card)             │
│  /.well-known/agent-card.json (A2A Agent Card)              │
│  skill://index.json           (Skills Index)                │
│                                                             │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐        │
│  │ Trainer  │ │  Katib   │ │   Hub    │ │  Spark   │  ...   │
│  │  client  │ │  client  │ │  client  │ │  client  │        │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘        │
│       v             v            v             v            │
│  TrainerClient  KatibClient  RegistryClient  SparkClient    │
└────────────────────────────┬────────────────────────────────┘
                             │
                             v
┌─────────────────────────────────────────────────────────────┐
│                  Kubernetes / Kubeflow CRDs                 │
│  TrainJob · Experiment · SparkApp · RegisteredModel · Run   │
└─────────────────────────────────────────────────────────────┘
```

**Companion MCP servers** (separate repos, separate concerns) would handle auxiliary use cases that don't map to the unified SDK's client surface:

```
┌─────────────────────────────────────────────────────────────┐
│           Companion Servers (separate repos)                │
│           Federated via Agentgateway when needed            │
│                                                             │
│  spark-history-server-mcp ── Completed Spark job analysis  │
│  docs-agent               ── Kubeflow documentation RAG    │
└─────────────────────────────────────────────────────────────┘
```

**Design boundary:**
- If a capability maps to a Kubeflow SDK client method, it belongs in `kubeflow/mcp-server`
- If a capability wraps a non-SDK API (Spark History REST, documentation index), separate repo
- Agentgateway would federate the monolith + companions for orgs that need a single endpoint

### Three Integration Standards

```
                          User / Agent
                               │
          ┌────────────────────┼────────────────────┐
          │                    │                    │
          v                    v                    v
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│  Agent Skills    │ │   MCP Tools      │ │  A2A Delegation  │
│  (learn)         │ │   (execute)      │ │  (delegate)      │
│                  │ │                  │ │                  │
│ "How do I        │ │ "Submit this     │ │ "Train model and │
│  fine-tune?"     │ │  TrainJob"       │ │  register it"    │
│                  │ │                  │ │                  │
│ skill://kubeflow │ │ fine_tune(       │ │ POST /a2a        │
│  /training-      │ │  model,          │ │  tasks/send      │
│   patterns       │ │  confirmed=True) │ │                  │
└──────────────────┘ └──────────────────┘ └──────────────────┘
```

| Standard | Role in Kubeflow | Specification |
|----------|-----------------|---------------|
| MCP | Agent-to-tool access for each component | [modelcontextprotocol.io](https://modelcontextprotocol.io/) |
| A2A | Agent-to-agent delegation for cross-component tasks | [a2a-protocol.org](https://a2a-protocol.org/) |
| Agent Skills | Portable instructions teaching agents Kubeflow workflows | [agentskills.io](https://agentskills.io/) |

### User Stories

```
┌────────────┐     ┌────────────┐     ┌────────────┐     ┌────────────┐
│  1. LEARN  │     │ 2. DISCOVER│     │ 3. EXECUTE │     │ 4. DELEGATE│
│            │     │            │     │            │     │            │
│ Agent reads│────>│ Agent finds│────>│ Agent calls│────>│ Agent sends│
│ SKILL.md   │     │ tools via  │     │ fine_tune  │     │ complex    │
│ from       │     │ MCP Server │     │ get_logs   │     │ task via   │
│ skills.sh  │     │ Card       │     │ ...        │     │ A2A        │
└────────────┘     └────────────┘     └────────────┘     └────────────┘
```

#### Story 1: Federated Discovery

A data scientist configures one MCP endpoint (Agentgateway). Their agent would automatically discover training, optimization, data processing, and model registry tools — all prefixed by component (`trainer__fine_tune`, `spark__investigate_failure`, `katib__suggest_config`). No manual multi-server configuration required.

#### Story 2: Cross-Component Delegation via A2A

An orchestrator agent (LangGraph) receives "prepare dataset, train model, register it." It would discover Kubeflow's Agent Card, delegate the task via A2A. The Kubeflow platform agent would internally coordinate: Spark MCP for data prep, Trainer MCP for fine-tuning, Registry MCP for model registration. The orchestrator receives a single completion with artifacts.

#### Story 3: Skill-Based Onboarding

A developer installs `kubeflow-training` from skills.sh. Their agent (Claude Code, Codex, Cursor) reads the SKILL.md and immediately knows the correct tool sequence for distributed training on Kubernetes — without reading documentation or discovering tools first.

#### Story 4: Enterprise Policy Enforcement

A platform team deploys Agentgateway with CEL policies: data scientists can call training and monitoring tools but not `delete_training_job` or `patch_runtime`. The gateway would enforce this at the routing layer — agents are denied, not asked to behave.

#### Story 5: Guided Workflow via MCP Prompts

A user invokes `/mcp__kubeflow__fine_tune_workflow` (a slash command in Claude Code / Cursor). The prompt template would walk through pre-flight checks, runtime discovery, job submission, and monitoring — producing consistent, structured output regardless of which user invoked it. No need to remember the tool sequence or write free-form instructions.

#### Story 6: Kagent-Managed Autonomous Agent

A platform admin declares a kagent Agent CRD with `kubeflow-mcp` as a ToolServer. The agent would autonomously run nightly fine-tuning jobs, monitor convergence, register successful models, and report failures — all governed by K8s RBAC and agentgateway policies. Zero human invocation after initial deployment.

---

## Design Details

### Component 1: Unified Client Architecture (aligned with Kubeflow SDK)

The MCP server would mirror the Kubeflow SDK's unified package model. Each SDK client gets a corresponding MCP client module:

| SDK Client | MCP Client Module | SDK Status | MCP Status |
|-----------|-------------------|-----------|-----------|
| `kubeflow.trainer.TrainerClient` | `kubeflow_mcp/trainer/` | Available | Shipped (23 tools) |
| `kubeflow.katib.KatibClient` | `kubeflow_mcp/optimizer/` | Available | Stub — [#34](https://github.com/kubeflow/mcp-server/issues/34) |
| `kubeflow.hub.ModelRegistryClient` | `kubeflow_mcp/hub/` | Available | Stub — [#181](https://github.com/kubeflow/mcp-server/issues/181) |
| `kubeflow.spark.SparkClient` | `kubeflow_mcp/spark/` | Available | Planned — [#5](https://github.com/kubeflow/mcp-server/issues/5) |
| `kubeflow.pipelines.PipelinesClient` | `kubeflow_mcp/pipelines/` | Available | Planned — [#182](https://github.com/kubeflow/mcp-server/issues/182) |
| `kubeflow.feast.FeastClient` | TBD | Planned | Future |

Each client module would export the same contract:
- `TOOLS` — list of MCP tool functions
- `CLIENT_TOOL_DESCRIPTIONS` — descriptions for discovery
- `CLIENT_TOOL_ANNOTATIONS` — readOnlyHint, destructiveHint metadata
- `INSTRUCTION_SECTIONS` — phase-ordered guidance for agents
- `CLIENT_RESOURCES` — MCP resources (guides, patterns)
- `SECTION_ORDER` — canonical ordering for this client's phases

**Why monolith, not federation for SDK clients:**
- One `pip install kubeflow-mcp` — same as one `pip install kubeflow`
- Single process means zero-latency cross-client coordination (e.g., training complete then register model)
- Persona filtering works across all tools uniformly
- Progressive/semantic tool modes compress the full surface without per-server configuration
- One OTel trace spans the entire workflow (train, optimize, register)

### Component 2: Companion Server Federation (Agentgateway)

Companion servers wrap APIs outside the unified SDK scope. For enterprise deployments that need a single agent endpoint, Agentgateway would federate the monolith with companion servers:

```yaml
apiVersion: agentgateway.io/v1alpha1
kind: AgentgatewayBackend
metadata:
  name: kubeflow-platform
  namespace: kubeflow
spec:
  mcp:
    targets:
      - name: kubeflow
        service:
          name: kubeflow-mcp
          port: 8080
      - name: spark-history
        service:
          name: spark-history-mcp
          port: 8080
      - name: docs
        service:
          name: kubeflow-docs-agent
          port: 8080
```

Tool namespacing: `{backend}_{tool}` (e.g., `kubeflow_fine_tune`, `spark-history_investigate_failure`).

Policy enforcement via `AgentgatewayPolicy`:

```yaml
apiVersion: agentgateway.io/v1alpha1
kind: AgentgatewayPolicy
metadata:
  name: data-scientist-policy
spec:
  targetRefs:
    - kind: Gateway
      name: kubeflow-gateway
  backend:
    mcp:
      authorization:
        action: Allow
        policy:
          matchExpressions:
            - 'mcp.tool.name.startsWith("trainer_") && !mcp.tool.name.contains("delete")'
            - 'mcp.tool.name.startsWith("spark_")'
            - 'mcp.tool.name == "registry_get_model"'
```

### Component 3: A2A Agent Card and Delegation — [#183](https://github.com/kubeflow/mcp-server/issues/183)

Each Kubeflow MCP server would publish an A2A Agent Card at `/.well-known/agent-card.json`:

```json
{
  "name": "kubeflow-trainer",
  "description": "Distributed model training and fine-tuning on Kubernetes",
  "url": "https://kubeflow-mcp.example.com/a2a",
  "version": "1.0.0",
  "capabilities": {
    "streaming": true,
    "pushNotifications": false
  },
  "skills": [
    {
      "id": "fine-tune",
      "name": "Fine-Tune LLM",
      "description": "LoRA/QLoRA fine-tuning of HuggingFace models on Kubernetes GPUs"
    },
    {
      "id": "distributed-training",
      "name": "Distributed Training",
      "description": "Multi-node PyTorch distributed training with fault tolerance"
    }
  ],
  "authentication": {
    "schemes": ["bearer"]
  }
}
```

A federated Kubeflow Agent Card would aggregate all component skills for platform-level delegation.

### Component 4: Agent Skills Packages — [#184](https://github.com/kubeflow/mcp-server/issues/184)

Skills would be published to skills.sh and exposed via MCP Resources (`skill://` URI scheme per [SEP-2640](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/main/seps/2640-skills-extension.md)):

```
kubeflow-training/
├── SKILL.md                  # Entry point — teaches agent the training workflow
├── patterns/
│   ├── lora-sft.md           # LoRA fine-tuning pattern
│   ├── grpo.md               # GRPO reward training
│   └── distributed.md        # Multi-node distributed training
└── mcp.json                  # Points to kubeflow-mcp-server for tool access
```

`SKILL.md` would follow the [Agent Skills specification](https://agentskills.io/specification): progressive disclosure (name + description loaded first; full content only when skill is activated), portable across all major agent harnesses (Claude Code, Codex, Cursor, OpenClaw).

Mapping to existing MCP Resources — the Trainer MCP server already exposes:
- `trainer://guides/training-patterns`
- `trainer://guides/platform-fixes`
- `trainer://guides/troubleshooting`

These can map directly to `skill://kubeflow/training-patterns`, etc. The gap is conforming to the `skill://` URI convention and publishing a `skill://index.json` resource.

### Component 5: Cross-Component Coordination Conventions — [#186](https://github.com/kubeflow/mcp-server/issues/186)

To prevent tool collision and enable smooth multi-server usage:

| Convention | Rule |
|-----------|------|
| Tool naming | `{verb}_{noun}` — no component prefix in standalone mode; gateway adds prefix |
| Response hints | Include `next_steps` field referencing tools from other servers when appropriate |
| Shared types | Common response shapes (`ToolResponse`, `PreviewResponse`) across all Kubeflow MCP servers |
| Resource URIs | `{component}://guides/{topic}` pattern (already used by Trainer) |
| Persona mapping | Consistent persona names across servers (`readonly`, `data-scientist`, `ml-engineer`, `platform-admin`) |

### Component 6: Discovery Endpoints — [#185](https://github.com/kubeflow/mcp-server/issues/185)

| Endpoint | Standard | Purpose |
|----------|----------|---------|
| `/.well-known/mcp.json` | MCP Server Card | Auto-discovery for MCP clients |
| `/.well-known/agent-card.json` | A2A Protocol | Agent-to-agent discovery |
| `skill://index.json` | [SEP-2640](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/main/seps/2640-skills-extension.md) | Skills discovery via MCP Resources |

### Component 7: MCP Prompts for Guided Workflows

MCP prompts are user-invoked templates (slash commands) for repeatable, structured workflows. The [Spark History Server MCP](https://github.com/kubeflow/mcp-apache-spark-history-server) already ships prompts (`investigate_failure`, `compare_applications`). The Kubeflow Trainer MCP should adopt the same pattern:

| Prompt | Purpose |
|--------|---------|
| `fine_tune_workflow` | Guided: pre-flight, discover runtime, submit, monitor |
| `troubleshoot_job` | Structured debugging: status, events, logs, hints |
| `register_after_training` | Post-training: extract artifacts, register in Model Registry |
| `optimize_hyperparameters` | Katib-driven: define search space, run trials, recommend config |

Prompts complement tools (model-controlled) and resources (app-controlled) as the user-controlled primitive. They would standardize how users invoke multi-step workflows without free-typing instructions every time.

### Component 8: Kagent as Agent Runtime

[Kagent](https://kagent.dev/) (CNCF Sandbox) runs AI agents as Kubernetes CRDs. Kubeflow MCP could serve as a ToolServer for kagent-managed agents:

```
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                       │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ kagent namespace                                      │  │
│  │   Agent CRD ── ModelConfig CRD ── ToolServer CRD      │  │
│  └──────────────────────────┬────────────────────────────┘  │
│                             │                               │
│                             v                               │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ agentgateway namespace                                │  │
│  │   Proxy (auth, CEL policies, audit, OTel)             │  │
│  └──────────────────────────┬────────────────────────────┘  │
│                             │                               │
│              ┌──────────────┼──────────────┐                │
│              v              v              v                │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ kubeflow namespace                                    │  │
│  │   kubeflow-mcp      (training, registry, pipelines)   │  │
│  │   spark-history-mcp (spark debugging)                 │  │
│  │   docs-agent        (documentation RAG)               │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

```yaml
apiVersion: kagent.dev/v1alpha2
kind: Agent
metadata:
  name: kubeflow-training-agent
spec:
  systemPrompt: "You are a Kubeflow training assistant..."
  toolServers:
    - name: kubeflow-mcp
      transport: http
      url: http://kubeflow-mcp.kubeflow.svc:8080/mcp
  modelConfig:
    ref: llama3-model-config
```

This would give platform teams:
- Declarative agent definitions (GitOps-friendly)
- RBAC and admission control via standard K8s mechanisms
- Agent traffic routed through agentgateway for policy enforcement
- OTel tracing for full agent-to-tool observability

The kagent + agentgateway + kubeflow-mcp stack would mirror the control-plane/data-plane split: kagent manages agent lifecycle, agentgateway handles connectivity, kubeflow-mcp provides tools.

---

## Implementation Plan

```
Phase A              Phase B              Phase C              Phase D              Phase E
Conventions          SDK Parity           Discovery            Federation           Platform
& Skills                                  & Delegation         & Kagent             Agent
─────────────────    ─────────────────    ─────────────────    ─────────────────    ──────────
 Conventions          Model Registry       MCP Server Card      Helm chart           Meta-agent
 Agent Skill          Pipelines            A2A Agent Card       CEL Policies         A2A tasks
 skill:// URIs        Katib                A2A endpoint         ToolServer CRD       Artifacts
 MCP Prompts          Spark                LangGraph demo       E2E tests
 MCP Registry                                                   agentregistry
```

### Phase A: Conventions & Skills (Low effort, immediate value)

1. Document cross-component conventions — [#186](https://github.com/kubeflow/mcp-server/issues/186)
2. Publish `kubeflow-training` Agent Skill to skills.sh — [#184](https://github.com/kubeflow/mcp-server/issues/184)
3. Map existing `trainer://guides/*` resources to `skill://` URIs — [#184](https://github.com/kubeflow/mcp-server/issues/184)
4. Add MCP Prompts for Trainer guided workflows (`fine_tune_workflow`, `troubleshoot_job`)
5. Publish to MCP Registry (`mcp-publisher publish`) — pending org auth

**Deliverable:** Any agent with skills.sh access could discover and learn Kubeflow training. User-invoked prompts would provide repeatable multi-step workflows without free-typing.

### Phase B: SDK Client Parity (Medium effort, high value)

1. Model Registry (Hub) client module — [#181](https://github.com/kubeflow/mcp-server/issues/181)
2. Pipelines client module — [#182](https://github.com/kubeflow/mcp-server/issues/182)
3. Katib (Optimizer) client module — [#34](https://github.com/kubeflow/mcp-server/issues/34)
4. Spark client module — [#5](https://github.com/kubeflow/mcp-server/issues/5)

**Deliverable:** Full SDK parity — all 5 Kubeflow SDK clients exposed as MCP tools.

### Phase C: Discovery & Delegation (Medium effort)

1. MCP Server Card at `/.well-known/mcp.json` — [#185](https://github.com/kubeflow/mcp-server/issues/185)
2. A2A Agent Card and `/a2a` endpoint — [#183](https://github.com/kubeflow/mcp-server/issues/183)
3. Example: LangGraph agent delegating to Kubeflow via A2A

**Deliverable:** Orchestrator agents could discover and delegate to Kubeflow without manual config.

### Phase D: Agentgateway Federation (Medium effort)

1. Helm chart overlay: deploy Agentgateway alongside Kubeflow MCP servers
2. `AgentgatewayBackend` and `AgentgatewayPolicy` templates
3. Kagent ToolServer manifest: expose `kubeflow-mcp` as a kagent-compatible ToolServer CRD
4. Documentation: single-endpoint setup for multi-component access
5. E2E test: agent uses federated endpoint to call trainer + spark tools
6. Evaluate [agentregistry](https://github.com/agentregistry-dev/agentregistry) (CNCF Sandbox applicant) for enterprise catalog and governance

**Deliverable:** One endpoint, all Kubeflow tools, with enterprise policy enforcement. Kagent agents could declare `kubeflow-mcp` as a ToolServer for autonomous K8s-native workflows.

### Phase E: Platform Agent (Higher effort, future)

1. A meta-agent that coordinates across Kubeflow components for complex multi-step tasks
2. Accepts A2A task delegation, orchestrates internal MCP calls
3. Returns consolidated results and artifacts

**Deliverable:** "Train and register this model" as a single A2A task.

---

## Relationship to Existing KEPs

| KEP | Relationship |
|-----|-------------|
| KEP-936 (MCP Server) | Phase 4 already plans A2A and Agentgateway. This KEP formalizes the cross-component architecture that 936 implements for the Trainer client |
| KEP-872 (Spark MCP) | Companion server federated via Agentgateway. Its prompts pattern (`investigate_failure`) should be adopted by the Trainer MCP for guided workflows |
| KEP-867 (Docs Agent) | Could serve as a `resource://` provider — agents would fetch Kubeflow conceptual context mid-workflow instead of hallucinating. Could also serve `skill://` resources for documentation navigation |

### Integration with CNCF Ecosystem

| Project | Role | Integration |
|---------|------|-------------|
| [kagent](https://kagent.dev/) (CNCF Sandbox) | Agent runtime | Kubeflow MCP as a ToolServer CRD; agents managed as K8s resources |
| [agentgateway](https://agentgateway.dev/) (Linux Foundation) | Data plane | MCP federation, A2A routing, CEL policies, auth, audit |
| [agentregistry](https://github.com/agentregistry-dev/agentregistry) (CNCF Sandbox applicant) | Catalog and governance | Publish, curate, approve MCP servers and skills for enterprise |
| [MCP Registry](https://registry.modelcontextprotocol.io/) | Public discovery | Metadata catalog for public MCP server discoverability |

---

## Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| A2A protocol still maturing (v1.0 in early 2026) | Agent Card is stable; task lifecycle is additive. Start with discovery only |
| Agentgateway API changes | Pin to stable CRD versions. Use Helm chart abstraction |
| Skills specification governance (Anthropic-originated) | Now under Agentic AI Foundation (Linux Foundation). Same neutrality path as MCP |
| Fragmented ownership across WGs | This KEP would be owned by WG ML Experience; coordination with individual component WGs via shared conventions document only |
| Enterprise adoption hesitancy | All standards are open (Apache 2.0, Linux Foundation). No vendor lock-in |
| Tool surface explosion in federated mode | Agentgateway supports CEL tool filtering. Progressive/semantic modes reduce visible tools per component |
| Kagent is early-stage (CNCF Sandbox) | Integration is optional and additive. Kubeflow MCP works standalone. ToolServer manifest is just a YAML example |

---

## Test Plan

- **Unit tests:** Agent Card generation, skill index serialization
- **Integration tests:** Agentgateway routing to multiple MCP backends on Kind
- **E2E tests:** Agent discovers federated endpoint, calls tools across 2+ components
- **Conformance:** Tool naming conventions validated in CI (schema snapshot tests)

---

## Graduation Criteria

| Milestone | Criteria |
|-----------|---------|
| Alpha | Skills published, Agent Cards served, conventions documented |
| Beta | Agentgateway Helm chart, federation tested with 2+ components, A2A delegation works with at least one orchestrator framework |
| Stable | Production deployments, security audit of federation layer, 3+ components federated |

---

## Drawbacks

- Adds operational complexity (Agentgateway as a new dependency)
- Skills and A2A are newer standards with smaller adoption than MCP
- Cross-component coordination requires governance across multiple Kubeflow WGs

---

## Alternatives

### Alternative 1: Pure Federation (separate MCP servers per component)

Each SDK client gets its own MCP server repo and process.

**Rejected because:** Contradicts the unified SDK model (`pip install kubeflow` gives all clients). Adds operational overhead (5+ processes), prevents zero-latency cross-client coordination, and fragments persona/policy enforcement. Adopted only for companion servers wrapping non-SDK APIs (Spark History REST, Docs index).

### Alternative 2: Custom Federation Protocol

Build a Kubeflow-specific tool routing layer.

**Rejected because:** Agentgateway already solves this as a Linux Foundation project with broad adoption. Reinventing it wastes effort and reduces interoperability.

### Alternative 3: No Architecture — Let Components Evolve Independently

Status quo.

**Rejected because:** Users already report confusion configuring multiple MCP connections. Without conventions, tool naming will collide, personas will diverge, and the overall experience degrades as more components gain MCP support.

---

## References

- [Model Context Protocol](https://modelcontextprotocol.io/)
- [Agent2Agent Protocol](https://a2a-protocol.org/)
- [Agent Skills Specification](https://agentskills.io/)
- [Skills over MCP (SEP-2640)](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/main/seps/2640-skills-extension.md)
- [Agentgateway](https://agentgateway.dev/)
- [Kagent](https://kagent.dev/)
- [Agentregistry](https://github.com/agentregistry-dev/agentregistry)
- [MCP Registry](https://registry.modelcontextprotocol.io/)
- [Kubeflow MCP Server](https://github.com/kubeflow/mcp-server)
- [Spark History Server MCP](https://github.com/kubeflow/mcp-apache-spark-history-server)

---

*Created: 2026-08-26*
