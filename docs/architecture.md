# Execution library design

Status: proposed implementation contract. This describes the target architecture;
it does not claim these APIs exist in the current 0.1.0 package. The owner has
requested a reusable library and incremental adoption by three existing projects.

## Principles

1. **One implementation for common operations.** Provisioning, ownership checks,
   accounting, supervision, transport and recovery belong in this package. A
   consumer supplies its research workload, policy inputs and result verifier.
2. **Explicit side effects.** Imports and specification validation do not read
   credentials or contact a provider. Live access is deliberately assembled at an
   application entry point; resource mutations require a guarded execution context.
3. **Bounded autonomous execution.** Every attempt has finite resource, spending,
   time, transfer and retry limits. Transient corrections can continue under the
   same approved scope. Unknown provider state requires reconciliation.
4. **Recovery is part of normal execution.** Persist intent before side effects;
   make transitions restartable and record failure evidence. At most one create
   request is sent per attempt. A retry uses a new durable attempt identity.
5. **Keep stronger invariants.** Migration must preserve ownership, budgets,
   source binding and recovery requirements even when their old implementation
   differs. Existing module boundaries need not survive the migration.
6. **Small, typed contracts.** Use configuration and explicit extension points for
   demonstrated variation. Avoid per-project branches, arbitrary provider request
   hooks and an extensibility framework without an actual second use case.
7. **Independent ownership.** Shared code does not combine project approvals,
   credentials, private artifacts or budgets. Each project has an authoritative
   state location and controlling host; other projects may operate independently.
8. **Controlled evolution.** Version public contracts and persisted schemas,
   document compatibility, test packaged artifacts and upgrade consumers explicitly.

## Scope and boundaries

| Shared library | Consumer project |
| --- | --- |
| CPU/GPU offers, bounded provider errors, project/attempt inventory | Scientific question, data rights and owner approval evidence |
| Guarded create/delete, exact identity, resource policy checks | Resource eligibility and selection preferences within the approved boundary |
| Durable reservations, attempt history, caps and billing reconciliation | Workload source/configuration and permitted upload/output manifests |
| Host qualification, watchdog, finite execution, cleanup | Dependency installation commands and model/protocol execution logic |
| Bounded upload/download, direct/proxy adapters, process I/O | Scientific verification, block selection and final analysis |
| Retry eligibility, backoff, restart and operational evidence | Decision to revise scientific design or implement a source correction |

An adapter that implements its own provider HTTP, budget arithmetic, watchdog or
generic SSH retries has crossed this boundary. A genuinely special workload
operation stays local and uses bounded capabilities supplied by the engine.

The library is local control software for cooperating callers. Account credentials
can have wider authority than its guards. A host/provider with administrative
access can inspect colocated workload state. Neither namespace checks nor process
separation constitutes provider IAM or host isolation.

## Proposed application API

Keep the `runpod_experiments` import namespace. Add a small application facade
over the existing lower-level components; classify stable versus internal APIs
explicitly before exposing the facade. The names below are design proposals.

| Contract | Responsibility |
| --- | --- |
| `ProjectPolicy` | Project ID, namespace registry digest, allowed host, state root and configured ceilings |
| `ApprovalScope` | Validated approval evidence, stable objective/disclosure digests, cumulative limits and any owner-imposed deadline |
| `ExecutionPlan` | Immutable job recipe: approved resource constraints, source/package/config digests, transfer manifests and finite retry policy |
| `Workload` | Bounded prepare/start/observe/collect/verify stages using an attempt context |
| `Engine` | Preflight, finite execution, inspection and reconciliation |
| `ExecutionOutcome` | Operational outcome, scientific verdict, cleanup state, billing state and private evidence references |

`Engine.preflight(plan)` validates compatibility, source/policy and capabilities
without provisioning. Any provider reads must be explicitly selected and reported.
`Engine.execute(plan, workload)` owns admission, supervision, permitted retries,
collection and cleanup. `Engine.reconcile(attempt_id)` resolves recorded intent
against exact provider state. Read-only inspection must never resume mutations.

The workload context exposes only the current attempt's bounded transport,
remaining time, stage journal, stop request and artifact sink. It does not expose
an unrestricted provider client or a method to create another resource.

For each admission the engine derives and persists an immutable internal
`AttemptPlan`: unique attempt ID, exact selected request/quote, package/source
digests, technical window and cleanup reserve. A retry can select another eligible
offer within the same approved constraints and produces a new AttemptPlan. A source
correction requires a newly validated ExecutionPlan under the same applicable
ApprovalScope; the engine never edits a frozen attempt or changes source itself.

An approval adapter validates project-owned evidence before the library receives
the common scope. The engine independently enforces its generic fields. A string
called approval_reference and a successful arbitrary callback are not sufficient
evidence. Adapters are application code, not an authorization service.

Use immutable validated values: frozen dataclasses with immutable nested values,
canonical serialized bytes and SHA-256 digests at persistence boundaries. Reject
nonfinite numbers, boolean-as-integer limits, duplicate JSON keys, unsupported
schema versions and malformed or unsupported fields. Amounts use integer USD
microdollars; conversion/rounding rules belong in one module.

## Identity and state model

An approved experiment can contain bounded rounds and logical jobs. An attempt
records one resource create request for a job. A retry creates a new attempt under
the same applicable caps; it never resets its parent's spending or consumed claims.
Several processes or workload blocks may share a Pod. A multi-stage job may use
successive CPU/GPU attempts, and the parallel profile may admit two resource
attempts when explicitly configured and funded.

Use one versioned SQLite execution store per project on its authorized host,
shared across that project's local checkouts. Transactions cover reservation of
slots/cost exposure, immutable provenance and create-claim transitions together.
Record append-only events alongside materialized current state. Pin schema
version; refuse unsupported schemas without silently recreating or truncating them.

| Dimension | Required states/meaning |
| --- | --- |
| Resource lifecycle | reserved, create_claimed, active, stopping, terminated; plus never-sent cancellation, proven rejection and ambiguous state |
| Accounting | estimated exposure, pending bill, posted observations and subsequent verified adjustments |
| Workload | not_started, running, operational_failure, completed |
| Scientific verdict | not_evaluated, passed, failed, inconclusive |

These dimensions must remain independent. A failed scientific gate can still
produce a complete collected result. A terminated Pod can still have a pending
bill. A posted observation is not automatically proof the provider will never
adjust the charge. Preserve previous observations when recording an adjustment.

There is no transaction spanning SQLite and the provider. Commit a one-use create
claim before sending the request and disable hidden transport retries for create.
After timeout or an unexpected response, retain the reservation and any candidate
ID. Reconcile unique name, exact ID and live detail; do not infer no-create merely
from an empty list. Delete retries must revalidate the same exact identity.

## Admission, timing and autonomy

Admission transactionally checks project slots, unresolved attempts, one immutable
approval/round ceiling and aggregate hourly exposure. Reserve setup, work and
cleanup, including declared storage/other cost estimates. Hierarchical
parent/child ceilings are part of the later bounded-parallel milestone. Provider
billing delay or failure can defeat a strict monetary ceiling; record the
remaining uncertainty.

Initial deployment supports one controlling host per project, serial resource
execution, then an explicitly tested two-resource profile. Independent projects
can use different hosts. A future second controller for the same project needs
real coordination; shared code, Git or a local file do not supply that service.

Keep owner-imposed expiry separate from finite operational windows. A technical
window or host proof may be renewed within the approval only when its contract
allows it. Never extend an actual owner deadline or modify an active attempt's
immutable plan. Persist UTC deadlines and boot/host identity; monotonic values are
valid only within their originating process/boot context.

The retry policy classifies outcomes: transient read/transport failures, verified
capacity rejection, setup failure requiring correction, scientific failure,
policy failure and unresolved provider state. It includes finite attempt/time
limits, bounded backoff and rechecks of remaining budget, source, quote and host.
Scientific failures do not automatically trigger a search for a passing result.
Source editing remains an agent/developer operation, followed by validation and
a new bound attempt. The library does not edit code or authorize new research.

## Provider, supervision and transport

Use explicit `list_account_pods`, `list_project_pods` and `get_attempt_pod`
operations. Account inventory is observational. Foreign resources do not occupy
project slots. Unknown project-owned resources or ambiguous attempts require
reconciliation. Legacy adapters preserve the old `pods()` meaning until removed.

The provider adapter normalizes CPU/GPU offers, scheduled resource details,
sanitized errors and exact-Pod billing evidence. Selection preferences are inputs,
not hardcoded GPU names or project IDs. Validate actual shape, rate, image,
location, storage, mounts and ports against the admitted plan.

A separately qualified supervisor starts before create. It verifies plan identity,
freshness, real interpreter/PID and loaded package digest. Stop is latched across
restart and telemetry failures; provider absence requires reconciled detail and
inventory evidence. A detached-process flag, heartbeat or keep-awake request is
not proof of survival through the controlling tool's exit or host power loss.
Record provider-side expiry support and whether it was actually configured.

Use bounded direct SSH first, with a qualified proxy transport for required
consumers. Unify small bundles, streaming files and archives under manifests with
path validation, per-file/total byte limits, local/remote hashes, atomic publication,
timeouts and bounded output. Interactive process framing remains workload-owned.
Transfer resume is a later capability unless needed and separately qualified.

The serial workload adapter owns finite upload and collection timeout/chunk
settings. The library validates them against a one-hour operation ceiling and a
one-million-byte chunk ceiling. Upload timeout bounds the complete reviewed bundle
transfer; collection timeout bounds each file retrieval and is further clamped to
the immutable attempt deadline. The detached collector records its effective byte,
timeout and chunk settings in a versioned attempt-local limits file, accepts only
the prior default-only format or the current schema, and rejects unknown versions
or relaunch settings that differ. Project values such as Partition's 256 KiB
chunks and 600-second collection timeout remain caller configuration rather than
shared defaults.

An optional uploaded Python setup script provides a bounded pre-workload stage
for project-owned dependency verification or installation. Its path must be in
the hash-reviewed upload manifest; the engine launches it through the same exact
Pod SSH guard, checks the supervisor and attempt deadline while it runs, records
the stage, and cleans up before the collector or workload starts on failure.
Controller recovery records an interrupted setup as not replayed. The script's
contents and package policy stay with the consumer.

Host-key trust is explicit policy. Approved model credentials use an ephemeral
delivery channel and never enter plan serialization, argument logs, diagnostics,
manifests or returned artifacts. Journals/results are private by default with
allowlisted export fields. Collect permitted failure evidence before cleanup
where possible; collection failure must not prevent independent termination.

## Package and release boundary

Keep the control core dependency-light on Python 3.11+; scientific/CUDA/HE packages
stay in workload environments. Declare OpenSSH and remote utility capabilities.
Test the built wheel in a fresh environment, including the detached supervisor's
actual interpreter and package location. Preserve the existing Windows base-Python
PID correction without assuming the base interpreter has virtualenv packages.

Each consumer installs an immutable version/artifact and records its digest.
Runtime must not depend on a mutable sibling checkout. Public API, execution
schema and evidence schema have explicit versions. Experimental 0.x releases
document breaking changes and migrations; 1.0 requires demonstrated stable
contracts and the distribution readiness checks in the implementation plan.

## Deliberate deferrals

No general DAG scheduler, provider marketplace, automatic scientific optimization,
multi-host controller service or arbitrary plugin loader is needed for the first
integration. Build explicit extension points for CPU/GPU policy, approval adapters,
workloads and direct/proxy transport. Introduce further abstractions when a real
consumer demonstrates the requirement.
