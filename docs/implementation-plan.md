# Implementation and consumer migration plan

Status: M0 design and M1 are complete. M2 remains open pending account-level
qualification of billing and resource quotes. The isolated
`design/unified-execution` worktree now has an internal schema-v4 SQLite store,
store-backed cleanup supervisor and one-shot create controller, store-verified
direct/proxy transfer paths, durable stage events and an internal serial
coordinator for one attempt, finite job retries and controller-restart cleanup.
M3 through M6 exit criteria remain open; this is not a qualified production
engine or consumer migration. Read
[architecture](architecture.md), [testing](testing-strategy.md) and
[contribution rules](../CONTRIBUTING.md) together. The earlier
[unification study](unification-roadmap.md) supplies the capability inventory.

## Working rules

- Implement and validate one bounded milestone at a time in an isolated branch or
  worktree. Keep the currently pinned library checkout and active controllers
  intact until that consumer's cutover gate passes.
- Add tests alongside each behavioral change. After the consumer migrations,
  perform a separate integrated acceptance phase. Testing is not deferred until
  all infrastructure has been replaced.
- Commit explicit paths, retain negative results and record revision/environment.
  A milestone completes only when its exit evidence exists; a plan is not that
  evidence. Documentation examples must identify proposed versus implemented APIs.
- Use synthetic provider/workload fixtures for development. Paid qualification
  requires its own applicable owner authorization and permitted export bundle.
- A temporary compatibility facade delegates to shared behavior and has a named
  caller inventory and deletion gate. Do not build a second long-lived engine.

## Milestone sequence

| ID | Deliverable | Dependencies | Exit evidence |
| --- | --- | --- | --- |
| M0 | Architecture, implementation/test plan and contribution contract | Existing cross-project study | Document consistency, source provenance and reviewable decisions |
| M1 | Validated specifications, error/outcome taxonomy, capability/schema versions and offline test harness | M0 | Invalid values fail without credentials/network; arbitrary synthetic project works |
| M2 | Provider/inventory/ownership adapter and normalized quote/error/billing observations | M1 | Exact identity, pagination, rejection/ambiguity and redaction contracts pass |
| M3 | Transactional project store and serial accounting/admission | M1 | Restart, process races, caps, pending/adjusted charges and crash boundaries pass |
| M4 | Qualified launcher, independent supervisor and recovery | M2, M3 | Parent-exit survival, real PID/package identity and exact cleanup tests pass |
| M5 | Bounded transfer/process contracts and collection | M1, M2 | Hashes, byte/time limits, secrets, failure evidence and direct/proxy fixtures pass |
| M6 | Serial engine with finite retry/reconcile and workload stages | M2, M3, M4, M5 | Complete fake CPU/GPU attempts plus injected failures/restart pass |
| M7 | Hybrid consumer migration | M6 | Installed artifact and existing interactive/streaming requirements pass |
| M8 | MIAP consumer migration | M7 | Sequential batch recovery and independent result finalization pass |
| M9 | Partition serial consumer migration | M8 | CPU/GPU execution and service-process workload requirements pass |
| M10 | Bounded parallel admission and partition dispatcher migration | M3, M4, M6, M9 | Two-slot races, parent/child budgets and independent watchdogs pass |
| M11 | Integrated acceptance, duplication retirement and release | M7, M8, M9, M10 | All consumer gates, release artifact checks and documented retirement complete |

M0 completed as documentation in commit `3a13149`; M1 is complete in commit
`b399a2a`. M2's provider observation and synthetic billing normalization are
implemented on the design branch, but M2 stays open until account-level billing
behavior is qualified. M3's first local store tranche is implemented and
validated; its remaining exit criteria and M4 through M11 remain open. The order
lets consumer experience improve the contracts before the parallel cutover.
If serial migration exposes a missing common capability, implement it centrally
with regression coverage before continuing that migration.

## M1: specifications and development boundary

Add immutable validated project, approval, execution/attempt, resource, transfer,
retry and outcome specifications. Start with plain Python protocols/dataclasses; publish
only the application-facing subset. Document serialization and compatibility.
Keep unknown provider state and scientific failure distinct in the error taxonomy.

Create a fake provider, synthetic workload and isolated state fixtures shared by
core conformance tests. Test assembly must deny live-provider construction and
credential discovery, including child processes. Define stage/event schemas and
redacted operational evidence. Include a fourth synthetic project to ensure the
engine does not embed the three current project names or special-case their IDs.

The M1 baseline added frozen schema-v2 contract records and stage events,
canonical UTF-8 JSON envelope helpers (sorted keys, compact form, one
MiB maximum), duplicate-key/nonfinite rejection, typed failure categories, an
opt-in test-mode provider guard, and synthetic fixtures. Approval scopes bind
the hash of their project policy. `validate_attempt` checks attempt identity,
retries, approved resource/region, time limits, expiry and source/config/package/
policy digests. The focused contract suite passes 21 tests; the full existing
library suite passes 68 tests. A Windows checkout initially failed the existing
reviewed-source digest because Git converted the file to CRLF; `.gitattributes`
now pins that file to LF (commit `54aedf4`) and the runner test passes.

These are internal experimental Python APIs. Encoded documents use the exact
`kind`, `payload`, `schema_version` envelope; decoding validates that envelope
and returns raw payload data without reconstructing dataclasses. No engine,
durable store, authorization verifier or stable public API exists yet. The
test-mode environment flag is a development barrier, not a security boundary.

## M2: provider boundary

Evolve current RunPod transport, registry, planning, resource-policy and ownership
modules behind explicit scoped inventory and guarded-mutation interfaces. Preserve
legacy inventory semantics only in documented temporary facades.

Normalize CPU/GPU offers and observed shape/rate; retain raw provider responses
only in private evidence storage when permitted. Add bounded sanitized error
categories and exact-Pod billing records/adjustments. Verified rejection evidence
must be separate from generic HTTP errors. Disable automatic create retries.
Prevent public workload hooks from reaching raw mutation transport.

M2's current branch work adds a read-only `ProviderAdapter` with account/project
inventory, exact attempt detail checks and normalized CPU/GPU quote methods.
Inventory rejects malformed or duplicate identities. Catalog pagination,
item totals and response-body sizes are bounded. Provider errors are reduced to
safe category/code values; generic HTTP
errors never prove capacity rejection, and mutation errors remain ambiguous.
`QuoteObservation` stores compute and storage estimates separately, with
microdollar rounding upward and a hash of the source response. The read-only
adapter parses exact-Pod billing into immutable `BillingObservation` records
through REST v2. It requires an attempt's bounded start/end interval, calls
`GET /v2/billing/pods` with `podId` and `bucketSize`, and validates response query
metadata, record count, Pod identity, explicit period bounds and `totalAmount`.
The provider may snap query bounds out to full buckets; the returned metadata
must cover the requested interval and each billed row must overlap it. Amounts
round upward to microdollars; the complete response is hashed, raw billing rows
are not retained, and empty responses produce no observation and cannot settle a
hold. Adjustments require the caller to link the exact superseded row. The
request and response follow the official
[REST v2 billing reference](https://docs.runpod.io/api-reference-v2/billing/get-pod-billing-history).
The create-request builder follows the documented
[REST v2 Pod creation schema](https://docs.runpod.io/api-reference-v2/pods/create-a-pod).
As of 2026-11-12, RunPod's official
[migration guide](https://docs.runpod.io/api-reference-v2/migrate-from-v1)
says REST v1 is scheduled to retire on 2026-11-15; this branch now uses v2 for
both Pod and billing reads. This is schema review and synthetic testing only,
not live account qualification.

The new billing parser and transport path have synthetic coverage, but no
provider account was queried. M2 remains open pending permitted account-level
qualification of bucket boundaries, delayed/adjusted rows and actual billing
behavior. Existing controllers still use their current guarded low-level path;
the new adapter does not change their runtime behavior. On Windows with Python
3.12.10, the provider adapter suite passed 18 tests, the connection suite passed
11 tests, and the full offline library suite passed 112 tests. No provider
account or resources were accessed.

## M3: durable state and accounting

Implement a versioned SQLite store with immutable identity/provenance and event
history. Transactionally link approval scope, slots, budget holds and create
claims. Treat one approval scope as the immutable experiment-round ceiling;
defer hierarchical parent/child budgets to M10's parallel admission work.
Account for later charges and evidence-checked adjustments. Preserve history when
scientific source is corrected under an unchanged approval scope. Keep an actual
owner deadline distinct from an operational window that may be renewed under that
approval.

Do not migrate production databases automatically. First build read-only legacy
inspectors and explicit import/bridge validation. Establish the canonical state
root and host identity across multiple checkouts. A second controller host is a
separate future capability, not a side effect of using the same package.

The internal `StateStoreConfig` requires explicit
`RUNPOD_EXPERIMENTS_STATE_ROOT` absolute path and
`RUNPOD_EXPERIMENTS_CONTROLLER_HOST` configured identity. It resolves one file
per project at `<state-root>/<project-id>/execution.sqlite3`; it has no
working-directory fallback and rejects unsafe project/database paths. Its
`open_execution_store()` helper passes the configured project path and host to
the SQLite store. Newly created directories use owner-only mode on POSIX; on
Windows they inherit the configured parent ACL. The host label is configuration,
not proof of machine identity. Every cooperating checkout must still use the
same root and approved host identity, and the owner must review the chosen path
and Windows ACL before using it for live state.

### Current M3 slice (internal and unreleased)

`runpod_experiments.execution_store.ExecutionStore` currently supplies a
schema-v4 SQLite ledger for one project on one configured controller host. It
canonicalizes the database path and immutably binds the store to the supplied
project and host identities. `StateStoreConfig` provides the path formula and
explicit configuration inputs, while deployment still must set the same root in
every checkout and ensure the configured host identity matches the actual
controller. The store atomically stores immutable approval, plan and attempt
documents; project/approval slot
counts; selected-rate hourly exposure; aggregate per-approval billed and
estimated totals; and a one-use create claim. It persists candidate Pod binding,
stop/reconciliation/confirmed-absence transitions, append-only exact-Pod billing
observations and superseding adjustments. Database triggers protect immutable
attempt identity and audit/evidence rows. Event rows form a verifiable hash chain
that detects accidental mutation or reordering of those event rows. Unknown,
unversioned and incomplete databases fail closed without automatic migration.
`verify_attempt_lifecycle_projection()` compares lifecycle state, create-claim
and Pod identity fields with the reserved/lifecycle event projection before
recovery code uses them. It is an explicit M4 guard, not protection against an
actor who can rewrite the whole database and recompute its hashes.

Admission estimates exposure by rounding up
`ResourceProfile.hourly_micro_usd * (runtime_seconds + cleanup_reserve_seconds)`
to the next microdollar. Callers must put the full known recurring hourly cost,
including separately quoted storage, into that selected profile. A terminated
attempt retains its estimate until the caller supplies reviewed billing evidence
whose declared coverage includes the create-claim and termination timestamps.
Later billing observations reopen the hold. The store cannot verify that an
evidence digest or coverage claim is true, and no provider invoice importer is
implemented; settlement remains pending caller-side validation. A confirmed
create absence releases its hold only after the caller has independently
reconciled exact scoped provider inventory. The local hash chain is not tamper-proof
against a party that can rewrite the database, and it does not authenticate a
caller-supplied billing or provider-reconciliation claim.

Each persisted charge is bounded to SQLite's signed integer range, while
aggregated costs use Python's arbitrary-precision integers. This preserves the
full sum when actual posted charges exceed an approved cap or the database
integer range; admission then remains closed because remaining budget is zero.

The store tests cover restart/reopen across reservation, claim, binding, stop
and settlement; one-use claims and idempotent local retries; project and approval
admission caps; fresh-database plus last-budget and last-slot process races;
forced process exit during reservation and immediately after writes at create
claim, Pod binding, stop request, termination, reconciliation, confirmed create
absence, pre-create cancellation, billing observation and billing completion,
both immediately before event insertion and immediately after event insertion but
before transaction commit; process exit between approval/job/plan reservation
writes and between billing-row insertion and the accounting-hold update;
scoped project/host identity; ambiguous create/stop reconciliation; immutable
evidence rows; billing adjustments; and an end-to-end fake path from normalized
provider billing through transactional accounting. Each crash case reopened the
store and verified unchanged attempt/accounting state and event chain. On Windows
with Python 3.12.10, all 25 focused store tests and all 119 library tests passed.
Over-cap posted billing remains recorded, and further admission is denied; the
aggregate also remains exact when individually valid charges sum above SQLite's
integer limit. Direct mutations of the mutable Pod/state projection are rejected
by the new integrity check.
Three state-configuration tests passed; one symlink test was skipped because
this Windows host does not permit directory symlink creation. These runs used local
temporary databases and fake/local fixtures; no RunPod credentials, account
calls or resources were used. This is not yet a production engine or stable API.
Remaining M3 work includes verifying common root/host configuration across the
consumer checkouts, crash checks at additional SQL-write boundaries, and further
accounting edge cases. M10 owns hierarchical parent/child budget admission. M2
still needs account-level billing qualification. Consumers continue on their
existing package pins until their separate migration gates pass.

## M4: supervision

Consolidate detached launch and persistent process identity checks. Qualify the
installed artifact under the exact interpreter used by each child. Cover the
existing Windows launcher/base-Python PID behavior and job-survival requirements;
declare Linux support only after its session/termination tests pass.

Unify readiness, stop latching, elapsed-time recovery, finite grace/cleanup,
telemetry retries and provider-confirmed absence. Recovery can operate on an
exact durable attempt even when normal workload bootstrap fails. A damaged ledger
must not authorize a different resource or be reported as successful termination.

The legacy detached entrypoint still supports its `AttemptLedger` and file-backed
`ResourcePolicy` behavior unchanged. The additive
`request_delete_for_store_attempt()` primitive verifies the lifecycle projection,
derives project scope from the hash-verified plan and approval documents,
requires a latched stopping state and exact live project/Pod identity, and does
not record termination. `StoreCleanupMonitor` now binds unique reserved-name
candidates, latches stops, and requires three scoped provider absence observations
before recording create absence or termination. Its observation count is
intentionally in-memory, so restart repeats checks conservatively. It reconstructs
the selected resource profile from the hash-verified attempt and plan, and checks
live Pod identity, selected region, CPU shape, image digest, disk, mounts, status
and hourly rate. For GPU profiles the v2 response's `gpu.memory` is treated as
host system RAM, and the monitor checks ID/count, total vCPU and system-memory
minima plus the CUDA floor. It still fails closed because Pod detail does not
expose per-card VRAM and that runtime evidence is not independently collected.
`launch_execution_store()` validates the existing state root/host/attempt, derives
the deadline from durable state and starts this monitor in a separate process.
The supervisor state records the hash of the exact attempt plan for future
readiness checks. The legacy CLI and launcher remain available.

On Windows with Python 3.12.14, 30 focused store tests and all 124 library tests
passed; one symlink-capability test was skipped. A fresh no-dependency venv
installed the test wheel (SHA-256
`25ce293cdf3cdacc5ef4a19862ae2a304546244ddcaf4b27c768534c85d6fb17`), and the
installed-package detached test proved the supervisor PID matches the process
state and continues after its launcher parent exits. That test uses a local fake
provider gate; it makes no RunPod requests. Live API response compatibility,
Windows power-loss behavior, restart recovery during an active deletion, and
resource-shape qualification against an authorized account remain open. The new
store-backed path has not been connected to a create engine or consumer. GPU
attempts cannot pass this watchdog's current fail-closed runtime check.
Do not mix the two ledgers for one attempt or run both controllers against the
same job. Cut over only after restart, ambiguous-create reconciliation,
exact-Pod deletion and confirmed-absence conformance tests pass.

## M5 and M6: transport, workload execution and retries

Promote bounded streaming upload and archive download into the common transfer
contract. Implement direct SSH and the required proxy handshake against common
integrity/limit tests. Keep protocol-specific framing in workload adapters. Add
explicit ephemeral secret delivery and redaction, with private failure journals.

The serial engine admits one already-reserved attempt, starts supervision,
claims/sends one create, verifies the scheduled resource, runs bounded workload
stages, collects allowlisted outcomes and requests cleanup. Job retries use a
fresh reservation callback and require an allowed typed failure plus terminal
cleanup; the callback remains responsible for current quotes and the store
enforces the remaining approved scope and budget. Controller restart recovery
closes any interrupted stage as not replayed, asks a running collector for one
final pass, and relies on the independent supervisor to reconcile provider
termination. It does not relaunch a stale supervisor or replay a workload.
Operational and scientific verdicts remain separate; the coordinator returns
`NOT_EVALUATED` until the consumer's verifier decides the scientific result.

`SSHInteractiveProcess`, bundle upload, bounded remote launch, result collection
and a detached collector support an `ExecutionStore` attempt in addition to the
legacy ledger. Shared resolution verifies the lifecycle projection, project
policy, selected resource/region, declared ports and live provider detail before
opening the explicitly selected SSH endpoint. Direct remains the default. The
new `SSHProxySession` supports finite PTY commands and framed, bounded file
transfers; larger proxy files use digest-checked chunks and uploads publish by
atomic rename. Store-backed upload and collection can select this route,
including from a detached collector. Raw binary pipes, long-lived process
launch and interrupted large-file resume remain open. Fake-provider coverage
exercises the direct paths and proxy framing/route selection; unexpected live
shape prevents SSH startup, and the legacy paths remain covered. The internal
`SerialExecutionEngine` records durable stage events, passes manifest file and
aggregate limits into direct or proxy collectors, coordinates one reserved
attempt, retries only after a clean terminal failure, and recovers an
interrupted attempt without rerunning its workload. A stale or missing
supervisor leaves cleanup pending for reconciliation. GPU SSH and workload
access remain blocked by unavailable per-card VRAM runtime evidence. Full M6
acceptance still requires a successful fake GPU attempt through an explicit
runtime-evidence seam, additional fault-injection/restart cases, stable
package/API review and consumer acceptance. No consumer source or package pin
changed in this slice.

M5 proxy-transport evidence on Windows/Python 3.12.14: the full offline suite
passed 152 tests with one directory-symlink capability skip. The tests cover
bounded local subprocess output, strict proxy status/payload frames, malformed
and oversized responses, opt-in store route selection, hash-checked proxy
upload, and route persistence in a detached collector. No provider account,
credentials, or SSH endpoint was contacted. A fresh no-dependency wheel
`0.2.0.dev0` installed in a new venv outside the checkout and imported the proxy
session/fetcher, route endpoint, and detached collector; wheel SHA-256:
`3ca89fe4684bff9f59084f1c5574dee3254b07c43dbed47abbaabdc2fb54d54e`. The wheel
manifest contained only package modules and standard distribution metadata.
This validates the local artifact and fake path, not live provider proxy
compatibility or remote shell behavior.

Chunk-transfer follow-up (2026-11-13): large proxy reads now verify remote
file identity and SHA-256 before and across bounded chunks; direct and proxy
uploads stream reviewed sources, verify the published digest and size, and
publish by atomic rename. Upload health checks run between chunks against the
supervisor and immutable attempt deadline. The full offline suite
passed 195 tests with one platform-capability skip, including regressions
that abort chunked direct upload when its health check fails, stop the serial
engine before workload launch when supervision exits during upload, and stop
the workload when its collector exits. The supervisor-loss case restarts
supervision once for cleanup; both integrated failures confirm exact fake-Pod
termination. A successful attempt also verifies that the remote workload
timeout is clamped to the five seconds remaining before its attempt deadline.
The generated remote
Python bodies were syntax-checked and transfer behavior used local fake SSH
runners only. No provider account, credentials, SSH endpoint, consumer source,
package pin, state or resource was contacted or changed. This still does not
qualify remote shell behavior, live proxy compatibility or interrupted-transfer
resume. Wheel `0.2.0.dev0` built from commit `c0f9e64` installed in a fresh
temporary venv outside the checkout; proxy session/fetcher, serial engine and
uploader imports succeeded, and the installed supervisor CLI displayed its
options. The follow-up wheel built from the current source also installed and
passed the same imports and CLI check; SHA-256:
`c1697f880f56586eae39d7714cc7d381c9f12de8e1bab091f3809ec538640bf4`.
Consumer disposition: no source, package pin, state, or live process changed.
Hybrid's binary interactive flow and MIAP's batch orchestration remain on their
current paths; Partition can evaluate the finite proxy transfer capability while
keeping service framing and workload acceptance in its adapter. All consumers
still need their own acceptance checks and exact artifact pins before adoption.

### M6.1: bounded serial coordinator slice (incomplete)

The design branch now includes a store-backed `SerialExecutionEngine` for one
reserved attempt, a finite job retry loop, typed durable stage events, and
controller-restart cleanup that never replays an interrupted workload. Retries
require an allowed typed failure, a terminal prior attempt, and confirmed or
not-required cleanup. The caller reserves the next immutable attempt after a
fresh quote; store accounting remains the guard for remaining cost and slots.
The detached collector now receives each result's declared byte ceiling and
the plan's per-file and aggregate transfer limits. Direct and proxy fetchers
enforce those limits while reading. Recovery relies on the already running
independent supervisor; it leaves cleanup pending if that supervisor is stale.

On Windows with Python 3.12.14, the full offline suite passed 158
tests with one directory-symlink capability skip. New fake tests cover a
complete CPU attempt and provider-confirmed cleanup, safe retry gating and
restart cleanup without workload replay. A wheel built from this revision
installed in a new venv outside the checkout; imports for the serial engine and
collector succeeded, and the installed collector CLI displayed its limits
options. Wheel SHA-256:
`a3336edf22abd5da6ceb78b6ce78137bfa04aabf5975a0afcaf3ec748512ed06`. No
RunPod account, credential, SSH endpoint or resource was contacted.

This is a validated implementation slice, not M6 completion. A successful GPU
attempt remains blocked by the missing independent per-card VRAM runtime
evidence seam; recovery when the supervisor itself has died is deliberately
manual; scientific output verification, secret delivery and consumer-specific
approval integration remain open. Do not update consumer pins until their
separate acceptance gates pass.

### M6.2: GPU evidence contract foundation (not an admission path)

`runpod_experiments.gpu_runtime` now defines a strict local parser and typed
attestation envelope for bounded `nvidia-smi` output. It requires a unique
per-card UUID, name and memory value, checks card count and per-card VRAM against
the selected profile, and checks the CUDA UMD driver floor. The evidence binds
the immutable attempt, plan, selected profile, exact Pod identity, SSH
known-hosts policy digest and fixed probe specification. It retains only a hash
of the raw outputs. Its pure tests use synthetic output and never invoke SSH or
provider APIs. The full offline suite passed 165 tests with one existing
directory-symlink capability skip. A wheel installed into a fresh local venv;
the 40-entry wheel SHA-256 was
`ac4a71a2b78458a3fc47131433361c74ebe327e8ba3660222756f04ee83ee041`.

The selected profile's integer memory floor follows RunPod's GPU catalog `GB`
values. The probe normalizes `nvidia-smi` MiB to bytes and compares against
decimal GB (1,000,000,000 bytes), which accepts a nominal 48 GB card reported at
about 46,000 MiB. [RunPod's GPU type reference](https://github.com/runpod/docs/blob/main/references/gpu-types.mdx)
labels each model's memory in GB.

The CUDA UMD version is the latest CUDA version the installed driver supports;
it does not prove which CUDA toolkit a workload has installed. Workload images
and application runtimes remain a consumer acceptance concern. NVIDIA documents
the query fields and this driver/toolkit distinction in its [nvidia-smi
reference](https://docs.nvidia.com/deploy/nvidia-smi/index.html) and
[CUDA driver compatibility matrix](https://docs.nvidia.com/datacenter/tesla/drivers/latest/cuda-toolkit-driver-and-architecture-matrix.html).

This contract is deliberately not wired into SSH endpoint resolution or the
cleanup supervisor. The existing GPU guard still fails closed. M6.3 must add a
fixed read-only direct-SSH probe owned by the detached supervisor, a persisted
first-RUNNING evidence deadline, hash-chain binding, strict evidence reload on
every GPU SSH operation and bounded cleanup when proof is missing or invalid.
Only then can a complete fake GPU attempt pass. Local proof is an integrity
guard for cooperating processes, not provider-signed hardware attestation or a
security boundary against a hostile local account.
No consumer code, pin or private run state changed in this foundation slice;
Hybrid's current flow is unchanged, and MIAP and Partition remain unintegrated.

### M6.3: supervisor-owned GPU evidence path (implementation slice)

The detached store supervisor now runs a fixed, bounded, read-only direct SSH
probe for GPU attempts. SSH config and forwarding are disabled, host-key checks
are strict, local output is capped, both local and remote commands have finite
timeouts, and only the key-file path is passed to the supervisor process. The
query, time bounds, selected profile, immutable plan/attempt, exact Pod and
known-hosts policy digest are included in the evidence contract. No raw GPU
output or SSH diagnostics are persisted.

The first valid RUNNING observation starts a durable 300-second proof window,
capped by the attempt deadline. A restart reuses that timestamp. Unavailable
SSH waits only inside the window; malformed evidence, profile mismatch, changed
host-key policy, corrupt sidecar or event-chain mismatch fails closed and asks
the supervisor to clean up the exact attempt. The sidecar is atomically written
under the attempt's supervisor directory and its digest is bound into the
store's stage-event chain. Store-backed upload, workload, interactive SSH and
collection paths recheck the attestation and current known-hosts digest before
resolving a GPU endpoint. `SerialExecutionEngine` waits for that evidence before
upload, checks the live supervisor heartbeat, and passes only credential paths
again if cleanup needs one supervisor restart. CPU behavior is unchanged.

Synthetic tests cover the complete fake GPU attempt, success, low VRAM, timeout
and restart handling, host-key changes, tampered evidence, strict SSH command
construction, and the pre-upload engine gate. This evidence does not qualify a
real RunPod image, SSH endpoint, provider billing behavior, or actual hardware;
the local hash chain is not provider-signed or resistant to a hostile local
account. The API remains internal and unreleased. Hybrid, MIAP and Partition
source, pins and state were not changed; all three remain unintegrated and must
run their own acceptance gates before cutover. M6 as a whole remains open for
failure/restart matrix completion, API/package review and consumer acceptance.

On Windows with Python 3.12.14, the full offline suite passed 181 tests with one
existing directory-symlink capability skip; the final focused GPU probe/runtime/
supervision suite passed 23 tests. A fresh offline venv installed the 41-entry
candidate wheel and imported the GPU probe, serial engine and supervisor from
outside the checkout; the installed supervisor CLI displayed its options. Wheel
SHA-256: `8082e431168b76709884c38063bdac02ddc74dbb3e6da4e962c56071e9dc15ed`.
These checks validate only local package contents and synthetic behavior.

M6 fault-matrix review (2026-11-13): upload health-check abort, supervisor exit
during upload, collector exit during workload, and remaining-time clamping now
have regression coverage. An uploader returning after its attempt deadline is
rejected before collection or workload launch, and a running workload is
stopped when the attempt deadline expires. Controller restart from intermediate
upload/collector stages remains open. This remains M6 local test work before
any consumer pin changes; package artifact import and CLI checks have passed
for the current internal wheel, while stable API review remains open.

Project-owned transfer settings (2026-11-13): `AttemptWorkload` now carries
bounded `upload_timeout_seconds`, `upload_chunk_bytes`,
`collection_timeout_seconds`, and `collection_chunk_bytes`. Timeouts are capped
at 3600 seconds and chunks at 1,000,000 bytes. The upload timeout bounds one
reviewed bundle transfer; the collection timeout bounds each file retrieval and
is clamped to the immutable attempt deadline. Detached collector settings are
persisted alongside byte limits and checked against its launch arguments. This
allows Partition to supply its existing 1200/600-second windows and 256 KiB
chunks without changing common defaults. Focused upload/proxy/collector/engine
tests passed (31 tests); the full suite passed 196 tests with one existing
platform-capability skip. A fresh isolated wheel installed and imported the
transfer settings, serial engine and collector, and its collector CLI displayed
the new options. Wheel `0.2.0.dev0` SHA-256:
`285cc8eedd1b78d687a33762cb6982a5691a360075cc21bfec1f5fc9a8d4fc6b`.
No consumer source, pin, state, provider account, credential, SSH endpoint or
resource was touched. M9 still needs consumer archive-contract, service-role,
cleanup and CPU/GPU acceptance; this adapter contract alone does not qualify
Partition for cutover.

### Store-backed one-shot create slice (internal; qualification remains open)

The current planning slice advances the canonical contract envelope to v3 and
the local execution-store schema to v4. `ExecutionPlan` carries a typed,
non-secret `PodProvisioningSpec` with a digest-pinned image, explicit ports and
reviewed Secure/SSH settings. CPU request payloads include the selected flavor
and vCPU count. GPU payloads express per-GPU host RAM/vCPU minima derived from
the total Pod profile and bind the requested CUDA floor; VRAM remains a separate
profile requirement because the provider's Pod detail reports host RAM there.
Each `AttemptPlan` carries the exact normalized `QuoteObservation`, including
CPU host RAM or GPU VRAM-per-card evidence and the CUDA floor. Its digest,
selected resource, units, hourly total and region are checked together.
Reservation rebuilds the allowlisted CPU/GPU Pod
request from those immutable records and rejects a request digest mismatch
before writing any reservation. The store can rebuild that same request after
restart only after verifying its stored documents and lifecycle projection.

This is a breaking internal schema change. Existing non-empty contract documents
and store databases are not migrated; the store refuses any non-current schema.
`StoreCreateController` now performs a bounded preflight: it verifies a separate
ready supervisor and heartbeat, checks account inventory for project/name
conflicts, confirms the local public-key fingerprint is registered, refreshes
the selected resource quote and region, persists hash-bound evidence, and
consumes one durable claim before sending at most one create request. It
rechecks the supervisor after claiming. A timeout or malformed response holds
the exposure for reconciliation; this path never retries or deletes an unknown
resource. The request contract deliberately has no arbitrary fields, mounts or
secret values; extensions require an explicit schema change and tests.

This remains an internal component, not the M6 serial engine or a consumer API.
Provider payload and account response qualification, GPU runtime VRAM
conformance, secret delivery, restart recovery, and owner-specific approval
integration remain open. The offline tests use fake providers and do not call
RunPod. Existing non-empty v3 store databases and v2 contract documents have no
migration path; consumers remain on their existing package pins until the
separate migration gates pass.

On Windows with Python 3.12.14, the full offline suite passed 146 tests with one
directory-symlink capability skip. A fresh no-dependency venv installed the
candidate wheel `0.2.0.dev0` (SHA-256
`57dfdbde0427718fd41ca145932635a5e049c9e9a978c71bb075800690fd4c1f`); from
outside the checkout, the installed artifact imported contract schema 3, store
schema 4, the create controller, and the store-backed SSH/collector helpers.
CPU/GPU fake-provider create paths, guarded CPU transport operations, shape
rejection and CPU reconstruction from a reopened store are covered by the full
suite. These were local synthetic checks only; no RunPod credentials, account
endpoints or resources were used.

## Consumer migrations

Read-only source audit on 2026-11-12 found Hybrid scripts still import
`runpod_experiments` by adding `../runpod-experiments-system` to `sys.path`; its
revision file records the sibling checkout rather than an installed package.
The searched MIAP and partitioned source trees do not yet reference this package
or `StateStoreConfig`. This does not establish external environment variables,
ACLs or deployed runtime settings. All three consumers still need explicit
installed-version pins and project-specific state configuration during their
separate migrations.

Each consumer receives an installed immutable package pin, a small workload/policy
adapter, conformance tests, an operator runbook, an upgrade record, updated agent
instructions and a retirement inventory. No runtime dependency on a sibling
checkout remains at completion. Scientific dependencies stay in the consumer.

| Consumer | Preserve in the project | Replace with shared behavior | Required cutover demonstration |
| --- | --- | --- | --- |
| Hybrid | Trusted-client protocol, key boundary, scientific source/export and result checks | Sibling-path imports, repeated generic runner stages, large-file transport and lifecycle wiring | Installed supervisor imports correct artifact; interactive streams, failures, hashes and cleanup retain behavior |
| MIAP | Batch schedule, approved objective/disclosures, model eligibility, scientific block verification and final analysis | Generic API/quote, ledger arithmetic, attempt/create guards, host/watchdog launch, SSH mechanics and operational retries | Interrupted multi-block fake run resumes verified outputs once; missing blocks cannot finalize; all attempts retain cumulative accounting |
| Partition serial | Workload/service roles, protocol/data boundary and numerical checks | Historical generic transport/controller inheritance, CPU/GPU selection mechanics, accounting and serial lifecycle | CPU/GPU fake paths, proxy archive transfer and independent service processes work with exact cleanup |
| Partition parallel | Approved workload grouping and scientific scheduling | Parallel coordinator/admission, parent/child accounting and watchdog plumbing | Competing reservations never exceed configured slots, total/hourly caps or exact identities |

Before each cutover, inventory every current importer, launch command, critical
source binding, namespace contract copy and host deployment instruction. Classify
each item as retained project behavior, migrated common behavior or historical
only, with a replacement and deletion condition. Use the local audit for the
initial concrete file list; re-read current source before editing it.

Existing agent instructions mandate particular guarded clients. Update those
instructions and their source bindings deliberately when the shared replacement
passes the same guards; do not bypass them during development. Install the
consumer maintenance rules from CONTRIBUTING.md at this step. Pending charges
remain attached to the original approval. For a continuing envelope, reconcile
all commitments before using an explicit state bridge; no import may reset a cap.

### Compatibility audit follow-up (2026-11-13)

The read-only review confirms that the current shared package is a useful base,
but the consumer cutovers still have concrete contract gaps:

- Hybrid's binary SSH transport can use an `ExecutionStore` attempt. The design
  branch now includes an internal `InteractiveDriver` interface that runs a
  bounded client-controlled request/response session under the serial engine's
  supervisor and cleanup path. M7 still needs Hybrid conformance for its
  protocol, failure behavior, hashes, and scientific checks; those remain in
  Hybrid.
- MIAP can delegate provider, attempt, SSH, and cleanup mechanics. Its exact
  one-use approval receipt, source/host binding, credential-delivery decision,
  and scientific acceptance remain in its adapter. Approval or accounting
  state must not cross project boundaries.
- Partition's serial path can map to the shared CPU/GPU attempt lifecycle, and
  the design branch now has bounded digest-checked chunk transfer over proxy
  SSH. It still cannot resume an interrupted file transfer. Its current study
  uses 256 KiB archive chunks and a 600-second collection window. The serial
  workload adapter now accepts bounded project-owned upload/collection timeouts
  and chunk sizes, and the detached collector persists those settings and clamps
  each fetch to the immutable attempt deadline. This closes the generic
  parameter-plumbing gap; M9 still needs Partition conformance for its archive
  contract, service roles, exact cleanup and full CPU/GPU acceptance before
  migration.
- Partition's two-resource dispatcher still requires M10's atomic parent/child
  caps, slot admission, and independent per-resource supervision; serial
  parity alone is not evidence for this cutover.

These findings came from source inspection and local fake-path coverage only.
No consumer source, state, package pin, or provider resource was changed. The
interactive and chunk-transfer interfaces have local coverage; complete the
shared M6 contract and fault-injection gates before consumer migration. A
consumer cutover also requires exact reconciliation of its existing attempts,
controllers, and pending accounting in that project's own namespace.

## M10 and M11: parallelism and completion

Implement the two-resource profile only after serial recovery works. Reserve
parent/child limits and slots atomically across processes, recheck unreconciled
provider resources, and give every resource independent supervision. Several
roles on one Pod remain a workload choice rather than an instance allowance.

Execute the final acceptance matrix in [testing](testing-strategy.md). Qualify live
CPU/GPU and proxy capabilities only within applicable finite authorizations; a
local fake-provider pass does not qualify the live path. If a particular live
gate awaits approval, record exactly that gate rather than claiming completion.

Retire duplication after all active callers and deployment/source bindings have
moved. Historical reports, receipts and commit history remain reproducible. Keep
rollback artifacts and instructions, but never run old and new controllers against
the same job. Refresh prior evidence after any material correction.

## Releases and external sharing

Proposed sequencing is a 0.2 development release after M6, a reviewed serial
integration release after M9, and a later parallel-capable release after M10/M11.
These are planning labels; choose exact versions from the actual compatibility
changes. Do not change the current package version until a release is prepared.

Before claiming a stable external release:

- Define supported public APIs, schemas, upgrade/deprecation policy and platform
  capability matrix. Advertise tested support separately from intended support.
- Test an sdist and wheel installed in clean environments; inspect their entire
  file manifests and hashes. Exclude private consumer source/data/state and keys.
- Add CI using synthetic fixtures on the supported Python/OS matrix. Consumer
  private acceptance stays in its own permitted environment.
- Maintain change notes, examples, operator/recovery runbooks and a private
  security-reporting route. Avoid automatic external telemetry.
- Obtain the owner's license/contributor-ownership and publication decisions.
  Those decisions do not block local engineering, but must precede an external
  release that promises reuse rights or a new publication channel.

## Completion record

For every milestone record commit/artifact digests, supported capabilities,
tests/results, known limits, remaining callers and rollback state. For provider
qualification additionally record approval/attempt/provider references privately,
actual versus pending costs and termination evidence. Mark the migration complete
only when all three consumer entry points use the package and their generic
operational duplication has been retired under these gates.

M0 evidence: documentation review and provenance recorded in design commit
`3a13149`; no source or consumer behavior changed. M1 validation: 21 focused
contract tests and 68 full-suite tests pass; no provider access or paid resource
was used. The source checkout fix is commit `54aedf4`; the M1 contracts and
offline harness are in commit `b399a2a`. The main shared-package checkout remains
pinned and unchanged.
