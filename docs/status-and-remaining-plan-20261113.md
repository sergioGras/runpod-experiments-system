# Evidence audit and remaining implementation plan

Current audit date: 2026-11-22. The CPU/GPU resource-kind change is at `8654afe` on
`design/unified-execution`; following commits update this evidence report.
This report records offline implementation and preview evidence, not a
production cutover. Earlier milestone paragraphs describe their source
snapshots; later follow-up sections supersede their interim test and artifact
counts. No provider call or paid run was part of this audit.

## Evidence standard

Implemented means source exists. Offline verified means a named synthetic or
local process test has passed. Installed verified means imports were checked
outside the source checkout. Live qualified means the exact candidate has
passed the authorized provider/host scenario. Adopted means a consumer has pinned
that candidate and passed its own acceptance and reconciliation gates.
These statuses are separate; a test count alone does not establish coverage.

## What the current evidence establishes

| Area | Evidence | Limit |
| --- | --- | --- |
| Contracts | `tests/test_contracts.py`: immutable specifications, bounded values and serialization | Internal APIs and schemas; compatibility policy still needs release review |
| Provider boundary | `tests/test_provider.py`: synthetic inventory, quotes, billing and sanitized errors | Account responses, delayed billing finality and current API compatibility unqualified |
| Store/admission | `tests/test_execution_store.py`: actual SQLite, independent-process serial/group cap races, crash rollback, holds, adjustments, immutable parent/child admission and explicit v4 migration; two-child fake-provider execution confirms independent watchdogs and exact cleanup | One configured host; private-store migration and cross-host coordination remain unverified |
| Supervision | Store tests include a real detached child surviving launcher-parent exit; GPU supervision tests bind runtime evidence before SSH | Not a complete supported-host/interpreter/package qualification matrix |
| Transfers | `tests/test_connections.py`, `tests/test_proxy_ssh.py`, `tests/test_store_create.py`: exact identity, direct/proxy framing, chunks, hashes and bounds | Shared transfers do not resume interrupted files; proxy is finite PTY commands, not binary interactive workload transport |
| Serial engine | `tests/test_store_create.py`, `tests/test_gpu_supervision.py`: complete fake CPU/GPU attempts, finite retries, deadline failures and restart cleanup | No integrated live qualification or consumer acceptance |
| Setup | `tests/test_store_create.py`: invalid process handle, poll error, nonzero exit, timeout, supervisor loss, and terminate/kill failure all fail before collection/workload and still attempt provider cleanup | A local process stop that cannot be confirmed still requires reconciliation |
| Interactive I/O | `tests/test_interactive_workload.py`, `tests/test_interactive_ssh.py`, `tests/test_binary_process.py`: bounded sessions and process I/O | No consumer protocol or scientific-equivalence proof |
| Packaging | Candidate wheel and sdist install outside checkout; installed contract/admission tests and focused consumer slices pass; detached supervisor/collector import policy-bound consumer factories; archive contents were reviewed | Temporary candidates are not release assets; supported-host, ACL, and live provider qualification remain open |
| Consumers | Isolated opt-in adapters in Hybrid, MIAP, and Partition pass focused offline acceptance against installed artifacts; Partition now maps P020 parent/child limits to shared admission and tests same-group guarded creation | No active entrypoint or dependency pin adopts the engine; MIAP active-Pod restart/credential delivery, Hybrid phase mapping, and Partition's custom worker lifecycle and coordinator cutover remain open |

M10 parent/child admission follow-up (2026-11-22; local development candidate,
not a release):

- The full source suite passed **239 tests, with one Windows directory-symlink
  capability skip**. The installed wheel and sdist each passed all 28 contract
  tests and all 11 `AdmissionGroupTests`, including independent-process races
  for the final parent slot and total cap.
- Candidate wheel: 131,107 bytes; SHA-256
  `6d4de2f29f768265914b6da9d616a452f24a01224f4ea37ac1aa30abf80c0b71`.
  Candidate sdist: 181,843 bytes; SHA-256
  `25906f3ee3457f56609b5e362801425045cecb47f3814cf85ab67e755faba626`.
  Both archives were inspected; the sdist installed into a temporary target and
  imported from that installed location.
- Against the installed candidate wheel, the focused adapter slices passed:
  MIAP **7**, Partition **14**, and Hybrid **28** tests. Hybrid's test-only
  detached-supervisor fixture was aligned to the shared bootstrap/provider
  protocol; consumer runtime code and active package pins were not changed.
- At that initial checkpoint, still open: a complete two-child fake-provider
  run proving independent watchdogs and exact cleanup, Partition P020
  parent/child adapter parity, all
  consumer-specific lifecycle gates, live qualification and cutover. No provider
  call or paid resource was used.

M10 interim follow-up validation (2026-11-22; superseded by final artifact revalidation below):

- After fixing project-wide create serialization for verified parent-group
  siblings, the full source suite passed **241 tests, with one Windows
  directory-symlink capability skip**. Five repeated two-child lifecycle runs
  also passed; one child failed at the workload boundary while its sibling
  completed and both exact Pods were independently stopped and confirmed.
- The final preview wheel is 132,070 bytes, SHA-256
  `f64f9a8c9ce9fb11eccecc66c2eb7f2415fa9773a4d480eccdfbbe8e90f5af0e`; the
  sdist is 185,415 bytes, SHA-256
  `2584bbb87e2dee1f5ce6710dc0cbf256e3286ae5e578e34156c28fc258cab6be`. The
  archives contain 47 and 78 entries respectively; the wheel and sdist each
  pass 40 installed-artifact contract/admission tests. Their imports were
  verified outside the checkout, and the archive review found no consumer
  source paths.
- Against that installed wheel, focused tests pass MIAP **7**, Partition **18**,
  and Hybrid **28**. Partition's four new synthetic parity tests map one-use
  child approvals to `AdmissionGroupScope`, reserve separate CPU/GPU plans in
  the shared store, allow only an exact active group sibling through its
  project guard, and reject an unknown project Pod before sending a create.
- P020's active dispatcher and SQLite coordinator remain unchanged. Its proxy
  worker/progress/collection lifecycle, ambiguous-create adoption parity,
  shared-store cutover and private-store reconciliation are still open. No
  provider, credential, paid compute or private result was used.

These interim follow-up results supersede the 239-test candidate checkpoint.
The final peer-integrity and artifact revalidation below supersedes its counts,
hashes and installed consumer evidence. Live qualification, schema migration
and consumer cutover remain separate reviewed gates.

M10 final peer-integrity and artifact revalidation (2026-11-22; local
development candidate, not a release):

- The final source suite passed **242 tests, with one Windows directory-symlink
  capability skip**. Same-group create claims now verify every sibling's
  lifecycle projection before admitting concurrent claims; a corrupt peer
  projection fails closed. The two-child fake-provider lifecycle passed five
  repeated runs, including independent watchdogs, exact cleanup and sibling
  failure isolation.
- Final wheel: 132,162 bytes, SHA-256
  `63447bdf948e4498fe51d9eee507656e38d8eb9247669d7aef53215ceca12475`.
  Final sdist: 185,716 bytes, SHA-256
  `93cce134c9c971f4434e6d346121955e1e1c09e3071b7a3707d38bd7d5fe50ae`.
  The archives contain 47 and 78 entries. The wheel and sdist were installed
  outside the checkout and each passed all 41 installed-artifact contract and
  admission tests. Archive review confirmed the parallel engine is packaged,
  the sdist includes the source tests, and neither artifact contains consumer
  checkout paths.
- Against the final installed wheel, the focused consumer slices passed MIAP
  **7**, Partition **18**, and Hybrid **28** tests. Partition's synthetic
  adapter also asserts that the CPU child create request contains no GPU field
  while the GPU child request contains no CPU field.
- P020's active dispatcher and SQLite coordinator remain unchanged. Its proxy
  worker/progress/collection lifecycle, ambiguous-create adoption parity,
  shared-store cutover and private-store reconciliation are still open. No
  provider, credential, paid compute or private result was used.

Live qualification, schema migration and consumer cutover remain separate
reviewed gates.

The earlier candidate passed 226 tests with one Windows directory-symlink
capability skip. It also passed the focused MIAP, Partition, and Hybrid slices
of 72, 42, and 13 tests against the earlier installed sdist. Those counts
predate the resource-kind contract below.

Current CPU/GPU contract candidate (`0.2.0.dev0`; temporary local artifacts,
not release assets):

- Wheel: 125,370 bytes; SHA-256
  `f8be6aac7bdf09efa27f21a7026226050db7b776dd97954884cf9bb018d19b7d`.
- Sdist: 172,813 bytes; SHA-256
  `50a0720cba8972f8d71e0b3ff39094c350c4425eb9dfedbd6ef7cb460eefb22a`.

The full source and installed-wheel suites passed **227 tests, with one
Windows directory-symlink capability skip**. The final wheel rebuild has the
same 46 archive entry names and contents as the fully tested wheel; its archive
hash differs because build metadata is timestamped. The final wheel and the
installed final sdist each passed all 27 contract tests. The final sdist install
also loaded the supervisor CLI. Against the installed final sdist, MIAP and
Partition each passed three adapter tests, and Hybrid passed all 14
protocol/watchdog tests. The archive audit found 46 wheel and 77 sdist entries,
with no consumer repositories or private run artifacts. These remain local
development candidates, not published releases.

The CPU/GPU rule adds no serialized field or schema change. It does change
validation semantics: previously stored plans that mix CPU and GPU options
must be reviewed and split into separate plans before using this candidate.
CPU-only and GPU-only plans retain their existing representations. No active
consumer currently pins this package.

An older production-library smoke or a successful consumer experiment validates
its recorded source and scope. It does not qualify this new engine or transfer
approval to another project.

## Current milestone disposition

| Milestones | Current status | Evidence needed to close |
| --- | --- | --- |
| M0–M1 | Design and specification baseline complete | Re-review changes when public contracts are frozen |
| M2 | Implemented and synthetically verified; qualification open | Reviewed account quote/billing observations and API compatibility |
| M3 | Serial transactional core verified; deployment/migration open | Canonical state root/actual-host/ACL checks and explicit legacy-state handling |
| M4 | Detached process and supervision components verified; host qualification open | Exact installed candidate survives foreground exit on every supported controlling host; failure/reboot behavior recorded |
| M5–M6 | Internal serial CPU/GPU/interactive implementation and enumerated setup/WAL fault paths verified offline; installed wheel/sdist reviewed | Public API/schema, credential-delivery and supported-host review; complete consumer lifecycle acceptance; then authorized live qualification |
| M7–M9 | Isolated adapters exist; Hybrid's interactive path runs through the serial engine; MIAP's synthetic batch, promotion, finalizer and bounded restart mapping and Partition's CPU proxy/archive slice pass offline; CPU/GPU plans cannot mix; no active consumer adopts an adapter | Map actual workload phases and GPU need; finish MIAP active-Pod restart/credential acceptance, Hybrid phase-specific CPU/GPU and failure/integrity parity, Partition supervised worker launch/progress and GPU workload acceptance; reconcile before cutover |
| M10 | Parent admission and bounded parallel runner verified; installed wheel/sdist and all three focused consumer slices pass; Partition P020 admission/API parity has an opt-in synthetic adapter | Map P020's proxy worker/progress/ambiguous-create lifecycle, reconcile private state, complete consumer cutovers, and separately qualify live provider/host behavior |
| M11 | Open | Integrated acceptance, duplication retirement, rollback and release evidence |

## Ordered remaining work

### Completed setup cleanup and WAL slice

Source review at `5fec8af` identified a concrete setup-cleanup risk:
`run_attempt` calls `setup_process.poll()` outside its cleanup error guard in
`finally`. A poll exception, or a launcher result without a callable poll method,
could escape before `_cleanup` requested provider termination. This was fixed in
`runpod_experiments/serial_engine.py`; failure is retained and the independent
provider cleanup path still runs. An unconfirmed local process stop escalates
the outcome to reconciliation required.

All five setup failure paths listed below and deterministic WAL retry exhaustion
are now covered. Setup failures stop before collection/workload launch. The
provider cleanup path still runs when local process polling or stopping fails,
and an unconfirmed local process stop produces a reconciliation-required
outcome. The WAL retry recognizes extended SQLite busy codes and stops at its
configured deadline; unrelated I/O errors fail immediately.

Setup cleanup milestone (2026-11-13): the regression suite covers invalid
process handles, poll errors, nonzero setup exit, configured timeout, supervisor
loss, and terminate/kill failure. All seven focused setup tests passed. They
verify the collector and workload do not start after setup failure and provider
cleanup is still attempted; an unconfirmed local process stop is reported for
reconciliation. WAL tests cover transient text locks, extended
`SQLITE_BUSY_SNAPSHOT`, non-retryable I/O errors and deadline exhaustion. The
full suite passed 216 tests with one platform-capability skip. Wheel and sdist
`0.2.0.dev0` both installed in clean venvs, and the execution-store and
serial-engine imports were asserted to originate under their respective
`site-packages`. The installed `runpod-experiment-supervisor --help` entry point
loaded. The detached-supervisor parent-exit test passed from both installed
environments. The sdist archive audit found only package source, build metadata,
and generic tests; no run data, credentials, weights, checkpoints, or consumer
tracking files were present. SHA-256 hashes:

- Wheel: `7c6f9fc32524450a41a8d68a0e38abfe9d804ac838a217078f7f6a19a5c7cf11`
- Sdist: `fdb83a5221f2df2e50ad130566e832b891f2456608703490a2ba0f90574f97e5`

### Host reboot and deadline slice

Review found that a restarted supervisor rebuilt its monotonic deadline from
current wall time and had no persisted OS boot identity. The supervisor now
reuses its original monotonic deadline only when the persisted attempt, policy
and boot identity still match. A changed boot, invalid prior state or backward
wall-clock movement latches cleanup; unsupported operating systems fail before
provider credentials are loaded. Synthetic tests cover same-boot restart,
reboot, clock rollback and unavailable boot identity. Linux uses the kernel boot
UUID; Windows uses an uptime-derived boot-time estimate with a conservative
five-second comparison tolerance. Other platforms remain unsupported pending a
qualified boot identity source.

The supervisor also takes an OS-released per-attempt process lock to prevent
duplicate monitors. If a create was claimed but supervisor state is missing on
relaunch, it latches stop before provider access. State symlinks fail closed.
These paths are covered by the local suite and real detached-process check.

This closes local deadline-renewal paths. It does not provide provider-side
expiry, survive host power loss, or attest the configured controller-host label
to a physical machine. The Windows identity is an estimate; clock adjustment
between process starts can trigger conservative cleanup. Linux runtime and
host-specific restart behavior still need qualification.

This milestone changes common behavior only. Consumer adapters can be prepared
in isolated checkouts after these cleanup guarantees are established; production
cutovers still require reconciliation and consumer acceptance.

| Order | Work and deliverable | Completion criterion |
| --- | --- | --- |
| 1 | Close the remaining common fault matrix. Setup exit/timeout/supervisor-loss, process-stop failure, bounded WAL exhaustion/error-code coverage, and local deadline/boot-change latching are implemented. Review collector and recovery scenarios and fill any demonstrated gaps. | Each required scenario has a passing test or a named unsupported capability; failure never launches the next stage or bypasses cleanup/accounting. |
| 2 | Freeze a serial integration candidate. Review public vs internal APIs, schemas, direct/proxy launcher capabilities and approved secret-delivery needs. Build and inspect wheel plus sdist, preserve hashes and a durable artifact. | Clean isolated installs and documented module origins; secret values absent from plans/argv/logs/artifacts; supported capabilities and migration rules explicit. |
| 3 | Qualify deployment without paid compute. Exercise the exact installed artifact and interpreter through detached supervision, parent exit, PID and package identity checks. Define canonical project state roots, host identities and ACLs. | Host-specific evidence includes source/artifact/environment and actual child identity; unsupported OS/host combinations remain explicitly unsupported. |
| 4 | Prepare Hybrid adapter and offline acceptance in an isolated checkout. Preserve framing, local trusted-client work, setup verification and scientific acceptance. Replace sibling-path loading with the immutable package pin. | Existing protocol/failure/integrity checks pass through the common engine; deployment/source bindings and rollback are reviewable. Reconcile active legacy controls before changing their runtime or state. |
| 5 | Prepare MIAP adapter and interrupted-batch acceptance. Consumer owns approval conversion, batch/checkpoint identity and final result verification; common engine owns operational attempts, accounting and cleanup. | Verified blocks finalize once, incomplete/duplicate blocks are rejected, restart retains cumulative exposure and scientific failure does not trigger an operational retry. |
| 6 | Prepare Partition serial adapter. Preserve archive verification, workload/service topology and numerical checks; configure CPU/GPU selection and transfer limits through the common package. | Fake CPU/GPU, required proxy transport, service-process failures, archives and exact cleanup pass without a second generic controller. |
| 7 | Complete required live qualification and serial cutovers at reconciled boundaries. First prepare finite proposals with exact export inventories and costs. GET-only compatibility review can be prepared separately; provisioning/disclosure needs applicable approval. | Exact candidate CPU/GPU/transport paths pass, approved artifacts are collected and verified, exact termination is confirmed, charges remain posted or explicitly pending. Consumer records and guards reference the same artifact. |
| 8 | Implement M10 parallel admission centrally, then migrate the dispatcher. | Independent processes cannot exceed parent/child total/hourly caps or slots; one failing resource cannot terminate another or a foreign resource. |
| 9 | Run M11 integrated acceptance and retire duplication. | All consumers pin reviewed artifacts; no active caller uses sibling imports or retired generic controllers; rollback, maintenance records, agent instructions, CI/platform matrix and license/export review pass. |

Steps 1–3 and isolated offline consumer preparation do not require paid
experiments. Existing unresolved provider operations gate live admission and
replacement of their active controls; they do not prevent all independent
engineering. Live and cutover work may reveal new common defects; fix these in
the library with synthetic regressions and revalidate affected consumers.

## Consumer boundaries and maintenance

- Hybrid retains its application protocol, trusted-client computation and result
  verifier. The adapter supplies reviewed setup and interactive workload hooks.
- MIAP retains approved objectives, model/data eligibility, batch scheduling and
  scientific finalization. The adapter must not reset old holds or consumed claims.
- Partition retains workload roles, scientific scheduling and numerical checks.
  Multiple services on one Pod are distinct from multiple admitted Pods.
- Each approved workload phase selects CPU when an accelerator is unnecessary
  and GPU only when its requirements need one. A workflow that changes device
  between phases uses separate single-kind plans; CPU/GPU fallback mixing is
  rejected by the shared contract.
- The shared package owns reusable provider, ownership, accounting, supervision,
  transport, operational retry/recovery and evidence behavior. Reusable fixes
  belong here; consumers upgrade through pinned artifacts and conformance checks.

### M10 source contract and implementation status confirmed on 2026-11-22

Partition's P020 coordinator is a Windows, single-controlling-host SQLite
coordinator. A finite parent approval binds a list of one-use child approvals.
Reservations are serialized with `BEGIN IMMEDIATE`; a competing reservation
cannot take a slot after the parent ceiling is reached. A child commits its
entire `max_total` against the parent on first reservation, including a
recognized no-Pod rejection; the aggregate hourly ceiling is checked against
currently active children. Parent runtime starts on the first reservation and
does not restart for later children. A sent or ambiguous create never frees its
slot optimistically. Lost create responses can be adopted by exact random name
for cleanup only. Each child gets its own watchdog and exact-Pod cleanup.
Failures in one child are caught at its execution boundary so the other child
can complete; sibling or foreign Pods cannot be deleted through that child's
guard.

The shared store already enforces project-wide slots across approvals and
per-approval total/hourly budgets, but has no parent scope binding multiple
approvals. M10 must place the parent and child admission rows in the same
transactional project store as attempt reservations; a second independent
ledger would create a crash window between parent admission and attempt
reservation. Parent total should conservatively commit each child approval's
full ceiling once, while active hourly use should sum selected attempt rates.
Each child attempt retains its own resource policy and watchdog. A group may
contain CPU-only or GPU-only plans according to each approved workload's actual
need; no plan may mix resource kinds, and a CPU-capable phase must not request
a GPU merely because another child requires one.

The shared candidate now defines `AdmissionGroupScope`, binds each child
approval to one parent row, and reserves parent commitment, child attempt,
project/approval/group slots and active hourly exposure in one `BEGIN IMMEDIATE`
transaction. A child's full approval ceiling is committed once. The parent
deadline is fixed by the first reservation and checked again before create.
`ParallelExecutionEngine` runs a bounded wave of already reserved child jobs
through the existing serial engine; each child keeps its own supervisor and an
exception from one child becomes a sanitized outcome without cancelling its
sibling. Child CPU/GPU resource kinds remain separate per plan.

New stores use schema v5. Existing v4 stores still open for serial work and are
not changed on open. `migrate_to_parallel_schema()` is an explicit operation
that adds only the parent tables, preserves serial rows, and refuses active
attempts or incomplete billing. It invalidates old package versions for that
store, so it belongs only at a reviewed, reconciled cutover boundary.

The full shared source regression passes 239 tests with one Windows
directory-symlink capability skip. Installed wheel and sdist each pass 28
contract tests plus 11 admission-group tests, including independent-process
races for the last slot and aggregate commitment, one-use child binding, mixed
CPU/GPU child plans, parent-clock continuity, pre-create deadline recheck, v4
migration preservation/refusal, and sibling exception isolation. Focused
installed-wheel consumer slices pass MIAP 7, Partition 14, and Hybrid 28 tests.
A complete fake-provider run with two independent watchdogs and exact sibling
cleanup, and Partition P020 adapter parity, remain open. The legacy coordinator
tests are evidence of its old contract, not proof of new shared behavior.

No private consumer source, state, scientific outputs or provider identities are
included in this report. Current live reconciliation status must be checked in
each consumer's own records and authorized operational context.

## Follow-up implementation evidence (2026-11-13)

The consumer-bound factory path now applies to the controller, detached
supervisor and collector. Each process reloads the factory from the attempt's
hash-bound `ProjectPolicy`. Client methods are checked by purpose with
least-privilege method sets. `GuardedProjectApiClient` delegates through the
consumer's existing `ProjectPodApi.call`, and the shared run guard binds create
to the exact durable create claim and delete to the exact stopping Pod. This is
a local guard layer, not provider IAM.

Consumer `ProjectPolicy` records also bind bounded absolute module roots.
Controller loading prioritizes the shared package before these roots; detached
supervisor and collector launchers pass them explicitly so a base interpreter
can load repository-local adapters without relying on inherited `PYTHONPATH`.

Backward compatibility was tightened before packaging: the contract envelope
stays at schema v3, both new `ProjectPolicy` fields follow its old positional
fields, and policy payloads from before either factory field are verified
against their original serialized bytes/hash. Restart and typed-plan checks
continue to use the exact approved legacy digest. Regressions cover original,
intermediate and current representations, factory-root import in the controller,
and policy-root import in real detached supervisor and collector processes.

The full shared suite passed **226 tests, one skipped** against the installed
wheel from outside the source checkout. The skip is the Windows directory-
symlink capability case. Detached process cases loaded the shared package
from the installed location and consumer fake factories only from the
policy-bound module roots. A fresh venv installed the sdist, resolved package
modules under its `site-packages`, and loaded the supervisor CLI help.

Built candidate artifacts are temporary local files, not release assets:

- Wheel `runpod_experiments_system-0.2.0.dev0-py3-none-any.whl`, 125,182 bytes,
  SHA-256 `7f8f5be5a2f6aade5944ca9197509b4f1f6227f53af76378c707b5e844d457fa`.
- Sdist `runpod_experiments_system-0.2.0.dev0.tar.gz`, 172,300 bytes,
  SHA-256 `49f88c1717133914d52caaf066edf24dfa8242f9761ff5516aab7a635b56078a`.

The wheel has 46 entries and the sdist 77. Archive review found only shared
package source, build metadata, README and generic synthetic tests. No consumer
repositories, run records, credentials, weights, checkpoints or private results
were included.

Isolated consumer worktrees contain opt-in serial adapters only:

- MIAP: 72 adapter/lifecycle tests passed against the installed sdist. Its
  active driver, approval cards, credential delivery, consumed-attempt ledger,
  watchdog and result finalizer are unchanged. Batch restart/finalizer
  acceptance remains open.
- Partition: 42 adapter/RunPod/P020 controller tests passed against the installed
  sdist. The adapter binds only the legacy serial API. The two-slot P020/M10
  dispatcher, parent/child caps, service roles, archive transport and science
  verifier remain untouched.
- Hybrid: 13 protocol/watchdog tests passed against the installed sdist. Its
  earlier preview commit remains isolated and unpushed; its trusted-client
  computation and result verifier have not been cut over.

These are offline synthetic/fake-path checks against the installed package;
they do not qualify a live provider or deployment. No consumer entry point
imports its adapter, no consumer dependency pin changed, no original checkout
or persisted run state was modified, and no provider call, resource creation,
paid run, upload or disclosure occurred. Live qualification, consumer parity,
deployment checks and release remain open.

Earlier installed-wheel checks (2026-11-13): MIAP and Partition each passed
all three tests in `tests.test_shared_runpod_adapter`. In addition to factory
construction, these exercise the policy-loaded consumer factory and the
consumer's own `ProjectPodApi` with a synthetic durable create claim, denial
before claim, exact Pod deletion after stop, and lifecycle-projection check.
Hybrid's 14 protocol/watchdog tests passed against the earlier installed wheel;
one runs the real Hybrid bridge through `SerialExecutionEngine` with fake setup,
interactive transport and cleanup.

### Consumer acceptance extension — 2026-11-13; follow-up review — 2026-11-22

The current shared sdist candidate (`95d0443`, SHA-256
`50a0720cba8972f8d71e0b3ff39094c350c4425eb9dfedbd6ef7cb460eefb22a`) was used
for additive offline acceptance in the isolated preview branches:

- **Hybrid:** 14 protocol/watchdog checks remain green. The successful
  interactive bridge runs through `SerialExecutionEngine` with synthetic CPU
  setup, transport and cleanup. HYB-005's actual capture phase needs GPU while
  its encrypted-compute phase is CPU; it needs separate single-kind plans. The
  preview has not mapped those phases into one approved workflow.
- **MIAP:** all seven adapter tests pass on the latest installed-wheel run.
  The fixture serially admits five CPU-only synthetic attempts under one
  hash-bound upload manifest. A
  read-only recovery view maps only shared `terminated` attempts into MIAP's
  pending/billed vocabulary and exercises promotion/finalization. Deterministic
  FakeBackend output passes MIAP's actual strict block verifier, worker-summary
  validator, frozen config validator and two-observation absence routine; the
  latter sees only fake empty inventories and exact-ID 404s. Analysis remains
  stubbed because NumPy is unavailable, and synthetic watchdog/provider
  evidence does not validate science or live provider state. The updated seven
  tests pass against a local installed wheel (`3851dbb`, SHA-256
  `21d124523312c788b9d297828614ee21a6b597f3cec8ef9f573ea8efac6f4964`); its
  package code matches the earlier sdist candidate. The shared-engine restart
  cases map an unclaimed reservation to MIAP `void` and keep a claimed create
  unresolved with its exposure held when no Pod/supervisor is confirmed.
  Active-Pod/supervisor restart acceptance and private model-token delivery
  remain open. The active text job remains GPU-required (48 GB and CUDA 12.8
  floors); the CPU fixture does not classify that workload as CPU-capable.
- **Partition:** five adapter tests plus four archive and five proxy regressions
  pass (14 total). The new serial fixture matches P028's CPU3c shape, verifies
  framed proxy chunks and archive/per-file hashes, then confirms exact cleanup.
  P028's actual workload is explicitly CPU-only; GPU-only plans remain available
  for accelerator-required studies. P014/P028 use one Pod per attempt; the
  worker's head evaluator is a nested child process, while its body/tail roles
  execute within the worker. Their legacy controller launches a detached
  bootstrap and polls status/progress. Shared proxy upload and archive collection
  are covered, but the shared engine's supervised workload-launch/progress path
  is not. Four legacy P014 progress regressions also pass for identity,
  retrieval, success forwarding and timeout child reaping (with a NumPy import
  placeholder only); they do not test shared-engine parity. P020's separate
  two-Pod dispatcher and M10 caps are also not covered.

This confirms useful compatibility seams, not production plan/approval mapping.
Each project must bind its own workload requirement and finite approval into
the shared single-kind plan. No consumer entrypoint or dependency pin changed;
the active controllers and local run state remain untouched. No provider call,
Pod mutation, paid run, upload or disclosure occurred. Deployment/live
qualification, interrupted-service parity, reconciled cutover, M10 and release
remain open.
