# Evidence audit and remaining implementation plan

Audit date: 2026-11-13. The CPU/GPU resource-kind change is at `8654afe` on
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
| Store/admission | `tests/test_execution_store.py`: actual SQLite, independent-process cap races, crash rollback, holds, adjustments and event integrity | One configured host; no hierarchical budgets or production legacy-state bridge |
| Supervision | Store tests include a real detached child surviving launcher-parent exit; GPU supervision tests bind runtime evidence before SSH | Not a complete supported-host/interpreter/package qualification matrix |
| Transfers | `tests/test_connections.py`, `tests/test_proxy_ssh.py`, `tests/test_store_create.py`: exact identity, direct/proxy framing, chunks, hashes and bounds | Shared transfers do not resume interrupted files; proxy is finite PTY commands, not binary interactive workload transport |
| Serial engine | `tests/test_store_create.py`, `tests/test_gpu_supervision.py`: complete fake CPU/GPU attempts, finite retries, deadline failures and restart cleanup | No integrated live qualification or consumer acceptance |
| Setup | `tests/test_store_create.py`: invalid process handle, poll error, nonzero exit, timeout, supervisor loss, and terminate/kill failure all fail before collection/workload and still attempt provider cleanup | A local process stop that cannot be confirmed still requires reconciliation |
| Interactive I/O | `tests/test_interactive_workload.py`, `tests/test_interactive_ssh.py`, `tests/test_binary_process.py`: bounded sessions and process I/O | No consumer protocol or scientific-equivalence proof |
| Packaging | Candidate wheel and sdist install outside checkout; the wheel suite passes from site-packages; detached supervisor/collector import policy-bound consumer factories; archive contents were reviewed | Temporary candidates are not release assets; supported-host, ACL, and live provider qualification remain open |
| Consumers | Isolated opt-in adapters in Hybrid, MIAP, and Partition pass focused offline suites against the installed sdist | No active entrypoint or dependency pin adopts the engine; complete consumer lifecycle/batch/service acceptance remains open |

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
| M7–M9 | Isolated adapters exist for Hybrid, MIAP, and Partition; Hybrid's successful interactive path runs through the serial engine; plans now reject mixed CPU/GPU options; no consumer adopts an adapter from an active entrypoint or pin | Complete project-side GPU-need evidence, Hybrid failure/integrity and guarded-provider acceptance, MIAP interrupted-batch/finalizer, and Partition service/archive acceptance; reconcile each consumer before cutover |
| M10 | Hierarchical parallel admission not delivered by the serial store | Atomic parent/child caps, two-slot process races and independent resource cleanup |
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
- The shared package owns reusable provider, ownership, accounting, supervision,
  transport, operational retry/recovery and evidence behavior. Reusable fixes
  belong here; consumers upgrade through pinned artifacts and conformance checks.

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

The current candidate was then installed from the sdist. MIAP and Partition
again passed three adapter tests each, Hybrid passed 14 protocol/watchdog tests,
and all use CPU-only plans. None of these tests maps production GPU need or
replaces consumer-owned approval evidence. They still do not constitute MIAP
batch or Partition service/archive acceptance, nor live qualification.
