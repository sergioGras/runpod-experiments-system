# Evidence audit and remaining implementation plan

Audit date: 2026-11-13. Shared implementation reviewed at
`03525440626a875a8de71bbc9cf30c4d39dc7e43` on `design/unified-execution`.
This audit adds documentation, not a production cutover. Earlier milestone
paragraphs describe their source snapshots; this report reconciles their current
status. No provider call or consumer modification was part of this audit.

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
| Transfers | `tests/test_connections.py`, `tests/test_proxy_ssh.py`, `tests/test_store_create.py`: exact identity, direct/proxy framing, chunks, hashes and bounds | Isolated candidate adds offline-verified resume for direct uploads; proxy resume and all live transport behavior remain unqualified |
| Serial engine | `tests/test_store_create.py`, `tests/test_gpu_supervision.py`: complete fake CPU/GPU attempts, finite retries, deadline failures and restart cleanup | No integrated live qualification or consumer acceptance |
| Setup | Success before workload, timeout clamping and interrupted-stage recovery are covered in store-create tests | Dedicated nonzero-exit, configured-timeout, supervisor-loss and process-stop-failure regressions are not established by those success/restart cases |
| Interactive I/O | `tests/test_interactive_workload.py`, `tests/test_interactive_ssh.py`, `tests/test_binary_process.py`: bounded sessions and process I/O | No consumer protocol or scientific-equivalence proof |
| Packaging | Candidate wheel installs and imports outside checkout, with collector CLI loading from that environment | No complete wheel/sdist release inspection or full detached-host deployment qualification for this artifact |
| Consumers | Read-only source review identifies adapters and retained project responsibilities | None has adopted the new serial engine |

The latest recorded full offline run passed **200 tests, with one skipped**
Windows directory-symlink capability case, on Python 3.12.14. This was the
implementation run preceding this documentation audit; it was not repeated for
documentation changes. An earlier run exposed a fresh-database WAL transition
lock. Commit `0352544` added bounded busy/locked retry; its focused regression,
cross-process admission test and full suite passed. Timeout exhaustion and
SQLite error-code branches still warrant explicit adversarial coverage.

Candidate wheel: `runpod_experiments_system-0.2.0.dev0-py3-none-any.whl`.
SHA-256: `7f4b741f878af57498824033fc54be67f9c51485bf018384097073801b8a75dc`.
During this audit, the existing isolated installation was checked from outside
the checkout using Python `-I`: execution-store and serial-engine module paths
were asserted to reside below the venv's `site-packages`; the setup field and
installed collector CLI were checked, and the wheel hash was verified.
Earlier import checks run from the checkout alone cannot establish installed
module origin. Temporary local wheel storage is not a durable release artifact.

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
| M5–M6 | Internal serial CPU/GPU/interactive implementation verified in substantial fake paths | Close enumerated fault gaps, credential requirements, artifact and API review; then consumer and authorized live acceptance |
| M7–M9 | Consumers not migrated | Hybrid, then MIAP, then Partition serial adapter acceptance and reconciled cutovers |
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

## Isolated direct-resume candidate — 2026-11-13

Commit `6296795` on branch `codex/resumable-transfer`, based on shared design
commit `32b1b90`, prototypes resumable direct SSH uploads. A per-file partial
name is bound to the reviewed SHA-256. Each invocation probes the remote
destination and prefix,
compares the prefix hash with the local source, and resumes at the verified
offset. A corrupt prefix resets; an oversized prefix is rejected. Publication
uses a same-directory hard link for no-clobber atomicity, and a lost publication
acknowledgement is reconciled by checking the exact destination size and hash.
The behavior now applies to small and chunked files on the direct route. The
proxy route is unchanged.

Offline evidence: `tests/test_upload_resume.py` runs the actual receiver shell
commands through Git Bash with local temporary files and no SSH/provider
networking. Seven cases pass, including a real partial write, corrupt and
oversized partials, mismatched destination protection, idempotent completed
files, lost publish acknowledgement and transfer-guard stopping. Also passed
`test_connections.py` (14 tests) and `test_store_create.py` (32 tests) on
Python 3.12.10. These checks do not qualify SSH, RunPod's remote shell, transfer
latency or billing.

Before rebasing onto `32b1b90`, a broad run on base `508c181` reported 214 tests,
one skipped, and one failed test with a cleanup error. The detached-supervisor
test relies on a temporary `sitecustomize.py` loaded through `PYTHONPATH`; this
workstation's embeddable Python
`python312._pth` ignores that variable in child interpreters. The test child
therefore missed its fake provider opener and remained in stopping/unresolved
state after provider-read errors. No provider create or model transfer was
performed. The runtime configuration was restored. After rebasing onto
`32b1b90`, the focused set passed 78 tests: direct-resume receiver (7),
connections (14), store-create (32), host identity (5), persisted
attempts/deadlines (14) and launch (6). A filtered current-source run discovered
223 tests, excluded only the detached-supervisor test above, and ran 222 tests
successfully with one platform-capability skip. The full unfiltered suite still
needs that detached test run under an interpreter that loads its fake provider
hook reliably before treating this branch as fully qualified.

Consumer disposition: none adopts this branch. Hybrid retains its current
resumable uploader and pinned shared revision; MIAP and Partition require their
own direct-route conformance checks before any pin change. Same-Pod partials do
not survive Pod replacement. No installed artifact, live SSH test, proxy resume,
release or consumer cutover is established.
