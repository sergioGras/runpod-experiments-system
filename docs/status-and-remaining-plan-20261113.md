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
| Transfers | `tests/test_connections.py`, `tests/test_proxy_ssh.py`, `tests/test_store_create.py`: exact identity, direct/proxy framing, chunks, hashes and bounds | Shared transfers do not resume interrupted files; proxy is finite PTY commands, not binary interactive workload transport |
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

### Immediate implementation slice after this audit

Source review at `5fec8af` identified a concrete setup-cleanup risk:
`run_attempt` calls `setup_process.poll()` outside its cleanup error guard in
`finally`. A poll exception, or a launcher result without a callable poll method,
could escape before `_cleanup` requested provider termination. This was fixed in
`runpod_experiments/serial_engine.py`; failure is retained and the independent
provider cleanup path still runs. An unconfirmed local process stop escalates
the outcome to reconciliation required.

The next focused milestone should:

1. Reproduce invalid setup handles and poll errors with a fake provider, and
   ensure local setup-process inspection/termination errors cannot bypass the
   independent provider cleanup path. Preserve the original setup failure and
   report an unconfirmed local stop as reconciliation required.
2. Exercise nonzero setup exit, configured setup timeout and supervisor loss
   during setup. Assert that collector/workload launch never follows failure,
   exact provider cleanup runs, and accounting exposure remains pending until
   separately reconciled.
3. Exercise terminate/wait/kill failures without suppressing provider cleanup.
   Keep every wait finite and report unconfirmed cleanup honestly.
4. Add deterministic WAL retry exhaustion and SQLite error-code cases, including
   extended busy/locked codes and immediate rejection of unrelated codes.
5. Run focused regressions and the relevant store/engine suites. A source fix
   then warrants the broader offline suite and a fresh installed artifact check;
   record the actual results before committing the completed milestone.

Setup cleanup milestone (2026-11-13): the regression suite now covers invalid
process handles, poll errors, nonzero setup exit, configured timeout, supervisor
loss, and terminate/kill failure. All seven setup tests passed. They verify the
collector and workload do not start after setup failure and provider cleanup is
still attempted; an unconfirmed local process stop is reported for
reconciliation. WAL tests cover transient text locks, extended
`SQLITE_BUSY_SNAPSHOT`, non-retryable I/O errors and deadline exhaustion. The
full suite passed 206 tests with one platform-capability skip before the
additional invalid-handle regression was added; all seven focused setup tests
passed afterward. A fresh wheel
installed into a clean venv; execution-store and serial-engine imports were
asserted to originate under that venv's `site-packages`, and the collector CLI
loaded. Wheel `0.2.0.dev0` SHA-256:
`61fb65f5a349440b8c5d1acea61a25ce3e73c7eccfbc73383bbb4114e90dfd9d`.

This milestone changes common behavior only. Consumer adapters can be prepared
in isolated checkouts after these cleanup guarantees are established; production
cutovers still require reconciliation and consumer acceptance.

| Order | Work and deliverable | Completion criterion |
| --- | --- | --- |
| 1 | Finish the common fault matrix. Map requirements to concrete tests; add setup nonzero/timeout/supervisor-loss cases, termination failure handling and bounded WAL exhaustion/error-code cases. Review remaining clock/reboot, collector and recovery scenarios. | Each required scenario has a passing test or a named unsupported capability; failure never launches the next stage or bypasses cleanup/accounting. |
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
