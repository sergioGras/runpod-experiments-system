# Testing and rollout contract

This is the planned acceptance contract for the [implementation milestones](implementation-plan.md).
Tests accompany implementation; a final integrated phase follows consumer adoption.
Historical test counts apply to their recorded snapshots. The
[current evidence audit](status-and-remaining-plan-20261113.md) distinguishes
new-engine offline evidence from consumer adoption and live qualification.

## Test boundaries

| Level | Purpose | Provider access |
| --- | --- | --- |
| Specification/unit | Validation, policy math, state transitions, sanitized errors | None |
| Component | Real SQLite/processes and fake provider/SSH endpoints | Local controlled fixtures only |
| Engine conformance | Complete synthetic CPU/GPU jobs, failures, retry and recovery | Fake provider only |
| Consumer acceptance | Project workload/approval adapters plus actual common engine | Fake provider by default |
| Installed-artifact/platform | Clean package install, detached supervision and parent-exit behavior | Fake endpoints; no paid resources |
| Live qualification | Authorized CPU/GPU/transport path and provider cleanup | Explicit separately invoked finite scope |

Ordinary discovery and CI must have no provider secrets. The test harness must
prevent live provider construction before credential lookup and propagate that
restriction to child processes. Use isolated credential/home locations, explicit
fake endpoints and network restrictions appropriate to the test host. Patching
one imported factory or a socket in only the parent does not cover subprocesses.
Never use a real account to prove that an unapproved create guard rejects a call.

## Required behavioral matrix

| Contract | Minimum meaningful scenarios |
| --- | --- |
| Validation | Missing/unknown versions, nonfinite numbers, bool limits, duplicate keys, malformed identities/hashes; no side effects on import/validation |
| Ownership | Sibling and unknown resources, name/ID mismatch, duplicate names, renamed Pod, corrupted registry, exact-run mutation and scoped inventory |
| Provider | Malformed/repeated pagination, bounded error bodies, redaction, wrong rate/shape/region/image/mounts, changed quotes and rejected capacity |
| Resource selection | One resource kind per plan; CPU-only plans cannot select GPU profiles, and GPU plans retain VRAM/CUDA/runtime-attestation checks |
| GPU runtime evidence | Malformed/oversized `nvidia-smi` output, duplicate cards, per-card VRAM/count floors, driver CUDA UMD floor and attempt/Pod/profile/host-key binding; synthetic bytes only |
| Create preflight | Current quote and region, registered-key fingerprint, project inventory, supervisor readiness/heartbeat, one durable claim, post-claim supervisor change, ambiguous response with no retry and held exposure |
| Admission | Independent-process races for the last slot/budget, serial limit, two-slot profile, parent/child hourly and total caps, previous unresolved attempts |
| Persistence | Crash/reopen at reservation, claim, response, candidate binding, cleanup and settlement; duplicate events; schema mismatch and failed writes |
| Accounting | Setup/work/cleanup reserves, pending bills, known candidate mismatch, posted/adjusted bills, idempotent settlement, rounding and immutable parent cap |
| Time/host | Wrong host, wall-clock rollback/jump, reboot identity, stale proof, expired technical window and nonrenewable owner deadline |
| Supervisor | Failed launch, actual PID/interpreter, package mismatch, heartbeat delay, foreground/tool exit, transient/persistent telemetry failure and stop latching |
| Reconciliation | Definitive rejection versus timeout, unknown create response, returned candidate ID, repeated detail/list absence and no optimistic release |
| Transport | Direct/proxy host-key behavior, handshake failure, corrupt/truncated/oversized data, symlinks/path escape, stalled input/output, remote/local hash mismatch |
| Credentials | Secret delivered only to approved exact workload, absent from serialized plans/argv/logs/artifacts, sanitized error/stage evidence |
| Workload | Install/start/observe/collect failures, partial evidence, scientific failed verdict, independent cleanup and no automatic scientific retries |
| Restart | Verified output promotion once, completed-block recovery, missing/duplicate outputs, finalization without a new resource, exact prior provenance |
| Parallel cleanup | One resource fails while another continues within policy; deleting one cannot touch the other or a sibling project |
| Package | Wheel/sdist contents, offline install from prepared artifacts, public import examples, supervisor imports the exact installed distribution |

Use property-based generation where it materially probes arithmetic/state
invariants; keep such tools development-only. Do not replace adversarial scenarios
with assertions that merely mirror method implementations. Use cross-process
tests for transactional claims; thread tests alone do not establish that contract.

## Consumer acceptance

Hybrid must demonstrate interactive protocol I/O, large streaming transfer,
local/remote integrity and useful failure evidence through the common engine.
The protocol and numerical verifier remain the project's acceptance authority.

MIAP must demonstrate a synthetic interrupted batch: recovered verified blocks
are promoted once, failed setup consumes the correct allowance, restart preserves
the cap and finalization refuses incomplete outputs. Scientific criteria and
selection do not change during infrastructure migration.

Partition must demonstrate a serial CPU workload, an eligible GPU workload and
its required proxy transfer. Its separate parallel acceptance tests use competing
parent/child reservations and one watchdog per exact resource. Logical role
processes on one host do not count as separate Pods or establish provider isolation.

At every consumer step run relevant existing guards against the replacement and
the new engine conformance suite. Run the consumer's broader acceptance suite
before cutover. Broaden/repeat testing after a new change or unresolved concern;
do not repeatedly run unchanged expensive scientific workloads as a substitute
for a focused regression.

## Final integrated acceptance

1. Build an inspected candidate artifact and pin that exact artifact in all three
   isolated consumer environments. Record revisions, Python/OS and package hashes.
2. Run the complete library suite and relevant consumer acceptance. Include mixed
   fake account inventory so each project remains isolated while others are active.
3. Run installed-package and supervisor parent-exit tests on the actual supported
   controlling hosts. Use temporary test credentials and fake provider endpoints.
4. Complete any required authorized live qualifications. Refresh offers, reconcile
   state before create, collect allowlisted outputs and confirm exact termination.
   Preserve costs as posted or pending; an SSH exit is not termination evidence.
5. Check no production caller still imports duplicated generic infrastructure,
   a mutable sibling checkout or internal compatibility APIs scheduled for removal.
6. Verify instructions, runbooks, source bindings, manifests and rollback artifacts
   match the candidate. Record every failed gate and the correction/retest evidence.

## Cutover and rollback

Perform cutover only at a reconciled boundary with an assigned host and an exact
approved artifact. Archive legacy state read-only; validate any continuing-envelope
bridge before admitting a new attempt. A migration must neither release an
unresolved hold nor charge an unrelated experiment's cap.

If the candidate fails offline acceptance, keep production on its existing pin
and fix the shared package. If it fails after resource admission, finish exact
reconciliation/cleanup before considering rollback. Select the prior verified
artifact explicitly; never activate both controllers or erase failed attempt
history. Removing compatibility code requires every caller to have crossed its
documented gate.

## Evidence and publication

Record scenario IDs, source/artifact/configuration digests, fixture seeds where
applicable, environment, command, outcome and limitations. Keep evidence private
unless reviewed for disclosure. A central regression derived from a private
consumer must use a reviewed synthetic reproduction, not its raw source/data.

The earlier cross-project study ran 47 library tests and 96 selected consumer
tests successfully on Python 3.12.14. Those results belong to their recorded source
snapshots. This design phase does not claim they validate newer consumer commits
or the still-unimplemented common engine.
