# Implementation and consumer migration plan

Status: design prepared; production migration has not begun. Read
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

M0 is the current deliverable. M1 through M11 are planned, not completed. The
order lets consumer experience improve the contracts before the parallel cutover.
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

## M2: provider boundary

Evolve current RunPod transport, registry, planning, resource-policy and ownership
modules behind explicit scoped inventory and guarded-mutation interfaces. Preserve
legacy inventory semantics only in documented temporary facades.

Normalize CPU/GPU offers and observed shape/rate; retain raw provider responses
only in private evidence storage when permitted. Add bounded sanitized error
categories and exact-Pod billing records/adjustments. Verified rejection evidence
must be separate from generic HTTP errors. Disable automatic create retries.
Prevent public workload hooks from reaching raw mutation transport.

## M3: durable state and accounting

Implement a versioned SQLite store with immutable identity/provenance and event
history. Transactionally link approval scope, slots, budget holds and create
claims. Support parent and round ceilings, later charges and evidence-checked
adjustments. Preserve history when scientific source is corrected under an
unchanged approval scope. Keep an actual owner deadline distinct from an
operational window that may be renewed under that approval.

Do not migrate production databases automatically. First build read-only legacy
inspectors and explicit import/bridge validation. Establish the canonical state
root and host identity across multiple checkouts. A second controller host is a
separate future capability, not a side effect of using the same package.

## M4: supervision

Consolidate detached launch and persistent process identity checks. Qualify the
installed artifact under the exact interpreter used by each child. Cover the
existing Windows launcher/base-Python PID behavior and job-survival requirements;
declare Linux support only after its session/termination tests pass.

Unify readiness, stop latching, elapsed-time recovery, finite grace/cleanup,
telemetry retries and provider-confirmed absence. Recovery can operate on an
exact durable attempt even when normal workload bootstrap fails. A damaged ledger
must not authorize a different resource or be reported as successful termination.

## M5 and M6: transport, workload execution and retries

Promote bounded streaming upload and archive download into the common transfer
contract. Implement direct SSH and the required proxy handshake against common
integrity/limit tests. Keep protocol-specific framing in workload adapters. Add
explicit ephemeral secret delivery and redaction, with private failure journals.

The serial engine admits a plan, starts supervision, claims/sends one create,
verifies the scheduled resource, runs workload stages, collects outcomes and
requests cleanup. Retry eligibility is based on typed failure, reconciled state,
remaining scope/capacity/budget and finite policy. Restart can recover verified
results without creating a new resource. Operational failure and scientific
failure both retain permitted evidence and independent cleanup.

## Consumer migrations

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
