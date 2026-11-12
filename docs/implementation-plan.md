# Implementation and consumer migration plan

Status: M0 design and M1 are complete. M2 remains open pending verified per-Pod
billing evidence. The initial M3 SQLite store slice passes its focused and full
offline checks on the isolated `design/unified-execution` worktree; M3's full
exit criteria remain open. Production migration has not begun. Read
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

M1 implementation currently adds frozen schema-v1 contract records and stage
events, canonical UTF-8 JSON envelope helpers (sorted keys, compact form, one
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
adapter now parses exact-Pod billing into immutable `BillingObservation` records.
It requires an attempt's bounded start/end interval, calls the documented REST
v1 `/billing/pods` operation with `grouping=podId`, and uses its `amount`,
`podId` and bucket-start `time` fields. Bucket ends are derived from the requested
bucket size because the published response schema has no end field. Rows outside
the requested interval and overlapping buckets fail closed. Amounts round upward
to microdollars; raw billing rows are not retained; empty responses produce no
observation and cannot settle a hold. Adjustments require the caller to link the
exact superseded row. See the official
[RunPod REST v1 OpenAPI](https://github.com/runpod/docs/blob/main/api-reference/openapi.json)
for the endpoint and schema. The response contract differs from the v2-shaped
draft method on the main checkout, so this branch uses the documented v1 contract
and does not claim the draft v2 response is validated.

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
schema-v2 SQLite ledger for one project on one configured controller host. It
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

The current M4 code still uses the legacy `AttemptLedger` and file-backed
`ResourcePolicy`; M3's `ExecutionStore` is not its source of lifecycle truth.
`ProviderAdapter` deliberately exposes read-only operations, while cleanup
mutations remain behind the existing ownership guard. The additive
`request_delete_for_store_attempt()` primitive verifies the lifecycle projection,
derives project scope from the hash-verified plan and approval documents,
requires a latched stopping state and exact live project/Pod identity, and does
not record termination. `StoreCleanupMonitor` now binds unique reserved-name
candidates, latches stops, and requires three scoped provider absence observations
before recording create absence or termination. Its observation count is
intentionally in-memory, so restart repeats checks conservatively. The detached
entrypoint still constructs the legacy monitor; store-backed launch/recovery and
resource-shape supervision remain open.
Do not mix the two ledgers for one attempt or run both controllers against the
same job. Cut over only after restart, ambiguous-create reconciliation,
exact-Pod deletion and confirmed-absence conformance tests pass.

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
