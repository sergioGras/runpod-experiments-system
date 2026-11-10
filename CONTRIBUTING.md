# Contributing and maintaining consumers

The goal is one maintained implementation of RunPod operational behavior. Read
the [architecture](docs/architecture.md), [implementation plan](docs/implementation-plan.md)
and [testing contract](docs/testing-strategy.md). They describe the proposed common
engine; current 0.1.0 components do not yet implement the entire design.

## Where a change belongs

| Change | Home |
| --- | --- |
| Provider access, CPU/GPU quote parsing, ownership, accounting, watchdog, retries, transport or operational recovery | Shared package |
| Scientific protocol, model/backend environment, data selection or numerical/statistical acceptance | Consumer project |
| Approved resource/disclosure limits and eligibility preferences | Consumer configuration validated by the package |
| A reusable capability discovered while adapting a consumer | Shared package with a synthetic contract test |

Do not create a permanent local copy, monkeypatch or fork of common infrastructure
to make one project work. First reproduce the issue at the narrowest common
boundary. If it is genuinely workload-specific, keep it in a small consumer
adapter and explain why it cannot be expressed through existing capabilities.

## Change workflow

1. Identify the affected contract, failure and consumers. Reconcile any active
   resource before changing its controls. Preserve its current source/artifact pin.
2. Prepare a minimal synthetic reproduction in this repository. Inspect source
   ownership and disclosure rights before transferring anything from a private
   consumer; never copy its data, credentials, raw logs or scientific source into
   a public regression, issue or provider prompt.
3. Implement the fix in an isolated branch/worktree with meaningful regression
   coverage. Keep imports/ordinary tests free of live-provider access, including
   subprocesses. Run relevant existing contracts and broader tests when warranted.
4. Record public API/schema/capability impact, upgrade requirements and remaining
   limits. Update examples/docs and release notes for user-visible behavior.
5. Build/test the exact candidate artifact when packaging or process loading is
   affected. A detached supervisor must load the same reviewed package as its plan.
6. Run affected consumer conformance checks and record a disposition for all three
   consumers: upgrade required, unaffected with reason, or not yet integrated.
7. Prepare a reviewed version/artifact and update applicable consumer pins, source
   bindings, instructions and validation evidence. Reconcile state at cutover;
   never silently replace the package under an active controller.

A provider incident can require immediate exact-run cleanup through the current
guarded controller. That operational response does not require swapping libraries
mid-run. Any temporary workaround must have an issue/record, an expiry or removal
gate and a shared follow-up; it must not become an undocumented fourth engine.

## Required consumer maintenance record

Each migrated project must keep a small versioned integration record containing:

- Installed package version/revision, artifact hash, supported schema/capabilities
  and how the exact supervisor interpreter locates that artifact.
- Project namespace policy, canonical private state location and controlling-host
  rule, with private values kept outside Git when appropriate.
- Workload/approval adapter entry points, conformance commands and latest evidence.
- Remaining compatibility callers, their replacements and retirement criteria.
- Upgrade/cutover and rollback procedure, including unresolved bills/resources.

During each migration update that project's agent instructions with this policy:

> Generic RunPod provisioning, ownership, spending, watchdog, retry, transfer and
> operational-recovery changes belong in runpod-experiments-system. Read this
> project's integration record and installed package contract first. Reproduce
> generic failures with reviewed synthetic fixtures in the shared library, add
> regression coverage there, assess all consumers, and adopt the corrected pinned
> artifact with the project's acceptance checks and source bindings. Keep research
> logic and result acceptance local. Do not copy or patch shared implementation
> inside this project, and do not replace controls under an active run.

Install this instruction only with the corresponding validated integration
record. Existing legacy guarded-client instructions remain applicable until that
cutover; the migration must update them deliberately rather than bypass them.

## Compatibility and releases

Document stable public APIs separately from internal functions. Version persisted
schemas and refuse unknown versions. Do not silently migrate live state, change
the meaning of an inventory method, release an ambiguous reservation or reinterpret
an old approval. Deprecation requires an alternative and a caller migration gate.

Before release, inspect wheel/sdist contents and distribution destination. Include
only reviewed reusable code, synthetic fixtures and documentation. Consumer
private acceptance belongs in its own environment. Publishing a release/tag,
uploading new artifacts or choosing a license requires the owner's applicable
authorization; an implementation budget alone grants none of those rights.

Before broader third-party distribution, the owner must choose the license,
review contributor/source ownership and define a private security-reporting
channel. Do not imply reuse rights or operational guarantees that have not been
established. Provider billing delays, host failures and account-wide credentials
remain material limitations of a local controller.
