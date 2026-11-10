# Shared RunPod library instructions

## Architecture and consumers

- Read README.md, docs/architecture.md, docs/implementation-plan.md,
  docs/testing-strategy.md and CONTRIBUTING.md before changing shared contracts.
  These distinguish the planned engine from the currently implemented components.
- Implement common RunPod behavior here: provider access, ownership, financial
  accounting, supervision, retry/recovery, transport and operational evidence.
  Keep scientific algorithms, result acceptance and project approvals/data local
  to the consumers. Use configuration and narrow workload interfaces; avoid
  hardcoded consumer IDs and duplicated per-project engines.
- For every behavior change identify affected consumers, add a meaningful common
  regression, and record required pin/contract updates. Do not permanently fix a
  generic failure by copying or monkeypatching this package inside a consumer.
- Update consumer agent instructions and integration records at validated cutover
  using CONTRIBUTING.md. Existing guarded-client/source-binding requirements stay
  effective until their replacements pass equivalent acceptance.

## Execution and confidentiality

- Imports, validation and ordinary tests must not discover credentials or contact
  live providers. Tests use synthetic fixtures and explicit fake adapters;
  subprocesses must inherit the same isolation. Run appropriate checks for each
  change; include installed-artifact and real PID tests when launch/loading changes.
- Read private consumer material only as needed. Review ownership/disclosure
  before moving anything here. Never commit credentials, keys, private source or
  documents, raw data/results, model weights, checkpoints or run databases.
- No library method, example, budget or approval-reference string authorizes paid
  execution. Live qualification must use an explicitly authorized project scope,
  finite limits, exact resource ownership, independent supervision, permitted
  transfers, durable accounting and provider-confirmed cleanup.
- Unknown/foreign resources remain read-only. Ambiguous create state retains its
  claim and exposure until reconciled. Provider delays and host failure mean local
  accounting cannot guarantee a hard provider billing cap.

## Safe development and handoff

- A consumer may pin the exact Git HEAD and cleanliness of the main checkout.
  Develop in an isolated branch/worktree until installed-artifact adoption removes
  that coupling. Never change an active controller's package or state in place.
- Preserve unrelated changes. Commit validated milestones with explicit paths;
  never stage the whole tree indiscriminately. Fetch/inspect before a push and
  verify classification/destination. Never force-push or publish private artifacts.
- Record milestone evidence, compatibility impact and incomplete gates honestly.
  Do not describe planned APIs, unrun live paths or baseline old-stack tests as
  completed integrated functionality. Publishing/releasing follows the owner's
  authorization and source/license review requirements in CONTRIBUTING.md.
