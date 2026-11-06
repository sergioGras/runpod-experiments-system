# RunPod experiments system

Shared infrastructure for autonomous research experiments using RunPod. This first milestone provides local budget accounting and incremental artifact preservation; it does not provision resources or connect to RunPod yet.

## Principles

* The owner sets one aggregate USD maximum per experiment round. It is a ceiling, not a spending target. Estimated cost and runtime are planning inputs, not additional owner-set caps.
* Account for all attempts, setup and cleanup. Record actual provider charges when available; reconcile delayed billing. Local accounting is best effort and cannot guarantee a hard provider-side cap.
* Preserve attempt history and collect results incrementally so interruption does not erase completed work. Retry useful corrective work within the remaining envelope.
* Projects retain their own data boundaries, approvals, run identities and resource ownership. Shared credentials do not grant one project authority over another project's resources.
* The caller remains responsible for owner approval, provider reconciliation, cleanup and data disclosure. A library function or an allocated wallet balance does not by itself authorize a particular experiment.

## Current foundation

`runpod_experiments.budget.BudgetLedger` persists a cap and idempotent charges in SQLite using integer microdollars. A caller may record an estimated outstanding amount and use `remaining` to guide retries; delayed or unknown costs must be included in that estimate. This is accounting, not a controller, lock on provider spending, or billing integration.

`runpod_experiments.artifacts.ArtifactStore` saves immutable, content-addressed local copies of collected result bytes and appends a manifest entry for each collection. The caller supplies safe relative artifact names and controls where the private store is kept. It does not yet perform remote collection or validate disclosure rights.

Run `python -m unittest discover -s tests` for local tests. No credentials, private results, weights, or run databases belong in Git.

Before live execution the system still needs a provider transport, durable resource ownership and ambiguous-create reconciliation, independent supervision and cleanup, continuous remote collection, and integration with each project's own approval policy.
