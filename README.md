# RunPod experiments system

Shared infrastructure for autonomous research experiments using RunPod. Current foundations provide local budget accounting, read-only provider access and incremental artifact collection. They do not provision resources.

## Principles

* The owner sets one aggregate USD maximum per experiment round. It is a ceiling, not a spending target. Estimated cost and runtime are planning inputs, not additional owner-set caps.
* Account for all attempts, setup and cleanup. Record actual provider charges when available; reconcile delayed billing. Local accounting is best effort and cannot guarantee a hard provider-side cap.
* Preserve attempt history and collect results incrementally so interruption does not erase completed work. Retry useful corrective work within the remaining envelope.
* Projects retain their own data boundaries, approvals, run identities and resource ownership. Shared credentials do not grant one project authority over another project's resources.
* The caller remains responsible for owner approval, provider reconciliation, cleanup and data disclosure. A library function or an allocated wallet balance does not by itself authorize a particular experiment.

## Current foundation

`runpod_experiments.budget.BudgetLedger` persists a cap and idempotent charges in SQLite using integer microdollars. A caller may record an estimated outstanding amount and use `remaining` to guide retries; delayed or unknown costs must be included in that estimate. This is accounting, not a controller, lock on provider spending, or billing integration.

`runpod_experiments.runpod.RunPodClient` reads paginated Pod inventory and exact Pod details via REST v2. It can read local runpodctl configuration. `ownership.delete_attempt` requests deletion only after the stored project, exact Pod ID and live Pod name match. Credentials and raw inventory must remain private.

`attempts.AttemptLedger` persists unique attempts and a one-time create claim, preserving ambiguous creates for reconciliation rather than issuing a second create automatically. `supervision.CleanupMonitor` guards deletion and requires three consecutive reconciled absence observations before recording termination. `run_supervisor` adds heartbeat and stop latching around that monitor. These are tested local components: no detached launcher, guarded create path, account-wide project namespace enforcement, or integrated paid controller exists yet. A claimed create without a bound provider ID remains unresolved and cannot be deleted by this monitor. These controls are accidental-mutation guards, not provider IAM.

`runpod_experiments.ssh.SSHFetcher` downloads bounded named files over direct SSH using an existing key and a verified known-hosts file. `runpod_experiments.collection` polls a caller-supplied fetcher, preserves changed snapshots and retries failed files on later polls. `ArtifactStore` saves content-addressed local copies and a manifest. Use a private local store and a reviewed allowlist of result names; the caller is responsible for verifying the endpoint belongs to the exact intended run. Producers should publish complete files by atomic rename when coherent snapshots are required. Proxy SSH and interrupted large-file resume are not yet supported.

Run `python -m unittest discover -s tests` for local tests. No credentials, private results, weights, or run databases belong in Git.

Before paid execution the system still needs a guarded create path, a registered project namespace, automatic or operator-resolved ambiguous-create reconciliation, a qualified independent supervisor and launcher, budget integration, operational collection supervision, and integration with each project's own approval policy. The SSH path and supervision loops have only been exercised with test doubles, not with a RunPod instance.
