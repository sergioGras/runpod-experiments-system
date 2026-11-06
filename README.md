# RunPod experiments system

Shared infrastructure for autonomous research experiments using RunPod. Current foundations include local budget accounting, provider access, guarded lifecycle components and incremental artifact collection. No paid execution has been performed with this library.

## Principles

* The owner sets one aggregate USD maximum per experiment round. It is a ceiling, not a spending target. Estimated cost and runtime are planning inputs, not additional owner-set caps.
* Account for all attempts, setup and cleanup. Record actual provider charges when available; reconcile delayed billing. Local accounting is best effort and cannot guarantee a hard provider-side cap.
* Preserve attempt history and collect results incrementally so interruption does not erase completed work. Retry useful corrective work within the remaining envelope.
* Projects retain their own data boundaries, approvals, run identities and resource ownership. Shared credentials do not grant one project authority over another project's resources.
* The caller remains responsible for owner approval, provider reconciliation, cleanup and data disclosure. A library function or an allocated wallet balance does not by itself authorize a particular experiment.

## Current foundation

`runpod_experiments.budget.BudgetLedger` persists a cap and idempotent charges in SQLite using integer microdollars. A caller may record an estimated outstanding amount and use `remaining` to guide retries; delayed or unknown costs must be included in that estimate. This is accounting, not a controller, lock on provider spending, or billing integration.

`runpod_experiments.runpod.RunPodClient` reads paginated Pod inventory and exact Pod details via REST v2. It can read local runpodctl configuration. `ownership.delete_attempt` requests deletion only after the stored project, exact Pod ID and live Pod name match. Credentials and raw inventory must remain private.

`attempts.AttemptLedger` persists unique attempts and a one-time create claim. `registry.ProjectRegistry` validates cooperating project prefixes. `controller.create_attempt` checks a live supervisor heartbeat, project namespace, remaining estimated budget and instance inventory before one create request; a unique name and durable claim allow the supervisor to reconcile an ambiguous response. `supervision.CleanupMonitor` monitors the pinned resource shape and hourly ceiling, guards deletion, and requires three consecutive reconciled absence observations before recording termination. `run_supervisor` adds heartbeat and stop latching; `supervisor_process.launch` starts it in a separate process. A hashed policy file binds the supervisor to the shape and hourly ceiling checked at create. `planning.runtime_from_remaining` derives a time envelope from the remaining aggregate cap and a current price estimate. These are locally tested components and accident-prevention guards, not provider IAM or an integrated paid controller. They require all cooperating local controllers to share the same ledger and namespace registry; they do not coordinate other hosts or uncooperative account users.

`runpod_experiments.ssh.SSHFetcher` downloads bounded named files over direct SSH using an existing key and a verified known-hosts file. `run_collection.collect_loop` rechecks the recorded provider ID/name and direct endpoint on every poll, preserves changed snapshots, and retries failed files. `ArtifactStore` saves content-addressed local copies and a manifest. `upload.upload_bundle` checks an entire reviewed hash-pinned manifest before sending any file to the exact recorded Pod. Use a private local store and reviewed allowlists; callers must still approve disclosure and verify the SSH host key. Producers should publish complete files by atomic rename when coherent snapshots are required. Proxy SSH and interrupted large-file resume are not yet supported.

Install the package into an isolated Python 3.11+ environment with `python -m pip install .`, pinned to a reviewed source revision by the consuming project. Run `python -m unittest discover -s tests` for local tests. No credentials, private results, weights, or run databases belong in Git.

`examples/remote_hello.py` is a deterministic synthetic workload that atomically checkpoints and verifies each completed step. It has not been uploaded or run on RunPod.

Before paid execution the system still needs account-wide coordination, reliable price and budget validation, qualification of the independent supervisor and launcher, orchestration of upload/workload/collection, and integration with each project's own approval policy. Caller-provided estimates are not verified provider quotes. The current controller supports one active instance and requires one shared ledger for cooperating controllers; it is not a cross-host lock. The SSH path and lifecycle operations have only been exercised with test doubles, not with a RunPod instance.
