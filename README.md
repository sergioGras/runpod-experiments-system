# RunPod experiments system

Shared infrastructure for autonomous research experiments using RunPod. Current foundations include local budget accounting, provider access, guarded lifecycle components and incremental artifact collection. Live qualification and spending authorization belong to each consuming project.

The [cross-project unification roadmap](docs/unification-roadmap.md) proposes the common execution contracts and staged migration for the three consuming projects. It describes planned work, not currently supported capabilities.

Development status: M0 and M1 are complete. M2's read-only provider adapter now
includes synthetic-tested parsers for the documented REST v2 billing and
resource-quote shapes. M2 remains open pending account-level qualification of
billing behavior. The isolated design branch contains an internal SQLite
admission/accounting store, a store-backed cleanup supervisor, and a
store-backed one-shot create controller with offline test evidence. It binds a
fresh quote, selected region, registered SSH-key fingerprint, project inventory,
and independent supervisor state to a durable one-use create claim. The create
path has not been qualified against a RunPod account or connected to an execution
engine. M3 and M4 remain open; no consumer migration has begun. These changes
are not part of the pinned 0.1.0 package; existing consumers and their current
package pins remain unchanged.

Detailed design and development documents:

- [Architecture and principles](docs/architecture.md)
- [Implementation milestones and consumer migrations](docs/implementation-plan.md)
- [Testing, acceptance and rollback](docs/testing-strategy.md)
- [Contribution and consumer maintenance rules](CONTRIBUTING.md)

These documents define the target design. Current package capabilities are described below.

## Principles

* The owner sets one aggregate USD maximum per experiment round. It is a ceiling, not a spending target. Estimated cost and runtime are planning inputs, not additional owner-set caps.
* Account for all attempts, setup and cleanup. Record actual provider charges when available; reconcile delayed billing. Local accounting is best effort and cannot guarantee a hard provider-side cap.
* Preserve attempt history and collect results incrementally so interruption does not erase completed work. Retry useful corrective work within the remaining envelope.
* Projects retain their own data boundaries, approvals, run identities and resource ownership. Shared credentials do not grant one project authority over another project's resources.
* The caller remains responsible for owner approval, provider reconciliation, cleanup and data disclosure. A library function or an allocated wallet balance does not by itself authorize a particular experiment.

## Current foundation

`runpod_experiments.budget.BudgetLedger` persists a cap, idempotent charges and estimated attempt exposures in SQLite using integer microdollars. The create path reserves exposure before provisioning and retains it after an ambiguous response until charges are reconciled. Delayed or unknown costs beyond that estimate must also be accounted for. This is accounting, not a provider billing limit.

`runpod_experiments.runpod.RunPodClient` reads paginated Pod inventory, exact Pod details, CPU/GPU catalog records and exact-Pod billing via REST v2. `pod_billing` sends the Pod ID and a bounded billing interval; `ProviderAdapter.observe_pod_billing` validates the response query metadata and explicit record period, then maps `totalAmount`, `podId`, `startTime` and `endTime` into immutable `BillingObservation` records. It rounds amounts upward to microdollars and hashes the complete response without retaining raw rows. Missing or changing billing rows do not prove zero final cost and must not release a local exposure reservation. Billing and quote shapes have synthetic tests only; account-specific behavior has not been qualified. Normalized CPU/GPU quotes include conservative host-RAM or VRAM-per-card evidence, and GPU catalog lookup can request a CUDA floor; regional availability remains an explicit quote field. `ownership.delete_attempt` requests deletion only after the stored project, exact Pod ID and live Pod name match. Credentials and raw inventory must remain private.

`attempts.AttemptLedger` persists unique attempts and a one-time create claim. `registry.ProjectRegistry` validates cooperating project prefixes. `controller.create_attempt` checks a live supervisor heartbeat, project namespace, remaining estimated budget and instance inventory before one create request; a unique name and durable claim allow the supervisor to reconcile an ambiguous response. CPU and GPU planning helpers bind the exact request to the current Secure catalog rate and capacity, and the supervisor compares nested provider shape fields while allowing extra reported metadata. GPU regional availability still needs a separate current check and the supervisor verifies the actual scheduled data center. `supervision.CleanupMonitor` monitors the pinned resource shape and hourly ceiling, guards deletion, and requires three consecutive reconciled absence observations before recording termination. `run_supervisor` adds heartbeat and stop latching; `supervisor_process.launch` starts it in a separate process. A hashed policy file binds the supervisor to the shape and hourly ceiling checked at create. `planning.runtime_from_remaining` derives a time envelope from the remaining aggregate cap and a current price estimate. These are locally tested components and accident-prevention guards, not provider IAM or an integrated paid controller. They require all cooperating local controllers to share the same ledger and namespace registry; they do not coordinate other hosts or uncooperative account users.

The design branch also adds `StoreCleanupMonitor` and
`supervisor_process.launch_execution_store` for an existing M3 attempt. This
path reconstructs the selected CPU/GPU profile from hash-verified plan documents,
checks live identity, region, CPU shape, image digest, disk, mount state and
hourly rate, and only deletes through the store-backed ownership guard. It checks
GPU identity/count, vCPU and host system RAM minima, and the CUDA floor, then
deliberately fails closed because REST v2 does not expose per-card VRAM in the Pod
detail. It is not connected to a create engine or any consumer. Provider-detail
shape checks have synthetic evidence only;
account-level and live-host qualification remain open. Never run both supervisor
paths against one attempt.

`runpod_experiments.store_create.StoreCreateController` adds the matching
store-backed, one-shot create boundary. Before claiming, it verifies a ready
separate supervisor process and fresh heartbeat, checks account inventory for
name/project conflicts, confirms the selected public-key fingerprint is
registered, refreshes the CPU/GPU quote and selected-region availability, and
persists hash-bound preflight evidence. It then consumes one durable create
claim, rechecks the supervisor, and sends at most one create request. An
ambiguous response retains the exposure and requires reconciliation; this
controller never retries create or deletes an unknown resource. Fake-provider
tests cover the CPU/GPU request paths, stale/mismatched preflight, inventory and
SSH-key guards, supervisor changes, malformed responses, and ambiguous timeout.
This is an internal component, not a complete serial engine, live qualification,
or consumer integration. The quote/catalog and SSH-key API paths still need
account-level compatibility checks before use with a real account.

`runpod_experiments.ssh.SSHFetcher` downloads bounded named files over direct SSH using an existing key and a pinned known-hosts file. Store-backed upload and collection can explicitly select the provider's proxy route; `SSHProxySession` runs one finite command through its interactive PTY and carries bounded files in strict base64 frames. Direct remains the default. The proxy transport does not support raw binary streams, long-lived interactive protocols or interrupted large-file resume. `collection_process` can run independently of the foreground controller and records the selected route; it rechecks the recorded provider ID/name and live endpoint on every poll, preserves changed snapshots, and retries failed files. `ArtifactStore` saves content-addressed local copies and a manifest. `upload.upload_bundle` checks an entire reviewed hash-pinned manifest before sending any file to the exact recorded Pod. Use a private local store and reviewed allowlists; callers must still approve disclosure and review the SSH host-key trust. Producers should publish complete files by atomic rename when coherent snapshots are required.

Upload, bounded remote launch, and result collection now also accept an `ExecutionStore` attempt. Those call paths verify the event chain, exact project/Pod identity, selected CPU/GPU shape and selected endpoint before opening SSH; direct remains the default, while upload and collection can select the proxy PTY route. A detached store-backed collector reopens only the configured project store and its bound attempt, including the explicit route and the plan's per-file and aggregate result limits.

`serial_engine.SerialExecutionEngine` is an internal preview coordinator. `run_attempt` accepts one new, already reserved immutable attempt, starts the independent supervisor, uses the guarded one-shot create, performs hash-checked upload, collects allowlisted results, runs a bounded direct workload process and requests cleanup. `run_job` applies only the plan's finite retry policy and calls a consumer-supplied reservation callback for each fresh quote and immutable attempt; it retries only an allowed typed failure after the previous attempt is terminal and cleanup is confirmed. `recover_attempt` closes interrupted stage records without replaying workload code and asks the surviving collector and supervisor to finish cleanup. If the supervisor is stale or cleanup cannot be confirmed, the result remains in reconciliation. The attempt outcome deliberately leaves the scientific verdict `NOT_EVALUATED`. This is not yet a stable consumer API or production engine: GPU transport remains fail-closed without per-card VRAM runtime evidence; proxy workloads require a workload-specific process launcher; automatic recovery of a dead supervisor, scientific output verification and consumer approval adapters are open.

`runpod_experiments.interactive_ssh.SSHInteractiveProcess` starts a long-lived direct SSH command only after rechecking exact attempt/Pod identity and RUNNING state. With an `ExecutionStore`, it also verifies the stored lifecycle chain, project namespace, selected region and live CPU/GPU shape before reading the direct endpoint; GPU sessions remain fail-closed when per-card VRAM cannot be verified. It requires the previously pinned host key, separates binary stdin/stdout from a private stderr log, bounds each message/read and applies backpressure with a bounded queue. It supports interactive request/response protocols where the trusted client performs a nonlinear operation between encrypted server calls. The caller must still keep the independent supervisor active, use a finite remote timeout, implement protocol framing/size limits, and terminate the process on failure; this transport is not a provider lifecycle or budget controller.

Install the package into an isolated Python 3.11+ environment with `python -m pip install .`, pinned to a reviewed source revision by the consuming project. Run `python -m unittest discover -s tests` for local tests. No credentials, private results, weights, or run databases belong in Git.

`examples/remote_hello.py` is a deterministic synthetic workload that atomically checkpoints and verifies each completed step. `examples/remote_hello_runner.py` composes a candidate CPU request, supervisor, upload, execution, collection and cleanup. A fake-provider driver test passes; the runner has not been uploaded or run on RunPod. An approval reference is a record, not proof of owner approval. The SSH bootstrap uses trust on first use: it pins the first observed key without independently authenticating it. The project must explicitly review that boundary before disclosure.

Before paid execution the system still needs account-wide coordination, qualification of the independent supervisor and collector, independent review of the runner and its trust boundary, and integration with each project's own approval policy. The CPU path rechecks a current catalog quote but storage and other cost estimates can differ from billing. The current controller supports one active instance per project and requires one shared ledger for cooperating controllers; it is not a cross-host lock. The package suite uses local subprocesses and provider test doubles; consuming projects maintain their own live qualification evidence. The Windows supervisor requests prevention of automatic idle sleep; explicit sleep, power loss or process failure can still interrupt it.

## Finite interactive I/O and stage evidence

`binary_process.BinaryProcess` provides finite-lifetime local binary subprocess pipes with bounded output queues and timed sends/receives. A stalled send terminates its process instead of waiting indefinitely for stdin to drain. Shutdown cancels queue backpressure so an unread full output queue cannot strand the reader thread. The lifetime timer kills the child even while native work blocks; this bounds that child process only, not arbitrary descendants or provider billing.

`SSHInteractiveProcess` reuses those pipes, adds finite connection/keepalive settings, and launches the remote command with GNU timeout plus a five-second kill grace. The local SSH lifetime also remains bounded. These controls terminate a transport/workload process; callers must still reconcile the provider Pod through the independent lifecycle supervisor. Preparing client keys before opening SSH remains appropriate; keepalives do not prove or repair every remote idle policy.

`telemetry.StageJournal` flushes/fsyncs stage starts, ends and exceptions with process peak RSS observations. A killed native call leaves its started stage available for collection. Windows uses PeakWorkingSetSize; Linux uses ru_maxrss converted to bytes. These are process peaks, not per-stage allocation deltas or whole-machine usage. Callers choose export-safe fields and private journal paths.

Validation: 47 local tests passed, including real child-process stalled-input, full-output-queue shutdown, lifetime termination and retained partial-stage tests. No provider resource or credential is used by these new tests.

`create_attempt(..., cleanup_reserve_seconds=120)` now accepts the caller's declared positive cleanup margin. Both the remaining-budget runtime check and pre-create exposure reservation use that value, so a 180-second cleanup plan reserves its full cost instead of silently assuming 120 seconds. The existing default is retained for callers that already use 120 seconds; tests cover a larger reservation and invalid boolean input.
