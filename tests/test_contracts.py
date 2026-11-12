from dataclasses import FrozenInstanceError
import math
import subprocess
import sys
from pathlib import Path
import unittest
from unittest.mock import patch

from runpod_experiments.contracts import (
    ApprovalScope, AttemptPlan, BillingObservation, BillingStatus, CleanupStatus, ContractError,
    ExecutionOutcome, ExecutionPlan, FailureCategory, ManifestFile, PodProvisioningSpec,
    QuoteObservation,
    OperationalStatus, ProjectPolicy, ResourceKind, ResourceProfile,
    RetryPolicy, SCHEMA_VERSION, ScientificVerdict, StageEvent, StageStatus, TransferDirection,
    TransferManifest, canonical_json_bytes, decode_document, encode_document,
    sha256_bytes, strict_json_loads, validate_attempt,
)
from runpod_experiments.errors import ExecutionError
from runpod_experiments.runpod import ProviderError, RunPodClient
from support import FakeProvider, SyntheticWorkload, test_mode_environment, temporary_state


ZERO = "0" * 64
ONE = "1" * 64
COMMIT = "a" * 40


def policy(**changes):
    fields = dict(project_id="synthetic-nebula", pod_name_prefixes=("nebula-",),
                  namespace_registry_sha256=ZERO, controller_hosts=("WINDOWS-TEST",),
                  max_active_resources=1)
    fields.update(changes)
    return ProjectPolicy(**fields)


def approval(project=None, **changes):
    project = project or policy()
    fields = dict(approval_id="test-approval-001", project_id="synthetic-nebula",
                  controller_host="WINDOWS-TEST", approval_evidence_sha256=ZERO,
                  project_policy_sha256=sha256_bytes(encode_document("project_policy", project)),
                  objective_sha256=ONE, disclosure_sha256="2" * 64,
                  max_total_micro_usd=10_000_000, max_hourly_micro_usd=500_000,
                  max_runtime_seconds=3600, max_attempts=3, expires_epoch=2_000_000_000)
    fields.update(changes)
    return ApprovalScope(**fields)


def cpu_profile(**changes):
    fields = dict(kind=ResourceKind.CPU, resource_id="cpu-small",
                  hourly_micro_usd=120_000, region_ids=("EU-TEST-1",),
                  image_digest="sha256:" + ZERO, disk_gb=20, cpu_count=4,
                  memory_gb=8)
    fields.update(changes)
    return ResourceProfile(**fields)


def execution_plan(**changes):
    project = policy()
    fields = dict(job_id="hello-job", project=project, approval=approval(project),
                  source_commit=COMMIT, source_sha256=ZERO, config_sha256=ONE,
                  package_sha256="2" * 64, resource_options=(cpu_profile(),),
                  provisioning=PodProvisioningSpec("runpod/base@" + cpu_profile().image_digest,
                                                  ("22/tcp",)),
                  transfers=TransferManifest((), 1024, 4096),
                  retry=RetryPolicy(2, 3600,
                                    (FailureCategory.TRANSIENT_READ,
                                     FailureCategory.VERIFIED_CAPACITY_REJECTION)))
    fields.update(changes)
    return ExecutionPlan(**fields)


def attempt(plan=None, **changes):
    plan = plan or execution_plan()
    resource = plan.resource_options[0]
    quote = QuoteObservation(resource.kind, resource.resource_id, resource.cpu_count,
                             resource.cpu_count, resource.hourly_micro_usd, 0, True,
                             resource.region_ids, 1_900_000_000, ONE)
    fields = dict(attempt_id="attempt-001", job_id=plan.job_id,
                  approval_id=plan.approval.approval_id, attempt_number=1,
                  request_sha256=ZERO,
                  quote_sha256=sha256_bytes(encode_document("quote_observation", quote)),
                  selected_quote=quote,
                  policy_sha256=sha256_bytes(encode_document("project_policy", plan.project)),
                  source_sha256=plan.source_sha256, config_sha256=plan.config_sha256,
                  package_sha256=plan.package_sha256,
                  selected_resource=resource, selected_region=resource.region_ids[0],
                  runtime_seconds=300, cleanup_reserve_seconds=60,
                  created_epoch=1_900_000_000, deadline_epoch=1_900_000_360)
    fields.update(changes)
    return AttemptPlan(**fields)


class ProjectAndApprovalContracts(unittest.TestCase):
    def test_synthetic_project_is_not_hardcoded_and_policy_is_immutable(self):
        value = policy(project_id="fourth-independent-project")
        self.assertEqual(value.project_id, "fourth-independent-project")
        with self.assertRaises(FrozenInstanceError):
            value.max_active_resources = 2
        scope = approval(project=value, project_id=value.project_id)
        self.assertEqual(execution_plan(project=value, approval=scope).project, value)

    def test_rejects_overlapping_namespaces_and_unsupported_slot_count(self):
        with self.assertRaises(ContractError):
            policy(pod_name_prefixes=("lab-", "lab-run-"))
        with self.assertRaises(ContractError):
            policy(max_active_resources=3)
        with self.assertRaises(ContractError):
            policy(max_active_resources=True)

    def test_rejects_bool_budgets_and_malformed_evidence(self):
        with self.assertRaises(ContractError):
            approval(max_attempts=True)
        with self.assertRaises(ContractError):
            approval(approval_evidence_sha256="not-a-hash")


class ResourceAndTransferContracts(unittest.TestCase):
    def test_cpu_and_gpu_requirements_are_distinct(self):
        gpu = cpu_profile(kind=ResourceKind.GPU, resource_id="test-gpu",
                          gpu_count=1, gpu_memory_gb=24, minimum_cuda="12.8")
        self.assertEqual(gpu.kind, ResourceKind.GPU)
        with self.assertRaises(ContractError):
            cpu_profile(gpu_count=1)
        with self.assertRaises(ContractError):
            cpu_profile(kind=ResourceKind.GPU, gpu_count=0, gpu_memory_gb=0,
                        minimum_cuda="12.8")

    def test_manifest_rejects_path_escape_duplicate_and_size_overrun(self):
        entry = ManifestFile("bundle/worker.py", ZERO, 12, TransferDirection.UPLOAD)
        with self.assertRaises(ContractError):
            ManifestFile("../worker.py", ZERO, 12, TransferDirection.UPLOAD)
        with self.assertRaises(ContractError):
            TransferManifest((entry, entry), 100, 100)
        with self.assertRaises(ContractError):
            TransferManifest((entry,), 10, 100)
        with self.assertRaises(ContractError):
            ManifestFile("C:/private/file", ZERO, 0, TransferDirection.UPLOAD)

    def test_manifest_is_immutable_and_accepts_zero_byte_files(self):
        entry = ManifestFile("results/empty.txt", ZERO, 0, TransferDirection.DOWNLOAD)
        value = TransferManifest((entry,), 100, 100)
        self.assertEqual(value.files[0].size_bytes, 0)


class ExecutionContractTests(unittest.TestCase):
    def test_provisioning_image_is_pinned_and_has_no_url_scheme(self):
        digest = cpu_profile().image_digest
        image_ref = "registry.example/team/base@" + digest
        self.assertEqual(PodProvisioningSpec(image_ref, ("22/tcp",)).image_ref, image_ref)
        for invalid in (
                "https://registry.example/team/base@" + digest,
                "registry.example/team/base:latest",
                "registry.example/team/base@sha256:" + "z" * 64,
                "user:password@registry.example/team/base@" + digest):
            with self.subTest(image_ref=invalid), self.assertRaises(ContractError):
                PodProvisioningSpec(invalid, ("22/tcp",))

    def test_plan_requires_provisioning_digest_to_match_every_resource(self):
        with self.assertRaises(ContractError):
            execution_plan(provisioning=PodProvisioningSpec(
                "runpod/base@sha256:" + "f" * 64, ("22/tcp",)))

    def test_plan_binds_project_approval_and_hourly_limits(self):
        self.assertEqual(execution_plan().approval.project_id, "synthetic-nebula")
        with self.assertRaises(ContractError):
            execution_plan(approval=approval(project_id="another-project"))
        with self.assertRaises(ContractError):
            execution_plan(approval=approval(controller_host="OTHER-HOST"))
        with self.assertRaises(ContractError):
            execution_plan(resource_options=(cpu_profile(hourly_micro_usd=900_000),))
        with self.assertRaises(ContractError):
            execution_plan(project=policy(namespace_registry_sha256="f" * 64))
        with self.assertRaises(ContractError):
            execution_plan(retry=RetryPolicy(1, 3601, ()))

    def test_serial_profile_rejects_a_two_slot_approval(self):
        with self.assertRaises(ContractError):
            execution_plan(approval=approval(max_active_resources=2))

    def test_retries_cannot_include_scientific_or_ambiguous_failures(self):
        with self.assertRaises(ContractError):
            RetryPolicy(3, 900, (FailureCategory.SCIENTIFIC_FAILURE,))
        with self.assertRaises(ContractError):
            RetryPolicy(3, 900, (FailureCategory.AMBIGUOUS_OPERATION,))
        with self.assertRaises(ContractError):
            RetryPolicy(3, 900, (FailureCategory.TRANSIENT_READ,), 1000, 100)

    def test_attempt_plan_binds_request_quote_policy_source_and_package(self):
        item = attempt()
        self.assertEqual(item.attempt_number, 1)
        with self.assertRaises(ContractError):
            attempt(attempt_number=0)

    def test_attempt_requires_matching_persisted_quote(self):
        plan = execution_plan()
        item = attempt(plan)
        validate_attempt(plan, item)
        with self.assertRaises(ContractError):
            validate_attempt(plan, attempt(plan, quote_sha256=ZERO))
        other = QuoteObservation(ResourceKind.CPU, "cpu-other", 4, 4,
                                 item.selected_resource.hourly_micro_usd, 0, True,
                                 item.selected_resource.region_ids, item.created_epoch, ONE)
        with self.assertRaises(ContractError):
            validate_attempt(plan, attempt(
                plan, selected_quote=other,
                quote_sha256=sha256_bytes(encode_document("quote_observation", other))))
        future = QuoteObservation(ResourceKind.CPU, item.selected_resource.resource_id,
                                 4, 4, item.selected_resource.hourly_micro_usd, 0, True,
                                 item.selected_resource.region_ids, item.created_epoch + 1, ONE)
        with self.assertRaises(ContractError):
            validate_attempt(plan, attempt(
                plan, selected_quote=future,
                quote_sha256=sha256_bytes(encode_document("quote_observation", future))))
        region_mismatch = QuoteObservation(
            ResourceKind.CPU, item.selected_resource.resource_id, 4, 4,
            item.selected_resource.hourly_micro_usd, 0, True, (), item.created_epoch, ONE)
        with self.assertRaises(ContractError):
            validate_attempt(plan, attempt(
                plan, selected_quote=region_mismatch,
                quote_sha256=sha256_bytes(encode_document("quote_observation", region_mismatch))))

    def test_attempt_must_remain_within_plan_approval_and_provenance(self):
        plan = execution_plan()
        validate_attempt(plan, attempt(plan))
        for changes in (
                {"job_id": "other-job"}, {"approval_id": "other-approval"},
                {"attempt_number": 3}, {"selected_region": "US-TEST-1"},
                {"selected_resource": cpu_profile(resource_id="unapproved")},
                {"runtime_seconds": 3601}, {"deadline_epoch": plan.approval.expires_epoch + 1},
                {"source_sha256": "f" * 64}, {"config_sha256": "f" * 64},
                {"package_sha256": "f" * 64},
                {"policy_sha256": "f" * 64}):
            with self.subTest(changes=changes), self.assertRaises(ContractError):
                validate_attempt(plan, attempt(plan, **changes))
        with self.assertRaises(ContractError):
            attempt(cleanup_reserve_seconds=301)
        limited = execution_plan(approval=approval(max_runtime_seconds=350),
                                 retry=RetryPolicy(2, 350,
                                                   (FailureCategory.TRANSIENT_READ,
                                                    FailureCategory.VERIFIED_CAPACITY_REJECTION)))
        with self.assertRaises(ContractError):
            validate_attempt(limited, attempt(limited))

    def test_scientific_failure_can_follow_operational_success(self):
        outcome = ExecutionOutcome(OperationalStatus.SUCCEEDED, ScientificVerdict.FAILED,
                                   CleanupStatus.CONFIRMED, BillingStatus.PENDING)
        self.assertEqual(outcome.scientific_verdict, ScientificVerdict.FAILED)
        with self.assertRaises(ContractError):
            ExecutionOutcome(OperationalStatus.FAILED, ScientificVerdict.NOT_EVALUATED,
                            CleanupStatus.FAILED, BillingStatus.PENDING)

    def test_posted_billing_requires_evidence_and_error_text_is_sanitized(self):
        with self.assertRaises(ContractError):
            ExecutionOutcome(OperationalStatus.SUCCEEDED, ScientificVerdict.PASSED,
                            CleanupStatus.CONFIRMED, BillingStatus.POSTED)
        error = ExecutionError(FailureCategory.TRANSIENT_READ, "catalog_timeout")
        self.assertEqual(str(error), "transient_read:catalog_timeout")
        with self.assertRaises(ValueError):
            ExecutionError(FailureCategory.TRANSIENT_READ, "HTTP timeout with bearer secret")

    def test_stage_events_are_typed_and_redacted(self):
        started = StageEvent("event-001", "hello-job", "attempt-001", 1,
                             1_900_000_000, "upload", StageStatus.STARTED)
        failed = StageEvent("event-002", "hello-job", "attempt-001", 2,
                            1_900_000_001, "upload", StageStatus.FAILED,
                            FailureCategory.TRANSPORT_FAILURE, "connection_timeout", (ZERO,))
        self.assertEqual(started.status, StageStatus.STARTED)
        self.assertEqual(failed.error_code, "connection_timeout")
        with self.assertRaises(ContractError):
            StageEvent("event-003", "hello-job", "attempt-001", 3,
                       1_900_000_002, "upload", StageStatus.FAILED)
        with self.assertRaises(ContractError):
            StageEvent("event-004", "hello-job", "attempt-001", 4,
                       1_900_000_003, "upload", StageStatus.FAILED,
                       FailureCategory.TRANSPORT_FAILURE,
                       "HTTP timeout with bearer secret")

    def test_billing_observation_is_exact_scoped_and_append_only(self):
        bill = BillingObservation("bill-001", "synthetic-nebula", "attempt-001",
                                  "pod-001", 250_000, 1_900_000_000,
                                  1_900_003_600, 1_900_003_700, ZERO)
        adjusted = BillingObservation("bill-002", "synthetic-nebula", "attempt-001",
                                      "pod-001", 275_000, 1_900_000_000,
                                      1_900_003_600, 1_900_004_000, ONE,
                                      supersedes_observation_id=bill.observation_id)
        self.assertEqual(adjusted.supersedes_observation_id, bill.observation_id)
        with self.assertRaises(ContractError):
            BillingObservation("bill-003", "synthetic-nebula", "attempt-001",
                               "pod-001", 250_000, 1_900_000_000,
                               1_900_000_000, 1_900_003_700, ZERO)
        with self.assertRaises(ContractError):
            BillingObservation("bill-004", "synthetic-nebula", "attempt-001",
                               "pod-001", 250_000, 1_900_000_000,
                               1_900_003_600, 1_900_003_700, ZERO,
                               supersedes_observation_id="bill-004")


class CanonicalDocumentTests(unittest.TestCase):
    def test_document_roundtrip_is_stable_and_versioned(self):
        plan = execution_plan()
        encoded = encode_document("execution_plan", plan)
        self.assertEqual(encoded, encode_document("execution_plan", plan))
        decoded = decode_document(encoded, expected_kind="execution_plan")
        self.assertEqual(decoded["job_id"], "hello-job")
        self.assertEqual(decoded["schema_version"], SCHEMA_VERSION)
        self.assertEqual(sha256_bytes(encoded), sha256_bytes(encoded))

    def test_parser_rejects_duplicates_nonfinite_and_unknown_schema(self):
        for raw in ('{"x":1,"x":2}', '{"x":NaN}', '{"x":1e999}'):
            with self.subTest(raw=raw), self.assertRaises(ContractError):
                strict_json_loads(raw)
        with self.assertRaises(ContractError):
            canonical_json_bytes({"x": math.inf})
        with self.assertRaises(ContractError):
            canonical_json_bytes({"x": "a" * (1_048_577)})
        with self.assertRaises(ContractError):
            strict_json_loads(b" " * 1_048_577)
        with self.assertRaises(ContractError):
            decode_document('{"kind":"execution_plan","payload":{},"schema_version":3}',
                            expected_kind="execution_plan")
        with self.assertRaises(ContractError):
            decode_document('{"kind":"other","payload":{},"schema_version":1}',
                            expected_kind="execution_plan")

    def test_decode_requires_exact_document_envelope(self):
        with self.assertRaises(ContractError):
            decode_document('{"kind":"execution_plan","payload":{},"schema_version":1,"extra":true}',
                            expected_kind="execution_plan")
        with self.assertRaises(ContractError):
            strict_json_loads(b"\xff")


class OfflineHarnessTests(unittest.TestCase):
    def test_fake_provider_filters_project_and_consumes_attempt_once(self):
        provider = FakeProvider(project_prefixes=("nebula-",), inventory=[
            {"id": "sibling-1", "name": "other-run-1"},
            {"id": "project-1", "name": "nebula-run-1"}])
        self.assertEqual(len(provider.list_account_pods()), 2)
        self.assertEqual([pod["id"] for pod in provider.list_project_pods(("nebula-",))], ["project-1"])
        created = provider.create_once("attempt-002", {"name": "nebula-run-2"})
        self.assertEqual(created["id"], "fake-pod-3")
        with self.assertRaises(RuntimeError):
            provider.create_once("attempt-002", {"name": "nebula-run-2"})
        self.assertTrue(provider.delete_exact(created["id"], created["name"]))
        self.assertFalse(provider.delete_exact("sibling-1", "other-run-1"))

    def test_synthetic_workload_has_bounded_named_failure(self):
        workload = SyntheticWorkload(fail_at="start")
        with self.assertRaisesRegex(RuntimeError, "synthetic stage failure"):
            workload.run_stage("start")
        self.assertEqual(SyntheticWorkload().run_stage("collect"), b"synthetic-result")

    def test_config_factory_stops_before_credential_read_in_child_process(self):
        command = """from pathlib import Path
from runpod_experiments.runpod import RunPodClient, ProviderError
try:
    RunPodClient.from_config(Path('missing-test-config.toml'))
except ProviderError as error:
    print(str(error))
else:
    raise SystemExit('provider access unexpectedly allowed')
"""
        with temporary_state() as state:
            env = test_mode_environment()
            env["HOME"] = state
            env["USERPROFILE"] = state
            result = subprocess.run([sys.executable, "-c", command], cwd=Path.cwd(), env=env,
                                    capture_output=True, text=True, timeout=10, check=True)
        self.assertEqual(result.stdout.strip(), "Provider access is disabled in test mode")
        self.assertEqual(result.stderr, "")

    def test_provider_config_is_blocked_in_parent_too(self):
        with temporary_state() as state:
            env = test_mode_environment()
            env["HOME"] = state
            env["USERPROFILE"] = state
            with patch.dict("os.environ", env):
                with self.assertRaisesRegex(ProviderError, "disabled in test mode"):
                    RunPodClient.from_config(Path(state) / "missing.toml")
                with self.assertRaisesRegex(ProviderError, "disabled in test mode"):
                    RunPodClient("synthetic-test-key", opener=lambda *_args, **_kwargs:
                                 self.fail("test mode attempted provider network access"))


if __name__ == "__main__":
    unittest.main()
