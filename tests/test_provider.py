import io
import json
import unittest
from unittest.mock import patch

from runpod_experiments.contracts import FailureCategory, ProjectPolicy
from runpod_experiments.errors import (
    ExecutionError, ProviderOperation, normalize_provider_error,
)
from runpod_experiments.billing import billing_period_bounds
from runpod_experiments.provider import PodObservation, ProviderAdapter
from runpod_experiments.quotes import normalize_cpu_quote, normalize_gpu_quote
from runpod_experiments.runpod import ProviderError, RunPodClient
from support import FakeProvider


ZERO = "0" * 64


def project_policy():
    return ProjectPolicy("synthetic-nebula", ("nebula-",), ZERO,
                         ("WINDOWS-TEST",), max_active_resources=1)


class InventoryAdapterTests(unittest.TestCase):
    def test_account_inventory_is_project_filtered_without_retaining_raw_payload(self):
        provider = FakeProvider(project_prefixes=("nebula-",), inventory=[
            {"id": "foreign-1", "name": "other-run-1", "status": "RUNNING",
             "ssh": {"private_detail": "discard"}},
            {"id": "owned-1", "name": "nebula-run-1", "status": "EXITED",
             "ssh": {"private_detail": "discard"}},
        ])
        adapter = ProviderAdapter(provider)
        all_pods = adapter.list_account_pods()
        project_pods = adapter.list_project_pods(project_policy())
        self.assertEqual(len(all_pods), 2)
        self.assertEqual(project_pods, (PodObservation("owned-1", "nebula-run-1", "EXITED"),))
        self.assertFalse(hasattr(project_pods[0], "ssh"))

    def test_duplicate_or_malformed_inventory_fails_closed(self):
        for inventory, expected in (
                ([{"id": "pod-1", "name": "a-run-1", "status": "RUNNING"},
                  {"id": "pod-1", "name": "b-run-1", "status": "RUNNING"}],
                 FailureCategory.RECONCILIATION_REQUIRED),
                ([{"id": "pod-1", "name": "same-run", "status": "RUNNING"},
                  {"id": "pod-2", "name": "same-run", "status": "RUNNING"}],
                 FailureCategory.RECONCILIATION_REQUIRED),
                ([{"id": "bad/id", "name": "nebula-run-1", "status": "RUNNING"}],
                 FailureCategory.RESOURCE_MISMATCH),
        ):
            with self.subTest(inventory=inventory):
                error = None
                try:
                    ProviderAdapter(FakeProvider(inventory=inventory)).list_account_pods()
                except ExecutionError as caught:
                    error = caught
                self.assertIsNotNone(error)
                self.assertEqual(error.category, expected)
                self.assertNotIn("nebula-run-1", str(error))

    def test_exact_attempt_detail_checks_project_id_and_name(self):
        provider = FakeProvider(inventory=[
            {"id": "pod-1", "name": "nebula-run-1", "status": "RUNNING"}])
        adapter = ProviderAdapter(provider)
        self.assertEqual(adapter.get_attempt_pod("pod-1", "nebula-run-1", project_policy()).pod_id,
                         "pod-1")
        with self.assertRaises(ExecutionError) as caught:
            adapter.get_attempt_pod("pod-1", "other-run-1", project_policy())
        self.assertEqual(caught.exception.category, FailureCategory.AUTHORIZATION)
        with self.assertRaises(ExecutionError) as caught:
            adapter.get_attempt_pod("pod-1", "nebula-run-2", project_policy())
        self.assertEqual(caught.exception.category, FailureCategory.RECONCILIATION_REQUIRED)

    def test_missing_attempt_detail_requires_reconciliation(self):
        adapter = ProviderAdapter(FakeProvider())
        with self.assertRaises(ExecutionError) as caught:
            adapter.get_attempt_pod("pod-404", "nebula-run-1", project_policy())
        self.assertEqual(caught.exception.category, FailureCategory.RECONCILIATION_REQUIRED)

    def test_inventory_provider_failure_is_translated_and_redacted(self):
        class FailingProvider:
            def pods(self):
                raise ProviderError("body contains private provider text", 503, kind="http")

        with self.assertRaises(ExecutionError) as caught:
            ProviderAdapter(FailingProvider()).list_account_pods()
        self.assertEqual(caught.exception.category, FailureCategory.TRANSIENT_READ)
        self.assertNotIn("private provider text", str(caught.exception))


class ProviderErrorNormalizationTests(unittest.TestCase):
    def test_read_failures_are_classified_without_provider_text(self):
        cases = ((ProviderError("hidden body", 503, kind="http"),
                  FailureCategory.TRANSIENT_READ, "http_503"),
                 (ProviderError("hidden body", 403, kind="http"),
                  FailureCategory.AUTHORIZATION, "http_403"),
                 (ProviderError("private host detail", kind="transport"),
                  FailureCategory.TRANSIENT_READ, "transport"),
                 (ProviderError("invalid raw response", kind="invalid_response"),
                  FailureCategory.INTERNAL, "invalid_response"))
        for error, category, code in cases:
            with self.subTest(error=type(error).__name__, code=code):
                normalized = normalize_provider_error(error, ProviderOperation.READ)
                self.assertEqual(normalized.category, category)
                self.assertEqual(normalized.code, code)
                self.assertNotIn(str(error), str(normalized))

    def test_mutation_errors_are_ambiguous_and_status_never_proves_capacity(self):
        for error in (ProviderError("timeout", kind="transport"),
                      ProviderError("capacity?", 409, kind="http")):
            normalized = normalize_provider_error(error, ProviderOperation.CREATE)
            self.assertEqual(normalized.category, FailureCategory.AMBIGUOUS_OPERATION)
            self.assertNotEqual(normalized.category, FailureCategory.VERIFIED_CAPACITY_REJECTION)


class QuoteNormalizationTests(unittest.TestCase):
    def test_cpu_quote_uses_secure_rate_conservative_microdollar_rounding(self):
        offer = {"id": "cpu3c", "price": {"securePerVcpu": 0.03000001},
                 "ramGbPerVcpu": 2, "vcpu": {"min": 2, "max": 32},
                 "availability": "HIGH",
                 "dataCenters": [{"id": "EU-CZ-1", "availability": "HIGH"},
                                 {"id": "US-CA-1", "availability": "NONE"}]}
        quote = normalize_cpu_quote(offer, resource_id="cpu3c", vcpu_count=4,
                                    observed_epoch=1_900_000_000,
                                    storage_hourly_usd="0.0000001")
        self.assertEqual(quote.compute_hourly_micro_usd, 120_004)
        self.assertEqual(quote.storage_hourly_micro_usd, 1)
        self.assertEqual(quote.total_hourly_micro_usd, 120_005)
        self.assertEqual(quote.region_ids, ("EU-CZ-1",))
        self.assertTrue(quote.catalog_available)

    def test_provider_adapter_exposes_normalized_quotes_not_raw_responses(self):
        cpu_offer = {"id": "cpu3c", "price": {"securePerVcpu": 0.03},
                     "ramGbPerVcpu": 2, "vcpu": {"min": 2, "max": 32},
                     "availability": "HIGH"}

        class CatalogProvider(FakeProvider):
            def cpu_offer(self, cpu_id, vcpu_count):
                self.requests.append(("cpu_offer", (cpu_id, vcpu_count)))
                return cpu_offer

        adapter = ProviderAdapter(CatalogProvider())
        quote = adapter.quote_cpu("cpu3c", 4, observed_epoch=1_900_000_000)
        self.assertEqual(quote.compute_hourly_micro_usd, 120_000)
        self.assertFalse(isinstance(quote, dict))

    def test_gpu_quote_keeps_capacity_and_region_evidence_separate(self):
        offer = {"id": "NVIDIA A40", "secure": True,
                 "price": {"secure": 0.59}, "maxCount": {"secure": 4}}
        quote = normalize_gpu_quote(offer, resource_id="NVIDIA A40", gpu_count=2,
                                    observed_epoch=1_900_000_000,
                                    storage_hourly_usd="0.001")
        self.assertEqual(quote.compute_hourly_micro_usd, 1_180_000)
        self.assertEqual(quote.storage_hourly_micro_usd, 1_000)
        self.assertEqual(quote.region_ids, ())
        self.assertTrue(quote.catalog_available)
        no_capacity = normalize_gpu_quote(
            {**offer, "maxCount": {"secure": 0}}, resource_id="NVIDIA A40",
            gpu_count=1, observed_epoch=1_900_000_000)
        self.assertFalse(no_capacity.catalog_available)

    def test_malformed_or_mismatched_quotes_fail_with_sanitized_error(self):
        base = {"id": "cpu3c", "price": {"securePerVcpu": 0.03},
                "ramGbPerVcpu": 2, "vcpu": {"min": 2, "max": 32},
                "availability": "HIGH"}
        for offer in ({**base, "id": "other"},
                      {**base, "price": {"securePerVcpu": True}},
                      {**base, "price": {"securePerVcpu": float("nan")}}):
            with self.subTest(offer=offer):
                with self.assertRaises(ExecutionError) as caught:
                    normalize_cpu_quote(offer, resource_id="cpu3c", vcpu_count=4,
                                        observed_epoch=1_900_000_000)
                self.assertEqual(caught.exception.category, FailureCategory.RESOURCE_MISMATCH)
                self.assertEqual(caught.exception.code, "cpu_quote_invalid")


class BillingObservationAdapterTests(unittest.TestCase):
    def billing_response(self, amount="0.1000001"):
        return [{"podId": "pod-1", "time": "2026-11-11T08:00:00Z",
                 "amount": amount, "timeBilledMs": 3_600_000}]

    def test_exact_pod_billing_becomes_safe_microdollar_observations(self):
        response = self.billing_response()

        class BillingProvider:
            def __init__(self): self.requests = []
            def pod_billing(self, pod_id, **kwargs):
                self.requests.append((pod_id, kwargs))
                return response

        provider = BillingProvider()
        adapter = ProviderAdapter(provider)
        observations = adapter.observe_pod_billing(
            project_id="synthetic-nebula", attempt_id="attempt-1",
            provider_pod_id="pod-1", observed_epoch=1_800_000_000,
            start_time="2026-11-11T08:00:00Z", end_time="2026-11-11T09:00:00Z")
        self.assertEqual(provider.requests[0][0], "pod-1")
        self.assertEqual(len(observations), 1)
        item = observations[0]
        self.assertEqual(item.amount_micro_usd, 100_001)
        self.assertEqual(item.project_id, "synthetic-nebula")
        self.assertEqual(item.attempt_id, "attempt-1")
        self.assertEqual(item.provider_pod_id, "pod-1")
        self.assertEqual(item.period_start_epoch, 1_794_384_000)
        self.assertEqual(item.period_end_epoch, 1_794_387_600)
        self.assertFalse(hasattr(item, "records"))
        repeated = adapter.observe_pod_billing(
            project_id="synthetic-nebula", attempt_id="attempt-1",
            provider_pod_id="pod-1", observed_epoch=1_800_000_000,
            start_time="2026-11-11T08:00:00Z", end_time="2026-11-11T09:00:00Z")
        self.assertEqual(repeated, observations)

    def test_absent_rows_are_not_zero_billing_and_revisions_need_explicit_links(self):
        response = self.billing_response()

        class BillingProvider:
            def pod_billing(self, _pod_id, **_kwargs): return response

        response.clear()
        self.assertEqual(ProviderAdapter(BillingProvider()).observe_pod_billing(
            project_id="synthetic-nebula", attempt_id="attempt-1",
            provider_pod_id="pod-1", observed_epoch=1_800_000_000,
            start_time="2026-11-11T08:00:00Z",
            end_time="2026-11-11T09:00:00Z"), ())

        response = self.billing_response("0.11")
        start, end = 1_794_384_000, 1_794_387_600
        revised, = ProviderAdapter(BillingProvider()).observe_pod_billing(
            project_id="synthetic-nebula", attempt_id="attempt-1",
            provider_pod_id="pod-1", observed_epoch=1_800_000_001,
            start_time="2026-11-11T08:00:00Z",
            end_time="2026-11-11T09:00:00Z",
            supersedes_observation_ids={(start, end): "bill-original"})
        self.assertEqual(revised.amount_micro_usd, 110_000)
        self.assertEqual(revised.supersedes_observation_id, "bill-original")

    def test_malformed_or_overlapping_billing_fails_closed(self):
        response = self.billing_response()
        response.append(dict(response[0]))

        class BillingProvider:
            def pod_billing(self, _pod_id, **_kwargs): return response

        with self.assertRaises(ExecutionError) as caught:
            ProviderAdapter(BillingProvider()).observe_pod_billing(
                project_id="synthetic-nebula", attempt_id="attempt-1",
                provider_pod_id="pod-1", observed_epoch=1_800_000_000,
                start_time="2026-11-11T08:00:00Z",
                end_time="2026-11-11T09:00:00Z")
        self.assertEqual(caught.exception.code, "pod_billing_malformed")
        self.assertNotIn("pod-1", str(caught.exception))

    def test_bucket_end_derivation_covers_all_documented_sizes(self):
        expected_seconds = {
            "hour": 3_600,
            "day": 86_400,
            "week": 604_800,
            "month": 29 * 86_400,
            "year": 365 * 86_400,
        }
        starts = {
            "hour": "2026-11-11T08:00:00Z",
            "day": "2026-11-11T00:00:00Z",
            "week": "2026-11-09T00:00:00Z",
            "month": "2024-01-31T00:00:00Z",
            "year": "2024-02-29T00:00:00Z",
        }
        for bucket, value in starts.items():
            with self.subTest(bucket=bucket):
                start, end = billing_period_bounds(value, bucket)
                self.assertEqual(end - start, expected_seconds[bucket])

    def test_observation_requires_attempt_bounds_and_rejects_unrequested_periods(self):
        class BillingProvider:
            def __init__(self): self.calls = 0
            def pod_billing(self, _pod_id, **_kwargs):
                self.calls += 1
                return [{"podId": "pod-1", "time": "2026-11-11T07:00:00Z",
                         "amount": 0.1}]

        provider = BillingProvider()
        adapter = ProviderAdapter(provider)
        with self.assertRaises(TypeError):
            adapter.observe_pod_billing(
                project_id="synthetic-nebula", attempt_id="attempt-1",
                provider_pod_id="pod-1", observed_epoch=1_800_000_000)
        self.assertEqual(provider.calls, 0)
        with self.assertRaises(ExecutionError) as caught:
            adapter.observe_pod_billing(
                project_id="synthetic-nebula", attempt_id="attempt-1",
                provider_pod_id="pod-1", observed_epoch=1_800_000_000,
                start_time="2026-11-11T08:00:00Z",
                end_time="2026-11-11T09:00:00Z")
        self.assertEqual(caught.exception.code, "pod_billing_malformed")
        self.assertEqual(provider.calls, 1)


class InventoryPaginationBoundsTests(unittest.TestCase):
    def test_pagination_and_item_limits_stop_before_unbounded_reads(self):
        calls = []

        def opener(request, timeout):
            calls.append(request.full_url)
            body = {"pods": [{"id": "pod-1", "name": "nebula-run-1", "status": "RUNNING"}],
                    "pagination": {"hasNextPage": True, "nextCursor": "next"}}
            return io.BytesIO(json.dumps(body).encode("utf-8"))

        client = RunPodClient("synthetic-key", opener=opener)
        with self.assertRaisesRegex(ProviderError, "page limit"):
            client.pods(max_pages=1)
        self.assertEqual(len(calls), 1)

        def two_pods(_request, timeout):
            body = {"pods": [{"id": "pod-1"}, {"id": "pod-2"}],
                    "pagination": {"hasNextPage": False}}
            return io.BytesIO(json.dumps(body).encode("utf-8"))

        with self.assertRaisesRegex(ProviderError, "item limit"):
            RunPodClient("synthetic-key", opener=two_pods).pods(max_pods=1)
        with self.assertRaisesRegex(ValueError, "bounds"):
            RunPodClient("synthetic-key", opener=two_pods).pods(max_pages=True)

    def test_provider_response_body_has_a_byte_limit(self):
        with patch("runpod_experiments.runpod.MAX_RESPONSE_BYTES", 4):
            client = RunPodClient("synthetic-key", opener=lambda *_args, **_kwargs:
                                  io.BytesIO(b"12345"))
            with self.assertRaisesRegex(ProviderError, "size limit") as caught:
                client.pods()
        self.assertEqual(caught.exception.kind, "invalid_response")


if __name__ == "__main__":
    unittest.main()
