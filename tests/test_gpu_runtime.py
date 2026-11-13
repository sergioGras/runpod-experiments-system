import unittest

from runpod_experiments.contracts import ResourceKind, ResourceProfile, encode_document, sha256_bytes
from runpod_experiments.gpu_runtime import (
    GPUAttestation, GPU_PROBE_SPEC_SHA256, GPURuntimeEvidenceError,
    decode_gpu_attestation, encode_gpu_attestation, parse_nvidia_smi,
    validate_gpu_attestation, validate_gpu_observation,
)


ZERO = "0" * 64


def profile(*, gpu_count=1, gpu_memory_gb=48, minimum_cuda="12.4"):
    return ResourceProfile(
        ResourceKind.GPU, "NVIDIA A40", 600_000, ("EU-TEST-1",),
        "sha256:" + ZERO, 30, 8, 32, gpu_count, gpu_memory_gb, minimum_cuda)


class GPURuntimeEvidenceTests(unittest.TestCase):
    def observation(self, *, rows=None, driver=None, epoch=1_900_000_000):
        rows = rows if rows is not None else b"GPU-a1,NVIDIA A40,46068\n"
        driver = driver if driver is not None else (
            b"Driver Version : 550.54.14\nCUDA Version : 12.8\n")
        return parse_nvidia_smi(rows, driver, observed_epoch=epoch)

    def test_parses_bounded_rows_and_driver_cuda_version(self):
        value = self.observation(
            rows=b'GPU-a1,"NVIDIA A40, 48GB",46068\nGPU-a2,NVIDIA A40,46068\n',
            driver=b"KMD Version : 550.54.14\nCUDA UMD Version : 12.8\n")
        self.assertEqual(len(value.devices), 2)
        self.assertEqual(value.devices[0].name, "NVIDIA A40, 48GB")
        self.assertEqual(value.devices[0].memory_mib, 46068)
        self.assertEqual(value.driver_version, "550.54.14")
        self.assertEqual(value.cuda_umd_version, "12.8")
        self.assertEqual(value.observed_epoch, 1_900_000_000)
        self.assertRegex(value.output_sha256, r"^[0-9a-f]{64}$")

    def test_parses_nvidia_smi_summary_header(self):
        value = self.observation(
            driver=b"NVIDIA-SMI 550.54.14 Driver Version: 550.54.14 CUDA Version: 12.8\n")
        self.assertEqual((value.driver_version, value.cuda_umd_version), ("550.54.14", "12.8"))

    def test_accepts_nominal_48_decimal_gb_card_reported_in_mib(self):
        value = self.observation()
        validate_gpu_observation(value, profile())

    def test_rejects_missing_duplicate_or_excessive_gpu_rows(self):
        for rows in (b"", b"GPU-a1,NVIDIA A40,46068\nGPU-a1,NVIDIA A40,46068\n",
                     b"GPU-a1,NVIDIA A40,46068,unexpected\n"):
            with self.subTest(rows=rows), self.assertRaises(GPURuntimeEvidenceError):
                self.observation(rows=rows)
        with self.assertRaises(GPURuntimeEvidenceError):
            self.observation(rows=b"GPU-a1,NVIDIA A40,46068\n" * 65)

    def test_rejects_bad_versions_memory_and_output_bounds(self):
        invalid_reports = (
            b"Driver Version : 550.54.14\n",
            b"Driver Version : unknown value\nCUDA Version : 12.8\n",
            b"Driver Version : 550.54.14\nCUDA Version : 12.x\n",
            b"Driver Version : 550.54.14\nCUDA Version : 99999999999.0\n",
        )
        for report in invalid_reports:
            with self.subTest(report=report), self.assertRaises(GPURuntimeEvidenceError):
                self.observation(driver=report)
        for row in (b"GPU-a1,NVIDIA A40,N/A\n", b"GPU-a1,NVIDIA A40,-1\n",
                    b"GPU-a1,NVIDIA A40,0\n", b"GPU-a1,NVIDIA A40," + b"9" * 5000 + b"\n"):
            with self.subTest(row=row), self.assertRaises(GPURuntimeEvidenceError):
                self.observation(rows=row)
        with self.assertRaises(GPURuntimeEvidenceError):
            self.observation(rows=b"x" * 32_769)

    def test_requires_per_card_count_vram_and_driver_floor(self):
        value = self.observation()
        with self.assertRaisesRegex(GPURuntimeEvidenceError, "gpu_count_mismatch"):
            validate_gpu_observation(value, profile(gpu_count=2))
        with self.assertRaisesRegex(GPURuntimeEvidenceError, "gpu_vram_below_profile"):
            validate_gpu_observation(value, profile(gpu_memory_gb=49))
        too_old = self.observation(driver=b"Driver Version : 550.54.14\nCUDA Version : 12.3\n")
        with self.assertRaisesRegex(GPURuntimeEvidenceError, "cuda_driver_below_profile"):
            validate_gpu_observation(too_old, profile())

    def test_attestation_is_bound_to_profile_attempt_pod_and_known_hosts_policy(self):
        selected = profile()
        observation = self.observation()
        expected = {
            "project_id": "project-a", "job_id": "job-a", "attempt_id": "attempt-a",
            "attempt_sha256": "1" * 64, "plan_sha256": "2" * 64,
            "pod_id": "pod-a", "pod_name": "project-a-run-1",
            "host_key_policy_sha256": "3" * 64,
        }
        attestation = GPUAttestation(
            **expected,
            profile_sha256=sha256_bytes(encode_document("resource_profile", selected)),
            probe_sha256=GPU_PROBE_SPEC_SHA256,
            observation=observation,
        )
        validate_gpu_attestation(attestation, profile=selected,
                                 expected=expected, now_epoch=observation.observed_epoch)
        self.assertEqual(decode_gpu_attestation(encode_gpu_attestation(attestation)), attestation)
        with self.assertRaisesRegex(GPURuntimeEvidenceError,
                                    "gpu_attestation_identity_mismatch"):
            validate_gpu_attestation(
                attestation, profile=selected,
                expected={**expected, "pod_id": "pod-b"},
                now_epoch=observation.observed_epoch)
        with self.assertRaisesRegex(GPURuntimeEvidenceError,
                                    "gpu_attestation_time_invalid"):
            validate_gpu_attestation(attestation, profile=selected, expected=expected,
                                     now_epoch=observation.observed_epoch - 1)

    def test_attestation_rechecks_observed_resources(self):
        selected = profile()
        observation = self.observation(rows=b"GPU-a1,NVIDIA A40,45000\n")
        expected = {
            "project_id": "project-a", "job_id": "job-a", "attempt_id": "attempt-a",
            "attempt_sha256": "1" * 64, "plan_sha256": "2" * 64,
            "pod_id": "pod-a", "pod_name": "project-a-run-1",
            "host_key_policy_sha256": "3" * 64,
        }
        attestation = GPUAttestation(
            **expected,
            profile_sha256=sha256_bytes(encode_document("resource_profile", selected)),
            probe_sha256=GPU_PROBE_SPEC_SHA256,
            observation=observation,
        )
        with self.assertRaisesRegex(GPURuntimeEvidenceError, "gpu_vram_below_profile"):
            validate_gpu_attestation(attestation, profile=selected,
                                     expected=expected, now_epoch=observation.observed_epoch)


if __name__ == "__main__":
    unittest.main()
