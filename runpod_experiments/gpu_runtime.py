"""Parse and validate bounded NVIDIA runtime evidence.

This module defines a local evidence contract only. The fixed SSH probe lives
in `gpu_probe`; an observation authorizes nothing until the store-backed
supervisor binds it to the exact attempt and transport callers verify that proof.
"""
from __future__ import annotations

import csv
from dataclasses import dataclass
import hashlib
import io
import re

from runpod_experiments.contracts import (
    ContractError, ResourceKind, ResourceProfile, canonical_json_bytes,
    decode_document, encode_document, sha256_bytes,
)


_MAX_PROBE_OUTPUT_BYTES = 32_768
_MAX_GPU_ROWS = 64
GPU_RUNTIME_ATTESTATION_GRACE_SECONDS = 300
GPU_PROBE_LOCAL_TIMEOUT_SECONDS = 20
GPU_PROBE_REMOTE_TIMEOUT_SECONDS = 15
_DRIVER_VERSION = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+-]{0,63}\Z")
_CUDA_VERSION = re.compile(r"[0-9]{1,3}\.[0-9]{1,3}\Z")
_SAFE_LABEL = re.compile(r"[^\x00-\x1f\x7f]{1,128}\Z")

# The remote adapter executes only these fixed, read-only queries.
# `nounits` leaves memory.total as an integer in MiB according to nvidia-smi's
# documented memory-field units. No caller-supplied shell fragments are used.
GPU_QUERY_FIELDS = "uuid,name,memory.total"
GPU_QUERY_FORMAT = "csv,noheader,nounits"
GPU_DRIVER_QUERY = "nvidia-smi | head -n 1"
GPU_REMOTE_QUERY = (
    "printf '%s\\n' '__RPE_DRIVER_BEGIN__'; " + GPU_DRIVER_QUERY +
    "; printf '%s\\n' '__RPE_GPU_BEGIN__'; "
    "nvidia-smi --query-gpu=" + GPU_QUERY_FIELDS + " --format=" + GPU_QUERY_FORMAT
)
GPU_PROBE_SPEC_SHA256 = hashlib.sha256(canonical_json_bytes({
    "remote_query": GPU_REMOTE_QUERY,
    "remote_timeout_seconds": GPU_PROBE_REMOTE_TIMEOUT_SECONDS,
    "local_timeout_seconds": GPU_PROBE_LOCAL_TIMEOUT_SECONDS,
    "gpu_query_fields": GPU_QUERY_FIELDS,
    "gpu_query_format": GPU_QUERY_FORMAT,
    "schema": 1,
})).hexdigest()


class GPURuntimeEvidenceError(ValueError):
    """Malformed, unbound, or insufficient local GPU evidence."""


@dataclass(frozen=True, slots=True)
class GPUDeviceObservation:
    uuid: str
    name: str
    memory_mib: int

    def __post_init__(self) -> None:
        if (not isinstance(self.uuid, str) or not _SAFE_LABEL.fullmatch(self.uuid.strip()) or
                self.uuid != self.uuid.strip()):
            raise GPURuntimeEvidenceError("invalid_gpu_uuid")
        if (not isinstance(self.name, str) or not _SAFE_LABEL.fullmatch(self.name.strip()) or
                self.name != self.name.strip()):
            raise GPURuntimeEvidenceError("invalid_gpu_name")
        if type(self.memory_mib) is not int or not 0 < self.memory_mib <= 2_147_483_647:
            raise GPURuntimeEvidenceError("invalid_gpu_memory")


@dataclass(frozen=True, slots=True)
class GPURuntimeObservation:
    devices: tuple[GPUDeviceObservation, ...]
    driver_version: str
    cuda_umd_version: str
    observed_epoch: int
    output_sha256: str

    def __post_init__(self) -> None:
        if (not isinstance(self.devices, tuple) or not self.devices or
                len(self.devices) > _MAX_GPU_ROWS or
                any(not isinstance(device, GPUDeviceObservation) for device in self.devices)):
            raise GPURuntimeEvidenceError("invalid_gpu_devices")
        uuids = tuple(device.uuid for device in self.devices)
        if len(set(uuids)) != len(uuids):
            raise GPURuntimeEvidenceError("duplicate_gpu_uuid")
        if (not isinstance(self.driver_version, str) or
                not _DRIVER_VERSION.fullmatch(self.driver_version)):
            raise GPURuntimeEvidenceError("invalid_driver_version")
        if (not isinstance(self.cuda_umd_version, str) or
                not _CUDA_VERSION.fullmatch(self.cuda_umd_version)):
            raise GPURuntimeEvidenceError("invalid_cuda_umd_version")
        if type(self.observed_epoch) is not int or self.observed_epoch <= 0:
            raise GPURuntimeEvidenceError("invalid_gpu_observed_epoch")
        if (not isinstance(self.output_sha256, str) or
                not re.fullmatch(r"[0-9a-f]{64}", self.output_sha256)):
            raise GPURuntimeEvidenceError("invalid_gpu_output_digest")


@dataclass(frozen=True, slots=True)
class GPUAttestation:
    """Normalized evidence bound to one exact immutable store attempt.

    This record is not a cryptographic hardware attestation. The strict-SSH
    supervisor verifier produces it and binds its digest to the store's
    append-only stage hash chain.
    """

    project_id: str
    job_id: str
    attempt_id: str
    attempt_sha256: str
    plan_sha256: str
    pod_id: str
    pod_name: str
    profile_sha256: str
    host_key_policy_sha256: str
    probe_sha256: str
    observation: GPURuntimeObservation
    schema_version: int = 1

    def __post_init__(self) -> None:
        for field in ("project_id", "job_id", "attempt_id", "pod_id", "pod_name"):
            value = getattr(self, field)
            if (not isinstance(value, str) or not value or len(value) > 256 or
                    any(ord(char) < 32 or ord(char) == 127 for char in value)):
                raise GPURuntimeEvidenceError("invalid_attestation_identity")
        for field in ("attempt_sha256", "plan_sha256", "profile_sha256",
                      "host_key_policy_sha256", "probe_sha256"):
            if (not isinstance(getattr(self, field), str) or
                    not re.fullmatch(r"[0-9a-f]{64}", getattr(self, field))):
                raise GPURuntimeEvidenceError("invalid_attestation_digest")
        if not isinstance(self.observation, GPURuntimeObservation):
            raise GPURuntimeEvidenceError("invalid_attestation_observation")
        if type(self.schema_version) is not int or self.schema_version != 1:
            raise GPURuntimeEvidenceError("unsupported_attestation_schema")


def gpu_profile_sha256(profile: ResourceProfile) -> str:
    if not isinstance(profile, ResourceProfile):
        raise GPURuntimeEvidenceError("gpu_profile_required")
    return sha256_bytes(encode_document("resource_profile", profile))


def encode_gpu_attestation(attestation: GPUAttestation) -> bytes:
    if not isinstance(attestation, GPUAttestation):
        raise GPURuntimeEvidenceError("gpu_attestation_required")
    return encode_document("gpu_attestation", attestation)


def decode_gpu_attestation(raw: bytes) -> GPUAttestation:
    """Decode the exact bounded sidecar schema; unknown fields fail closed."""
    if not isinstance(raw, bytes) or len(raw) > 65_536:
        raise GPURuntimeEvidenceError("gpu_attestation_out_of_bounds")
    try:
        payload = decode_document(raw, expected_kind="gpu_attestation")
        required = {
            "project_id", "job_id", "attempt_id", "attempt_sha256", "plan_sha256",
            "pod_id", "pod_name", "profile_sha256", "host_key_policy_sha256",
            "probe_sha256", "observation", "schema_version",
        }
        if set(payload) != required or not isinstance(payload.get("observation"), dict):
            raise ContractError("GPU attestation fields are invalid")
        observation = payload["observation"]
        if set(observation) != {
                "devices", "driver_version", "cuda_umd_version", "observed_epoch",
                "output_sha256"} or not isinstance(observation.get("devices"), list):
            raise ContractError("GPU observation fields are invalid")
        devices = []
        for device in observation["devices"]:
            if not isinstance(device, dict) or set(device) != {"uuid", "name", "memory_mib"}:
                raise ContractError("GPU device fields are invalid")
            devices.append(GPUDeviceObservation(
                device["uuid"], device["name"], device["memory_mib"]))
        runtime = GPURuntimeObservation(
            tuple(devices), observation["driver_version"], observation["cuda_umd_version"],
            observation["observed_epoch"], observation["output_sha256"])
        return GPUAttestation(
            project_id=payload["project_id"], job_id=payload["job_id"],
            attempt_id=payload["attempt_id"], attempt_sha256=payload["attempt_sha256"],
            plan_sha256=payload["plan_sha256"], pod_id=payload["pod_id"],
            pod_name=payload["pod_name"], profile_sha256=payload["profile_sha256"],
            host_key_policy_sha256=payload["host_key_policy_sha256"],
            probe_sha256=payload["probe_sha256"], observation=runtime,
            schema_version=payload["schema_version"])
    except (ContractError, KeyError, TypeError, ValueError):
        raise GPURuntimeEvidenceError("gpu_attestation_malformed") from None


def parse_nvidia_smi(gpu_csv: bytes, driver_report: bytes, *,
                     observed_epoch: int) -> GPURuntimeObservation:
    """Parse the two bounded outputs from the fixed nvidia-smi probe.

    GPU memory is normalized in MiB. The driver report's CUDA UMD version is
    preferred; the deprecated `CUDA Version` spelling is accepted for older
    drivers. The version describes driver support, not the workload toolkit.
    """
    if (not isinstance(gpu_csv, bytes) or not isinstance(driver_report, bytes) or
            len(gpu_csv) > _MAX_PROBE_OUTPUT_BYTES or
            len(driver_report) > _MAX_PROBE_OUTPUT_BYTES):
        raise GPURuntimeEvidenceError("probe_output_out_of_bounds")
    try:
        csv_text = gpu_csv.decode("utf-8", errors="strict")
        driver_text = driver_report.decode("utf-8", errors="strict")
        rows = list(csv.reader(io.StringIO(csv_text, newline=""), strict=True))
    except (UnicodeDecodeError, csv.Error):
        raise GPURuntimeEvidenceError("probe_output_malformed") from None
    if not rows or len(rows) > _MAX_GPU_ROWS:
        raise GPURuntimeEvidenceError("gpu_count_out_of_bounds")
    devices = []
    try:
        for row in rows:
            if len(row) != 3:
                raise GPURuntimeEvidenceError("gpu_row_malformed")
            uuid, name, memory = (value.strip() for value in row)
            if len(memory) > 10 or not re.fullmatch(r"[0-9]+", memory):
                raise GPURuntimeEvidenceError("gpu_memory_malformed")
            devices.append(GPUDeviceObservation(uuid, name, int(memory)))
    except (OverflowError, GPURuntimeEvidenceError):
        raise

    header = re.findall(
        r"^\s*NVIDIA-SMI\s+[^\s]+\s+Driver Version:\s*([^\s]+)\s+"
        r"CUDA Version:\s*([0-9]{1,3}\.[0-9]{1,3})\s*$",
        driver_text, flags=re.MULTILINE)
    version_fields = {}
    for label, value in re.findall(
            r"^\s*(KMD Version|Driver Version|CUDA UMD Version|CUDA Version)\s*:\s*([^\r\n]+?)\s*$",
            driver_text, flags=re.MULTILINE):
        version_fields.setdefault(label, []).append(value)
    driver_values = version_fields.get("KMD Version") or version_fields.get("Driver Version") or []
    cuda_values = version_fields.get("CUDA UMD Version") or version_fields.get("CUDA Version") or []
    if header and not driver_values and not cuda_values:
        driver_values, cuda_values = [header[0][0]], [header[0][1]]
    if (len(header) > 1 or len(driver_values) != 1 or len(cuda_values) != 1 or
            not _DRIVER_VERSION.fullmatch(driver_values[0]) or
            not _CUDA_VERSION.fullmatch(cuda_values[0])):
        raise GPURuntimeEvidenceError("driver_report_incomplete")
    output_sha256 = hashlib.sha256(canonical_json_bytes({
        "gpu_csv_sha256": hashlib.sha256(gpu_csv).hexdigest(),
        "driver_report_sha256": hashlib.sha256(driver_report).hexdigest(),
    })).hexdigest()
    return GPURuntimeObservation(tuple(devices), driver_values[0], cuda_values[0],
                                 observed_epoch, output_sha256)


def validate_gpu_observation(observation: GPURuntimeObservation,
                             profile: ResourceProfile) -> None:
    """Require the observed card count, decimal-GB VRAM floor and CUDA driver floor."""
    if (not isinstance(observation, GPURuntimeObservation) or
            not isinstance(profile, ResourceProfile) or profile.kind is not ResourceKind.GPU):
        raise GPURuntimeEvidenceError("gpu_profile_required")
    if len(observation.devices) != profile.gpu_count:
        raise GPURuntimeEvidenceError("gpu_count_mismatch")
    # Catalog GB is compared against MiB using decimal GB. For example, a
    # nominal 48 GB card may report about 46,000 MiB, which still exceeds 48
    # decimal GB after conversion from MiB.
    required_bytes = profile.gpu_memory_gb * 1_000_000_000
    if any(device.memory_mib * 1_048_576 < required_bytes for device in observation.devices):
        raise GPURuntimeEvidenceError("gpu_vram_below_profile")
    if not _CUDA_VERSION.fullmatch(profile.minimum_cuda):
        raise GPURuntimeEvidenceError("invalid_profile_cuda_floor")
    observed_cuda = tuple(int(part) for part in observation.cuda_umd_version.split("."))
    required_cuda = tuple(int(part) for part in profile.minimum_cuda.split("."))
    if observed_cuda < required_cuda:
        raise GPURuntimeEvidenceError("cuda_driver_below_profile")


def validate_gpu_attestation(attestation: GPUAttestation, *, profile: ResourceProfile,
                             expected: dict[str, str], now_epoch: int) -> None:
    """Bind evidence to expected attempt/Pod hashes and reject future observations."""
    if (not isinstance(attestation, GPUAttestation) or
            not isinstance(profile, ResourceProfile) or not isinstance(expected, dict)):
        raise GPURuntimeEvidenceError("gpu_attestation_required")
    profile_digest = gpu_profile_sha256(profile)
    required = {
        "project_id": expected.get("project_id"),
        "job_id": expected.get("job_id"),
        "attempt_id": expected.get("attempt_id"),
        "attempt_sha256": expected.get("attempt_sha256"),
        "plan_sha256": expected.get("plan_sha256"),
        "pod_id": expected.get("pod_id"),
        "pod_name": expected.get("pod_name"),
        "profile_sha256": profile_digest,
        "host_key_policy_sha256": expected.get("host_key_policy_sha256"),
        "probe_sha256": GPU_PROBE_SPEC_SHA256,
    }
    if (any(value is None for value in required.values()) or
            any(getattr(attestation, field) != value for field, value in required.items())):
        raise GPURuntimeEvidenceError("gpu_attestation_identity_mismatch")
    if type(now_epoch) is not int or now_epoch < attestation.observation.observed_epoch:
        raise GPURuntimeEvidenceError("gpu_attestation_time_invalid")
    validate_gpu_observation(attestation.observation, profile)
