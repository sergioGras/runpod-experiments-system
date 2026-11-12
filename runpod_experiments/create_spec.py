"""Build the narrow, canonical RunPod Pod request from stored typed inputs."""
from __future__ import annotations

from runpod_experiments.contracts import (
    ContractError, PodProvisioningSpec, ResourceKind, ResourceProfile,
)
from runpod_experiments.resource_policy import request_hash


def build_pod_create_request(
    provisioning: PodProvisioningSpec,
    resource: ResourceProfile,
    selected_region: str,
    pod_name: str,
) -> dict:
    """Return only the reviewed request fields represented by the plan schema."""
    if not isinstance(provisioning, PodProvisioningSpec):
        raise ContractError("provisioning must be a PodProvisioningSpec")
    if not isinstance(resource, ResourceProfile):
        raise ContractError("resource must be a ResourceProfile")
    if not isinstance(selected_region, str) or selected_region not in resource.region_ids:
        raise ContractError("selected region is outside the resource profile")
    if (not isinstance(pod_name, str) or not pod_name or pod_name != pod_name.strip() or
            len(pod_name) > 128 or not all(character.isprintable() for character in pod_name)):
        raise ContractError("Pod name is malformed")
    image_digest = provisioning.image_ref.rsplit("@", 1)[-1]
    if image_digest != resource.image_digest:
        raise ContractError("image reference differs from the selected resource profile")

    request = {
        "name": pod_name,
        "cloud": provisioning.cloud_type,
        "dataCenterIds": [selected_region],
        "image": provisioning.image_ref,
        "disk": resource.disk_gb,
        "ports": list(provisioning.ports),
        "startSsh": provisioning.start_ssh,
        "startJupyter": provisioning.start_jupyter,
    }
    if resource.kind is ResourceKind.CPU:
        request["cpu"] = {"id": resource.resource_id, "vcpuCount": resource.cpu_count}
    else:
        request["gpu"] = {
            "id": resource.resource_id,
            "count": resource.gpu_count,
            # The API fields are per GPU; the resource profile expresses the
            # minimum total host memory and vCPU shape for the whole Pod.
            "minRamPerGpu": (resource.memory_gb + resource.gpu_count - 1) // resource.gpu_count,
            "minVcpuCountPerGpu": (resource.cpu_count + resource.gpu_count - 1) // resource.gpu_count,
            "minCudaVersion": resource.minimum_cuda,
        }
    return request


def pod_create_request_sha256(
    provisioning: PodProvisioningSpec,
    resource: ResourceProfile,
    selected_region: str,
    pod_name: str,
) -> str:
    """Digest the exact canonical request that will be sent to the provider."""
    return request_hash(build_pod_create_request(
        provisioning, resource, selected_region, pod_name))
