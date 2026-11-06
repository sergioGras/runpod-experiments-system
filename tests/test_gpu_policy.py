import unittest

from runpod_experiments.planning import gpu_resource_policy, verify_gpu_quote
from runpod_experiments.resource_policy import ResourcePolicy
from runpod_experiments.runpod import RunPodClient


IMAGE = "runpod/pytorch@sha256:" + "a" * 64


def request():
    return {"name": "project-gpu-test-0001", "gpu": {"id": "NVIDIA A40", "count": 1},
            "cloud": "SECURE", "dataCenterIds": ["EU-SE-1"], "image": IMAGE,
            "disk": 30, "ports": ["22/tcp"], "startSsh": True, "startJupyter": False}


def offer(price=0.59):
    return {"id": "NVIDIA A40", "secure": True, "price": {"secure": price},
            "maxCount": {"secure": 10}, "memory": 48}


class GPUPolicyTests(unittest.TestCase):
    def test_gpu_policy_binds_request_rate_and_provider_shape(self):
        req = request()
        data = gpu_resource_policy(req, offer(), 0.002)
        self.assertAlmostEqual(data["max_hourly_usd"], 0.592)
        self.assertEqual(data["expected_shape"]["gpu"], {"id": "NVIDIA A40", "count": 1})
        policy = ResourcePolicy(data, "fingerprint")
        pod = {"status": "RUNNING", "cost": 0.59, "cloud": "SECURE", "image": IMAGE,
               "disk": 30, "ports": ["22/tcp"], "dataCenterId": "EU-SE-1",
               "gpu": {"id": "NVIDIA A40", "count": 1, "memory": 48, "vcpuCount": 8},
               "mounts": {"persistent": None, "network": None}}
        self.assertIsNone(policy.violation(pod))
        pod["gpu"]["id"] = "NVIDIA A100"
        self.assertEqual(policy.violation(pod), "unexpected_resource_shape")

    def test_secure_quote_recheck_blocks_rate_or_shape_change(self):
        req = request()
        policy = ResourcePolicy(gpu_resource_policy(req, offer(), 0.002), "fingerprint")
        class Client:
            def gpu_offer(self, gpu_id, count):
                self.assertions = (gpu_id, count)
                return offer(0.75)
        with self.assertRaisesRegex(ValueError, "differs"):
            verify_gpu_quote(Client(), req, policy, 0.002)
        class WrongGPU(Client):
            def gpu_offer(self, gpu_id, count):
                result = offer()
                result["id"] = "NVIDIA A100"
                return result
        with self.assertRaisesRegex(ValueError, "Invalid provider GPU quote"):
            verify_gpu_quote(WrongGPU(), req, policy, 0.002)

    def test_invalid_scope_and_quote_are_rejected(self):
        req = request()
        for mutate in (lambda x: x.update(cloud="COMMUNITY"),
                       lambda x: x.update(extra="field"),
                       lambda x: x.update(dataCenterIds=["invalid-region"]),
                       lambda x: x["gpu"].update(count=True)):
            bad = request()
            mutate(bad)
            with self.assertRaises(ValueError):
                gpu_resource_policy(bad, offer(), 0.002)
        for bad_offer in ({**offer(), "secure": False}, {**offer(), "maxCount": {"secure": 0}},
                          {**offer(), "price": {"secure": float("nan")}}):
            with self.assertRaises(ValueError):
                gpu_resource_policy(req, bad_offer, 0.002)

    def test_runpod_client_encodes_and_validates_gpu_catalog_path(self):
        observed = []
        class Client(RunPodClient):
            def _get(self, path):
                observed.append(path)
                return offer()
        client = Client("test-key")
        self.assertEqual(client.gpu_offer("NVIDIA A40", 1)["id"], "NVIDIA A40")
        self.assertEqual(observed, ["/catalog/gpus/NVIDIA%20A40?include=AVAILABILITY&product=POD&count=1"])
        with self.assertRaises(ValueError):
            client.gpu_offer("../bad", 1)
        with self.assertRaises(ValueError):
            client.gpu_offer("NVIDIA A40", True)


if __name__ == "__main__":
    unittest.main()
