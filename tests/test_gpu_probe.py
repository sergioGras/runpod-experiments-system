import hashlib
import io
from pathlib import Path
import subprocess
import tempfile
import unittest

from runpod_experiments.gpu_probe import (
    GPUProbeTransportError, _run_bounded, probe_gpu_runtime,
)
from runpod_experiments.provider import SSHRouteEndpoint


FRAME = (
    b"__RPE_DRIVER_BEGIN__\n"
    b"NVIDIA-SMI 550.54.14 Driver Version: 550.54.14 CUDA Version: 12.8\n"
    b"__RPE_GPU_BEGIN__\n"
    b"GPU-a1,NVIDIA A40,46068\n"
)


class GPUProbeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="gpu-probe-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.key = self.root / "id_ed25519"
        self.key.write_bytes(b"synthetic-private-key")
        self.known_hosts = self.root / "known_hosts"
        self.known_hosts.write_bytes(b"[gpu.example]:22 ssh-ed25519 AAAA\n")
        self.endpoint = SSHRouteEndpoint("gpu.example", 22, "root", "direct")

    def test_probe_uses_fixed_read_only_command_and_strict_direct_host_key(self):
        captured = {}

        def runner(argv, **kwargs):
            captured.update(argv=argv, kwargs=kwargs)
            return FRAME

        observed, policy_sha256 = probe_gpu_runtime(
            self.endpoint, self.key, self.known_hosts,
            observed_epoch=1_900_000_000, runner=runner)
        self.assertEqual(observed.devices[0].uuid, "GPU-a1")
        self.assertEqual(policy_sha256, hashlib.sha256(self.known_hosts.read_bytes()).hexdigest())
        argv = captured["argv"]
        self.assertEqual(argv[1:3], ["-F", "none"])
        self.assertIn("StrictHostKeyChecking=yes", argv)
        self.assertIn("UserKnownHostsFile=" + str(self.known_hosts), argv)
        self.assertIn("ProxyCommand=none", argv)
        self.assertIn("ForwardAgent=no", argv)
        self.assertIn("nvidia-smi --query-gpu=uuid,name,memory.total", argv[-1])
        self.assertNotIn("nvidia-smi -q", argv[-1])
        self.assertEqual(captured["kwargs"]["timeout_seconds"], 20)
        self.assertEqual(captured["kwargs"]["max_output_bytes"], 32_768)

    def test_rejects_non_direct_route_and_malformed_probe_frame(self):
        proxy = SSHRouteEndpoint("gpu.example", 443, "root", "proxy")
        with self.assertRaisesRegex(GPUProbeTransportError, "direct_route_required"):
            probe_gpu_runtime(proxy, self.key, self.known_hosts,
                              observed_epoch=1_900_000_000)
        with self.assertRaisesRegex(ValueError, "probe_frame_malformed"):
            probe_gpu_runtime(
                self.endpoint, self.key, self.known_hosts,
                observed_epoch=1_900_000_000, runner=lambda *_a, **_k: b"unexpected")

    def test_rejects_changed_known_hosts_policy_during_probe(self):
        def runner(_argv, **_kwargs):
            self.known_hosts.write_bytes(b"changed host key policy\n")
            return FRAME

        with self.assertRaisesRegex(GPUProbeTransportError, "known_hosts_changed"):
            probe_gpu_runtime(self.endpoint, self.key, self.known_hosts,
                              observed_epoch=1_900_000_000, runner=runner)

    def test_local_output_reader_enforces_byte_limit(self):
        class Process:
            def __init__(self, output):
                self.stdout = io.BytesIO(output)
                self.killed = False

            def wait(self, timeout=None):
                return 0

            def kill(self):
                self.killed = True

        valid = Process(b"12345")
        self.assertEqual(_run_bounded(["ssh"], timeout_seconds=2,
                                      max_output_bytes=5, popen=lambda *_a, **_k: valid),
                         b"12345")
        oversized = Process(b"123456")
        with self.assertRaisesRegex(GPUProbeTransportError, "output_exceeded"):
            _run_bounded(["ssh"], timeout_seconds=2, max_output_bytes=5,
                         popen=lambda *_a, **_k: oversized)
        self.assertTrue(oversized.killed)

    def test_local_timeout_terminates_the_ssh_process(self):
        class Process:
            stdout = io.BytesIO(b"")
            killed = False
            waits = 0

            def wait(self, timeout=None):
                self.waits += 1
                if self.waits == 1:
                    raise subprocess.TimeoutExpired("ssh", timeout)
                return -9

            def kill(self):
                self.killed = True

        process = Process()
        with self.assertRaisesRegex(GPUProbeTransportError, "timeout"):
            _run_bounded(["ssh"], timeout_seconds=2,
                         popen=lambda *_a, **_k: process)
        self.assertTrue(process.killed)


if __name__ == "__main__":
    unittest.main()
