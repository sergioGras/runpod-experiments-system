import tempfile
import unittest
from pathlib import Path

from runpod_experiments.host_identity import (
    current_boot_identity, same_boot_identity, supervisor_process_lock,
    valid_boot_identity,
)


class HostIdentityTests(unittest.TestCase):
    def test_linux_boot_uuid_is_read_and_validated(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "boot_id"
            path.write_text("01234567-89AB-CDEF-0123-456789ABCDEF\n", encoding="ascii")
            identity = current_boot_identity(
                platform_name="linux", linux_boot_id_path=path)
        self.assertEqual(identity, {
            "kind": "linux_boot_uuid",
            "value": "01234567-89ab-cdef-0123-456789abcdef",
        })
        self.assertTrue(valid_boot_identity(identity))

    def test_missing_or_malformed_linux_boot_uuid_is_unsupported(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "boot_id"
            self.assertIsNone(current_boot_identity(
                platform_name="linux", linux_boot_id_path=path))
            path.write_text("not-a-boot-id", encoding="ascii")
            self.assertIsNone(current_boot_identity(
                platform_name="linux", linux_boot_id_path=path))

    def test_windows_boot_estimate_has_a_small_restart_tolerance(self):
        first = current_boot_identity(
            platform_name="nt", clock=lambda: 1000.0, uptime_ms=30_000)
        near = current_boot_identity(
            platform_name="nt", clock=lambda: 1003.0, uptime_ms=30_000)
        far = current_boot_identity(
            platform_name="nt", clock=lambda: 1010.0, uptime_ms=30_000)
        self.assertTrue(valid_boot_identity(first))
        self.assertTrue(same_boot_identity(first, near))
        self.assertFalse(same_boot_identity(first, far))

    def test_unknown_platform_has_no_boot_identity(self):
        self.assertIsNone(current_boot_identity(platform_name="unsupported"))

    def test_supervisor_lock_prevents_duplicate_process_and_releases_cleanly(self):
        with tempfile.TemporaryDirectory() as directory:
            with supervisor_process_lock(directory):
                with self.assertRaisesRegex(RuntimeError, "supervisor_already_running"):
                    with supervisor_process_lock(directory):
                        self.fail("second supervisor acquired the same attempt lock")
            with supervisor_process_lock(directory):
                pass


if __name__ == "__main__":
    unittest.main()
