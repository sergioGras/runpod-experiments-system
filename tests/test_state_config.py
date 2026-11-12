import tempfile
import unittest
from pathlib import Path

from runpod_experiments.state_config import (
    CONTROLLER_HOST_ENV, STATE_ROOT_ENV, StateStoreConfig,
)
from runpod_experiments.errors import ExecutionError


class StateStoreConfigTests(unittest.TestCase):
    def test_same_explicit_root_resolves_same_database_across_checkouts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "shared-state"
            env = {STATE_ROOT_ENV: str(root), CONTROLLER_HOST_ENV: "LAB-HOST-1"}
            first_checkout = StateStoreConfig.from_environment(dict(env))
            second_checkout = StateStoreConfig.from_environment(dict(env))
            expected = root.resolve() / "synthetic-nebula" / "execution.sqlite3"
            self.assertEqual(first_checkout.database_path("synthetic-nebula"), expected)
            self.assertEqual(second_checkout.database_path("synthetic-nebula"), expected)
            self.assertNotEqual(first_checkout.database_path("synthetic-nebula"),
                                first_checkout.database_path("synthetic-other"))
            self.assertFalse(root.exists())
            prepared = first_checkout.prepare_database_path("synthetic-nebula")
            self.assertEqual(prepared, expected)
            self.assertTrue(prepared.parent.is_dir())
            store = second_checkout.open_execution_store(
                "synthetic-nebula", clock=lambda: 1_800_000_000)
            self.assertEqual(Path(store.path), expected)
            self.assertEqual(store.project_id, "synthetic-nebula")
            self.assertEqual(store.controller_host, "LAB-HOST-1")
            wrong_host = StateStoreConfig(root, "LAB-HOST-2")
            with self.assertRaises(ExecutionError) as caught:
                wrong_host.open_execution_store("synthetic-nebula")
            self.assertEqual(caught.exception.code, "store_identity_mismatch")

    def test_state_root_and_controller_host_must_be_explicit(self):
        for env in (
                {},
                {STATE_ROOT_ENV: "relative/path", CONTROLLER_HOST_ENV: "LAB-HOST"},
                {STATE_ROOT_ENV: "C:/state", CONTROLLER_HOST_ENV: ""},
                {STATE_ROOT_ENV: " C:/state ", CONTROLLER_HOST_ENV: "LAB-HOST"},
        ):
            with self.subTest(env=env):
                with self.assertRaises(ValueError):
                    StateStoreConfig.from_environment(env)

    def test_existing_non_directory_root_and_invalid_project_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root_file = Path(directory) / "state-file"
            root_file.write_text("not a directory", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "directory"):
                StateStoreConfig(root_file, "LAB-HOST")

            config = StateStoreConfig(Path(directory) / "state", "LAB-HOST")
            with self.assertRaisesRegex(ValueError, "project_id"):
                config.database_path("../other")
            config.state_root.mkdir()
            project_path = config.state_root / "synthetic-nebula"
            project_path.write_text("not a directory", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "project state path"):
                config.database_path("synthetic-nebula")
            project_path.unlink()
            project_path.mkdir()
            (project_path / "execution.sqlite3").mkdir()
            with self.assertRaisesRegex(ValueError, "database path"):
                config.database_path("synthetic-nebula")

    def test_project_and_database_symlinks_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "state"
            outside = Path(directory) / "outside"
            root.mkdir()
            outside.mkdir()
            config = StateStoreConfig(root, "LAB-HOST")
            project_link = root / "synthetic-nebula"
            try:
                project_link.symlink_to(outside, target_is_directory=True)
            except (OSError, NotImplementedError):
                self.skipTest("directory symlinks are unavailable on this host")
            with self.assertRaisesRegex(ValueError, "symlink"):
                config.database_path("synthetic-nebula")

            project_link.unlink()
            project_link.mkdir()
            db_link = project_link / "execution.sqlite3"
            try:
                db_link.symlink_to(outside / "other.sqlite3")
            except (OSError, NotImplementedError):
                self.skipTest("file symlinks are unavailable on this host")
            with self.assertRaisesRegex(ValueError, "symlink"):
                config.database_path("synthetic-nebula")


if __name__ == "__main__":
    unittest.main()
