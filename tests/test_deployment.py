"""Exercise the actual deployment script with fake HF/HTTP boundaries, no credentials."""
import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import textwrap
import types
import unittest
from unittest.mock import patch


SCRIPT = textwrap.dedent(
    (Path(__file__).resolve().parents[1] / ".github/workflows/ci-deploy.yml")
    .read_text(encoding="utf-8").split("python - <<'PY'\n", 1)[1].rsplit("          PY", 1)[0]
)


class DeploymentTests(unittest.TestCase):
    def run_deployment(self, existing, ready=True, recipient=None, current_recipient=None, export_matches=True):
        events, configured = [], set(existing)
        clock = [0]

        class FakeApi:
            def __init__(self, token):
                assert token == "test-hf-token"

            def get_space_secrets(self, **_):
                return {key: object() for key in configured}

            def get_space_variables(self, **_):
                return {} if current_recipient is None else {"MESH_KEY_RECOVERY_RECIPIENT": types.SimpleNamespace(value=current_recipient)}

            def add_space_variable(self, key, value, **_):
                assert key == "MESH_KEY_RECOVERY_RECIPIENT" and value == recipient
                events.append(("pin", value))

            def add_space_secret(self, key, value, **_):
                if key.endswith("ENCRYPTION_KEY"):
                    assert len(bytes.fromhex(value)) == 32
                events.append(("add", key))
                configured.add(key)

            def delete_space_secret(self, key, **_):
                events.append(("delete", key))
                configured.remove(key)

            def upload_folder(self, **_):
                assert "MESH_STATE_ENCRYPTION_KEY" in configured
                events.append(("upload",))
                return types.SimpleNamespace(oid="uploaded-revision")

            def get_space_runtime(self, **_):
                return types.SimpleNamespace(stage="RUNNING", raw={
                    "domains": [{"domain": "test-space.hf.space"}],
                    "sha": "uploaded-revision" if ready else "previous-revision",
                })

        class FakeResponse:
            headers = {"x-mesh-state-format": "mesh-state-aes256gcm-v1",
                       "x-mesh-recovery-recipient": recipient if export_matches else "wrong-recipient"}

            def read(self, _):
                return b"age-encryption.org/v1\nsynthetic encrypted backup"

            def __enter__(self):
                events.append(("health",))
                return self

            def __exit__(self, *_):
                return False

        def monotonic():
            clock[0] += 10
            return clock[0]

        def sleep(seconds):
            clock[0] += seconds

        with tempfile.TemporaryDirectory() as directory:
            output = io.StringIO()
            with patch.dict(os.environ, {
                "HF_TOKEN": "test-hf-token", "HF_SPACE_REPO": "https://huggingface.co/spaces/test/space",
                "GITHUB_SHA": "test-source-revision", "GITHUB_STEP_SUMMARY": str(Path(directory) / "summary"),
            }), patch.dict("sys.modules", {"huggingface_hub": types.SimpleNamespace(HfApi=FakeApi)}), \
                patch("urllib.request.urlopen", side_effect=lambda *_a, **_k: FakeResponse()), \
                patch("time.monotonic", side_effect=monotonic), patch("time.sleep", side_effect=sleep), \
                patch.object(Path, "read_text", return_value=json.dumps({"test/space": recipient} if recipient else {})), \
                patch("secrets.token_hex", return_value="ab" * 32), contextlib.redirect_stdout(output):
                failure = None
                try:
                    exec(compile(SCRIPT, "deployment", "exec"), {})
                except SystemExit as error:
                    failure = error
            self.assertNotIn("ab" * 32, output.getvalue())
        return events, failure

    def test_first_deployment_bounds_import_then_removes_permission_after_readiness(self):
        events, failure = self.run_deployment({"MESH_AUTH_ENCRYPTION_KEY"})
        self.assertIsNone(failure)
        migration = "MESH_STATE_PLAINTEXT_MIGRATION_UNTIL"
        self.assertLess(events.index(("add", migration)), events.index(("add", "MESH_STATE_ENCRYPTION_KEY")))
        self.assertLess(events.index(("upload",)), events.index(("health",)))
        self.assertLess(events.index(("health",)), events.index(("delete", migration)))
        self.assertEqual(sum(e == ("health",) for e in events), 2)

    def test_later_deployment_preserves_keys_and_never_reopens_plaintext_import(self):
        events, failure = self.run_deployment({"MESH_AUTH_ENCRYPTION_KEY", "MESH_STATE_ENCRYPTION_KEY"})
        self.assertIsNone(failure)
        self.assertFalse(any(e[0] in ("add", "delete") for e in events))

    def test_previous_container_cannot_confirm_upload_or_remove_import_permission(self):
        events, failure = self.run_deployment({"MESH_AUTH_ENCRYPTION_KEY"}, ready=False)
        self.assertIsInstance(failure, SystemExit)
        self.assertFalse(any(e[0] == "delete" for e in events))

    def test_retry_can_close_existing_import_without_replacing_keys(self):
        events, failure = self.run_deployment({"MESH_AUTH_ENCRYPTION_KEY", "MESH_STATE_ENCRYPTION_KEY",
                                               "MESH_STATE_PLAINTEXT_MIGRATION_UNTIL"})
        self.assertIsNone(failure)
        self.assertFalse(any(e[0] == "add" for e in events))
        self.assertIn(("delete", "MESH_STATE_PLAINTEXT_MIGRATION_UNTIL"), events)

    def test_recovery_pins_only_public_recipient_and_checks_export(self):
        recipient = "age1" + "q" * 58
        events, failure = self.run_deployment({"MESH_AUTH_ENCRYPTION_KEY", "MESH_STATE_ENCRYPTION_KEY"}, recipient=recipient)
        self.assertIsNone(failure)
        self.assertIn(("pin", recipient), events)
        self.assertFalse(any(e[0] == "add" for e in events))

    def test_recovery_never_silently_replaces_an_existing_recipient(self):
        events, failure = self.run_deployment({"MESH_AUTH_ENCRYPTION_KEY", "MESH_STATE_ENCRYPTION_KEY"},
            recipient="age1" + "q" * 58, current_recipient="age1" + "p" * 58)
        self.assertIsInstance(failure, SystemExit)
        self.assertFalse(any(e[0] in ("pin", "upload") for e in events))

    def test_recovery_export_must_match_the_requested_recipient(self):
        _, failure = self.run_deployment({"MESH_AUTH_ENCRYPTION_KEY", "MESH_STATE_ENCRYPTION_KEY"},
            recipient="age1" + "q" * 58, export_matches=False)
        self.assertIsInstance(failure, SystemExit)


if __name__ == "__main__":
    unittest.main()
