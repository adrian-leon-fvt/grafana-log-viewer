import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("run_with_cursor.py")


class RunWithCursorTest(unittest.TestCase):
    def test_basis_change_ignores_old_cursor_and_persists_new_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state_path = root / "state.json"
            output_path = root / "output.json"
            capture_path = root / "capture.json"
            state_path.write_text(
                json.dumps(
                    {
                        "last_timestamp": "2026-01-01T00:00:00+00:00",
                        "last_key": "old.mf4",
                    }
                ),
                encoding="utf-8",
            )
            child = (
                "import json,sys; "
                "from pathlib import Path; "
                "Path(sys.argv[1]).write_text(json.dumps(sys.argv[2:])); "
                "Path(sys.argv[4]).write_text(json.dumps({"
                "'last_timestamp':'2026-09-15T12:00:00+00:00',"
                "'last_key':'new.mf4'}))"
            )

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--state-file",
                    str(state_path),
                    "--cursor-output-file",
                    str(output_path),
                    "--cursor-basis",
                    "last-modified",
                    "--basis-change-lookback-seconds",
                    "86400",
                    "--",
                    sys.executable,
                    "-c",
                    child,
                    str(capture_path),
                    "{start}",
                    "{cursor_ts}",
                    "{cursor_out}",
                ],
                check=False,
            )

            self.assertEqual(result.returncode, 0)
            captured = json.loads(capture_path.read_text(encoding="utf-8"))
            self.assertEqual(captured[1], "")
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(state["cursor_basis"], "last-modified")
            self.assertEqual(state["last_key"], "new.mf4")

    def test_missing_output_preserves_cursor(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state_path = root / "state.json"
            output_path = root / "output.json"
            original = {
                "cursor_basis": "last-modified",
                "last_timestamp": "2026-09-15T12:00:00+00:00",
                "last_key": "existing.mf4",
            }
            state_path.write_text(json.dumps(original), encoding="utf-8")

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--state-file",
                    str(state_path),
                    "--cursor-output-file",
                    str(output_path),
                    "--cursor-basis",
                    "last-modified",
                    "--",
                    sys.executable,
                    "-c",
                    "pass",
                ],
                check=False,
            )

            self.assertEqual(result.returncode, 0)
            self.assertEqual(
                json.loads(state_path.read_text(encoding="utf-8")), original
            )


if __name__ == "__main__":
    unittest.main()
