import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from decoder.D65 import send_d65_data as d65


class D65InputFolderTest(unittest.TestCase):
    def test_local_folder_selection_and_cli_validation(self) -> None:
        now = datetime.now(timezone.utc)
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            upper = folder / "Upper" / "recording.mf4"
            upper.parent.mkdir()
            upper.touch()
            lower = folder / "Lower" / "recording.MF4"
            lower.parent.mkdir()
            lower.touch()
            with (
                patch.object(d65, "get_mdf_start_time", return_value=now),
                patch.object(d65, "get_d65_cancloud_folder") as default_folder,
            ):
                files = d65.get_all_unique_d65_files(
                    start=now - timedelta(seconds=1),
                    end=now + timedelta(seconds=1),
                    input_folder=folder,
                )
                self.assertEqual({path for path, _, _ in files}, {upper, lower})
                self.assertEqual({job for _, job, _ in files}, {"Upper", "Lower"})
                default_folder.assert_not_called()

            command = [
                sys.executable,
                "-m",
                "decoder.D65.send_d65_data",
                "--skip_post",
            ]
            for option in ("--input-folder", "--folder"):
                result = subprocess.run(
                    command + [option, str(folder)],
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
            for arguments, error in (
                (["--input-folder", str(upper)], "existing directory"),
                (["--input-folder", str(folder / "missing")], "existing directory"),
                (
                    ["--input-folder", str(folder), "--s3-streaming"],
                    "cannot be used with --s3-streaming",
                ),
            ):
                result = subprocess.run(
                    command + arguments, capture_output=True, text=True
                )
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn(error, result.stderr)


if __name__ == "__main__":
    unittest.main()
