import unittest
from pathlib import Path

from decoder.D65.send_d65_data import get_d65_dbc_files, resolve_dbc_folder


class D65DbcPathTest(unittest.TestCase):
    def test_flat_decoder_folder_is_primary(self) -> None:
        base = Path("/mnt/d/utils/grafana-log-viewer/decoder/D65/dbc")
        self.assertEqual(resolve_dbc_folder(str(base)), base)

        dbc_files = get_d65_dbc_files(str(base))
        self.assertTrue(all(path.parent == base for path in dbc_files["Upper"]))
        self.assertTrue(all(path.parent == base for path in dbc_files["Lower"]))


if __name__ == "__main__":
    unittest.main()
