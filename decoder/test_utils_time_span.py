import unittest
from datetime import datetime, timezone

from decoder.utils import format_time_span


class TimeSpanTest(unittest.TestCase):
    def test_formats_span(self) -> None:
        start = datetime(2026, 6, 18, 10, 0, tzinfo=timezone.utc)
        end = datetime(2026, 6, 19, 12, 3, 4, 500000, tzinfo=timezone.utc)
        self.assertEqual(format_time_span(start, end), "1d2h3m4.500s")


if __name__ == "__main__":
    unittest.main()
