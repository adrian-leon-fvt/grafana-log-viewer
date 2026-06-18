import unittest
from datetime import datetime, timezone

from decoder.utils import parse_time_arg


class ParseTimeArgTest(unittest.TestCase):
    def test_parses_absolute_naive_datetime_in_local_timezone(self) -> None:
        now = datetime(2026, 6, 18, 13, 0, tzinfo=timezone.utc)
        result = parse_time_arg("2026-06-17T11:00:00", now)
        self.assertEqual(result.isoformat(), "2026-06-17T11:00:00+00:00")

    def test_parses_offset_and_now(self) -> None:
        now = datetime(2026, 6, 18, 13, 0, tzinfo=timezone.utc)
        self.assertEqual(
            parse_time_arg("2h", now).isoformat(),
            "2026-06-18T11:00:00+00:00",
        )
        self.assertEqual(parse_time_arg("now", now), now)


if __name__ == "__main__":
    unittest.main()
