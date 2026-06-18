import unittest
from datetime import datetime, timezone, timedelta

from asammdf import Signal

from decoder.sending import _apply_job_watermark


class SignalWatermarkTest(unittest.TestCase):
    def setUp(self) -> None:
        self.start_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.signal = Signal(
            samples=[1, 2, 3, 4],
            timestamps=[0.0, 1.0, 2.0, 3.0],
            name="TestSignal",
            unit="u",
        )

    def test_returns_none_when_watermark_covers_signal(self) -> None:
        result = _apply_job_watermark(
            self.signal,
            self.start_time,
            self.start_time + timedelta(seconds=3),
        )
        self.assertIsNone(result)

    def test_keeps_signal_when_watermark_is_before_signal(self) -> None:
        result = _apply_job_watermark(
            self.signal,
            self.start_time,
            self.start_time - timedelta(seconds=1),
        )
        self.assertIsNotNone(result)
        self.assertEqual(list(result.timestamps), [0.0, 1.0, 2.0, 3.0])

    def test_trims_signal_after_watermark(self) -> None:
        result = _apply_job_watermark(
            self.signal,
            self.start_time,
            self.start_time + timedelta(seconds=1.5),
        )
        self.assertIsNotNone(result)
        self.assertEqual(list(result.timestamps), [2.0, 3.0])
        self.assertEqual(list(result.samples), [3, 4])


if __name__ == "__main__":
    unittest.main()
