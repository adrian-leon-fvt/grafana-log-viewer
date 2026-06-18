import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from decoder.utils import get_latest_vm_job_timestamp


class LatestVmJobTimestampTest(unittest.TestCase):
    def test_returns_latest_timestamp(self) -> None:
        class Response:
            status_code = 200

            def json(self):
                return {
                    "data": {
                        "result": [
                            {
                                "metric": {"job": "B3SR"},
                                "value": [1710000000, "1719999999"],
                            }
                        ]
                    }
                }

        with patch("decoder.utils.requests.get", return_value=Response()) as get:
            result = get_latest_vm_job_timestamp(
                "http://victoriametrics",
                "B3SR",
                metric_name="AllFansDriveStatus",
                label_filters={"message": "LOOP_ERRORS_DIAG"},
                lookback="30d",
            )

        self.assertTrue(result["has_data"])
        self.assertEqual(
            result["timestamp"],
            datetime.fromtimestamp(1719999999, tz=timezone.utc),
        )
        self.assertEqual(
            result["query"],
            'max(timestamp(AllFansDriveStatus{job="B3SR",message="LOOP_ERRORS_DIAG"}))',
        )
        get.assert_called_once()

    def test_returns_empty_when_no_series(self) -> None:
        class Response:
            status_code = 200

            def json(self):
                return {"data": {"result": []}}

        with patch("decoder.utils.requests.get", return_value=Response()):
            result = get_latest_vm_job_timestamp(
                "http://victoriametrics",
                "B3SR",
            )

        self.assertFalse(result["has_data"])
        self.assertIsNone(result["timestamp"])


if __name__ == "__main__":
    unittest.main()
