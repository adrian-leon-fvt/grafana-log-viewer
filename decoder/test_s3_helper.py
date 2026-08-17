import unittest
from unittest.mock import patch

from decoder.s3_helper import (
    EESBuckets,
    get_new_mf4_files_summary_from_s3,
    _parse_s3_timestamp,
)


class NewMf4FilesSummaryTest(unittest.TestCase):
    def test_parse_timestamp_with_and_without_z(self) -> None:
        self.assertEqual(
            _parse_s3_timestamp("20240724T173516").isoformat(),
            "2024-07-24T10:35:16-07:00",
        )
        self.assertEqual(
            _parse_s3_timestamp("2024-07-24T17:35:16Z").isoformat(),
            "2024-07-24T10:35:16-07:00",
        )

    def test_multibucket_summary_passthrough(self) -> None:
        calls: list[tuple[object, dict[str, str]]] = []

        def fake_list(*, bucket_name, start_time, end_time, **kwargs):
            calls.append((bucket_name, kwargs))
            if bucket_name == EESBuckets.S3_BUCKET_D65:
                return [{"Key": "d65/1.mf4"}, {"Key": "d65/2.mf4"}]
            if bucket_name == "garland-telematics":
                return [{"Key": "garland/1.mf4"}]
            return []

        with patch(
            "decoder.s3_helper.get_mf4_files_list_from_s3",
            side_effect=fake_list,
        ):
            summary = get_new_mf4_files_summary_from_s3(
                bucket_names=(EESBuckets.S3_BUCKET_D65, "garland-telematics"),
                start_time="2026-06-01T00:00:00+00:00",
                end_time="2026-06-02T00:00:00+00:00",
                posted_after="2026-06-01T12:00:00+00:00",
                Prefix="telemetry/",
            )

        self.assertTrue(summary["has_new_files"])
        self.assertEqual(summary["total_count"], 3)
        self.assertEqual(
            summary["buckets"]["d65-telematics"]["keys"],
            ["d65/1.mf4", "d65/2.mf4"],
        )
        self.assertEqual(
            summary["buckets"]["garland-telematics"]["count"],
            1,
        )
        self.assertEqual(
            calls,
            [
                (
                    EESBuckets.S3_BUCKET_D65,
                    {
                        "posted_after": "2026-06-01T12:00:00+00:00",
                        "Prefix": "telemetry/",
                    },
                ),
                (
                    "garland-telematics",
                    {
                        "posted_after": "2026-06-01T12:00:00+00:00",
                        "Prefix": "telemetry/",
                    },
                ),
            ],
        )

    def test_empty_summary_is_false(self) -> None:
        with patch(
            "decoder.s3_helper.get_mf4_files_list_from_s3",
            return_value=[],
        ):
            summary = get_new_mf4_files_summary_from_s3(
                bucket_names="d65-telematics",
                start_time="2026-06-01T00:00:00+00:00",
                end_time="2026-06-02T00:00:00+00:00",
            )

        self.assertFalse(summary["has_new_files"])
        self.assertEqual(summary["total_count"], 0)
        self.assertEqual(summary["buckets"]["d65-telematics"]["keys"], [])


if __name__ == "__main__":
    unittest.main()
