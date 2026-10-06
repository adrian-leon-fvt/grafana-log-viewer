import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from decoder.s3_helper import (
    EESBuckets,
    _resolve_s3_verify_setting,
    get_new_mf4_files_summary_from_s3,
    _parse_s3_timestamp,
)


class NewMf4FilesSummaryTest(unittest.TestCase):
    @patch.dict("os.environ", {}, clear=True)
    @patch("decoder.s3_helper.Path.is_file", return_value=False)
    def test_s3_tls_defaults_to_certifi(self, _is_file) -> None:
        self.assertTrue(_resolve_s3_verify_setting())

    @patch.dict(
        "os.environ",
        {"AWS_CA_BUNDLE": "/tmp/custom-ca.pem"},
        clear=True,
    )
    @patch("decoder.s3_helper.Path.exists", return_value=True)
    def test_s3_tls_uses_configured_ca_bundle(self, _exists) -> None:
        self.assertEqual(
            _resolve_s3_verify_setting(),
            "/tmp/custom-ca.pem",
        )

    @patch.dict("os.environ", {}, clear=True)
    @patch("decoder.s3_helper.Path.is_file", return_value=True)
    def test_s3_tls_local_ca_and_override_priority(self, _is_file) -> None:
        local_ca = Path(__file__).resolve().parents[1] / "Zscaler_Root_CA.crt"
        self.assertEqual(_resolve_s3_verify_setting(), str(local_ca))
        with (
            patch.dict("os.environ", {"AWS_CA_BUNDLE": "/tmp/custom-ca.pem"}),
            patch("decoder.s3_helper.Path.exists", return_value=True),
        ):
            self.assertEqual(_resolve_s3_verify_setting(), "/tmp/custom-ca.pem")
        with patch.dict("os.environ", {"AWS_S3_TLS_INSECURE": "true"}):
            self.assertIs(_resolve_s3_verify_setting(), False)

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

    @patch("decoder.s3_helper.create_s3_client")
    def test_last_modified_basis_includes_late_upload(self, create_client) -> None:
        old_recording_time = "2026-09-01T12:00:00Z"
        uploaded_today = datetime(2026, 9, 15, 12, 5, tzinfo=timezone.utc)
        fake_client = create_client.return_value
        fake_client.get_paginator.return_value.paginate.return_value = [
            {
                "Contents": [
                    {
                        "Key": "device/late.mf4",
                        "LastModified": uploaded_today,
                        "Size": 123,
                    }
                ]
            }
        ]
        fake_client.head_object.return_value = {
            "Metadata": {"timestamp": old_recording_time}
        }

        from decoder.s3_helper import get_mf4_files_list_from_s3

        files = get_mf4_files_list_from_s3(
            "test-bucket",
            start_time=datetime(2026, 9, 15, 12, 0, tzinfo=timezone.utc),
            end_time=datetime(2026, 9, 15, 12, 10, tzinfo=timezone.utc),
            time_basis="last-modified",
        )

        self.assertEqual([item["Key"] for item in files], ["device/late.mf4"])


if __name__ == "__main__":
    unittest.main()
