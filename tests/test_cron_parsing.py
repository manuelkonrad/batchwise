# SPDX-FileCopyrightText: 2025 Manuel Konrad
#
# SPDX-License-Identifier: MIT

import pytest

from batchwise.processor import Processor, Window


class TestCronParsing:
    """Detailed tests for cron expression parsing."""

    def create_processor(
        self, interval, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Helper to create a processor with given interval."""

        def sample_func(window: Window):
            pass

        return Processor(
            name="test_processor",
            func=sample_func,
            interval=interval,
            delay="1h",
            lookback="1d",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

    def test_every_minute(self, temp_checkpoint_dir, test_logger_name, utc_timezone):
        """Test every minute cron expression."""
        processor = self.create_processor(
            "* * * * *", temp_checkpoint_dir, test_logger_name, utc_timezone
        )
        result = processor._parse_cron("* * * * *")
        assert len(result["minute"]) == 60

    def test_every_five_minutes(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test every 5 minutes cron expression."""
        processor = self.create_processor(
            "*/5 * * * *", temp_checkpoint_dir, test_logger_name, utc_timezone
        )
        result = processor._parse_cron("*/5 * * * *")
        assert result["minute"] == [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]

    def test_specific_times(self, temp_checkpoint_dir, test_logger_name, utc_timezone):
        """Test specific times cron expression."""
        processor = self.create_processor(
            "0 9,12,18 * * *", temp_checkpoint_dir, test_logger_name, utc_timezone
        )
        result = processor._parse_cron("0 9,12,18 * * *")
        assert result["minute"] == [0]
        assert result["hour"] == [9, 12, 18]

    def test_weekday_only(self, temp_checkpoint_dir, test_logger_name, utc_timezone):
        """Test weekday only cron expression."""
        processor = self.create_processor(
            "0 9 * * 1-5", temp_checkpoint_dir, test_logger_name, utc_timezone
        )
        result = processor._parse_cron("0 9 * * 1-5")
        assert result["dow"] == [1, 2, 3, 4, 5]

    def test_first_day_of_month(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test first day of month cron expression."""
        processor = self.create_processor(
            "0 0 1 * *", temp_checkpoint_dir, test_logger_name, utc_timezone
        )
        result = processor._parse_cron("0 0 1 * *")
        assert result["day"] == [1]
        assert result["hour"] == [0]
        assert result["minute"] == [0]

    def test_complex_expression(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test complex cron expression."""
        processor = self.create_processor(
            "15,45 9-17/2 * 1,6,12 *",
            temp_checkpoint_dir,
            test_logger_name,
            utc_timezone,
        )
        result = processor._parse_cron("15,45 9-17/2 * 1,6,12 *")
        assert result["minute"] == [15, 45]
        assert result["hour"] == [9, 11, 13, 15, 17]
        assert result["month"] == [1, 6, 12]

    def test_invalid_range_order(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test invalid range order raises error."""
        processor = self.create_processor(
            "0 0 * * *", temp_checkpoint_dir, test_logger_name, utc_timezone
        )
        with pytest.raises(ValueError, match="Invalid cron range field"):
            processor._parse_cron("30-10 * * * *")

    def test_step_with_range(self, temp_checkpoint_dir, test_logger_name, utc_timezone):
        """Test step with range."""
        processor = self.create_processor(
            "10-50/10 * * * *", temp_checkpoint_dir, test_logger_name, utc_timezone
        )
        result = processor._parse_cron("10-50/10 * * * *")
        assert result["minute"] == [10, 20, 30, 40, 50]

    def test_sunday_variations(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test Sunday can be represented as 0 or 7."""
        # Use different checkpoint dirs to avoid interval mismatch
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmpdir1:
            processor = self.create_processor(
                "0 0 * * 0", Path(tmpdir1), test_logger_name, utc_timezone
            )
            result = processor._parse_cron("0 0 * * 0")
            assert 0 in result["dow"]

        with tempfile.TemporaryDirectory() as tmpdir2:
            processor2 = self.create_processor(
                "0 0 * * 7", Path(tmpdir2), test_logger_name, utc_timezone
            )
            result2 = processor2._parse_cron("0 0 * * 7")
            assert 7 in result2["dow"]
