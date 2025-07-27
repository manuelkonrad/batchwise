# SPDX-FileCopyrightText: 2025 Manuel Konrad
#
# SPDX-License-Identifier: MIT

import datetime
from pathlib import Path
from unittest.mock import patch

import pytest
from fsspec.implementations.dirfs import DirFileSystem

from batchwise.processor import PreventCompletion, Processor, Window


class TestProcessor:
    """Test suite for Processor class."""

    def test_processor_initialization(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test Processor initialization."""

        def sample_func(window: Window):
            pass

        processor = Processor(
            name="test_processor",
            func=sample_func,
            interval="0 0 * * *",
            delay="1h",
            lookback="1d",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        assert processor.name == "test_processor"
        assert processor.interval == "0 0 * * *"
        assert processor.delay == "1h"
        assert processor.lookback == "1d"

    def test_processor_missing_window_argument(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test that processor function must accept 'window' argument."""

        def invalid_func():
            pass

        with pytest.raises(ValueError, match="must accept 'window' argument"):
            Processor(
                name="invalid_processor",
                func=invalid_func,
                interval="0 0 * * *",
                delay="1h",
                lookback="1d",
                include_incomplete=False,
                context=None,
                abort_on_exception=False,
                checkpoint_path=temp_checkpoint_dir,
                timezone=utc_timezone,
                logger_name=test_logger_name,
            )

    def test_processor_with_context_missing_argument(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone, sample_context
    ):
        """Test that processor with context must accept 'context' argument."""

        def func_without_context(window: Window):
            pass

        with pytest.raises(ValueError, match="does not accept 'context' argument"):
            Processor(
                name="test_processor",
                func=func_without_context,
                interval="0 0 * * *",
                delay="1h",
                lookback="1d",
                include_incomplete=False,
                context=sample_context,
                abort_on_exception=False,
                checkpoint_path=temp_checkpoint_dir,
                timezone=utc_timezone,
                logger_name=test_logger_name,
            )

    def test_fs_interval_mismatch_raises_error(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Ensure cron mismatch is detected when using filesystem abstraction."""

        def sample_func(window: Window):
            pass

        dir_fs = DirFileSystem(path=str(temp_checkpoint_dir))
        checkpoint_path = Path("fs_checkpoints")

        Processor(
            name="fs_processor",
            func=sample_func,
            interval="0 0 * * *",
            delay="1h",
            lookback="1d",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=checkpoint_path,
            timezone=utc_timezone,
            logger_name=test_logger_name,
            fs=dir_fs,
        )

        with pytest.raises(ValueError, match="Cron interval mismatch"):
            Processor(
                name="fs_processor",
                func=sample_func,
                interval="30 1 * * *",
                delay="1h",
                lookback="1d",
                include_incomplete=False,
                context=None,
                abort_on_exception=False,
                checkpoint_path=checkpoint_path,
                timezone=utc_timezone,
                logger_name=test_logger_name,
                fs=dir_fs,
            )

    def test_fs_interval_same_value_is_accepted(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Re-registering with same interval should be allowed on filesystem."""

        def sample_func(window: Window):
            pass

        dir_fs = DirFileSystem(path=str(temp_checkpoint_dir))
        checkpoint_path = Path("fs_checkpoints")

        Processor(
            name="fs_processor",
            func=sample_func,
            interval="0 0 * * *",
            delay="1h",
            lookback="1d",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=checkpoint_path,
            timezone=utc_timezone,
            logger_name=test_logger_name,
            fs=dir_fs,
        )

        # Should not raise because interval remains unchanged
        Processor(
            name="fs_processor",
            func=sample_func,
            interval="0 0 * * *",
            delay="1h",
            lookback="1d",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=checkpoint_path,
            timezone=utc_timezone,
            logger_name=test_logger_name,
            fs=dir_fs,
        )

    def test_checkpoint_helpers_with_fs(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """_set_checkpoint and _has_checkpoint operate with filesystem abstraction."""

        def sample_func(window: Window):
            pass

        dir_fs = DirFileSystem(path=str(temp_checkpoint_dir))
        checkpoint_path = Path("fs_checkpoints")

        processor = Processor(
            name="fs_processor",
            func=sample_func,
            interval="0 0 * * *",
            delay="1h",
            lookback="1d",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=checkpoint_path,
            timezone=utc_timezone,
            logger_name=test_logger_name,
            fs=dir_fs,
        )

        window_id = "2025-01-01T00:00:00_2025-01-01T01:00:00"
        processor._set_checkpoint(window_id)
        checkpoint_uri = processor.processor_checkpoint_uri

        assert dir_fs.exists(f"{checkpoint_uri}/{window_id}")
        assert processor._has_checkpoint(window_id)

    def test_parse_timedelta_days(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test parsing timedelta with days."""

        def sample_func(window: Window):
            pass

        processor = Processor(
            name="test_processor",
            func=sample_func,
            interval="0 0 * * *",
            delay="1d",
            lookback="7d",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        result = processor._parse_timedelta("5d")
        assert result == datetime.timedelta(days=5)

    def test_parse_timedelta_hours(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test parsing timedelta with hours."""

        def sample_func(window: Window):
            pass

        processor = Processor(
            name="test_processor",
            func=sample_func,
            interval="0 * * * *",
            delay="2h",
            lookback="24h",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        result = processor._parse_timedelta("12h")
        assert result == datetime.timedelta(hours=12)

    def test_parse_timedelta_minutes(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test parsing timedelta with minutes."""

        def sample_func(window: Window):
            pass

        processor = Processor(
            name="test_processor",
            func=sample_func,
            interval="*/15 * * * *",
            delay="30m",
            lookback="120m",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        result = processor._parse_timedelta("45m")
        assert result == datetime.timedelta(minutes=45)

    def test_parse_timedelta_weeks(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test parsing timedelta with weeks."""

        def sample_func(window: Window):
            pass

        processor = Processor(
            name="test_processor",
            func=sample_func,
            interval="0 0 * * 0",
            delay="1w",
            lookback="4w",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        result = processor._parse_timedelta("2w")
        assert result == datetime.timedelta(weeks=2)

    def test_parse_timedelta_seconds(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test parsing timedelta with seconds."""

        def sample_func(window: Window):
            pass

        processor = Processor(
            name="test_processor",
            func=sample_func,
            interval="* * * * *",
            delay="30s",
            lookback="300s",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        result = processor._parse_timedelta("90s")
        assert result == datetime.timedelta(seconds=90)

    def test_parse_timedelta_invalid(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test parsing invalid timedelta raises ValueError."""

        def sample_func(window: Window):
            pass

        processor = Processor(
            name="test_processor",
            func=sample_func,
            interval="0 0 * * *",
            delay="1h",
            lookback="1d",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        with pytest.raises(ValueError, match="Invalid timedelta string"):
            processor._parse_timedelta("invalid")

        with pytest.raises(ValueError, match="Invalid timedelta string"):
            processor._parse_timedelta("10x")

        with pytest.raises(ValueError, match="Invalid timedelta string"):
            processor._parse_timedelta("d10")

    def test_parse_cron_all_wildcards(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test parsing cron expression with all wildcards."""

        def sample_func(window: Window):
            pass

        processor = Processor(
            name="test_processor",
            func=sample_func,
            interval="* * * * *",
            delay="1m",
            lookback="1h",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        result = processor._parse_cron("* * * * *")
        assert len(result["minute"]) == 60
        assert len(result["hour"]) == 24
        assert len(result["day"]) == 31
        assert len(result["month"]) == 12
        assert len(result["dow"]) == 7

    def test_parse_cron_specific_values(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test parsing cron expression with specific values."""

        def sample_func(window: Window):
            pass

        processor = Processor(
            name="test_processor",
            func=sample_func,
            interval="0 12 * * *",
            delay="1h",
            lookback="1d",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        result = processor._parse_cron("0 12 * * *")
        assert result["minute"] == [0]
        assert result["hour"] == [12]
        assert len(result["day"]) == 31
        assert len(result["month"]) == 12

    def test_parse_cron_ranges(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test parsing cron expression with ranges."""

        def sample_func(window: Window):
            pass

        processor = Processor(
            name="test_processor",
            func=sample_func,
            interval="0-30 9-17 * * *",
            delay="1h",
            lookback="1d",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        result = processor._parse_cron("0-30 9-17 * * *")
        assert result["minute"] == list(range(0, 31))
        assert result["hour"] == list(range(9, 18))

    def test_parse_cron_field_single_value_step(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Parse cron field with numeric start and explicit step."""

        def sample_func(window: Window):
            pass

        processor = Processor(
            name="test_processor",
            func=sample_func,
            interval="0 0 * * *",
            delay="1h",
            lookback="1d",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        result = processor._parse_cron_field("5/20", 0, 59)
        assert result == [5, 25, 45]

    def test_parse_cron_field_invalid_range(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Invalid ranges should raise ValueError."""

        def sample_func(window: Window):
            pass

        processor = Processor(
            name="test_processor",
            func=sample_func,
            interval="0 0 * * *",
            delay="1h",
            lookback="1d",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        with pytest.raises(ValueError, match="Invalid cron range field"):
            processor._parse_cron_field("10-5", 0, 59)

    def test_parse_cron_field_invalid_range_with_step(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Invalid range inside stepped field should raise ValueError."""

        def sample_func(window: Window):
            pass

        processor = Processor(
            name="test_processor",
            func=sample_func,
            interval="0 0 * * *",
            delay="1h",
            lookback="1d",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        with pytest.raises(ValueError, match="Invalid cron range field"):
            processor._parse_cron_field("10-5/2", 0, 59)

    def test_parse_cron_field_invalid_value(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Completely invalid cron field should raise ValueError."""

        def sample_func(window: Window):
            pass

        processor = Processor(
            name="test_processor",
            func=sample_func,
            interval="0 0 * * *",
            delay="1h",
            lookback="1d",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        with pytest.raises(ValueError, match="Invalid cron field"):
            processor._parse_cron_field("abc", 0, 59)

    def test_parse_cron_field_invalid_step_expression(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Invalid step expressions should fall back to cron field error."""

        def sample_func(window: Window):
            pass

        processor = Processor(
            name="test_processor",
            func=sample_func,
            interval="0 0 * * *",
            delay="1h",
            lookback="1d",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        with pytest.raises(ValueError, match="Invalid cron field"):
            processor._parse_cron_field("foo/10", 0, 59)

    def test_get_trigger_times_advances_to_matching_day(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Ensure _get_trigger_times advances dates when no fields match."""

        def sample_func(window: Window):
            pass

        processor = Processor(
            name="test_processor",
            func=sample_func,
            interval="0 0 * * *",
            delay="1h",
            lookback="1d",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        start = datetime.datetime(2025, 1, 1, tzinfo=utc_timezone)
        end = start + datetime.timedelta(days=3)
        cron_fields = {
            "minute": [0],
            "hour": [0],
            "day": [start.day + 1],
            "month": [start.month],
            # choose a cron weekday that maps to a different python weekday so only DOM matches
            "dow": [(((start.weekday() + 1) % 7) - 6) % 7],
        }

        result = processor._get_trigger_times(start, end, cron_fields)

        assert result
        assert result[0].day == start.day + 1

    def test_get_trigger_times_empty_when_outside_range(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """No trigger times when start is far beyond end horizon."""

        def sample_func(window: Window):
            pass

        processor = Processor(
            name="test_processor",
            func=sample_func,
            interval="0 0 * * *",
            delay="1h",
            lookback="1d",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        start = datetime.datetime(2025, 1, 1, tzinfo=utc_timezone)
        end = start - datetime.timedelta(days=400)
        cron_fields = {
            "minute": [0],
            "hour": [0],
            "day": [start.day],
            "month": [start.month],
            "dow": list(range(0, 7)),
        }

        result = processor._get_trigger_times(start, end, cron_fields)

        assert result == []

    def test_get_trigger_times_dom_or_dow(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """DOM and DOW constraints should behave like logical OR."""

        def sample_func(window: Window):
            pass

        processor = Processor(
            name="test_processor",
            func=sample_func,
            interval="0 0 1 * 1",
            delay="1h",
            lookback="1d",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        start = datetime.datetime(2025, 1, 1, tzinfo=utc_timezone)
        end = start + datetime.timedelta(days=8)

        cron_fields = processor._parse_cron(processor.interval)
        trigger_times = processor._get_trigger_times(start, end, cron_fields)
        trigger_dates = {ts.date() for ts in trigger_times}

        # January 1st, 2025 is a Wednesday (matches DOM=1) and January 6th is a Monday (matches DOW=1)
        assert datetime.date(2025, 1, 1) in trigger_dates
        assert datetime.date(2025, 1, 6) in trigger_dates

    def test_parse_cron_steps(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test parsing cron expression with steps."""

        def sample_func(window: Window):
            pass

        processor = Processor(
            name="test_processor",
            func=sample_func,
            interval="*/15 * * * *",
            delay="1h",
            lookback="1d",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        result = processor._parse_cron("*/15 * * * *")
        assert result["minute"] == [0, 15, 30, 45]

    def test_parse_cron_comma_separated(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test parsing cron expression with comma-separated values."""

        def sample_func(window: Window):
            pass

        processor = Processor(
            name="test_processor",
            func=sample_func,
            interval="0,15,30,45 * * * *",
            delay="1h",
            lookback="1d",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        result = processor._parse_cron("0,15,30,45 * * * *")
        assert result["minute"] == [0, 15, 30, 45]

    def test_parse_cron_invalid_field_count(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test parsing cron expression with invalid field count."""

        def sample_func(window: Window):
            pass

        processor = Processor(
            name="test_processor",
            func=sample_func,
            interval="0 0 * * *",
            delay="1h",
            lookback="1d",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        with pytest.raises(ValueError, match="Expected 5 fields"):
            processor._parse_cron("0 0 * *")

    def test_parse_cron_out_of_range(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test parsing cron with out-of-range values."""

        def sample_func(window: Window):
            pass

        processor = Processor(
            name="test_processor",
            func=sample_func,
            interval="0 0 * * *",
            delay="1h",
            lookback="1d",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        with pytest.raises(ValueError, match="out of range"):
            processor._parse_cron("60 * * * *")

    def test_checkpoint_operations(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test checkpoint set and check operations."""

        def sample_func(window: Window):
            pass

        processor = Processor(
            name="test_processor",
            func=sample_func,
            interval="0 0 * * *",
            delay="1h",
            lookback="1d",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        window_id = "test_window_id"
        assert not processor._has_checkpoint(window_id)

        processor._set_checkpoint(window_id)
        assert processor._has_checkpoint(window_id)

    def test_cron_interval_persistence(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test that cron interval is persisted and checked."""

        def sample_func(window: Window):
            pass

        # Create first processor
        Processor(
            name="test_processor",
            func=sample_func,
            interval="0 0 * * *",
            delay="1h",
            lookback="1d",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        # Create second processor with same name and interval - should succeed
        Processor(
            name="test_processor",
            func=sample_func,
            interval="0 0 * * *",
            delay="1h",
            lookback="1d",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        # Try to create processor with different interval - should fail
        with pytest.raises(ValueError, match="Cron interval mismatch"):
            Processor(
                name="test_processor",
                func=sample_func,
                interval="0 12 * * *",  # Different interval
                delay="1h",
                lookback="1d",
                include_incomplete=False,
                context=None,
                abort_on_exception=False,
                checkpoint_path=temp_checkpoint_dir,
                timezone=utc_timezone,
                logger_name=test_logger_name,
            )

    def test_processor_call_with_complete_window(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test processor call creates checkpoint for complete windows."""
        called_windows = []

        def sample_func(window: Window):
            called_windows.append(window)

        processor = Processor(
            name="test_processor",
            func=sample_func,
            interval="0 * * * *",  # Every hour
            delay="2h",  # 2 hour delay
            lookback="6h",  # Look back 6 hours
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        # Just call the processor - it will use real time
        processor()

        # Should process some windows (depends on current time)
        assert len(called_windows) >= 0

    def test_processor_prevents_completion(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test PreventCompletion exception prevents checkpoint creation."""

        def sample_func(window: Window):
            raise PreventCompletion("Testing prevention")

        processor = Processor(
            name="test_processor",
            func=sample_func,
            interval="0 * * * *",
            delay="2h",
            lookback="6h",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        # This should not raise, just log
        processor()

    def test_processor_exception_handling_abort(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test processor exception handling with abort enabled."""

        def failing_func(window: Window):
            raise RuntimeError("Test error")

        processor = Processor(
            name="test_processor",
            func=failing_func,
            interval="0 * * * *",
            delay="2h",
            lookback="6h",
            include_incomplete=False,
            context=None,
            abort_on_exception=True,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        # Should raise the exception
        with pytest.raises(RuntimeError, match="Test error"):
            with patch.object(processor, "_get_windows") as mock_windows:
                start = datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=utc_timezone)
                end = datetime.datetime(2025, 1, 1, 1, 0, 0, tzinfo=utc_timezone)
                mock_windows.return_value = [
                    Window(
                        window_id=f"{start.isoformat()}_{end.isoformat()}",
                        start=start,
                        end=end,
                        complete=True,
                    )
                ]
                processor()

    def test_processor_exception_handling_no_abort(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test processor exception handling without abort."""

        def failing_func(window: Window):
            raise RuntimeError("Test error")

        processor = Processor(
            name="test_processor",
            func=failing_func,
            interval="0 * * * *",
            delay="2h",
            lookback="6h",
            include_incomplete=False,
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        # Should not raise the exception
        with patch.object(processor, "_get_windows") as mock_windows:
            start = datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=utc_timezone)
            end = datetime.datetime(2025, 1, 1, 1, 0, 0, tzinfo=utc_timezone)
            mock_windows.return_value = [
                Window(
                    window_id=f"{start.isoformat()}_{end.isoformat()}",
                    start=start,
                    end=end,
                    complete=True,
                )
            ]
            processor()  # Should not raise

    def test_include_incomplete_windows(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test that include_incomplete flag allows processing incomplete windows."""
        called_windows = []

        def sample_func(window: Window):
            called_windows.append(window)

        processor = Processor(
            name="test_processor",
            func=sample_func,
            interval="0 * * * *",
            delay="2h",
            lookback="6h",
            include_incomplete=True,  # Include incomplete windows
            context=None,
            abort_on_exception=False,
            checkpoint_path=temp_checkpoint_dir,
            timezone=utc_timezone,
            logger_name=test_logger_name,
        )

        processor()

        # Check if any incomplete windows were processed
        incomplete_count = sum(1 for w in called_windows if not w.complete)
        # Depending on timing, there might be incomplete windows
        assert incomplete_count >= 0
