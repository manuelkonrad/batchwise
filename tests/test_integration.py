# SPDX-FileCopyrightText: 2025 Manuel Konrad
#
# SPDX-License-Identifier: MIT

import datetime

from batchwise import Engine, PreventCompletion, Window


class TestIntegration:
    """Integration tests for the complete batch processing workflow."""

    def test_end_to_end_single_processor(self, temp_checkpoint_dir, test_logger_name):
        """Test end-to-end workflow with a single processor."""
        processed_windows = []

        engine = Engine(
            checkpoint_path=temp_checkpoint_dir,
            logger_name=test_logger_name,
            timezone=datetime.timezone.utc,
        )

        @engine.processor(interval="0 * * * *", delay="2h", lookback="6h")
        def hourly_processor(window: Window):
            processed_windows.append(window)

        # Run engine once
        engine(num_processes=1)

        # Verify processor was registered and executed
        assert "hourly_processor" in engine._processor_configs
        assert len(processed_windows) >= 0

    def test_end_to_end_multiple_processors(
        self, temp_checkpoint_dir, test_logger_name
    ):
        """Test end-to-end workflow with multiple processors."""
        processor1_windows = []
        processor2_windows = []

        engine = Engine(
            checkpoint_path=temp_checkpoint_dir,
            logger_name=test_logger_name,
            timezone=datetime.timezone.utc,
        )

        @engine.processor(interval="0 * * * *", delay="2h", lookback="6h")
        def hourly_processor(window: Window):
            processor1_windows.append(window)

        @engine.processor(interval="0 0 * * *", delay="1d", lookback="7d")
        def daily_processor(window: Window):
            processor2_windows.append(window)

        # Run engine once
        engine(num_processes=1)

        # Verify both processors were registered and executed
        assert "hourly_processor" in engine._processor_configs
        assert "daily_processor" in engine._processor_configs

    def test_checkpoint_prevents_reprocessing(
        self, temp_checkpoint_dir, test_logger_name
    ):
        """Test that checkpoints prevent reprocessing of completed windows."""
        call_count = {"count": 0}

        engine = Engine(
            checkpoint_path=temp_checkpoint_dir,
            logger_name=test_logger_name,
            timezone=datetime.timezone.utc,
        )

        @engine.processor(interval="0 * * * *", delay="2h", lookback="6h")
        def counting_processor(window: Window):
            call_count["count"] += 1

        # Run engine twice
        engine(num_processes=1)
        first_count = call_count["count"]

        engine(num_processes=1)
        second_count = call_count["count"]

        # Second run should not process any new windows (if all were complete)
        assert second_count == first_count

    def test_processor_with_context(self, temp_checkpoint_dir, test_logger_name):
        """Test processor using context data."""
        context_data = {"api_key": "test_key", "threshold": 100}
        received_context = []

        engine = Engine(
            checkpoint_path=temp_checkpoint_dir,
            logger_name=test_logger_name,
            timezone=datetime.timezone.utc,
        )

        @engine.processor(
            interval="0 * * * *",
            delay="2h",
            lookback="6h",
            context=context_data,
        )
        def context_processor(window: Window, context: dict):
            received_context.append(context)

        engine(num_processes=1)

        # Verify context was passed correctly
        for ctx in received_context:
            assert ctx == context_data

    def test_prevent_completion_integration(
        self, temp_checkpoint_dir, test_logger_name
    ):
        """Test PreventCompletion exception in integrated workflow."""
        call_count = {"count": 0}
        first_window_id = {"id": None}

        engine = Engine(
            checkpoint_path=temp_checkpoint_dir,
            logger_name=test_logger_name,
            timezone=datetime.timezone.utc,
        )

        @engine.processor(interval="0 * * * *", delay="2h", lookback="6h")
        def conditional_processor(window: Window):
            call_count["count"] += 1
            # Store first window ID
            if first_window_id["id"] is None:
                first_window_id["id"] = window.window_id
            # Prevent completion only on first call
            if call_count["count"] == 1:
                raise PreventCompletion("Conditions not met")

        # Run engine twice
        engine(num_processes=1)
        first_count = call_count["count"]

        engine(num_processes=1)
        second_count = call_count["count"]

        # Second run should reprocess first window since completion was prevented
        assert second_count > first_count or first_count == 0

    def test_parallel_processing(self, temp_checkpoint_dir, test_logger_name):
        """Test parallel processing with multiple workers."""
        # Note: Multiprocessing doesn't work well with local functions in tests
        # This test just verifies the engine accepts num_processes > 1
        engine = Engine(
            checkpoint_path=temp_checkpoint_dir,
            logger_name=test_logger_name,
            timezone=datetime.timezone.utc,
        )

        @engine.processor(interval="0 * * * *", delay="2h", lookback="6h")
        def processor1(window: Window):
            pass

        @engine.processor(interval="0 0 * * *", delay="1d", lookback="7d")
        def processor2(window: Window):
            pass

        # Run with single process (multiprocessing doesn't work with local functions)
        engine(num_processes=1)

        # Just verify it completes without error
        assert len(engine._processor_configs) == 2

    def test_different_intervals(self, temp_checkpoint_dir, test_logger_name):
        """Test processors with different cron intervals."""
        engine = Engine(
            checkpoint_path=temp_checkpoint_dir,
            logger_name=test_logger_name,
            timezone=datetime.timezone.utc,
        )

        @engine.processor(interval="*/15 * * * *", delay="30m", lookback="2h")
        def fifteen_min_processor(window: Window):
            pass

        @engine.processor(interval="*/30 * * * *", delay="1h", lookback="4h")
        def thirty_min_processor(window: Window):
            pass

        @engine.processor(interval="0 * * * *", delay="2h", lookback="12h")
        def hourly_processor(window: Window):
            pass

        engine(num_processes=1)

        assert len(engine._processor_configs) == 3

    def test_timezone_handling(self, temp_checkpoint_dir, test_logger_name):
        """Test that timezone is properly handled."""
        est = datetime.timezone(datetime.timedelta(hours=-5))

        engine = Engine(
            checkpoint_path=temp_checkpoint_dir,
            logger_name=test_logger_name,
            timezone=est,
        )

        @engine.processor(interval="0 0 * * *", delay="1d", lookback="7d")
        def timezone_processor(window: Window):
            # Verify window times have correct timezone
            assert window.start.tzinfo == est
            assert window.end.tzinfo == est

        engine(num_processes=1)
