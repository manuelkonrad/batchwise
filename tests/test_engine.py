# SPDX-FileCopyrightText: 2025 Manuel Konrad
#
# SPDX-License-Identifier: MIT

import datetime
import logging
import pickle
from pathlib import Path, PurePosixPath
from unittest.mock import Mock, patch

import pytest
from fsspec.implementations.dirfs import DirFileSystem

from batchwise import Engine, Window
from batchwise.engine import run_processor
from batchwise.processor import Processor


class TestEngine:
    """Test suite for Engine class."""

    def test_engine_initialization(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Test Engine initialization."""
        engine = Engine(
            checkpoint_path=temp_checkpoint_dir,
            logger_name=test_logger_name,
            timezone=utc_timezone,
        )

        assert engine._checkpoint_path == temp_checkpoint_dir
        assert engine._logger_name == test_logger_name
        assert engine._timezone == utc_timezone
        assert len(engine._processor_configs) == 0

    def test_engine_default_initialization(self):
        """Test Engine with default parameters."""
        engine = Engine()

        assert engine._checkpoint_path == Path("./batchwise_checkpoints")
        assert isinstance(engine._logger_name, str)
        assert engine._timezone == datetime.timezone.utc

    def test_engine_initialization_with_dirfilesystem(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Ensure Engine can be initialized with an fsspec DirFileSystem."""
        dir_fs = DirFileSystem(path=str(temp_checkpoint_dir))
        engine = Engine(
            checkpoint_path=Path("checkpoints"),
            logger_name=test_logger_name,
            timezone=utc_timezone,
            fs=dir_fs,
        )

        @engine.processor(interval="*/30 * * * *", delay="1h", lookback="1d")
        def dirfs_processor(window: Window):
            pass

        Processor(**engine._processor_configs["dirfs_processor"])
        processor_uri = PurePosixPath(Path("checkpoints") / "dirfs_processor")
        cron_uri = processor_uri / "cron_expression"

        assert engine._fs is dir_fs
        assert dir_fs.exists(str(processor_uri))
        with dir_fs.open(str(cron_uri), "r") as cron_file:
            assert cron_file.read().strip() == "*/30 * * * *"

    def test_engine_with_dirfilesystem_is_picklable(
        self, temp_checkpoint_dir, test_logger_name, utc_timezone
    ):
        """Engine with fsspec handler remains picklable."""
        dir_fs = DirFileSystem(path=str(temp_checkpoint_dir))
        engine = Engine(
            checkpoint_path=temp_checkpoint_dir,
            logger_name=test_logger_name,
            timezone=utc_timezone,
            fs=dir_fs,
        )

        pickled = pickle.dumps(engine)
        restored = pickle.loads(pickled)

        assert isinstance(restored, Engine)
        assert isinstance(restored._fs, DirFileSystem)
        assert restored._checkpoint_path == temp_checkpoint_dir
        assert restored._logger_name == test_logger_name
        assert restored._timezone == utc_timezone

    def test_register_processor(self, temp_checkpoint_dir, test_logger_name):
        """Test registering a processor using decorator."""
        engine = Engine(
            checkpoint_path=temp_checkpoint_dir, logger_name=test_logger_name
        )

        @engine.processor(interval="0 0 * * *", delay="1h", lookback="1d")
        def test_processor(window: Window):
            pass

        assert "test_processor" in engine._processor_configs
        assert engine._processor_configs["test_processor"]["name"] == "test_processor"

    def test_register_processor_with_context(
        self, temp_checkpoint_dir, test_logger_name, sample_context
    ):
        """Test registering a processor with context."""
        engine = Engine(
            checkpoint_path=temp_checkpoint_dir, logger_name=test_logger_name
        )

        @engine.processor(
            interval="0 0 * * *",
            delay="1h",
            lookback="1d",
            context=sample_context,
        )
        def test_processor_with_context(window: Window, context: dict):
            pass

        assert "test_processor_with_context" in engine._processor_configs

    def test_duplicate_processor_name_raises_error(
        self, temp_checkpoint_dir, test_logger_name
    ):
        """Test that duplicate processor names raise ValueError."""
        engine = Engine(
            checkpoint_path=temp_checkpoint_dir, logger_name=test_logger_name
        )

        @engine.processor(interval="0 0 * * *", delay="1h", lookback="1d")
        def duplicate_processor(window: Window):
            pass

        with pytest.raises(ValueError, match="Processor name must be unique"):

            @engine.processor(interval="0 0 * * *", delay="1h", lookback="1d")
            def duplicate_processor(window: Window):
                pass

    def test_run_processor_invokes_processor(
        self, temp_checkpoint_dir, test_logger_name
    ):
        """run_processor instantiates Processor and calls it."""
        engine = Engine(
            checkpoint_path=temp_checkpoint_dir, logger_name=test_logger_name
        )

        @engine.processor(interval="*/30 * * * *", delay="1h", lookback="1d")
        def test_processor(window: Window):
            pass

        config = engine._processor_configs["test_processor"]

        with patch("batchwise.engine.Processor") as mock_processor_cls:
            instance = Mock()
            mock_processor_cls.return_value = instance

            run_processor(config)

        mock_processor_cls.assert_called_once_with(**config)
        instance.assert_called_once_with()

    def test_run_processor_handles_exception_no_abort(
        self, temp_checkpoint_dir, test_logger_name, caplog
    ):
        """run_processor logs and swallows when abort flag is False."""
        engine = Engine(
            checkpoint_path=temp_checkpoint_dir, logger_name=test_logger_name
        )

        @engine.processor(interval="*/30 * * * *", delay="1h", lookback="1d")
        def failing_processor(window: Window):
            pass

        config = engine._processor_configs["failing_processor"]

        with (
            patch("batchwise.engine.Processor") as mock_processor_cls,
            caplog.at_level(logging.ERROR, logger=test_logger_name),
        ):
            instance = Mock()
            instance.side_effect = RuntimeError("Test error")
            mock_processor_cls.return_value = instance

            run_processor(config)

        assert "failed with exception" in caplog.text

    def test_run_processor_handles_exception_with_abort(
        self, temp_checkpoint_dir, test_logger_name
    ):
        """run_processor re-raises when abort flag is True."""
        engine = Engine(
            checkpoint_path=temp_checkpoint_dir, logger_name=test_logger_name
        )
        engine._abort_on_exception = True

        @engine.processor(interval="*/30 * * * *", delay="1h", lookback="1d")
        def failing_processor(window: Window):
            pass

        config = engine._processor_configs["failing_processor"]

        with patch("batchwise.engine.Processor") as mock_processor_cls:
            instance = Mock()
            instance.side_effect = RuntimeError("Test error")
            mock_processor_cls.return_value = instance

            with pytest.raises(RuntimeError, match="Test error"):
                run_processor(config)

    def test_run_in_loop(self, temp_checkpoint_dir, test_logger_name):
        """Test running processors sequentially in loop."""
        engine = Engine(
            checkpoint_path=temp_checkpoint_dir, logger_name=test_logger_name
        )
        calls = []

        @engine.processor(interval="*/30 * * * *", delay="1h", lookback="1d")
        def processor_1(window: Window):
            calls.append("processor_1")

        @engine.processor(interval="*/30 * * * *", delay="1h", lookback="1d")
        def processor_2(window: Window):
            calls.append("processor_2")

        with patch("batchwise.engine.run_processor") as mock_run:
            engine._run_in_loop()

        assert mock_run.call_count == 2
        assert list(engine._processor_configs.keys()) == ["processor_1", "processor_2"]

    def test_engine_call_single_process(self, temp_checkpoint_dir, test_logger_name):
        """Test calling engine with single process."""
        engine = Engine(
            checkpoint_path=temp_checkpoint_dir, logger_name=test_logger_name
        )

        @engine.processor(interval="*/30 * * * *", delay="1h", lookback="1d")
        def test_processor(window: Window):
            pass

        with patch.object(engine, "_run_in_loop") as mock_run:
            engine(num_processes=1)
            mock_run.assert_called_once()

    def test_engine_call_multi_process(self, temp_checkpoint_dir, test_logger_name):
        """Test calling engine with multiple processes."""
        engine = Engine(
            checkpoint_path=temp_checkpoint_dir, logger_name=test_logger_name
        )

        @engine.processor(interval="*/30 * * * *", delay="1h", lookback="1d")
        def test_processor(window: Window):
            pass

        with patch.object(engine, "_run_in_pool") as mock_run:
            engine(num_processes=2)
            mock_run.assert_called_once_with(2)

    def test_engine_call_invalid_num_processes(
        self, temp_checkpoint_dir, test_logger_name
    ):
        """Test calling engine with invalid num_processes raises ValueError."""
        engine = Engine(
            checkpoint_path=temp_checkpoint_dir, logger_name=test_logger_name
        )

        @engine.processor(interval="*/30 * * * *", delay="1h", lookback="1d")
        def test_processor(window: Window):
            pass

        with pytest.raises(
            ValueError, match="num_processes must cast to a positive integer"
        ):
            engine(num_processes=0)

    def test_engine_with_every_seconds(self, temp_checkpoint_dir, test_logger_name):
        """Test engine respects minimum interval."""
        engine = Engine(
            checkpoint_path=temp_checkpoint_dir, logger_name=test_logger_name
        )

        @engine.processor(interval="*/30 * * * *", delay="1h", lookback="1d")
        def test_processor(window: Window):
            pass

        # Mock _run_in_loop to avoid infinite loop, then test every_seconds logic
        with patch.object(engine, "_run_in_loop", side_effect=StopIteration):
            try:
                engine(num_processes=1, every_seconds=2)
            except StopIteration:
                pass  # Expected from mocked _run_in_loop

    def test_engine_every_seconds_triggers_sleep(
        self, temp_checkpoint_dir, test_logger_name
    ):
        """Engine waits when every_seconds is provided."""
        engine = Engine(
            checkpoint_path=temp_checkpoint_dir, logger_name=test_logger_name
        )

        @engine.processor(interval="*/30 * * * *", delay="1h", lookback="1d")
        def test_processor(window: Window):
            pass

        with (
            patch.object(engine, "_run_in_loop") as mock_loop,
            patch("batchwise.engine.time.time", return_value=100.0),
            patch("batchwise.engine.time.sleep") as mock_sleep,
        ):
            mock_loop.return_value = None

            def _sleep(duration):
                assert duration == pytest.approx(1.0)
                raise StopIteration()

            mock_sleep.side_effect = _sleep

            with pytest.raises(StopIteration):
                engine(num_processes=1, every_seconds=1)

            mock_loop.assert_called_once()
            mock_sleep.assert_called_once()

    def test_engine_every_seconds_non_positive_sleep_time(
        self, temp_checkpoint_dir, test_logger_name
    ):
        """Engine skips sleeping when computed interval is non-positive."""
        engine = Engine(
            checkpoint_path=temp_checkpoint_dir, logger_name=test_logger_name
        )

        @engine.processor(interval="*/30 * * * *", delay="1h", lookback="1d")
        def test_processor(window: Window):
            pass

        with (
            patch.object(
                engine, "_run_in_loop", side_effect=[None, StopIteration]
            ) as mock_loop,
            patch("batchwise.engine.time.time", side_effect=[100.0, 101.0, 200.0]),
            patch("batchwise.engine.time.sleep") as mock_sleep,
        ):
            with pytest.raises(StopIteration):
                engine(num_processes=1, every_seconds=0)

        assert mock_loop.call_count >= 1
        mock_sleep.assert_not_called()

    def test_run_in_pool_uses_pool(self, temp_checkpoint_dir, test_logger_name):
        """Ensure _run_in_pool leverages multiprocessing Pool."""
        engine = Engine(
            checkpoint_path=temp_checkpoint_dir, logger_name=test_logger_name
        )

        @engine.processor(interval="*/30 * * * *", delay="1h", lookback="1d")
        def test_processor(window: Window):
            pass

        with patch("batchwise.engine.Pool") as mock_pool:
            pool_instance = mock_pool.return_value.__enter__.return_value
            engine._run_in_pool(3)

            mock_pool.assert_called_once_with(processes=3)
            pool_instance.imap_unordered.assert_called_once()
            called_func, called_values = pool_instance.imap_unordered.call_args.args
            assert called_func is run_processor
            assert list(called_values) == list(engine._processor_configs.values())
