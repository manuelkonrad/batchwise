# SPDX-FileCopyrightText: 2025 Manuel Konrad
#
# SPDX-License-Identifier: MIT

import datetime
import logging
import time
from collections import OrderedDict
from multiprocessing import Pool
from pathlib import Path
from typing import Callable

from batchwise.processor import Processor


def run_processor(config: dict) -> None:
    """Run a single processor by name."""
    processor_name = config["name"]
    logger = logging.getLogger(config["logger_name"])
    try:
        processor = Processor(**config)
        logger.info(f"Running processor '{processor_name}'.")
        processor()
    except Exception as e:
        if config["abort_on_exception"]:
            raise e
        else:
            logger.error(f"Processor {processor_name} failed with exception: {e}")


class Engine:
    """Batch processing engine to manage and run processors."""

    def __init__(
        self,
        checkpoint_path: Path | str = "./batchwise_checkpoints",
        logger_name: str = "batchwise",
        timezone: datetime.tzinfo | None = datetime.timezone.utc,
        fs=None,
    ) -> None:
        """Initialize the batch processing engine.

        Args:
            checkpoint_path (Path | str): Path to store processor checkpoints.
            logger_name (str): Name of the logger to use.
            timezone (datetime.tzinfo | None): Timezone for processing windows.
            fs: Optional filesystem abstraction.
        """
        self._checkpoint_path = Path(checkpoint_path)
        self._processor_configs: dict[str, dict] = OrderedDict()
        self._abort_on_exception = False
        self._timezone = timezone
        self._logger_name = logger_name
        self._fs = fs

    def processor(
        self,
        interval: str,
        delay: str,
        lookback: str,
        include_incomplete: bool = False,
        context: dict | None = None,
    ) -> Callable:
        """Decorator to register a processor function.

        Args:
            interval (str): Cron expression for processing intervals.
            delay (str): Delay before considering a window complete.
            lookback (str): Lookback period for processing windows.
            include_incomplete (bool): Whether to include incomplete windows.
            context (dict | None): Additional context for the processor.

        Returns:
            Callable: Decorator function.
        """

        def _processor_inner(func: Callable) -> Callable:
            if func.__name__ in self._processor_configs:
                raise ValueError("Processor name must be unique.")
            self._processor_configs[func.__name__] = dict(
                name=func.__name__,
                func=func,
                interval=interval,
                delay=delay,
                lookback=lookback,
                include_incomplete=include_incomplete,
                context=context,
                abort_on_exception=self._abort_on_exception,
                checkpoint_path=self._checkpoint_path,
                timezone=self._timezone,
                logger_name=self._logger_name,
                fs=self._fs,
            )
            return func

        return _processor_inner

    def _run_in_loop(self) -> None:
        """Run registered processors sequentially."""
        for processor_config in self._processor_configs.values():
            run_processor(processor_config)

    def _run_in_pool(self, num_processes: int) -> None:
        """Run registered processors in parallel."""
        with Pool(processes=num_processes) as pool:
            list(pool.imap_unordered(run_processor, self._processor_configs.values()))

    def __call__(
        self,
        num_processes: int = 1,
        every_seconds: int | float | None = None,
    ) -> None:
        """Execute all registered processors.

        Args:
            num_processes (int): Number of processes to use for parallel execution. If 1, runs sequentially.
            every_seconds (int | float | None): Minimum time in seconds between full cycles. If None, runs only once.
        """
        logger = logging.getLogger(self._logger_name)
        while True:
            start_time = time.time()
            logger.info("Running engine iteration.")
            num_processes = int(num_processes)
            if num_processes > 1:
                self._run_in_pool(num_processes)
            elif num_processes == 1:
                self._run_in_loop()
            else:
                raise ValueError("num_processes must cast to a positive integer.")
            if every_seconds is not None:
                sleep_time = start_time + every_seconds - time.time()
                if sleep_time > 0:
                    time.sleep(sleep_time)
            else:
                break
