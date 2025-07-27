# SPDX-FileCopyrightText: 2025 Manuel Konrad
#
# SPDX-License-Identifier: MIT

import datetime
import logging
import re
from dataclasses import dataclass
from inspect import signature
from pathlib import Path, PurePosixPath
from typing import Callable


@dataclass
class Window:
    """Data class representing a processing window.

    Attributes:
        window_id (str): Unique identifier for the window.
        start (datetime.datetime): Start time of the window.
        end (datetime.datetime): End time of the window.
        complete (bool): Whether the window is complete.
    """

    window_id: str
    start: datetime.datetime
    end: datetime.datetime
    complete: bool


class PreventCompletion(Exception):
    """Exception which can be raised to prevent window completion."""

    pass


class Processor:
    """Batch processor handling time windows based on cron expressions.

    Attributes:
       name (str): Name of the processor.
       interval (str): Cron expression for processing intervals.
       delay (str): Delay before considering a window complete.
       lookback (str): Lookback period for processing windows.
    """

    def __init__(
        self,
        name: str,
        func: Callable,
        interval: str,
        delay: str,
        lookback: str,
        include_incomplete: bool,
        context: dict | None,
        abort_on_exception: bool,
        checkpoint_path: Path,
        timezone: datetime.tzinfo | None,
        logger_name: str,
        fs=None,
    ):
        """Initialize the processor.

        Args:
            name (str): Name of the processor.
            func (Callable): Processor function to be called for each window.
            interval (str): Cron expression for processing intervals.
            delay (str): Delay before considering a window complete.
            lookback (str): Lookback period for processing windows.
            include_incomplete (bool): Whether to include incomplete windows.
            context (dict | None): Additional context for the processor.
            abort_on_exception (bool): Whether to abort on exceptions.
            checkpoint_path (Path): Path to store processor checkpoints.
            timezone (datetime.tzinfo | None): Timezone for processing windows.
            logger_name (str): Name of the logger to use.
            fs: Optional filesystem abstraction.
        """
        self._name = name
        self._interval = interval
        self._delay = delay
        self._lookback = lookback
        self._include_incomplete = include_incomplete
        self._abort_on_exception = abort_on_exception
        self._func = func
        self._timezone = timezone
        self._logger_name = logger_name
        self._fs = fs

        # check function signature and prepare kwargs
        _expected_args = signature(self._func).parameters
        if "window" not in _expected_args:
            raise ValueError("Processor function must accept 'window' argument.")
        self._additional_kwargs = {}
        if context is not None:
            if "context" not in _expected_args:
                raise ValueError(
                    "Processor function does not accept 'context' argument."
                )
            self._additional_kwargs["context"] = context

        # prepare checkpoint path
        if self._fs:
            self.processor_checkpoint_uri = str(
                PurePosixPath(checkpoint_path / self._name)
            )
            if not self._fs.exists(self.processor_checkpoint_uri):
                self._fs.mkdir(self.processor_checkpoint_uri, create_parents=True)
        else:
            self.processor_checkpoint_path = Path(checkpoint_path) / self._name
            self.processor_checkpoint_path.mkdir(parents=True, exist_ok=True)

        # set or check cron interval file
        self._set_or_check_interval()

    @property
    def name(self):
        return self._name

    @property
    def interval(self):
        return self._interval

    @property
    def delay(self):
        return self._delay

    @property
    def lookback(self):
        return self._lookback

    def _raise_changed_interval(self, existing: str) -> None:
        """Raise error for changed cron interval."""
        raise ValueError(
            f"Cron interval mismatch for processor {self._name}: "
            f"existing '{existing}', new '{self._interval}'"
        )

    def _set_or_check_interval(self) -> None:
        """Set or check the cron interval for the processor."""

        CRON_EXPRESSION_FILENAME = "cron_expression"
        if self._fs:
            cron_uri = f"{self.processor_checkpoint_uri}/{CRON_EXPRESSION_FILENAME}"
            if not self._fs.exists(cron_uri):
                with self._fs.open(cron_uri, "w") as f:
                    f.write(self._interval)
            else:
                with self._fs.open(cron_uri, "r") as f:
                    existing_interval = f.read().strip()
                    if existing_interval != self._interval:
                        self._raise_changed_interval(existing_interval)
        else:
            cron_path = self.processor_checkpoint_path / CRON_EXPRESSION_FILENAME
            if not cron_path.exists():
                with open(cron_path, "w") as f:
                    f.write(self._interval)
            else:
                with open(cron_path, "r") as f:
                    existing_interval = f.read().strip()
                    if existing_interval != self._interval:
                        self._raise_changed_interval(existing_interval)

    def _set_checkpoint(self, window_id: str) -> None:
        """Set checkpoint for the given processor and window ID."""
        if self._fs:
            self._fs.touch(f"{self.processor_checkpoint_uri}/{window_id}")
        else:
            (Path(self.processor_checkpoint_path) / f"{window_id}").touch()

    def _has_checkpoint(self, window_id: str) -> bool:
        """Check if checkpoint exists for the given processor and window ID."""
        if self._fs:
            return self._fs.exists(f"{self.processor_checkpoint_uri}/{window_id}")
        else:
            return (Path(self.processor_checkpoint_path) / f"{window_id}").exists()

    def _parse_timedelta(self, td_str: str) -> datetime.timedelta:
        """Parse timedelta string like '1d', '12h', '30m' into timedelta object."""
        abbreviations = {
            "w": "weeks",
            "d": "days",
            "h": "hours",
            "m": "minutes",
            "s": "seconds",
        }
        match = re.match(r"^(\d+)([wdhms])$", td_str)
        if (
            not match
            or match.group(2) not in abbreviations
            or not match.group(1).isdigit()
        ):
            raise ValueError(f"Invalid timedelta string: {td_str}")
        else:
            return datetime.timedelta(
                **{abbreviations[match.group(2)]: int(match.group(1))}
            )

    def _check_in_range(self, value: int, min_val: int, max_val: int) -> None:
        """Check if a value is within a specified range."""
        if not min_val <= value <= max_val:
            raise ValueError(f"Cron {value} out of range ({min_val}-{max_val})")

    def _parse_cron_field(self, field: str, min_val: int, max_val: int) -> list[int]:
        """Parse a single cron field and return list of matching values."""
        if field == "*":
            return list(range(min_val, max_val + 1))

        elif "," in field:
            values = []
            for part in field.split(","):
                values.extend(self._parse_cron_field(part, min_val, max_val))
            return sorted(set(values))

        elif "/" in field:
            range_part, step_str = field.split("/")
            step = int(step_str)
            self._check_in_range(step, 1, max_val - min_val + 1)
            if range_part == "*":
                return list(range(min_val, max_val + 1, step))
            elif "-" in range_part:
                start_str, end_str = range_part.split("-")
                start = int(start_str.strip())
                end = int(end_str.strip())
                self._check_in_range(start, min_val, max_val)
                self._check_in_range(end, min_val, max_val)
                if start >= end:
                    raise ValueError(f"Invalid cron range field: {field}")
                return list(range(start, end + 1, step))
            elif range_part.strip().isdigit():
                single_value = int(range_part.strip())
                self._check_in_range(single_value, min_val, max_val)
                return list(range(single_value, max_val + 1, step))

        elif "-" in field:
            start_str, end_str = field.split("-")
            start = int(start_str.strip())
            end = int(end_str.strip())
            self._check_in_range(start, min_val, max_val)
            self._check_in_range(end, min_val, max_val)
            if start >= end:
                raise ValueError(f"Invalid cron range field: {field}")
            return list(range(start, end + 1))

        elif field.strip().isdigit():
            single_value = int(field.strip())
            self._check_in_range(single_value, min_val, max_val)
            return [single_value]

        raise ValueError(f"Invalid cron field: {field}")

    def _parse_cron(self, cron_expr: str) -> dict[str, list[int]]:
        """Parse cron expression into component fields."""
        parts = cron_expr.split()
        if len(parts) != 5:
            raise ValueError(
                f"Invalid cron expression: {cron_expr}. Expected 5 fields."
            )

        min_day, max_day = (1, 7) if "7" in parts[4] else (0, 6)

        return {
            "minute": self._parse_cron_field(parts[0], 0, 59),
            "hour": self._parse_cron_field(parts[1], 0, 23),
            "day": self._parse_cron_field(parts[2], 1, 31),
            "month": self._parse_cron_field(parts[3], 1, 12),
            "dow": self._parse_cron_field(parts[4], min_day, max_day),
        }

    def _get_trigger_times(
        self,
        start: datetime.datetime,
        end: datetime.datetime,
        cron_fields: dict[str, list[int]],
    ) -> list[datetime.datetime]:
        """Get all trigger times between start and end that match the cron expression."""
        trigger_times = []

        current = start.replace(second=0, microsecond=0)

        while current < end + datetime.timedelta(
            days=366
        ):  # safety limit to avoid infinite loop
            if (
                current.month in cron_fields["month"]
                and (
                    current.day in cron_fields["day"]
                    or current.weekday() in [(d + 6) % 7 for d in cron_fields["dow"]]
                )  # Convert Sunday=0 to Monday=0
            ):
                if current.hour in cron_fields["hour"]:
                    if current.minute in cron_fields["minute"]:
                        trigger_times.append(current)
                        if current >= end:
                            break
                        current_minute_index = cron_fields["minute"].index(
                            current.minute
                        )
                        if current_minute_index + 1 < len(cron_fields["minute"]):
                            current = current.replace(
                                minute=cron_fields["minute"][current_minute_index + 1]
                            )
                        else:
                            current_hour_index = cron_fields["hour"].index(current.hour)
                            if current_hour_index + 1 < len(cron_fields["hour"]):
                                current = current.replace(
                                    hour=cron_fields["hour"][current_hour_index + 1],
                                    minute=cron_fields["minute"][0],
                                )
                            else:
                                current += datetime.timedelta(days=1)
                                current = current.replace(
                                    hour=cron_fields["hour"][0],
                                    minute=cron_fields["minute"][0],
                                )
                    else:
                        current += datetime.timedelta(minutes=1)
                else:
                    current += datetime.timedelta(hours=1)
                    current = current.replace(minute=cron_fields["minute"][0])
            else:
                current += datetime.timedelta(days=1)
                current = current.replace(
                    hour=cron_fields["hour"][0], minute=cron_fields["minute"][0]
                )

        return trigger_times

    def _get_windows(self) -> list[Window]:
        """Convert cron expression to list of windows."""
        now = datetime.datetime.now(tz=self._timezone)

        # Parse timedelta strings
        delay = self._parse_timedelta(self._delay)
        lookback = self._parse_timedelta(self._lookback)

        # Calculate the earliest time to look back
        earliest_time = now - lookback

        # Calculate the latest time when a window can be considered complete
        latest_complete_time = now - delay

        # Parse cron expression
        cron_fields = self._parse_cron(self._interval)
        trigger_times = self._get_trigger_times(earliest_time, now, cron_fields)

        # Convert trigger times to windows
        windows = []
        for i in range(len(trigger_times) - 1):
            start_time = trigger_times[i]
            end_time = trigger_times[i + 1]

            # Determine if this window is complete
            complete = end_time <= latest_complete_time
            window_id = f"{start_time.isoformat()}_{end_time.isoformat()}"

            if (complete or self._include_incomplete) and not self._has_checkpoint(
                window_id=window_id
            ):
                # Create window
                window = Window(
                    window_id=window_id,
                    start=start_time,
                    end=end_time,
                    complete=complete,
                )
                windows.append(window)

        return windows

    def __call__(self) -> None:
        """Process all applicable windows."""
        logger = logging.getLogger(self._logger_name)
        for window in self._get_windows():
            try:
                self._func(window=window, **self._additional_kwargs)
                if window.complete:
                    self._set_checkpoint(window.window_id)
            except PreventCompletion as e:
                logger.info(f"Window {window.window_id} completion prevented: {e}")
            except Exception as e:
                if self._abort_on_exception:
                    raise e
                else:
                    logger.error(
                        f"Processing window {window.window_id} failed with exception: {e}"
                    )
