# SPDX-FileCopyrightText: 2025 Manuel Konrad
#
# SPDX-License-Identifier: MIT

import inspect
from typing import Any, Callable


class Processor:
    """Processes data windows using a user-defined function."""

    def __init__(
        self,
        function: Callable[..., Any],
        dataset_handlers: dict[str, Any],
        extend_before: int,
        extend_after: int,
        max_lookback: int,
        completion_delay: str,
        output_mode: str,
        post_processing_callback: Callable[..., None] | None,
        every: str | None,
        config: dict[str, Any],
    ) -> None:
        """Initialize with function, dataset handlers, and scheduling parameters."""
        self._function = function
        self._dataset_handlers = dataset_handlers
        self._extend_before = extend_before
        self._extend_after = extend_after
        self._max_lookback = max_lookback
        self._completion_delay = completion_delay
        self._output_mode = output_mode
        self._post_processing_callback = post_processing_callback
        self._every = every
        self._config = config
        self._signature = set(inspect.signature(self._function).parameters.keys())
        valid_parameters = (
            "source_data",
            "sink_data",
            "source_fs",
            "sink_fs",
            "context",
        )
        if not self._signature.issubset(valid_parameters):
            raise ValueError("Invalid function signature.")

    def __call__(self) -> None:
        """Execute the user-defined function over configured data windows."""
        windows = self._dataset_handlers["sink"].get_windows(
            self._max_lookback,
            self._extend_before,
            self._extend_after,
            self._completion_delay,
            self._output_mode,
            self._every,
        )
        for segments, can_be_completed in windows:
            input_dict: dict[str, Any] = {}
            if "source_data" in self._signature:
                if isinstance(self._dataset_handlers.get("source"), list):
                    input_dict["source_data"] = [
                        handler.get_dataframes(segments)
                        for handler in self._dataset_handlers["source"]
                    ]
                elif self._dataset_handlers.get("source"):
                    input_dict["source_data"] = self._dataset_handlers[
                        "source"
                    ].get_dataframes(segments)
                else:
                    input_dict["source_data"] = None
            if "sink_data" in self._signature:
                input_dict["sink_data"] = self._dataset_handlers["sink"].get_dataframes(
                    segments
                )
            if "source_fs" in self._signature:
                if isinstance(self._dataset_handlers["source"], list):
                    input_dict["source_fs"] = []
                    for handler in self._dataset_handlers["source"]:
                        source_fs_dict = {}
                        for column_name, column in handler.columns.items():
                            if column.column_type == "object" and column.uri_connection:
                                source_fs_dict[column_name] = column.uri_connection
                        input_dict["source_fs"].append(source_fs_dict)
                elif self._dataset_handlers["source"]:
                    input_dict["source_fs"] = {}
                    for column_name, column in self._dataset_handlers[
                        "source"
                    ].columns.items():
                        if column.column_type == "object" and column.uri_connection:
                            input_dict["source_fs"][column_name] = column.uri_connection
                else:
                    input_dict["source_fs"] = None
            if "sink_fs" in self._signature:
                input_dict["sink_fs"] = {}
                for column in self._dataset_handlers["sink"].columns:
                    if column.column_type == "object" and column.uri_connection:
                        input_dict["sink_fs"][column.name] = column.uri_connection
            if "context" in self._signature:
                input_dict["context"] = {"segments": segments, "config": self._config}
            result = self._function(**input_dict)
            overwrite = self._output_mode == "overwrite"
            self._dataset_handlers["sink"].write_dataframe(
                result, segments, overwrite=overwrite
            )
            if can_be_completed:
                self._dataset_handlers["sink"].set_completion(segments)
            if self._post_processing_callback:
                self._post_processing_callback(result)
