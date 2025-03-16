# SPDX-FileCopyrightText: 2025 Manuel Konrad
#
# SPDX-License-Identifier: MIT

import logging
import uuid
from collections import OrderedDict
from typing import Any, Callable, Literal

from batchwise.config import config
from batchwise.dataset import ArrowDataset
from batchwise.processor import Processor
from batchwise.store import FeatureStore

logger = logging.getLogger("batchwise")


class Engine:
    """Manages processors and orchestrates data processing."""

    def __init__(
        self,
        feature_store: FeatureStore | None = None,
        processor_default_config: dict[str, Any] | None = None,
    ) -> None:
        """Initialize with an optional feature store."""
        self._feature_store = feature_store or FeatureStore(
            catalog_paths=config.catalog_paths
        )
        self._processor_default_config = (
            processor_default_config or config.processor_default_config or {}
        )
        self._processor_configs = config.processor_configs or {}
        self._processors: OrderedDict[str, Processor] = OrderedDict()
        self._sinks: list[str] = []

    def processor(
        self,
        name: str,
        sink: str,
        source: str | list[str] | None = None,
        extend_before: int = 0,
        extend_after: int = 0,
        max_lookback: int = 10,
        completion_delay: str = "2 minutes",
        output_mode: Literal["overwrite", "complete", "append"] = "overwrite",
        post_processing_callback: Callable[..., None] | None = None,
        every: str | None = None,
        config: dict[str, Any] | None = None,
    ) -> Callable[..., None]:
        """Decorator to register a processor with given parameters."""

        def processor_inner(function):
            self.add_processor(
                name=name,
                source=source,
                sink=sink,
                function=function,
                extend_before=extend_before,
                extend_after=extend_after,
                max_lookback=max_lookback,
                completion_delay=completion_delay,
                output_mode=output_mode,
                post_processing_callback=post_processing_callback,
                every=every,
                config=config,
            )

        return processor_inner

    def _create_simple_dataset_handler(self, dataset_string: str) -> ArrowDataset:
        """Create a minimal Dataset handler from a dataset string."""
        file_format, dataset_path = dataset_string.split("@")
        columns = dict()
        partitioning_columns = []
        datetime_columns = []
        if "[" in dataset_path:
            dataset_path, partitioning_string = dataset_path.split("[")
            partitioning_string = partitioning_string.strip("]")
            partitioning_scheme, partition_columns = partitioning_string.split(":")
            partitioning_column_strings = partition_columns.split(",")

            for partitioning_column in partitioning_column_strings:
                column_name, column_type = partitioning_column.split("=")
                partitioning_columns.append(column_name)
                if column_type in ["date", "year", "month", "day", "hour", "minute"]:
                    datetime_columns.append(column_name)
                columns[column_name] = {
                    "column_type": column_type,
                }
        return ArrowDataset(
            name=str(uuid.uuid4()),
            file_format=file_format,
            dataset_uri=dataset_path,
            columns=columns,
            partitioning_columns=partitioning_columns,
            datetime_columns=datetime_columns,
        )

    def _get_dataset_handler(self, dataset_string: str) -> ArrowDataset:
        """Retrieve or build a dataset handler from registry or path."""
        if "@" in dataset_string:
            return self._create_simple_dataset_handler(dataset_string)
        else:
            catalog_name, dataset_name = dataset_string.split(":")
            if self._feature_store is not None:
                return self._feature_store.catalogs[catalog_name][dataset_name]
            else:
                raise ValueError("Feature store must be specified.")

    def _get_dataset_handlers(
        self, source: str | list[str] | None, sink: str
    ) -> dict[str, Any]:
        """Get dataset handlers for source and sink."""
        dataset_handlers: dict[str, Any] = {"source": None, "sink": None}
        if isinstance(source, list):
            dataset_handlers["source"] = []
            for dataset_string in source:
                dataset_handlers["source"].append(
                    self._get_dataset_handler(dataset_string)
                )
        elif source:
            dataset_handlers["source"] = self._get_dataset_handler(source)
        else:
            dataset_handlers["source"] = None
        if sink:
            dataset_handlers["sink"] = self._get_dataset_handler(sink)
        else:
            raise ValueError("Sink must be specified.")
        return dataset_handlers

    def add_processor(
        self,
        function: Callable[..., Any],
        name: str,
        source: str | list[str] | None,
        sink: str,
        extend_before: int,
        extend_after: int,
        max_lookback: int,
        completion_delay: str,
        output_mode: str,
        post_processing_callback: Callable[..., None] | None,
        every: str | None,
        config: dict[str, Any] | None,
    ) -> None:
        """Add a new processor to the Engine."""
        if sink not in self._sinks:
            self._sinks.append(sink)
        else:
            raise ValueError("Sink must be unique.")
        if name in self._processors:
            raise ValueError("Processor name must be unique.")
        processor_config = dict(self._processor_default_config)
        if name in self._processor_configs:
            processor_config.update(self._processor_configs[name])
        if config:
            processor_config.update(config)
        self._processors[name] = Processor(
            function=function,
            dataset_handlers=self._get_dataset_handlers(source, sink),
            extend_before=extend_before,
            extend_after=extend_after,
            max_lookback=max_lookback,
            completion_delay=completion_delay,
            output_mode=output_mode,
            post_processing_callback=post_processing_callback,
            every=every,
            config=processor_config,
        )

    def run_sequentially(self) -> None:
        """Run all registered processors in sequence."""
        for processor_name, processor in self._processors.items():
            logger.info(f"Running processor {processor_name}.")
            try:
                processor()
            except Exception as e:
                logger.error(f"Error while running processor {processor_name}: {e}")

    def __call__(self) -> None:
        """Executes run_sequentially when called."""
        self.run_sequentially()
