# SPDX-FileCopyrightText: 2025 Manuel Konrad
#
# SPDX-License-Identifier: MIT

import datetime
import functools
import json
import operator
from abc import abstractmethod
from pathlib import Path, PurePosixPath
from typing import Any, Literal

import fsspec
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import yaml
from fsspec.implementations.arrow import ArrowFSWrapper
from fsspec.implementations.dirfs import DirFileSystem
from PIL import Image
from pyarrow import dataset, fs
from pydantic import BaseModel, model_validator

from batchwise.constants import (
    ColumnType,
    ImageFormat,
    RawFormat,
    StringFormat,
    TypeGroups,
)


class FsConnection(BaseModel):
    """Manages filesystem connections via PyArrow or fsspec."""

    backend_fs_type: Literal["pyarrow", "fsspec"] = "pyarrow"
    protocol: str | None = "local"
    base_path: str | None = None
    kwargs: dict = {}

    def __getitem__(self, key: str) -> bytes:
        """Return file content as bytes from a given key."""
        fs = self.get_fsspec_fs()
        with fs.open(key, "rb") as f:
            return f.read()

    def __setitem__(self, key: str, value: bytes) -> None:
        """Write bytes content to a given key."""
        fs = self.get_fsspec_fs()
        with fs.open(key, "wb") as f:
            f.write(value)

    @model_validator(mode="after")
    def validate_base_path(self) -> "FsConnection":
        """Validate local base path if specified, adjusting it to absolute."""
        if self.protocol == "local" and self.base_path:
            self.base_path = str(Path(self.base_path).resolve())
        return self

    def get_fsspec_fs(
        self,
    ) -> fsspec.AbstractFileSystem | DirFileSystem | ArrowFSWrapper:
        """Get an fsspec-based filesystem."""
        if self.backend_fs_type == "fsspec":
            kwargs = {"auto_mkdir": True}
            kwargs.update(self.kwargs)
            base_fs = fsspec.filesystem(
                self.protocol,
                **kwargs,
            )
            if self.base_path:
                return DirFileSystem(self.base_path, base_fs)
            else:
                return base_fs
        elif self.backend_fs_type == "pyarrow":
            return ArrowFSWrapper(self.get_pyarrow_fs())

    def get_pyarrow_fs(self) -> fs.FileSystem:
        """Get a PyArrow filesystem from the current configuration."""
        if self.backend_fs_type == "fsspec":
            fsspec_fs = fsspec.filesystem(self.protocol, **self.kwargs)
            base_fs = fs.PyFileSystem(fs.FSSpecHandler(fsspec_fs))
        elif self.backend_fs_type == "pyarrow":
            if self.protocol == "local":
                base_fs = fs.LocalFileSystem(**self.kwargs)
            elif self.protocol == "s3":
                base_fs = fs.S3FileSystem(**self.kwargs)
            elif self.protocol == "gcs":
                base_fs = fs.GcsFileSystem(**self.kwargs)
            elif self.protocol == "hdfs":
                base_fs = fs.HadoopFileSystem(**self.kwargs)
            else:
                raise ValueError(
                    "Unknown pyarrow fs protocol: {}".format(self.protocol)
                )
        else:
            raise ValueError("Unknown backend fs type: {}".format(self.backend_fs_type))
        if self.base_path:
            return fs.SubTreeFileSystem(self.base_path, base_fs)
        else:
            return base_fs


class Column(BaseModel):
    """Represents a dataset column and its storage details."""

    column_type: ColumnType
    group: str | None = None
    uri_connection: FsConnection | None = None
    object_format: StringFormat | ImageFormat | RawFormat | None = None
    type_string: str | None = None
    description: str | None = None

    @model_validator(mode="before")
    @classmethod
    def extract_pa_type(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Extract an optional PyArrow type string from the column_type field."""
        if len(column_type_split := values["column_type"].split(":")) == 2:
            values["column_type"], values["type_string"] = column_type_split
        return values

    def read_uri_object(self, uri: str) -> Any:
        """Read object from URI respecting the column's specified format."""
        if self.uri_connection is None:
            raise ValueError("Column URI connection not specified.")
        fs = self.uri_connection.get_fsspec_fs()
        with fs.open(uri, "rb") as uri_file:
            if self.object_format in StringFormat:
                decoded = yaml.safe_load(uri_file)
            elif self.object_format in ImageFormat:
                decoded = np.array(Image.open(uri_file))
            elif self.object_format in RawFormat:
                if self.object_format == "binary":
                    decoded = uri_file.read()
                else:
                    decoded = uri_file.read().decode(self.object_format)
            elif self.object_format is None:
                decoded = uri_file.read()
            else:
                raise ValueError("Unknown object format: {}".format(self.object_format))
        return decoded

    def write_uri_object(self, uri: str, binary_object: Any) -> None:
        """Write object to URI in the column's specified format."""
        if self.uri_connection is None:
            raise ValueError("Column URI connection not specified.")
        fs = self.uri_connection.get_fsspec_fs()
        with fs.open(uri, "wb", auto_mkdir=True) as uri_file:
            if self.object_format in StringFormat:
                yaml.safe_dump(binary_object, uri_file)
            elif self.object_format in ImageFormat:
                Image.fromarray(binary_object).save(uri_file, self.object_format)
            elif self.object_format in RawFormat:
                if self.object_format == "binary":
                    uri_file.write(binary_object)
                else:
                    uri_file.write(binary_object.encode(self.object_format))
            elif self.object_format is None:
                uri_file.write(binary_object.encode(self.object_format))
            else:
                raise ValueError("Unknown object format: {}".format(self.object_format))


class AnnotationConfig(BaseModel):
    """Holds configuration for annotation tasks and file paths."""

    annotation_type: Literal["label", "value", "bbox", "polygon", "rle"]
    classes: dict[int, str] | None = None
    annotations_uri: str | None = None
    annotations_connection: FsConnection | None = FsConnection()
    task_id_column: str | None = None
    target_columns: list[str] | None = None

    def _get_full_path(self, path: str) -> str:
        """Build the full path to an annotation file."""
        if self.annotations_uri is None:
            raise ValueError("Annotations URI not specified.")
        full_path = str(PurePosixPath(self.annotations_uri, path)).replace(":/", "://")
        return full_path

    def set_annotation(self, path: str, annotation: dict[str, Any]) -> None:
        """Save annotation to the specified path."""
        if self.annotations_connection is None:
            raise ValueError("Annotations connection not specified.")
        full_path = self._get_full_path(path)
        self.annotations_connection[full_path] = json.dumps(annotation).encode("utf-8")

    def get_annotation(self, path: str) -> dict[str, Any]:
        """Load annotation from the specified path."""
        if self.annotations_connection is None:
            raise ValueError("Annotations connection not specified.")
        full_path = self._get_full_path(path)
        return json.loads(self.annotations_connection[full_path])


class Dataset(BaseModel):
    """Base representation of a structured dataset with columns and connections."""

    name: str
    catalog: str | None = None
    tags: list[str] | None = None
    description: str | None = None
    connections: dict[str, FsConnection] | None = None
    columns: dict[str, Column] = {}
    datetime_columns: list[str] = []
    timestamp_column: str | None = None
    primary_id_column: str | None = None
    annotation_configs: dict[str, AnnotationConfig] | None = None
    group_dag: dict[str, list] | None = None

    @model_validator(mode="before")
    @classmethod
    def inject_connections(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Add default connections into FsConnection objects."""
        for column in values["columns"].values():
            if isinstance(column.get("uri_connection"), str):
                column["uri_connection"] = values["connections"][
                    column["uri_connection"]
                ]
        for annotation_config in values.get("annotation_configs", {}).values():
            if isinstance(annotation_config.get("annotations_connection"), str):
                annotation_config["annotations_connection"] = values["connections"][
                    annotation_config["annotations_connection"]
                ]
        return values

    def get_column_names_by_type(
        self,
    ) -> tuple[str | None, str | None, dict[str, list[str]]]:
        """Return primary key column, timestamp column, and columns grouped by type."""
        columns_by_type: dict[str, list[str]] = {}
        for column_type in ColumnType:
            columns_by_type[column_type.value] = []
        for column_name, column in self.columns.items():
            if (
                column_name == self.primary_id_column
                and not column.column_type == "identifier"
            ):
                raise ValueError("Primary key must be of type identifier.")
            if (
                column_name == self.timestamp_column
                and not column.column_type == "timestamp"
            ):
                raise ValueError("Timestamp column must be of type timestamp.")
            if column.column_type in columns_by_type:
                columns_by_type[column.column_type].append(column_name)
            else:
                raise ValueError("Unknown column type: {}".format(column.column_type))
        return self.primary_id_column, self.timestamp_column, columns_by_type

    def _check_every(
        self, timestamp: datetime.datetime, every: str, window_unit: str
    ) -> bool:
        """Check if the 'every' schedule matches the given timestamp."""
        every_list = every.split()
        minute = timestamp.minute
        hour = timestamp.hour
        day = timestamp.day
        month = timestamp.month
        day_of_week = (timestamp.weekday() + 1) % 7

        if len(every_list) != 5:
            raise ValueError(
                "Every must have 5 components: minute hour day month day_of_week"
            )

        if window_unit == "hour" and every_list[0] != "0":
            raise ValueError("String must start with 0 for hourly windows.")
        elif window_unit == "day" and (every_list[0] != "0" or every_list[1] != "0"):
            raise ValueError("String must start with 0 0 for daily windows.")
        elif window_unit == "month" and (
            every_list[0] != "0"
            or every_list[1] != "0"
            or every_list[2] != "1"
            or every_list[4] != "*"
        ):
            raise ValueError(
                "String must start with 0 0 1 and end with * for monthly windows."
            )
        elif window_unit == "year" and (
            every_list[0] != "0"
            or every_list[1] != "0"
            or every_list[2] != "1"
            or every_list[3] != "1"
            or every_list[4] != "*"
        ):
            raise ValueError("String must be 0 0 1 1 * for yearly windows.")

        def check_field(field_value, pattern):
            # Wildcard means match everything
            if pattern == "*":
                return True

            # Handle multiple patterns separated by commas
            if "," in str(pattern):
                for p in str(pattern).split(","):
                    if check_field(field_value, p):
                        return True
                return False

            # Handle step values (with or without ranges)
            if "/" in str(pattern):
                range_part, step_part = str(pattern).split("/")
                step = int(step_part)

                # Handle */step syntax
                if range_part == "*":
                    return field_value % step == 0

                # Handle range/step syntax
                if "-" in range_part:
                    start, end = map(int, range_part.split("-"))
                    if start <= field_value <= end:
                        return (field_value - start) % step == 0
                    return False

                # Handle single value/step
                return int(range_part) == field_value

            # Handle ranges
            if "-" in str(pattern):
                start, end = map(int, str(pattern).split("-"))
                return start <= field_value <= end

            # Handle specific values
            try:
                return int(pattern) == field_value
            except ValueError:
                return False

        minute_match = check_field(minute, every_list[0])
        hour_match = check_field(hour, every_list[1])
        day_match = check_field(day, every_list[2])
        month_match = check_field(month, every_list[3])
        day_of_week_match = check_field(day_of_week, every_list[4])

        return (
            minute_match
            and hour_match
            and day_match
            and month_match
            and day_of_week_match
        )

    def get_windows(
        self,
        max_lookback: int,
        extend_before: int,
        extend_after: int,
        completion_delay: str,
        output_mode: str,
        every: str | None,
    ) -> list[list[Any]]:
        """Generate time-based windows according to dataset settings."""
        datetime_vars = TypeGroups.DATETIME_VARS

        if len(self.datetime_columns) > 0:
            if self.columns[self.datetime_columns[-1]].column_type == "date":
                window_unit = ColumnType.DAY.value
            else:
                window_unit = self.columns[self.datetime_columns[-1]].column_type.value
        else:
            raise ValueError("No datetime columns specified.")

        replacement_values = {
            key.value: 0
            for key in datetime_vars[
                datetime_vars.index(ColumnType[window_unit.upper()]) + 1 :
            ]
        }

        now = datetime.datetime.now(datetime.timezone.utc)
        now_rounded = now.replace(**replacement_values)  # type: ignore
        windows = []
        unit_delta = datetime.timedelta(**{window_unit + "s": 1})
        for i in range(max_lookback):
            segments: dict[str, dict[str, Any]] = {}
            for segment_name in ["inner", "before", "after"]:
                segments[segment_name] = {
                    "parts": [],
                    "start": None,
                    "end": None,
                    "unit": window_unit,
                }
            delta = datetime.timedelta(**{window_unit + "s": i})
            inner_timestamp = now_rounded - delta
            if every and not self._check_every(inner_timestamp, every, window_unit):
                continue
            segments["inner"]["parts"].append(inner_timestamp)
            segments["inner"]["start"] = inner_timestamp
            segments["inner"]["end"] = latest_end = inner_timestamp + unit_delta
            for j in range(extend_before, 0, -1):
                segments["before"]["parts"].append(
                    inner_timestamp - datetime.timedelta(**{window_unit + "s": j})
                )
            if len(segments["before"]["parts"]) > 0:
                segments["before"]["start"] = segments["before"]["parts"][0]
                segments["before"]["end"] = segments["before"]["parts"][-1] + delta
            for j in range(1, extend_after + 1):
                segments["after"]["parts"].append(
                    inner_timestamp + datetime.timedelta(**{window_unit + "s": j})
                )
            if len(segments["after"]["parts"]) > 0:
                segments["after"]["start"] = segments["after"]["parts"][0]
                segments["after"]["end"] = latest_end = (
                    segments["after"]["parts"][-1] + unit_delta
                )
            completed = self.check_completion(segments)
            can_be_completed = now - latest_end > datetime.timedelta(
                **{completion_delay.split()[1]: int(completion_delay.split()[0])}
            )
            if not completed and (
                can_be_completed or output_mode in ["overwrite", "append"]
            ):
                windows.append([segments, can_be_completed])
        return windows

    @abstractmethod
    def get_dataframes(
        self, segments: dict[str, dict[str, Any]]
    ) -> dict[str, pd.DataFrame]:
        """Retrieve dataframes for each segment."""
        pass


class RwDataset(Dataset):
    """Abstract base for datasets that support reading and writing."""

    @abstractmethod
    def write_dataframe(
        self, dataframe: pd.DataFrame, segments: dict[str, Any]
    ) -> None:
        """Write the given dataframe, partitioned by segments."""
        pass

    @abstractmethod
    def check_completion(self, segments: dict[str, Any]) -> bool:
        """Check if the dataset is marked complete for the given segments."""
        pass

    @abstractmethod
    def set_completion(self, segments: dict[str, Any]) -> None:
        """Mark the dataset as complete for the given segments."""
        pass


class ArrowDataset(RwDataset):
    """An Arrow-based dataset for partitioned reads and writes."""

    dataset_uri: str
    dataset_connection: FsConnection = FsConnection()
    file_format: Literal["parquet", "csv", "json", "feather", "binary"] = "parquet"
    partitioning_columns: list[str] = []
    partitioning_scheme: Literal["hive", "directory"] = "hive"
    infer_schema: bool = False

    @model_validator(mode="before")
    @classmethod
    def inject_dataset_connection(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Inject dataset_connections from default connections."""
        if isinstance(values.get("dataset_connection"), str):
            values["dataset_connection"] = values["connections"][
                values["dataset_connection"]
            ]
        return values

    @model_validator(mode="after")
    def _validate_partitions(self) -> "ArrowDataset":
        """Ensure partition columns match datetime columns where needed."""
        if len(self.partitioning_columns) == 0:
            self.partitioning_columns = self.datetime_columns
        if (
            not self.datetime_columns
            == self.partitioning_columns[: len(self.datetime_columns)]
        ):
            raise ValueError("Datetime columns must match the first partition columns.")

        if self.file_format == "binary":
            if (
                self.partitioning_columns + ["path"] == list(self.columns.keys())
                and self.columns["path"].column_type != "object"
            ):
                raise ValueError("Path column must be of type object.")
            elif self.partitioning_columns == list(self.columns.keys()):
                self.columns["path"] = Column(
                    column_type=ColumnType.OBJECT,
                    uri_connection=self.dataset_connection,
                    object_format=RawFormat.BINARY,
                )
            else:
                raise ValueError("Invalid columns for binary format.")
        self._get_datetime_partitioning_string()

        return self

    def get_dataframes(
        self, segments: dict[str, dict[str, Any]]
    ) -> dict[str, pd.DataFrame]:
        """Return dataframes for each requested segment from Arrow dataset."""
        filter_expressions = self._get_filter_expression(segments)
        dataframes = {}
        for segment_name, filter_expression in filter_expressions.items():
            if self.file_format == "binary":
                pa_ds = self._get_arrow_dataset()
                dataframes[segment_name] = pd.DataFrame(
                    {
                        "path": [
                            i.path
                            for i in pa_ds.get_fragments(filter=filter_expression)
                        ]
                    }
                )
            else:
                pa_ds = self._get_arrow_dataset(filter_expression=filter_expression)
                dataframes[segment_name] = pa_ds.to_table().to_pandas()
        return dataframes

    def get_dataframes_date_range(
        self, start: datetime.date, end: datetime.date
    ) -> pd.DataFrame:
        """Return a dataframe covering the specified date range."""
        unit_delta = datetime.timedelta(days=1)
        parts = []
        for i in range((end - start).days + 1):
            parts.append(start + datetime.timedelta(days=i))
        segments = {
            "range": {
                "parts": parts,
                "start": parts[0],
                "end": parts[-1] + unit_delta,
                "unit": "day",
            }
        }
        return self.get_dataframes(segments)["range"]

    def check_completion(self, segments: dict[str, Any]) -> bool:
        """Check if the given segments are marked complete."""
        time_partition = segments["inner"]["parts"][0].strftime(
            self._get_datetime_partitioning_string()
        )
        fs = self.dataset_connection.get_fsspec_fs()
        return fs.exists(time_partition + ".COMPLETED")

    def set_completion(self, segments: dict[str, Any]) -> None:
        """Mark the specified segments as complete."""
        time_partition = segments["inner"]["parts"][0].strftime(
            self._get_datetime_partitioning_string()
        )
        fs = self.dataset_connection.get_fsspec_fs()
        partition_path = str(PurePosixPath(self.dataset_uri, time_partition)).replace(
            ":/", "://"
        )
        fs.mkdir(partition_path, create_parents=True)
        fs.touch(str(PurePosixPath(partition_path, ".COMPLETED")).replace(":/", "://"))

    def write_dataframe(
        self,
        dataframe: pd.DataFrame,
        segments: dict[str, Any],
        overwrite: bool = False,
        mismatch: str = "error",
    ) -> None:
        """Write dataframe into partitioned files according to segments."""
        if len(self.datetime_columns) == 0:
            raise ValueError("No datetime columns specified.")

        if len(dataframe) == 0:
            return

        window_datetime = segments["inner"]["parts"][0]
        datetime_partitions = dataframe[self.datetime_columns].drop_duplicates()

        if mismatch == "error":
            if len(datetime_partitions) > 1:
                raise ValueError("Invalid window time range.")
            for column_name in self.datetime_columns:
                column = self.columns[column_name]
                if column.column_type == "date":
                    if datetime_partitions.iloc[0][
                        column_name
                    ] != window_datetime.strftime("%Y-%m-%d"):
                        raise ValueError("Invalid dataframe partition.")
                elif column.column_type in TypeGroups.DATETIME_VARS[:5]:
                    if datetime_partitions.iloc[0][column_name] != getattr(
                        window_datetime, column.column_type
                    ):
                        raise ValueError("Invalid dataframe partition.")
                else:
                    raise ValueError("Invalid dataframe partition.")
        elif mismatch == "ignore":
            pass
        else:
            raise ValueError("Invalid mismatch parameter.")
        datetime_partitioning_string = window_datetime.strftime(
            self._get_datetime_partitioning_string()
        )
        partitions = dataframe[self.partitioning_columns].drop_duplicates()
        fs = self.dataset_connection.get_pyarrow_fs()
        arrow_schema = None if self.infer_schema else self._get_arrow_schema()
        if len(self.partitioning_columns) > len(self.datetime_columns):
            if self.partitioning_scheme == "hive":
                partitioning_string = (
                    "/".join(
                        [
                            column_name + "={}"
                            for column_name in self.partitioning_columns[
                                len(self.datetime_columns) :
                            ]
                        ]
                    )
                    + "/"
                )
            elif self.partitioning_scheme == "directory":
                partitioning_string = "{}/" * len(
                    self.partitioning_columns[len(self.datetime_columns) :]
                )
            else:
                raise ValueError("Invalid partitioning scheme.")
        else:
            partitioning_string = ""
        for partition in partitions.itertuples(index=False):
            full_partitioning_string = datetime_partitioning_string
            filtered_dataframe = dataframe
            if len(self.partitioning_columns) > len(self.datetime_columns):
                full_partitioning_string += partitioning_string.format(
                    *partition[len(self.datetime_columns) :]
                )
                filtered_dataframe = dataframe[
                    functools.reduce(
                        operator.and_,
                        [
                            dataframe[column_name] == getattr(partition, column_name)
                            for column_name in self.partitioning_columns[
                                len(self.datetime_columns) :
                            ]
                        ],
                    )
                ]

            arrow_table = pa.table(filtered_dataframe, schema=arrow_schema)
            # fs.create_dir(full_partitioning_string, recursive=True)
            partition_path = str(
                PurePosixPath(self.dataset_uri, full_partitioning_string)
            ).replace(":/", "://")
            fs.create_dir(partition_path, recursive=True)
            table_path = str(
                PurePosixPath(partition_path, "data." + self.file_format)
            ).replace(":/", "://")
            if not fs.get_file_info(table_path).is_file or overwrite:
                if self.file_format == "parquet":
                    pq.write_table(
                        arrow_table,
                        table_path,
                        filesystem=fs,
                    )
                else:
                    raise ValueError("Invalid output file format.")

    def _get_datetime_partitioning_string(self) -> str:
        """Build directory structure format based on partition scheme and datetime columns."""
        if self.partitioning_scheme == "hive":
            format_strings = {
                ("date",): "{}=%Y-%m-%d/",
                ("date", "hour"): "{}=%Y-%m-%d/{}=%H/",
                ("date", "hour", "minute"): "{}=%Y-%m-%d/{}=%H/{}=%M/",
                ("year", "month"): "{}=%Y/{}=%m/",
                ("year", "month", "day"): "{}=%Y/{}=%m/{}=%d/",
                ("year", "month", "day", "hour"): "{}=%Y/{}=%m/{}=%d/{}=%H/",
                (
                    "year",
                    "month",
                    "day",
                    "hour",
                    "minute",
                ): "{}=%Y/{}=%m/{}=%d/{}=%H/{}=%M/",
            }
        elif self.partitioning_scheme == "directory":
            format_strings = {
                ("date",): "%Y-%m-%d/",
                ("date", "hour"): "%Y-%m-%d/%H/",
                ("date", "hour", "minute"): "%Y-%m-%d/%H/%M/",
                ("year", "month"): "%Y/%m/",
                ("year", "month", "day"): "%Y/%m/%d/",
                ("year", "month", "day", "hour"): "%Y/%m/%d/%H/",
                ("year", "month", "day", "hour", "minute"): "%Y/%m/%d/%H/%M/",
            }
        else:
            raise ValueError("Invalid partitioning scheme.")

        key = tuple(self._get_datetime_column_types())
        if key in format_strings:
            return format_strings[key].format(*self.datetime_columns)
        else:
            raise ValueError("Invalid partition columns.")

    def _get_datetime_column_types(self) -> list[str]:
        """Return the column types for datetime columns in the dataset."""
        return [
            self.columns[column_name].column_type
            for column_name in self.datetime_columns
        ]

    def _get_filter_expression(
        self, segments: dict[str, dict[str, Any]]
    ) -> dict[str, pc.Expression]:
        """Generate filter expressions for each segment."""
        datetime_column_types = self._get_datetime_column_types()
        window_unit = None
        if len(self.datetime_columns) > 0:
            if datetime_column_types[-1] == "date":
                window_unit = "day"
            else:
                window_unit = datetime_column_types[-1]

        expressions = {
            segments_name: pc.scalar(False) for segments_name in segments.keys()
        }
        expressions["undefined"] = pc.scalar(False)

        def build_expression(segment, part):
            filters = []
            for column_name in self.datetime_columns:
                column = self.columns[column_name]
                if column.column_type == "date":
                    filters.append(pc.field(column_name) == part.strftime("%Y-%m-%d"))
                elif column.column_type in TypeGroups.DATETIME_VARS[:5]:
                    filters.append(
                        pc.field(column_name) == getattr(part, column.column_type)
                    )
                if column.column_type == segment["unit"]:
                    break
            return functools.reduce(operator.and_, filters)

        for segment_name, segment in segments.items():
            if window_unit:
                for part in segment["parts"]:
                    expressions[segment_name] = expressions[
                        segment_name
                    ] | build_expression(segment, part)

                if (
                    segment["unit"] is None or segment["unit"] != window_unit
                ) and self.timestamp_column:
                    expressions[segment_name] = expressions[segment_name] | (
                        (pc.field(self.timestamp_column) >= segment["start"])
                        & (pc.field(self.timestamp_column) < segment["end"])
                    )
                elif segment["unit"] != window_unit:
                    raise ValueError(
                        "No suitable datetime or timestamp columns for filtering."
                    )

            else:
                expressions["undefined"] = pc.scalar(True)

        return expressions

    def _get_arrow_dataset(
        self, filter_expression: pc.Expression | None = None
    ) -> dataset.Dataset:
        """Build and optionally filter a PyArrow dataset from the dataset URI."""
        pa_fs = self.dataset_connection.get_pyarrow_fs()
        pa_fs.create_dir(self.dataset_uri, recursive=True)
        arrow_schema = None if self.infer_schema else self._get_arrow_schema()
        arrow_dataset = dataset.dataset(
            self.dataset_uri,
            format=self.file_format if self.file_format != "binary" else "parquet",
            filesystem=pa_fs,
            schema=arrow_schema,
            partitioning=self.partitioning_scheme
            if self.partitioning_scheme != "directory"
            else None,
        )
        if filter_expression is not None:
            arrow_dataset = arrow_dataset.filter(filter_expression)
        return arrow_dataset

    def _get_arrow_schema(self) -> pa.Schema:
        """Generate a PyArrow schema based on the dataset's column definitions."""
        fields = []
        for column_name, column in self.columns.items():
            fields.append(pa.field(column_name, self._get_column_schema(column)))
        return pa.schema(fields)

    def _get_column_schema(self, column: Column) -> pa.DataType:
        """Determine the PyArrow data type from the column's definition."""
        if column.type_string:
            return pa.type_for_alias(column.type_string)
        elif column.column_type in TypeGroups.STRING_TYPES:
            return pa.string()
        elif column.column_type in TypeGroups.FLOAT64_TYPES:
            return pa.float64()
        elif column.column_type in TypeGroups.INT8_TYPES:
            return pa.uint8()
        elif column.column_type in TypeGroups.INT16_TYPES:
            return pa.uint32()
        elif column.column_type in TypeGroups.INT32_TYPES:
            return pa.uint16()
        elif column.column_type in TypeGroups.TIMESTAMP_TYPES:
            return pa.timestamp("ns")
        elif column.column_type in TypeGroups.ARRAY_OF_STRUCT_TYPES:
            return pa.list_(pa.struct([("x", pa.float64()), ("y", pa.float64())]))
        elif column.column_type in TypeGroups.OBJECT_TYPES:
            if column.uri_connection:
                return pa.string()
            elif (
                column.object_format in StringFormat
                or column.object_format == RawFormat.UTF8
            ):
                return pa.string()
            elif (
                column.object_format in ImageFormat
                or column.object_format == RawFormat.BINARY
            ):
                return pa.binary()
            else:
                raise ValueError(
                    "Unknown object format: {}".format(column.object_format)
                )
        else:
            raise ValueError("Unknown column type: {}".format(column.column_type))
