# SPDX-FileCopyrightText: 2025 Manuel Konrad
#
# SPDX-License-Identifier: MIT

from enum import Enum


class ColumnType(str, Enum):
    """Types of columns in a DataFrame."""

    # String types
    IDENTIFIER = "identifier"
    CATEGORICAL = "categorical"
    TEXTUAL = "textual"
    DATE = "date"

    # Int8 types
    MONTH = "month"
    DAY = "day"
    HOUR = "hour"
    MINUTE = "minute"
    SECOND = "second"

    # Int16 types
    YEAR = "year"

    # Int32 types
    MICROSECOND = "microsecond"
    NANOSECOND = "nanosecond"

    # Timestamp types
    TIMESTAMP = "timestamp"

    # Float64 types
    NUMERICAL = "numerical"

    # Array of struct types
    CURVE = "curve"

    # Object types
    OBJECT = "object"


class TypeGroups:
    """Mapping of column types to groups of data types."""

    STRING_TYPES = {
        ColumnType.IDENTIFIER,
        ColumnType.CATEGORICAL,
        ColumnType.TEXTUAL,
        ColumnType.DATE,
    }

    INT8_TYPES = {
        ColumnType.MONTH,
        ColumnType.DAY,
        ColumnType.HOUR,
        ColumnType.MINUTE,
        ColumnType.SECOND,
    }

    INT16_TYPES = {ColumnType.YEAR}
    INT32_TYPES = {ColumnType.MICROSECOND, ColumnType.NANOSECOND}
    TIMESTAMP_TYPES = {ColumnType.TIMESTAMP}
    FLOAT64_TYPES = {ColumnType.NUMERICAL}
    ARRAY_OF_STRUCT_TYPES = {ColumnType.CURVE}
    OBJECT_TYPES = {ColumnType.OBJECT}

    DATETIME_VARS = [
        ColumnType.YEAR,
        ColumnType.MONTH,
        ColumnType.DAY,
        ColumnType.HOUR,
        ColumnType.MINUTE,
        ColumnType.SECOND,
        ColumnType.MICROSECOND,
    ]


class StringFormat(str, Enum):
    """Text-based formats that can be loaded using yaml.safe_load"""

    JSON = "json"
    YAML = "yaml"


class ImageFormat(str, Enum):
    """Image formats that can be loaded using PIL.Image"""

    IMG = "img"
    BMP = "bmp"
    PNG = "png"
    JPG = "jpg"
    JPEG = "jpeg"


class RawFormat(str, Enum):
    """Raw formats requiring explicit encoding"""

    UTF8 = "utf-8"
    BINARY = "binary"
