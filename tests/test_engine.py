# SPDX-FileCopyrightText: 2025 Manuel Konrad
#
# SPDX-License-Identifier: MIT

import datetime
import shutil
import subprocess  # nosec
from pathlib import Path

import pandas as pd
import yaml
from pytest import fixture

from batchwise import Engine, FeatureStore


@fixture
def prepared_tmp_paths(tmp_path):
    """
    Prepare temporary paths and datasets for testing.
    """
    now = datetime.datetime.now()

    # Create parquet files with three partitions
    dataset_1_path = tmp_path / "test_1_dataset"
    for i in range(3):
        current = now - datetime.timedelta(hours=i)
        df = pd.DataFrame(
            {
                "id": [str(i)],
                "value": [i],
                "date": [current.strftime("%Y-%m-%d")],
                "hour": [current.hour],
            }
        )
        output_path = (
            dataset_1_path / f"date={current.strftime('%Y-%m-%d')}/hour={current.hour}"
        )
        output_path.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path / "data.parquet")

    # Prepare dataset catalog definitions
    catalog_paths = tmp_path / "catalogs"
    catalog_paths.mkdir(parents=True, exist_ok=True)

    dataset_1_dict = {
        "name": "test_1",
        "catalog": "test",
        "dataset_uri": "test_1_dataset",
        "primary_id_column": "id",
        "dataset_connection": {
            "base_path": str(tmp_path),
        },
        "format": "parquet",
        "datetime_columns": ["date", "hour"],
        "columns": {
            "id": {"column_type": "identifier"},
            "value": {"column_type": "numerical"},
            "date": {"column_type": "date"},
            "hour": {"column_type": "hour"},
        },
    }

    with open(catalog_paths / "test_1.yaml", "w") as file:
        file.write(yaml.dump(dataset_1_dict))

    dataset_2_dict = {
        "name": "test_2",
        "catalog": "test",
        "dataset_uri": "test_2_dataset",
        "primary_id_column": "id",
        "dataset_connection": {
            "base_path": str(tmp_path),
        },
        "format": "parquet",
        "datetime_columns": ["date", "hour"],
        "columns": {
            "id": {"column_type": "identifier"},
            "value": {"column_type": "numerical"},
            "date": {"column_type": "date"},
            "hour": {"column_type": "hour"},
        },
    }

    with open(catalog_paths / "test_2.yaml", "w") as file:
        file.write(yaml.dump(dataset_2_dict))

    # Copy example engine
    engine_paths = tmp_path / "engines"
    engine_paths.mkdir(parents=True, exist_ok=True)
    example_engine_path = Path(__file__).parent / "example_engine.py"
    shutil.copy(example_engine_path, engine_paths / "example_engine.py")

    return tmp_path, catalog_paths, engine_paths, dataset_1_path


def test_simple(prepared_tmp_paths):
    """
    Test a simple engine processor.
    """
    base_path, _, _, dataset_1_path = prepared_tmp_paths
    engine = Engine()

    output_path = base_path / "output_simple"
    output_path.mkdir(parents=True, exist_ok=True)

    @engine.processor(
        name="test",
        source=f"parquet@{str(dataset_1_path)}[hive:date=date,hour=hour]",
        sink=f"parquet@{str(output_path)}[hive:date=date,hour=hour]",
    )
    def test_processor(source_data, sink_data, source_fs, context):
        return source_data["inner"]

    engine()


def test_feature_store(prepared_tmp_paths):
    """
    Test the feature store integration with the engine.
    """
    base_path, catalog_paths, engine_paths, _ = prepared_tmp_paths

    output_path = base_path / "output_feature_store"
    output_path.mkdir(parents=True, exist_ok=True)
    feature_store = FeatureStore(catalog_paths=[str(catalog_paths)])
    engine = Engine(feature_store=feature_store)

    @engine.processor(
        name="test",
        source="test:test_1",
        sink=f"parquet@{str(output_path)}[hive:date=date,hour=hour]",
    )
    def test_processor(source_data, sink_data, source_fs, context):
        return source_data["inner"]

    engine()


def test_cli(prepared_tmp_paths):
    """
    Test the CLI functionality.
    """
    _, catalog_paths, engine_paths, _ = prepared_tmp_paths

    cli_run = subprocess.run(  # nosec
        [
            "batchwise",
            "--engine_paths",
            str(engine_paths),
            "--catalog_paths",
            str(catalog_paths),
        ]
    )
    cli_run.check_returncode()
