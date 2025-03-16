# SPDX-FileCopyrightText: 2025 Manuel Konrad
#
# SPDX-License-Identifier: MIT


from pathlib import Path

import yaml
from pydantic import BaseModel, model_validator

from batchwise.dataset import ArrowDataset


class FeatureStore(BaseModel):
    """Feature store model for managing dataset catalogs."""

    catalogs: dict[str, dict[str, ArrowDataset]] = {}
    catalog_paths: list[str] = []

    @model_validator(mode="before")
    @classmethod
    def load_feature_store(cls, values: dict) -> dict:
        """Load all datasets from catalog paths."""
        catalog_paths = values["catalog_paths"]
        catalogs: dict[str, dict[str, ArrowDataset]] = dict()
        for catalog_path in catalog_paths:
            catalog_path = Path(catalog_path)
            if (
                Path(catalog_path, "defaults.yaml").exists()
                and Path(catalog_path, "defaults.json").exists()
            ):
                raise ValueError(
                    "Only one of defaults.yaml and defaults.json is allowed."
                )
            elif Path(catalog_path, "defaults.yaml").exists():
                defaults_path = Path(catalog_path, "defaults.yaml")
            elif Path(catalog_path, "defaults.json").exists():
                defaults_path = Path(catalog_path, "defaults.json")
            else:
                defaults_path = None
            defaults = dict()
            if defaults_path:
                with open(defaults_path, "r", encoding="utf-8") as defaults_file:
                    defaults = yaml.safe_load(defaults_file)
            for file_path in list(catalog_path.rglob("*.yaml")) + list(
                catalog_path.rglob("*.json")
            ):
                with open(file_path, "r", encoding="utf-8") as config_file:
                    dataset_dict = yaml.safe_load(config_file)
                    dataset_dict.setdefault("connections", dict())
                    for key, val in defaults.get("connections", {}).items():
                        dataset_dict["connections"].setdefault(key, val)
                    dataset_dict.setdefault("catalog", defaults.get("catalog"))
                    catalog = catalogs.setdefault(dataset_dict["catalog"], dict())
                    catalog[dataset_dict["name"]] = dataset_dict
        values["catalogs"] = catalogs
        return values
