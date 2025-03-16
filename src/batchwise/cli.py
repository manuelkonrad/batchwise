# SPDX-FileCopyrightText: 2025 Manuel Konrad
#
# SPDX-License-Identifier: MIT

import importlib.util
import logging
import time
from pathlib import Path

from batchwise.config import config

logger = logging.getLogger("batchwise")


def cli() -> None:
    """Run the command line interface loop."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    processing = True
    while processing:
        start_time = time.time()
        logger.info("Discovering and running engines.")
        for path in config.engine_paths:
            if Path(path).is_dir():
                file_paths = sorted(Path(path).rglob("*.py"))
            else:
                file_paths = [Path(path)]
            for file_path in file_paths:
                logger.info(f"Inspecting file {file_path}.")
                try:
                    spec = importlib.util.spec_from_file_location(
                        file_path.name.replace(".py", ""), file_path
                    )
                    if spec is None:
                        logger.warning(
                            f"Could not load spec from engine file path {file_path}."
                        )
                        continue
                    module = importlib.util.module_from_spec(spec)
                    if spec.loader is None:
                        logger.warning(
                            f"Could not load module from spec for {file_path}."
                        )
                        continue
                    spec.loader.exec_module(module)
                    if hasattr(module, "engine") and callable(module.engine):
                        logger.info(f"Running engine in {file_path}.")
                        module.engine()
                    else:
                        logger.info(f"No engine found in file {file_path}. Skipping.")
                except Exception as e:
                    logger.error(f"Error while running engine file {file_path}: {e}")
        if config.interval:
            logger.info("Waiting for next iteration.")
            time.sleep(max(0, config.interval - (time.time() - start_time)))
        else:
            logger.info(
                "Tried to run all engines once. Exiting since no interval is set."
            )
            processing = False
