# SPDX-FileCopyrightText: 2025 Manuel Konrad
#
# SPDX-License-Identifier: MIT

import datetime
import logging
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def temp_checkpoint_dir():
    """Fixture providing a temporary checkpoint directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def test_logger_name():
    """Fixture providing a test logger name."""
    logger_name = "test_batchwise"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    return logger_name


@pytest.fixture
def utc_timezone():
    """Fixture providing UTC timezone."""
    return datetime.timezone.utc


@pytest.fixture
def sample_context():
    """Fixture providing sample context data."""
    return {"key": "value", "number": 42}
