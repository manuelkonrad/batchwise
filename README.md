# batchwise

[![CI - Tests](https://github.com/manuelkonrad/batchwise/actions/workflows/tests.yml/badge.svg)](https://github.com/manuelkonrad/batchwise/actions/workflows/tests.yml)
[![CI - Bandit](https://github.com/manuelkonrad/batchwise/actions/workflows/bandit.yml/badge.svg)](https://github.com/manuelkonrad/batchwise/actions/workflows/bandit.yml)
[![CI - Build](https://github.com/manuelkonrad/batchwise/actions/workflows/build.yml/badge.svg)](https://github.com/manuelkonrad/batchwise/actions/workflows/build.yml)

[![License - MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://spdx.org/licenses/MIT.html)
[![PyPI - Version](https://img.shields.io/pypi/v/batchwise.svg)](https://pypi.org/project/batchwise)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/batchwise.svg)](https://pypi.org/project/batchwise)
[![Python Project Management - Hatch](https://img.shields.io/badge/%F0%9F%A5%9A-Hatch-4051b5.svg)](https://github.com/pypa/hatch)
[![Linting - Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Types - Mypy](https://img.shields.io/badge/types-Mypy-blue.svg)](https://github.com/python/mypy)
[![Security - Bandit](https://img.shields.io/badge/security-Bandit-yellow.svg)](https://github.com/PyCQA/bandit)


Lightweight batch processing engine.

## Table of Contents

- [Getting Started](#getting_started)
- [Examples](#examples)
- [License](#license)

## Getting Started

### Installation

```console
pip install batchwise
```

### Basic engine and processor

The core of `batchwise` consists of an `Engine` that manages one or more `Processor` functions. Each processor operates on time-based windows determined by cron expressions.

```python
import datetime
from batchwise import Engine, Window

# Initialize the engine (defaults are shown here)
engine = Engine(
    checkpoint_path="./batchwise_checkpoints",  # path or uri to checkpoint directory
    logger_name="batchwise",                    # name of logger instance
    timezone=datetime.timezone.utc,             # timezone (datetime.tzinfo object)
)

# Register a processor using the decorator
@engine.processor(
    interval="*/30 * * * *",   # Run every 30 minutes
    delay="1h",                # Wait 1 hour before considering window complete
    lookback="1d",             # Look back 1 day for processing windows
    include_incomplete=False,  # Only process complete windows
)
def my_processor(window: Window):
    print(f"Processing window: {window.start} to {window.end}")
    print(f"Window complete: {window.complete}")
    # Your processing logic here
    pass

# Run the engine
engine()
```

The `Window` object provides:
- `window_id`: Unique identifier for the window
- `start`: Start time of the window (datetime.datetime)
- `end`: End time of the window (datetime.datetime)
- `complete`: Boolean indicating if the window is complete

### Using an fsspec-compatible filesystem

`batchwise` supports any `fsspec`-compatible filesystem for checkpoint storage, enabling cloud storage integration:

```python
from pathlib import Path
from fsspec.implementations.dirfs import DirFileSystem
from fsspec.implementations.local import LocalFileSystem
from batchwise import Engine, Window

# For example, construct a DirFileSystem to wrap a base filesystem
fs = LocalFileSystem()
dir_fs = DirFileSystem(path=str(Path.cwd()), fs=fs)

engine = Engine(
    checkpoint_path="batchwise_checkpoints",  # sub-path on filesystem
    fs=dir_fs,
)

@engine.processor(
    interval="0 * * * *",
    delay="2h",
    lookback="1d",
)
def my_processor(window: Window):
    # Your processing logic here
    pass

engine()
```

For cloud storage, use the appropriate fsspec implementation (e.g., `s3fs`, `adlfs`, `gcsfs`).

### Additional context

You can pass additional context to your processors:

```python
from batchwise import Engine, Window

engine = Engine()

# Define a dictionary to be passed to the processor
config = {
    "database_url": "postgresql://...",
    "threshold": 100,
}

@engine.processor(
    interval="0 0 * * *",
    delay="1d",
    lookback="7d",
    context=config
)
def processor_with_context(window: Window, context: dict):
    # Access context parameters
    database_url = context["database_url"]
    threshold = context["threshold"]
    # Your processing logic here
    pass

engine()
```

**Important:** When using `context`, your processor function must accept a `context` parameter.

### Parallel and/or continuous processing

Run multiple processors in parallel using multiprocessing:

```python
from batchwise import Engine, Window

engine = Engine()

@engine.processor(interval="*/15 * * * *", delay="30m", lookback="2h")
def processor_1(window: Window):
    pass

@engine.processor(interval="*/30 * * * *", delay="1h", lookback="4h")
def processor_2(window: Window):
    pass

@engine.processor(interval="0 * * * *", delay="2h", lookback="12h")
def processor_3(window: Window):
    pass

# Run sequentially (default)
engine()

# Run with 3 parallel processes
engine(num_processes=3)

# Or run continuously with a minimum time between full cycles
engine(num_processes=3, every_seconds=60)  # Check and run every 60 seconds
```

## License

`batchwise` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
