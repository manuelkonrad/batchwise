# SPDX-FileCopyrightText: 2025 Manuel Konrad
#
# SPDX-License-Identifier: MIT

"""Test that imports work correctly."""


def test_import_engine():
    """Test importing Engine."""
    from batchwise import Engine

    assert Engine is not None


def test_import_window():
    """Test importing Window."""
    from batchwise import Window

    assert Window is not None


def test_import_prevent_completion():
    """Test importing PreventCompletion."""
    from batchwise import PreventCompletion

    assert PreventCompletion is not None


def test_import_all():
    """Test importing all public API."""
    from batchwise import Engine, PreventCompletion, Window

    assert all([Engine, Window, PreventCompletion])
