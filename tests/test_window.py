# SPDX-FileCopyrightText: 2025 Manuel Konrad
#
# SPDX-License-Identifier: MIT

import datetime

from batchwise.processor import Window


class TestWindow:
    """Test suite for Window dataclass."""

    def test_window_creation(self):
        """Test creating a Window instance."""
        start = datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        end = datetime.datetime(2025, 1, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
        window_id = f"{start.isoformat()}_{end.isoformat()}"

        window = Window(
            window_id=window_id,
            start=start,
            end=end,
            complete=True,
        )

        assert window.window_id == window_id
        assert window.start == start
        assert window.end == end
        assert window.complete is True

    def test_window_incomplete(self):
        """Test creating an incomplete window."""
        start = datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        end = datetime.datetime(2025, 1, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
        window_id = f"{start.isoformat()}_{end.isoformat()}"

        window = Window(
            window_id=window_id,
            start=start,
            end=end,
            complete=False,
        )

        assert window.complete is False

    def test_window_equality(self):
        """Test window equality comparison."""
        start = datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        end = datetime.datetime(2025, 1, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
        window_id = f"{start.isoformat()}_{end.isoformat()}"

        window1 = Window(window_id=window_id, start=start, end=end, complete=True)
        window2 = Window(window_id=window_id, start=start, end=end, complete=True)

        assert window1.window_id == window2.window_id
        assert window1.start == window2.start
        assert window1.end == window2.end
        assert window1.complete == window2.complete
