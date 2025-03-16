# SPDX-FileCopyrightText: 2025 Manuel Konrad
#
# SPDX-License-Identifier: MIT

from batchwise import Engine

engine = Engine()


@engine.processor(
    name="test",
    source="test:test_1",
    sink="test:test_2",
)
def test_processor(source_data, sink_data, source_fs, context):
    return source_data["inner"]


engine()
