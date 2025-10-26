from datetime import timedelta

import pytest

from app.util import now, next_timestamp


@pytest.mark.parametrize(
    "interval, expected_seconds",
    [
        (30, [0, 30]),
        (20, [0, 20, 40]),
        (60, [0]),
        (90, [0, 30]),
        (300, [0]),
    ]
)
def test_next_timestamp(interval, expected_seconds):

    current = now()
    timestamp = next_timestamp(interval)

    assert timestamp.second in expected_seconds
    assert timestamp < current + timedelta(seconds=interval)
    assert timestamp > current
