"""A clock the tests move by hand."""

from datetime import UTC, datetime, timedelta

START = datetime(2026, 1, 1, tzinfo=UTC)
"""Where every :class:`FakeClock` starts."""


class FakeClock:
    """A clock tests move by hand."""

    def __init__(self) -> None:
        self.now = START

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)
