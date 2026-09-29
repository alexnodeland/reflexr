from importlib.metadata import version

import reflexr


def test_version_matches_the_distribution() -> None:
    assert reflexr.__version__ == version("reflexr")
