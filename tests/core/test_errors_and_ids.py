"""Rejections and identifiers."""

from reflexr.core import (
    DepthExceeded,
    Forbidden,
    InvalidState,
    NotFound,
    Rejection,
    UnsupportedProtocol,
    ValidationFailed,
    firing_id,
    new_event_id,
    new_id,
)


def test_rejections_have_stable_codes_and_payloads() -> None:
    assert Rejection("no").payload() == {"type": "rejected", "message": "no"}
    assert NotFound("run", "fir_1").payload() == {
        "type": "not_found",
        "message": "run fir_1 does not exist",
        "entity": "run",
        "id": "fir_1",
    }
    assert InvalidState("x").code == "invalid_state"
    assert Forbidden("x").code == "forbidden"
    assert UnsupportedProtocol("x").code == "unsupported_protocol"
    assert ValidationFailed("x", [{"loc": ["a"]}]).payload()["errors"] == [{"loc": ["a"]}]
    assert DepthExceeded(9, 8).payload() == {
        "type": "depth_exceeded",
        "message": "the event would be at causation depth 9, beyond the limit of 8",
        "depth": 9,
        "limit": 8,
    }


def test_identifiers() -> None:
    assert new_id("run").startswith("run_")
    assert new_event_id().startswith("evt_")
    assert firing_id("app:r", 0, "[]", 5) == firing_id("app:r", 0, "[]", 5)
    assert firing_id("app:r", 1, "[]", 5) != firing_id("app:r", 0, "[]", 5)
    assert firing_id("app:r", 0, "[]", 5).startswith("fir_")
