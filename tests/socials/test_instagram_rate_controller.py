from __future__ import annotations

from trr_backend.socials.instagram.rate_controller import InstagramRateController


def test_before_query_applies_existing_fast_mode_ramp_and_window_budget() -> None:
    slept: list[float] = []
    now = {"value": 1000.0}

    def _clock() -> float:
        return now["value"]

    def _sleep(secs: float) -> None:
        slept.append(round(secs, 3))
        now["value"] += secs

    controller = InstagramRateController(clock=_clock, sleeper=_sleep)

    controller.before_query("graphql_profile_posts", base_delay=0.5, fast_mode=True)
    controller.record_response("graphql_profile_posts", 200)
    controller.before_query("graphql_profile_posts", base_delay=0.5, fast_mode=True)

    assert slept == [0.25]


def test_handle_429_enters_cooldown_not_just_window_recheck() -> None:
    slept: list[float] = []
    now = {"value": 2000.0}

    def _clock() -> float:
        return now["value"]

    def _sleep(secs: float) -> None:
        slept.append(round(secs, 3))
        now["value"] += secs

    controller = InstagramRateController(clock=_clock, sleeper=_sleep)

    controller.before_query("graphql_profile_posts", base_delay=0.5, fast_mode=False)
    controller.record_response("graphql_profile_posts", 429)
    controller.before_query("graphql_profile_posts", base_delay=0.5, fast_mode=False)

    assert slept[-1] >= 1.0
