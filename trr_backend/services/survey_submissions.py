"""PostgreSQL ownership for submitted surveys; Firebase stores drafts only."""

from __future__ import annotations

from typing import Any

from psycopg2.extras import Json

from trr_backend.db import pg

TABLES = {
    "survey_x": "survey_x_responses",
    "global_profile": "survey_global_profile_responses",
    "rhoslc_s6": "survey_rhoslc_s6_responses",
    "rhop_s10": "survey_rhop_s10_responses",
}
X_FIELDS = {
    "view_live_tv_household": "view_live_tv_household",
    "view_platforms_subscriptions": "view_platforms_subscriptions",
    "primaryPlatform": "primary_platform",
    "watchFrequency": "watch_frequency",
    "watchMode": "watch_mode",
    "view_reality_cowatch": "view_reality_cowatch",
    "view_live_chats_social": "view_live_chats_social",
    "view_devices_reality": "view_devices_reality",
}
PROFILE_FIELDS = {
    "ageBracket": "age_bracket",
    "gender": "gender",
    "country": "country",
    "stateRegion": "state_region",
    "postalCode": "postal_code",
    "householdSize": "household_size",
    "childrenInHousehold": "children_in_household",
    "relationshipStatus": "relationship_status",
    "educationLevel": "education_level",
    "householdIncomeBand": "household_income_band",
    "profileReuseOk": "profile_reuse_ok",
    "profileEmail": "profile_email",
}
PROFILE_VIEW_FIELDS = {
    **X_FIELDS,
    "primaryPlatform": "view_bravo_platform_primary",
    "watchFrequency": "view_hours_week",
    "watchMode": "view_binge_style",
    **{
        key: key
        for key in (
            "view_platforms_household",
            "view_bravo_other_sources",
            "psych_other_reality_categories",
            "psych_online_engagement",
            "psych_purchase_behavior",
            "psych_watch_reasons",
            "pronouns",
        )
    },
}
PROFILE_JSON = {
    "view_platforms_household",
    "view_platforms_subscriptions",
    "view_devices_reality",
    "view_bravo_other_sources",
    "psych_other_reality_categories",
    "psych_online_engagement",
    "psych_purchase_behavior",
    "psych_watch_reasons",
    "pronouns",
    "extra",
}


def survey_key(key: str) -> str:
    key = "rhoslc_s6" if key == "rhoslc_s6_v1" else key
    if key not in TABLES:
        raise ValueError("unknown survey")
    return key


def _identity(user: dict[str, Any]) -> None:
    pg.execute("SELECT set_config('app.firebase_uid', %s, true)", [user["uid"]])


def _upsert(key: str, user: dict[str, Any], answers: dict[str, Any], username: Any = None) -> dict[str, Any]:
    key = survey_key(key)
    entries = {
        "app_user_id": user["uid"],
        "respondent_id": user["uid"],
        "app_user_email": user.get("email"),
        "source": "trr_app",
        **answers,
    }
    if isinstance(username, str):
        entries["app_username"] = username
    conflict = ["app_user_id"] if key in {"survey_x", "global_profile"} else ["app_user_id", "season_id", "episode_id"]
    columns = list(entries)
    names = ", ".join(f'"{name}"' for name in columns)
    updates = ", ".join(f'"{name}" = EXCLUDED."{name}"' for name in columns if name not in conflict)
    conflict_sql = ", ".join(conflict)
    rows = pg.execute_returning(
        f"INSERT INTO public.{TABLES[key]} ({names}) VALUES ({', '.join(['%s'] * len(columns))}) "
        f"ON CONFLICT ({conflict_sql}) DO UPDATE SET {updates}, updated_at = now() RETURNING *",
        list(entries.values()),
    )
    return rows[0]


def _profile_answers(payload: dict[str, Any]) -> dict[str, Any]:
    responses = payload.get("responses") or {}
    if not isinstance(responses, dict):
        raise ValueError("responses must be an object")
    answers = {column: payload[key] for key, column in PROFILE_FIELDS.items() if key in payload}
    answers.update({column: responses[key] for key, column in PROFILE_VIEW_FIELDS.items() if key in responses})
    if responses:
        answers["extra"] = responses
    for column in PROFILE_JSON & answers.keys():
        if answers[column] is not None:
            answers[column] = Json(answers[column])
    return answers


def submit_x(user: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    responses = payload.get("responses", payload)
    if not isinstance(responses, dict):
        raise ValueError("responses must be an object")
    answers = {}
    for field, column in X_FIELDS.items():
        value = responses.get(field, [] if field in {"view_platforms_subscriptions", "view_devices_reality"} else None)
        if field in {"view_platforms_subscriptions", "view_devices_reality"}:
            if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
                raise ValueError(f"{field} must contain strings")
        elif value is not None and not isinstance(value, str):
            raise ValueError(f"invalid {field}")
        answers[column] = value
    with pg.transaction():
        _identity(user)
        row = _upsert("survey_x", user, answers, payload.get("username"))
        profile = _profile_answers({"responses": responses})
        profile.pop("extra", None)  # A viewing survey must preserve unrelated profile answers.
        _upsert("global_profile", user, profile, payload.get("username"))
    return {"ok": True, "state": x_state(row)}


def submit_profile(user: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    with pg.transaction():
        _identity(user)
        _upsert("global_profile", user, _profile_answers(payload), payload.get("username"))
    return {"ok": True}


def x_state(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "completed": True,
        "completedAt": row["updated_at"],
        "responses": {field: row.get(column) for field, column in X_FIELDS.items()},
    }


def read_response(
    user: dict[str, Any], key: str, season_id: str | None = None, episode_id: str | None = None
) -> dict[str, Any] | None:
    key = survey_key(key)
    where, params = "app_user_id = %s", [user["uid"]]
    if key not in {"survey_x", "global_profile"}:
        if not season_id or not episode_id:
            raise ValueError("seasonId and episodeId are required")
        where += " AND season_id = %s AND episode_id = %s AND completed IS TRUE"
        params += [season_id, episode_id]
    with pg.transaction():
        _identity(user)
        return pg.fetch_one(f"SELECT * FROM public.{TABLES[key]} WHERE {where}", params)


def submit_show(user: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    key = survey_key(str(payload.get("surveyKey", "")))
    if key in {"survey_x", "global_profile"}:
        raise ValueError("use the dedicated survey endpoint")
    if payload.get("appUserId", user["uid"]) != user["uid"]:
        raise PermissionError("forbidden")
    responses = payload.get("responses") or {}
    if not isinstance(responses, dict):
        raise ValueError("responses must be an object")
    if responses.get("completed") is not True:
        raise ValueError("only submitted responses belong in PostgreSQL; save drafts separately")
    ranking = responses.get("ranking")
    if not isinstance(ranking, list) or not 1 <= len(ranking) <= 20:
        raise ValueError("ranking must contain 1 to 20 choices")
    for choice in ranking:
        if not isinstance(choice, dict) or any(
            not isinstance(choice.get(field), str) for field in ("id", "label", "img")
        ):
            raise ValueError("invalid ranking choice")
    if len({choice["id"] for choice in ranking}) != len(ranking):
        raise ValueError("ranking choices must be unique")
    if any(not isinstance(payload.get(field), str) or not payload[field] for field in ("seasonId", "episodeId")):
        raise ValueError("seasonId and episodeId are required")
    answers = {
        "show_id": payload.get("showId"),
        "season_id": payload["seasonId"],
        "episode_id": payload["episodeId"],
        "season_number": payload.get("seasonNumber"),
        "episode_number": payload.get("episodeNumber"),
        "ranking": Json(ranking),
        "completion_pct": 100,
        "completed": True,
    }
    if key == "rhoslc_s6" and "season_rating" in responses:
        rating = responses["season_rating"]
        if rating is not None and (
            isinstance(rating, bool) or not isinstance(rating, (int, float)) or not 0 <= rating <= 10
        ):
            raise ValueError("season rating must be between 0 and 10")
        answers["season_rating"] = rating
    with pg.transaction():
        _identity(user)
        _upsert(key, user, answers)
    return {"ok": True, "completed": True}
