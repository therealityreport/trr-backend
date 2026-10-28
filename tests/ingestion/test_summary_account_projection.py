"""Exact predicate equivalence on synthetic rows in the existing isolated PostgreSQL harness.

Run with --confcutdir=tests/ingestion; no configured TRR database is used.
"""

import json

import pytest

from tests.ingestion import test_canonical
from trr_backend.socials import social_season_analytics_impl as social_repo

database = test_canonical.database


def cases(native_columns):
    rows = []

    def add(name, payload, expected=True, account="bravotv"):
        rows.append({"case_name": name, "expected_match": expected, "expected_account": account, **payload})

    for column in ("source_account", "owner_username", "username"):
        add("direct_" + column, {column: " @@BrAvOtV "}, column in native_columns)
        add("raw_" + column, {"raw_data": {column: " @@BrAvOtV "}})

    variants = [
        ("array_string", [" @@BrAvOtV "], True, "bravotv"),
        ("single_string", " @@BrAvOtV ", True, "bravotv"),
        ("single_object", {"username": "BrAvOtV"}, True, "bravotv"),
        ("array_object", [{"username": "BrAvOtV"}], True, "bravotv"),
        ("handle", {"handle": " @BrAvOtV "}, True, "bravotv"),
        ("user_name", {"user_name": " @BrAvOtV "}, True, "bravotv"),
        ("name", {"name": " @BrAvOtV "}, True, "bravotv"),
        ("null_priority", {"username": None, "handle": "bravotv"}, True, "bravotv"),
        ("empty_priority", {"username": "", "handle": "bravotv"}, False, "bravotv"),
        ("wrong_priority", {"username": "other", "handle": "bravotv"}, False, "bravotv"),
        ("null", None, False, "bravotv"),
        ("empty_array", [], False, "bravotv"),
        ("empty_object", {}, False, "bravotv"),
        ("scalar_number", 7, False, "7"),
        ("scalar_boolean", True, False, "true"),
        ("array_number", [7], True, "7"),
        ("array_boolean", [True], True, "true"),
        ("object_number", {"username": 7}, True, "7"),
        ("nested_array", [["bravotv"]], False, "bravotv"),
        ("duplicate", ["bravotv", {"username": "bravotv"}], True, "bravotv"),
        ("nonmatching", ["other", None], False, "bravotv"),
    ]
    for raw in (False, True):
        for column in ("collaborators", "collaborators_detail"):
            for name, value, expected, account in variants:
                payload = {"raw_data": {column: value}} if raw else {column: value}
                present = raw or column in native_columns
                add(f"{'raw' if raw else 'column'}_{column}_{name}", payload, expected and present, account)

    add("no_identity", {}, False)
    add("tags_only", {"profile_tags": ["bravotv"], "mentions": ["bravotv"]}, False)
    add("empty_owner_raw_fallback", {"source_account": "", "raw_data": {"owner_username": "bravotv"}})
    add("raw_scalar", {"raw_data": "bravotv"}, False)
    add("raw_null", {"raw_data": None}, False)
    add("raw_numeric_owner", {"raw_data": {"source_account": 7}}, True, "7")
    return rows


@pytest.mark.parametrize("table_shape", ["catalog", "posts"])
def test_summary_native_account_projection_preserves_sql_semantics(database, table_shape):
    columns = {
        "source_account": "text",
        "owner_username": "text",
        "raw_data": "jsonb",
        "collaborators": "jsonb",
    }
    if table_shape == "posts":
        columns.update(username="text", collaborators_detail="jsonb")
    native_columns = frozenset(columns)
    rows = cases(native_columns)
    old = social_repo._instagram_account_match_sql(alias="p").replace("%s::text", "p.expected_account::text")
    projected = social_repo._instagram_account_match_sql(alias="p", native_columns=native_columns).replace(
        "%s::text", "p.expected_account::text"
    )
    shape = ", ".join(f"{name} {kind}" for name, kind in columns.items())
    sql = f"""
        select p.case_name, p.expected_match, ({old}) as previous_match, ({projected}) as projected_match
        from jsonb_to_recordset(%s::jsonb) as p(
          case_name text, expected_match boolean, expected_account text,
          profile_tags jsonb, mentions jsonb, {shape}
        )
        order by p.case_name
    """
    with database() as conn:
        conn.set_session(readonly=True)
        with conn.cursor() as cur:
            cur.execute("set local statement_timeout = '5s'")
            cur.execute(sql, [json.dumps(rows)])
            results = cur.fetchall()

    assert len(results) == len(rows)
    for name, expected, previous, actual in results:
        assert actual == previous == expected, (table_shape, name, expected, previous, actual)
