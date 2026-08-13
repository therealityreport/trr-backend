#!/usr/bin/env python
"""Poll the BravoTV backfill run and print one line per meaningful change."""

import time

import psycopg2
from dotenv import dotenv_values

RUN = "a8c979c2-fc96-4056-9af5-64b26d57ac7b"
ENV = dotenv_values("/Users/thomashulihan/Projects/TRR/TRR-Backend/.env")
URL = ENV.get("TRR_DB_URL")

prev = {}


def snapshot():
    conn = psycopg2.connect(URL, connect_timeout=10)
    try:
        cur = conn.cursor()
        cur.execute(
            "select status, config->>'launch_state' from social.scrape_runs where id=%s",
            (RUN,),
        )
        row = cur.fetchone() or (None, None)
        snap = {"run_status": row[0], "launch_state": row[1]}
        cur.execute(
            """select job_type, status, count(*) from social.scrape_jobs
               where run_id=%s group by 1,2 order by 1,2""",
            (RUN,),
        )
        snap["jobs"] = {f"{jt}:{st}": n for jt, st, n in cur.fetchall()}
        cur.execute(
            """select pages_scanned, posts_checked, posts_saved, status, exhausted
               from social.shared_account_run_frontiers where run_id=%s limit 1""",
            (RUN,),
        )
        f = cur.fetchone()
        if f:
            snap["frontier"] = f"pages={f[0]} checked={f[1]} saved={f[2]} status={f[3]} exhausted={f[4]}"
        cur.execute(
            """select job_type, left(coalesce(error_message,''),140) from social.scrape_jobs
               where run_id=%s and status='failed' order by completed_at desc limit 3""",
            (RUN,),
        )
        snap["failures"] = [f"{jt}: {em}" for jt, em in cur.fetchall()]
        return snap
    finally:
        conn.close()


while True:
    try:
        snap = snapshot()
    except Exception as exc:  # noqa: BLE001
        msg = str(exc)[:120].replace("\n", " ")
        if prev.get("err") != msg:
            print(f"MONITOR-ERROR: {msg}", flush=True)
            prev["err"] = msg
        time.sleep(60)
        continue
    prev.pop("err", None)
    for key in ("run_status", "launch_state", "frontier"):
        if snap.get(key) != prev.get(key):
            print(f"{key}: {prev.get(key)} -> {snap.get(key)}", flush=True)
    if snap["jobs"] != prev.get("jobs"):
        print(f"jobs: {snap['jobs']}", flush=True)
    for failure in snap["failures"]:
        if failure not in prev.get("failures", []):
            print(f"FAILED-JOB: {failure}", flush=True)
    prev.update(snap)
    if snap.get("run_status") in ("completed", "failed", "cancelled"):
        print(f"TERMINAL: run {snap['run_status']}", flush=True)
        break
    time.sleep(60)
