#!/usr/bin/env python3
"""Phase 5: runs Flink SQL statements against the fan-in topic through the Flink SQL REST API and prints results.

Called by scripts/flink-check.sh, which provides the credentials in the environment:
FLINK_API_KEY, FLINK_API_SECRET, FLINK_ORG, FLINK_ENV_ID, FLINK_ENV_NAME, FLINK_CLUSTER_NAME, FLINK_POOL,
FLINK_REGION, FLINK_CLOUD. Nothing secret is printed.
"""
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request

E = os.environ
BASE = f"https://flink.{E['FLINK_REGION']}.{E['FLINK_CLOUD']}.confluent.cloud/sql/v1/organizations/{E['FLINK_ORG']}" \
       f"/environments/{E['FLINK_ENV_ID']}"
AUTH = "Basic " + base64.b64encode(f"{E['FLINK_API_KEY']}:{E['FLINK_API_SECRET']}".encode()).decode()
RUN = str(int(time.time()) % 1000000)
TABLE = "`fanin-test-out`"
created = []
STEPS = [x for x in E.get("FLINK_STEPS", "").split(",") if x]  # run only these labels when set


def call(method, url, body=None):
    req = urllib.request.Request(url, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": AUTH, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            raw = r.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        detail = e.read().decode()[:600]
        raise RuntimeError(f"HTTP {e.code} {method} {url.split('/statements')[-1]}: {detail}") from None


def run(label, sql, props=None, max_rows=30, wait=240):
    """Creates a statement, waits for it, prints status and up to max_rows result rows."""
    if STEPS and label not in STEPS:
        return None
    name = f"fanin-test-{label}-{RUN}"
    properties = {"sql.current-catalog": E["FLINK_ENV_NAME"], "sql.current-database": E["FLINK_CLUSTER_NAME"]}
    properties.update(props or {})
    print(f"\n=== {label}\nSQL: {sql}")
    try:
        call("POST", f"{BASE}/statements", {"name": name, "organization_id": E["FLINK_ORG"],
                                            "environment_id": E["FLINK_ENV_ID"],
                                            "spec": {"statement": sql, "properties": properties,
                                                     "compute_pool_id": E["FLINK_POOL"], "stopped": False}})
    except RuntimeError as e:
        print(f"CREATE FAILED: {e}")
        return None
    created.append(name)
    deadline = time.time() + wait
    status = {}
    while time.time() < deadline:
        status = call("GET", f"{BASE}/statements/{name}")
        phase = status["status"]["phase"]
        if phase in ("COMPLETED", "FAILED", "STOPPED", "FAILING"):
            break
        # A running statement with results available is good enough for a bounded read
        if phase == "RUNNING" and status["status"].get("traits", {}).get("is_bounded"):
            time.sleep(2)
            continue
        time.sleep(3)
    phase = status["status"]["phase"]
    detail = status["status"].get("detail", "")
    print(f"STATUS: {phase}" + (f" ({detail[:500]})" if detail else ""))
    schema = status["status"].get("traits", {}).get("schema", {}).get("columns", [])
    if schema:
        print("COLUMNS: " + ", ".join(f"{c['name']} {c['type']['type']}" for c in schema))
    if phase == "FAILED":
        return None
    rows = []
    url = f"{BASE}/statements/{name}/results"
    pages = 0
    while url and len(rows) < max_rows and pages < 40:
        res = call("GET", url)
        data = (res.get("results") or {}).get("data") or []
        rows.extend(data)
        nxt = (res.get("metadata") or {}).get("next")
        if not nxt:
            break
        if not data:
            time.sleep(2)
            pages += 1
        url = nxt
    for r in rows[:max_rows]:
        print(f"ROW op={r.get('op')} {json.dumps(r.get('row'), ensure_ascii=False)[:300]}")
    if not rows:
        print("NO ROWS")
    return rows


def wait_for_key(timeout=300):
    """A new API key takes a while to be accepted: poll a read-only call until it stops answering 401."""
    deadline = time.time() + timeout
    start = time.time()
    while time.time() < deadline:
        try:
            call("GET", f"{BASE}/statements?page_size=1")
            print(f"Flink API key accepted after {time.time() - start:.0f} s")
            return True
        except RuntimeError as e:
            if "HTTP 401" not in str(e):
                print(f"unexpected answer while waiting for the key: {e}")
                return False
        time.sleep(10)
    print("Flink API key still rejected (401) after 5 minutes")
    return False


def main():
    snap = {"sql.snapshot.mode": "now"}
    if not wait_for_key():
        return 1
    try:
        run("show-tables", "SHOW TABLES")
        run("describe", f"DESCRIBE EXTENDED {TABLE}")
        run("select-raw", f"SELECT * FROM {TABLE} LIMIT 5", snap)
        run("select-str", f"SELECT CAST(`key` AS STRING) AS k, CAST(`val` AS STRING) AS v FROM {TABLE} LIMIT 5", snap)
        # Question 3: are headers visible?
        run("add-headers", f"ALTER TABLE {TABLE} ADD (`headers` MAP<BYTES, BYTES> METADATA VIRTUAL)")
        run("describe-after", f"DESCRIBE EXTENDED {TABLE}")
        run("headers-raw", f"SELECT `headers` FROM {TABLE} LIMIT 3", snap)
        src = "CAST(`headers`[CAST('fanin.source.topic' AS BYTES)] AS STRING)"
        run("source-topic", f"SELECT {src} AS source_topic, CAST(`val` AS STRING) AS v FROM {TABLE} LIMIT 10", snap)
        run("filter-by-topic", f"SELECT {src} AS source_topic, CAST(`key` AS STRING) AS k, CAST(`val` AS STRING) AS v "
            f"FROM {TABLE} WHERE {src} = 'fanin-test-src.a' LIMIT 10", snap)
        run("group-by-topic", f"SELECT source_topic, COUNT(*) AS records FROM (SELECT {src} AS source_topic "
            f"FROM {TABLE}) GROUP BY source_topic", snap, max_rows=60, wait=420)
        # The map lookup above returns NULL for BYTES keys. UNNEST of the header map works with any key type.
        unnest = f"{TABLE} t CROSS JOIN UNNEST(t.`headers`) AS h(k, v) WHERE CAST(h.k AS STRING) = 'fanin.source.topic'"
        run("unnest-source-topic", f"SELECT CAST(h.v AS STRING) AS source_topic, CAST(t.`val` AS STRING) AS v "
            f"FROM {unnest} LIMIT 10", snap)
        run("unnest-filter", f"SELECT CAST(h.v AS STRING) AS source_topic, CAST(t.`key` AS STRING) AS k, "
            f"CAST(t.`val` AS STRING) AS v FROM {unnest} AND CAST(h.v AS STRING) = 'fanin-test-src.a' LIMIT 10", snap)
        run("unnest-group", f"SELECT source_topic, COUNT(*) AS records FROM (SELECT CAST(h.v AS STRING) AS "
            f"source_topic FROM {unnest}) GROUP BY source_topic", snap, max_rows=120, wait=420)
    finally:
        for name in created:
            try:
                call("DELETE", f"{BASE}/statements/{name}")
            except RuntimeError:
                pass
        print(f"\nDeleted {len(created)} statements.")


if __name__ == "__main__":
    sys.exit(main())
