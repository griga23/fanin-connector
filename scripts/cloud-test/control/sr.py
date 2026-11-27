"""Schema Registry helper for test cleanup. Never prints a secret.

Usage: sr.py list PREFIX      subjects that start with PREFIX
       sr.py delete SUBJECT   soft delete, then hard delete
Needs SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY_API_KEY and SCHEMA_REGISTRY_API_SECRET in FANIN_ENV_FILE.
"""
import argparse
import base64
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import need, read_env  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["list", "delete"])
    ap.add_argument("arg", help="PREFIX for list, SUBJECT for delete")
    a = ap.parse_args()
    url, key, secret = need(read_env(), "SCHEMA_REGISTRY_URL", "SCHEMA_REGISTRY_API_KEY", "SCHEMA_REGISTRY_API_SECRET")
    url = url.rstrip("/")
    auth = base64.b64encode(f"{key}:{secret}".encode()).decode()

    def call(method, path):
        r = urllib.request.Request(url + path, method=method, headers={"Authorization": "Basic " + auth})
        try:
            with urllib.request.urlopen(r, timeout=30) as resp:
                return resp.status, json.loads(resp.read() or b"null")
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode()[:300]

    if a.cmd == "list":
        st, subs = call("GET", "/subjects")
        if st != 200:
            sys.exit(f"HTTP {st}: {subs}")
        for s in subs:
            if s.startswith(a.arg):
                print(s)
    else:
        s = urllib.parse.quote(a.arg, safe="")
        print("soft", call("DELETE", f"/subjects/{s}"))
        print("hard", call("DELETE", f"/subjects/{s}?permanent=true"))


if __name__ == "__main__":
    main()
