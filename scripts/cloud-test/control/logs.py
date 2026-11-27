"""Saves all connector log lines of a time window (all pages) to OUTFILE as JSON lines, and prints the line count.

Usage: logs.py CONNECTOR_ID START END LEVELS OUTFILE [SEARCH]
  START, END   ISO times, for example 2026-11-27T07:00:00Z
  LEVELS       for example "INFO|WARN|ERROR"
  SEARCH       optional --search-text filter
Needs CC_ENV and CC_CLUSTER and a logged-in confluent CLI. OUTFILE is overwritten.
"""
import argparse
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import cc_args  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("connector_id")
    ap.add_argument("start")
    ap.add_argument("end")
    ap.add_argument("levels")
    ap.add_argument("out")
    ap.add_argument("search", nargs="?")
    a = ap.parse_args()
    if os.path.exists(a.out):
        os.remove(a.out)
    cmd = ["confluent", "connect", "logs", a.connector_id, "--start-time", a.start, "--end-time", a.end,
           "--level", a.levels, *cc_args(), "--output-file", a.out]
    if a.search:
        cmd += ["--search-text", a.search]
    subprocess.run(cmd, capture_output=True, text=True)
    for _ in range(40):
        r = subprocess.run(cmd + ["--next"], capture_output=True, text=True)
        o = r.stdout + r.stderr
        if "Appended" not in o or "Appended 0" in o:
            break
    print(sum(1 for _ in open(a.out)) if os.path.exists(a.out) else 0, "lines")


if __name__ == "__main__":
    main()
