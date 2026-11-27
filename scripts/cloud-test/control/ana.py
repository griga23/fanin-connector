"""Forwarding gaps and latency of the records of SOURCE_TOPIC that were produced between START and END.

Usage: ana.py CSV START END SOURCE_TOPIC
  CSV          output of mon.py
  START, END   HH:MM:SS (UTC, today) or epoch milliseconds
Prints the record count, missing and duplicate sequence numbers, latency percentiles and the longest time between
two forwarded records ("gap").
"""
import argparse
import collections
import datetime


def to_ms(s):
    if s.isdigit():
        return int(s)
    day = datetime.datetime.now(datetime.timezone.utc).date()
    t = datetime.datetime.combine(day, datetime.time.fromisoformat(s), datetime.timezone.utc)
    return int(t.timestamp() * 1000)


def fmt(t):
    return datetime.datetime.fromtimestamp(t / 1000, datetime.timezone.utc).strftime("%H:%M:%S.%f")[:-3]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("csv")
    ap.add_argument("start")
    ap.add_argument("end")
    ap.add_argument("topic")
    a = ap.parse_args()
    lo, hi = to_ms(a.start), to_ms(a.end)
    rows, other = [], collections.Counter()
    for line in open(a.csv):
        s, pt, at, _rt, src = line.strip().split(",", 4)
        if src != a.topic:
            other[src] += 1
            continue
        s, pt, at = int(s), int(pt), int(at)
        if lo <= pt < hi:
            rows.append((s, pt, at))
    if len(rows) < 2:
        print("fewer than 2 rows in the window")
        return
    rows.sort()
    seqs = [r[0] for r in rows]
    dup = len(seqs) - len(set(seqs))
    missing = (seqs[-1] - seqs[0] + 1) - len(set(seqs))
    lat = sorted(r[2] - r[1] for r in rows)
    byat = sorted(r[2] for r in rows)
    gaps = [(byat[i + 1] - byat[i], byat[i]) for i in range(len(byat) - 1)]
    mg = max(gaps)
    worst = max(rows, key=lambda r: r[2] - r[1])
    print(f"window {a.start}-{a.end}: {len(rows)} records seq {seqs[0]}..{seqs[-1]} missing {missing} dup {dup}")
    print(f"latency ms p50 {lat[len(lat) // 2]} p99 {lat[int(len(lat) * .99)]} max {lat[-1]} "
          f"(seq {worst[0]} produced {fmt(worst[1])} appended {fmt(worst[2])})")
    print(f"max gap between forwarded records {mg[0]} ms, starting {fmt(mg[1])}; "
          f"gaps > 1 s: {[(g, fmt(t)) for g, t in gaps if g > 1000]}")
    print(f"sum of gaps > 1 s: {sum(g for g, t in gaps if g > 1000)} ms")
    if other:
        print("other source topics in the CSV:", dict(other))


if __name__ == "__main__":
    main()
