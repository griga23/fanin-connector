"""Small Kafka helper for the control-topic cloud tests. Credentials from FANIN_ENV_FILE; never prints a secret.

Commands:
  count TOPIC                        records per fanin.source.topic header (reads the whole topic)
  ends TOPIC...                      end offsets (sum over partitions) per topic
  ctl CONTROL_TOPIC NAME add|remove  writes one control record (key NAME, value "1" or null)
  dump TOPIC                         partition, offset, timestamp, key, value of each record
  offsets TARGET SOURCE_TOPIC        source offsets of SOURCE_TOPIC found in TARGET, as ranges per partition
  produce TOPIC N [--partition P] [--timestamp MS]
                                     writes N small JSON records {"seq", "ts"}; timestamp 0 or unset = now
  timestamps TOPIC PARTITION LO HI   timestamp type, min and max of offsets LO..HI-1
"""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import kafka_conf  # noqa: E402


def consumer():
    from confluent_kafka import Consumer
    return Consumer({**kafka_conf(), "group.id": "fanin-cloud-test-assign-only", "enable.auto.commit": False,
                     "fetch.message.max.bytes": 8388608})


def read_all(c, topic, timeout_s=120):
    from confluent_kafka import TopicPartition
    md = c.list_topics(topic, timeout=30).topics[topic]
    parts = [TopicPartition(topic, p, 0) for p in md.partitions]
    ends = {}
    for tp in parts:
        lo, hi = c.get_watermark_offsets(tp, timeout=30)
        ends[tp.partition] = hi
        tp.offset = lo
    c.assign(parts)
    pos = {p: -1 for p in ends}
    out = []
    deadline = time.time() + timeout_s
    while any(pos[p] + 1 < ends[p] for p in ends) and time.time() < deadline:
        m = c.poll(1.0)
        if m is None or m.error():
            continue
        pos[m.partition()] = m.offset()
        out.append(m)
    return out


def ranges(offsets):
    out = []
    for x in sorted(offsets):
        if out and x == out[-1][1] + 1:
            out[-1][1] = x
        else:
            out.append([x, x])
    return [tuple(r) for r in out]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("count").add_argument("topic")
    sub.add_parser("ends").add_argument("topics", nargs="+")
    p = sub.add_parser("ctl")
    p.add_argument("topic")
    p.add_argument("name")
    p.add_argument("op", choices=["add", "remove"])
    sub.add_parser("dump").add_argument("topic")
    p = sub.add_parser("offsets")
    p.add_argument("target")
    p.add_argument("source")
    p = sub.add_parser("produce")
    p.add_argument("topic")
    p.add_argument("n", type=int)
    p.add_argument("--partition", type=int, default=None)
    p.add_argument("--timestamp", type=int, default=0, help="record timestamp in ms; 0 = now")
    p = sub.add_parser("timestamps")
    p.add_argument("topic")
    p.add_argument("partition", type=int)
    p.add_argument("lo", type=int)
    p.add_argument("hi", type=int)
    a = ap.parse_args()

    from confluent_kafka import Producer, TopicPartition

    if a.cmd == "count":
        c = consumer()
        counts = {}
        for m in read_all(c, a.topic):
            h = dict(m.headers() or [])
            src = (h.get("fanin.source.topic") or b"?").decode()
            counts[src] = counts.get(src, 0) + 1
        c.close()
        for k in sorted(counts):
            print(f"{k} {counts[k]}")
        print(f"TOTAL {sum(counts.values())}")
    elif a.cmd == "ends":
        c = consumer()
        for tn in a.topics:
            md = c.list_topics(tn, timeout=30).topics[tn]
            if md.error:
                print(tn, "missing")
                continue
            print(tn, sum(c.get_watermark_offsets(TopicPartition(tn, p), timeout=30)[1] for p in md.partitions))
        c.close()
    elif a.cmd == "ctl":
        p = Producer({**kafka_conf(), "acks": "all"})
        p.produce(a.topic, key=a.name.encode(), value=(b"1" if a.op == "add" else None))
        left = p.flush(30)
        print(f"control {a.op} {a.name}" + (f" (NOT DELIVERED, {left} left)" if left else ""))
    elif a.cmd == "dump":
        c = consumer()
        for m in read_all(c, a.topic):
            print(m.partition(), m.offset(), m.timestamp()[1], m.key(), m.value())
        c.close()
    elif a.cmd == "offsets":
        c = consumer()
        per = {}
        for m in read_all(c, a.target):
            h = dict(m.headers() or [])
            if (h.get("fanin.source.topic") or b"").decode() == a.source:
                per.setdefault(int(h["fanin.source.partition"]), []).append(int(h["fanin.source.offset"]))
        md = c.list_topics(a.source, timeout=30).topics[a.source]
        for p in sorted(md.partitions):
            o = per.get(p, [])
            end = c.get_watermark_offsets(TopicPartition(a.source, p), timeout=30)[1]
            print(f"p{p}: {len(o)} records in target, source end {end}, offset ranges {ranges(o)}")
        c.close()
    elif a.cmd == "produce":
        p = Producer({**kafka_conf(), "acks": "all", "linger.ms": 5})
        for i in range(a.n):
            ms = a.timestamp or int(time.time() * 1000)
            kw = {"timestamp": ms}
            if a.partition is not None:
                kw["partition"] = a.partition
            p.produce(a.topic, key=f"k-{i}".encode(), value=json.dumps({"seq": i, "ts": ms}).encode(), **kw)
        left = p.flush(30)
        where = f"{a.topic}-{a.partition}" if a.partition is not None else a.topic
        print(f"wrote {a.n - left} records to {where}, timestamp {a.timestamp or 'now'}")
    elif a.cmd == "timestamps":
        c = consumer()
        c.assign([TopicPartition(a.topic, a.partition, a.lo)])
        ts = []
        while len(ts) < a.hi - a.lo:
            m = c.poll(5)
            if m is None:
                break
            if m.error() or m.offset() >= a.hi:
                continue
            ts.append(m.timestamp())
        c.close()
        if not ts:
            print(a.topic, f"offsets {a.lo}-{a.hi - 1}", "no records")
        else:
            print(a.topic, f"offsets {a.lo}-{a.hi - 1}", "n", len(ts), "ts type", sorted({x[0] for x in ts}),
                  "min", min(x[1] for x in ts), "max", max(x[1] for x in ts))


if __name__ == "__main__":
    main()
