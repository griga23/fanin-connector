"""Reads partition 0 of the target topic from its end (assign, no commits) and appends one CSV line per record:
seq,produce_ms,append_ms,recv_ms,source_topic. Use a target topic with message.timestamp.type=LogAppendTime, so
append_ms is the time the connector wrote the record. Runs until stopped (Ctrl-C).

Usage: mon.py TARGET OUT_CSV
Credentials from FANIN_ENV_FILE. Never prints a secret.
"""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import kafka_conf  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("target")
    ap.add_argument("out")
    a = ap.parse_args()
    from confluent_kafka import OFFSET_END, Consumer, TopicPartition
    c = Consumer({**kafka_conf(), "group.id": "fanin-cloud-test-assign-only", "enable.auto.commit": False})
    c.assign([TopicPartition(a.target, 0, OFFSET_END)])
    with open(a.out, "a") as f:
        try:
            while True:
                m = c.poll(1.0)
                if m is None or m.error():
                    continue
                h = dict(m.headers() or [])
                src = (h.get("fanin.source.topic") or b"?").decode()
                try:
                    v = json.loads(m.value())
                    s, pt = v.get("seq", -1), v.get("ts", -1)
                except Exception:  # noqa: BLE001 - a record of another writer
                    s, pt = -1, -1
                f.write(f"{s},{pt},{m.timestamp()[1]},{int(time.time() * 1000)},{src}\n")
                f.flush()
        except KeyboardInterrupt:
            c.close()


if __name__ == "__main__":
    main()
