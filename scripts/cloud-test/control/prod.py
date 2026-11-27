"""Steady producer for the gap measurement: RATE records/s into TOPIC, value JSON {"seq", "ts"}, record timestamp =
the time of the send. Prints the sequence number every 10 s. Runs until stopped (Ctrl-C).

Usage: prod.py TOPIC RATE
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
    ap.add_argument("topic")
    ap.add_argument("rate", type=int)
    a = ap.parse_args()
    from confluent_kafka import Producer
    p = Producer({**kafka_conf(), "acks": "all", "linger.ms": 5})
    seq = 0
    t0 = last = time.time()
    try:
        while True:
            now = time.time()
            due = int((now - t0) * a.rate)
            while seq < due:
                ms = int(time.time() * 1000)
                p.produce(a.topic, value=json.dumps({"seq": seq, "ts": ms}).encode(), timestamp=ms)
                seq += 1
            p.poll(0)
            if now - last >= 10:
                print(time.strftime("%H:%M:%SZ", time.gmtime()), "seq", seq, flush=True)
                last = now
            time.sleep(0.005)
    except KeyboardInterrupt:
        print("unflushed", p.flush(30), "last seq", seq)


if __name__ == "__main__":
    main()
