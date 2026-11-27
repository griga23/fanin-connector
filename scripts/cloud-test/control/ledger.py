"""Prints the committed offsets of consumer groups with their metadata (the control-topic ledger).

Usage: ledger.py GROUP...   (the group of a connector is connect-<connector id> on Confluent Cloud)
Credentials from FANIN_ENV_FILE. Never prints a secret.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import kafka_conf  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("groups", nargs="+")
    a = ap.parse_args()
    from confluent_kafka import ConsumerGroupTopicPartitions
    from confluent_kafka.admin import AdminClient
    admin = AdminClient(kafka_conf())
    rc = 0
    for group in a.groups:
        try:
            res = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group)])[group].result()
        except Exception as e:  # noqa: BLE001 - report and go on with the next group
            print(group, "ERROR", type(e).__name__, e)
            rc = 1
            continue
        for tp in sorted(res.topic_partitions, key=lambda x: (x.topic, x.partition)):
            print(f"{group} {tp.topic}-{tp.partition} offset={tp.offset} metadata={tp.metadata!r}"
                  + (f" error={tp.error}" if tp.error else ""))
    sys.exit(rc)


if __name__ == "__main__":
    main()
