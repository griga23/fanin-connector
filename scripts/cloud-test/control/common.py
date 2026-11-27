"""Shared settings for the control-topic cloud test helpers.

Reads the credentials file named by FANIN_ENV_FILE (KEY=VALUE lines). Never prints a secret.
"""
import os
import re
import sys

KAFKA_KEYS = ("BOOTSTRAP_SERVERS", "KAFKA_CLUSTER_API_KEY", "KAFKA_CLUSTER_API_SECRET")


def read_env(path=None):
    """Returns the KEY=VALUE pairs of the credentials file. Accepts 'export ' and quoted values."""
    path = path or os.environ.get("FANIN_ENV_FILE")
    if not path:
        sys.exit("FANIN_ENV_FILE is not set: point it to the credentials file (see README.md)")
    out = {}
    with open(path) as f:
        for line in f:
            m = re.match(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$", line.rstrip("\n"))
            if m:
                v = m.group(2).strip()
                if len(v) >= 2 and v[0] == v[-1] and v[0] in "'\"":
                    v = v[1:-1]
                out[m.group(1)] = v
    return out


def need(env, *keys):
    missing = [k for k in keys if not env.get(k)]
    if missing:
        sys.exit(f"missing in FANIN_ENV_FILE: {', '.join(missing)}")
    return [env[k] for k in keys]


def kafka_conf():
    """librdkafka settings for SASL_SSL / PLAIN with the cluster API key."""
    bootstrap, key, secret = need(read_env(), *KAFKA_KEYS)
    return {"bootstrap.servers": bootstrap, "security.protocol": "SASL_SSL", "sasl.mechanisms": "PLAIN",
            "sasl.username": key, "sasl.password": secret}


def cc_args():
    """--environment / --cluster arguments for the confluent CLI, from CC_ENV and CC_CLUSTER."""
    e, c = os.environ.get("CC_ENV"), os.environ.get("CC_CLUSTER")
    if not e or not c:
        sys.exit("CC_ENV and CC_CLUSTER must be set (environment and Kafka cluster IDs)")
    return ["--environment", e, "--cluster", c]


def source_topic(prefix, name):
    """Source topic name: SRC_PREFIX + PREFIX + '-' + name + SRC_SUFFIX."""
    return f"{os.environ.get('SRC_PREFIX', 'src.')}{prefix}-{name}{os.environ.get('SRC_SUFFIX', '')}"
