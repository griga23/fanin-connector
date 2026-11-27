"""Creates a fan-in custom connector on Confluent Cloud for the control-topic tests.

Usage: mkconn.py NAME [--plugin ID] [--prefix P] [--control TOPIC|-] [--dry-run] [key=value ...]

  NAME        connector name
  --plugin    custom plugin ID (default: PLUGIN_ID)
  --prefix    test prefix (default: PREFIX, else demo-c). The regex matches SRC_PREFIX + P + '-' ... + SRC_SUFFIX,
              the target topic is P-fanin-out and the control topic P-control.
  --control   control topic name, or '-' for a connector without the control topic (feature off)
  key=value   extra connector settings, for example fanin.control.add.start=LATEST or
              consumer.override.default.api.timeout.ms=10000. They override the defaults.
  --dry-run   print the config with the secrets masked and create nothing

Needs FANIN_ENV_FILE (BOOTSTRAP_SERVERS, KAFKA_CLUSTER_API_KEY, KAFKA_CLUSTER_API_SECRET), CC_ENV and CC_CLUSTER.
The config with the secrets is only written to a mode-600 temporary file, removed after the CLI call. No secret is
printed.
"""
import argparse
import json
import os
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import KAFKA_KEYS, cc_args, need, read_env  # noqa: E402

SECRET_KEYS = ("kafka.api.secret", "fanin.target.api.secret")


def dots(s):
    """Escapes the dots of a topic name for the regex (topic names allow only [a-zA-Z0-9._-])."""
    return s.replace(".", "\\.")


def build(name, plugin, prefix, control, extra, env):
    bootstrap, key, secret = need(env, *KAFKA_KEYS)
    src_prefix = os.environ.get("SRC_PREFIX", "src.")
    src_suffix = os.environ.get("SRC_SUFFIX", "")
    cfg = {
        "name": name, "connector.class": "com.example.fanin.FanInSinkConnector",
        "confluent.connector.type": "CUSTOM", "confluent.custom.plugin.id": plugin,
        "confluent.custom.connect.plugin.runtime": "4.2", "confluent.custom.connect.java.version": "21",
        "confluent.custom.schema.registry.auto": "false",
        "kafka.auth.mode": "KAFKA_API_KEY", "kafka.api.key": key, "kafka.api.secret": secret,
        "tasks.max": "1",
        "topics.regex": "^" + dots(src_prefix + prefix + "-") + ".+" + dots(src_suffix) + "$",
        "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
        "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
        "header.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
        "fanin.target.topic": f"{prefix}-fanin-out", "fanin.target.bootstrap.servers": bootstrap,
        "fanin.target.api.key": key, "fanin.target.api.secret": secret,
        "fanin.producer.max.request.size": "6291456", "fanin.timestamp.mode": "COPY",
    }
    if control != "-":
        cfg.update({"fanin.control.topic": control or f"{prefix}-control",
                    "fanin.control.missing.warn.ms": "120000", "fanin.control.max.lag.ms": "60000"})
    cfg.update(extra)
    return cfg


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("name")
    ap.add_argument("--plugin", default=os.environ.get("PLUGIN_ID"))
    ap.add_argument("--prefix", default=os.environ.get("PREFIX", "demo-c"))
    ap.add_argument("--control", default=None, help="control topic, '-' for none (default: PREFIX-control)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("extra", nargs="*", metavar="key=value")
    a = ap.parse_args()
    if not a.plugin:
        sys.exit("no plugin ID: set PLUGIN_ID or pass --plugin")
    extra = {}
    for kv in a.extra:
        if "=" not in kv:
            sys.exit(f"not key=value: {kv}")
        k, v = kv.split("=", 1)
        extra[k] = v
    cfg = build(a.name, a.plugin, a.prefix, a.control, extra, read_env())
    if a.dry_run:
        shown = {k: ("<hidden>" if k in SECRET_KEYS or "secret" in k or "password" in k else v)
                 for k, v in cfg.items()}
        print(json.dumps({"name": a.name, "config": shown}, indent=2))
        return
    fd, path = tempfile.mkstemp(suffix=".json")
    os.chmod(path, 0o600)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump({"name": a.name, "config": cfg}, f)
        r = subprocess.run(["confluent", "connect", "cluster", "create", "--config-file", path, *cc_args(),
                            "-o", "json"], capture_output=True, text=True)
    finally:
        os.remove(path)
    if r.returncode:
        err = r.stderr[:1500]
        for k in SECRET_KEYS:
            if cfg.get(k):
                err = err.replace(cfg[k], "<hidden>")
        print("FAILED", a.name, err)
        sys.exit(1)
    d = json.loads(r.stdout)
    print("connector", d["name"], d["id"], "created")


if __name__ == "__main__":
    main()
