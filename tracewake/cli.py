"""CLI.

  tracewake demo   [--from policies/v1.json --to policies/v2.json] [--fixtures fixtures/orchestrator] [--out reports/demo.md]
  tracewake proof  --bootstrap host:9092 [--topic ...] [--from ...] [--to ...] [--fixtures ...] [--out ...] [--json ...]
  tracewake lake   --bootstrap host:9092 --catalog http://host:8181 --s3-endpoint http://host:9000 [--namespace default] ...
  tracewake publish --bootstrap host:9092 --topic T --policy policies/v1.json TRACE.jsonl [TRACE.jsonl ...]
  tracewake ledger --bootstrap host:9092 --topic T --db ledger.db
  tracewake echo   --from policies/v1.json --to policies/v2.json (--fixtures DIR | --db ledger.db) [--out ...] [--json ...]
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Optional, Sequence

from .bus import KafkaBus, MemoryBus
from .echo import echo
from .ledger import WakeLedger
from .policy import Policy
from .proof import run_proof
from .recorder import Recorder, load_trace_dir

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_FIXTURES = str(ROOT / "fixtures" / "orchestrator")
DEFAULT_OLD = str(ROOT / "policies" / "v1.json")
DEFAULT_NEW = str(ROOT / "policies" / "v2.json")


def _write(path: Optional[str], text: str) -> None:
    if path:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(text, encoding="utf-8")


def _out(text: str) -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        pass
    print(text)


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(prog="tracewake", description="Event-sourced governance for AI agents.")
    sub = p.add_subparsers(dest="cmd", required=True)

    pol = argparse.ArgumentParser(add_help=False)
    pol.add_argument("--from", dest="old", default=DEFAULT_OLD, help="policy that was in force (capabilities.json format)")
    pol.add_argument("--to", dest="new", default=DEFAULT_NEW, help="policy to replay under")
    fx = argparse.ArgumentParser(add_help=False)
    fx.add_argument("--fixtures", default=DEFAULT_FIXTURES, help="directory of recorded DecisionTrace JSONL files")
    rep = argparse.ArgumentParser(add_help=False)
    rep.add_argument("--out", default=None, help="write the Markdown report here")
    rep.add_argument("--json", dest="json_out", default=None, help="write the JSON report here")
    kafka = argparse.ArgumentParser(add_help=False)
    kafka.add_argument("--bootstrap", required=True, help="Kafka-compatible bootstrap server, e.g. localhost:9092")
    kafka.add_argument("--topic", default=None)
    kafka.add_argument("--partitions", type=int, default=3)

    sub.add_parser("demo", parents=[pol, fx, rep], help="run the full proof on an in-memory log (no broker needed)")
    sub.add_parser("proof", parents=[kafka, pol, fx, rep], help="run the full proof against a Kafka-compatible log")

    lake = sub.add_parser("lake", parents=[kafka, pol, fx, rep],
                          help="run the proof on a Table Topic, then check the Iceberg table against the ledger and query it")
    lake.add_argument("--catalog", required=True, help="Iceberg REST catalog URI, e.g. http://localhost:8181")
    lake.add_argument("--s3-endpoint", required=True, help="S3-compatible endpoint the table's files live on")
    lake.add_argument("--s3-access-key", default="minioadmin")
    lake.add_argument("--s3-secret-key", default="minioadmin")
    lake.add_argument("--s3-region", default="us-east-1")
    lake.add_argument("--namespace", default="default", help="Iceberg namespace the broker writes into")
    lake.add_argument("--commit-interval-ms", type=int, default=2000)
    lake.add_argument("--timeout", type=float, default=240.0, help="seconds to wait for the table to show every row")

    pub = sub.add_parser("publish", parents=[kafka], help="publish recorded trace files to a topic")
    pub.add_argument("--policy", default=DEFAULT_OLD, help="policy in force when the traces were recorded")
    pub.add_argument("files", nargs="+")

    led = sub.add_parser("ledger", parents=[kafka], help="rebuild a ledger from a topic and print run summaries")
    led.add_argument("--db", default="ledger.db")

    ec = sub.add_parser("echo", parents=[pol, rep], help="replay recorded decisions under a changed policy")
    src = ec.add_mutually_exclusive_group()
    src.add_argument("--fixtures", default=None)
    src.add_argument("--db", default=None)

    args = p.parse_args(argv)

    if args.cmd in ("demo", "proof", "lake"):
        old, new = Policy.load(args.old), Policy.load(args.new)
        runs = load_trace_dir(args.fixtures)
        if not runs:
            print(f"no trace files found in {args.fixtures}", file=sys.stderr)
            return 2
        if args.cmd == "demo":
            bus, topic, command = MemoryBus(), "tracewake.demo", "tracewake demo"
        elif args.cmd == "proof":
            bus = KafkaBus(args.bootstrap, partitions=args.partitions)
            topic = args.topic or f"tracewake.proof.{int(time.time())}"
            command = f"tracewake proof --bootstrap {args.bootstrap} --topic {topic}"
        else:
            from .lake import table_topic_configs
            bus = KafkaBus(args.bootstrap, partitions=args.partitions,
                           topic_configs=table_topic_configs(args.commit_interval_ms))
            topic = args.topic or f"tracewake_lake_{int(time.time())}"
            command = (f"tracewake lake --bootstrap {args.bootstrap} --catalog {args.catalog} "
                       f"--s3-endpoint {args.s3_endpoint} --topic {topic}")
        try:
            report = run_proof(bus, topic, runs, old, new, command=command)
            text = report.render_markdown()
            payload = report.to_dict()
            passed = report.passed
            if args.cmd == "lake":
                from .lake import catalog_properties, run_lake
                published = report.numbers["events"] + report.numbers["duplicates_injected"]
                ledger = WakeLedger()
                ledger.ingest(bus, topic)
                lake_report = run_lake(
                    catalog_properties(args.catalog, args.s3_endpoint, args.s3_access_key, args.s3_secret_key, args.s3_region),
                    args.namespace, topic, ledger, published,
                    commit_interval_ms=args.commit_interval_ms, timeout_s=args.timeout)
                ledger.close()
                text = text + "\n\n" + lake_report.render_markdown()
                payload = {"passed": passed and lake_report.passed, "proof": payload, "lake": lake_report.to_dict()}
                passed = passed and lake_report.passed
        finally:
            bus.close()
        _write(args.out, text)
        _write(args.json_out, json.dumps(payload, indent=2, default=str) + "\n")
        _out(text)
        return 0 if passed else 1

    if args.cmd == "publish":
        policy = Policy.load(args.policy)
        bus = KafkaBus(args.bootstrap, partitions=args.partitions)
        try:
            recorder = Recorder(bus, args.topic or "agent.decisions", policy_id=policy.policy_id)
            total = sum(recorder.publish_file(f) for f in args.files)
        finally:
            bus.close()
        _out(f"published {total} events from {len(args.files)} files to {args.topic or 'agent.decisions'} "
             f"(policy {policy.policy_id})")
        return 0

    if args.cmd == "ledger":
        bus = KafkaBus(args.bootstrap, partitions=args.partitions)
        ledger = WakeLedger(args.db)
        try:
            stats = ledger.ingest(bus, args.topic or "agent.decisions")
            _out(json.dumps({"ingest": stats.to_dict(), "runs": [ledger.summary(r) for r in ledger.runs()]},
                            indent=2, default=str))
        finally:
            ledger.close()
            bus.close()
        return 0

    if args.cmd == "echo":
        old, new = Policy.load(args.old), Policy.load(args.new)
        if args.db:
            ledger = WakeLedger(args.db)
            runs = ledger.all_events()
            ledger.close()
        else:
            runs = load_trace_dir(args.fixtures or DEFAULT_FIXTURES)
        report = echo(runs, old, new)
        text = report.render_markdown()
        _write(args.out, text)
        _write(args.json_out, json.dumps(report.to_dict(), indent=2, default=str) + "\n")
        _out(text)
        return 0 if report.faithfulness["mismatches"] == 0 else 1

    return 2


if __name__ == "__main__":
    sys.exit(main())
