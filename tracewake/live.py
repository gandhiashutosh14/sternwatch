"""Live recording: a running governed agent streams its decisions to the log as it makes them.

The recorded fixtures prove the replay; this proves the plumbing. The governed-agent-orchestrator is
imported from a checkout, a Recorder is subscribed to each run's DecisionTrace before the run
starts, the agent plans and acts (the approval gate is approved by the harness), and every event
reaches the log from inside ``DecisionTrace.emit``. Afterwards the ledger is rebuilt from the log
alone and compared, event for event, with what the agent's own in-memory journal holds, and the
live runs are replayed under the changed policy.
"""
from __future__ import annotations

import asyncio
import hashlib
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .bus import Bus
from .echo import echo
from .envelope import digest_envelopes, envelopes_for
from .ledger import WakeLedger
from .policy import Policy
from .proof import Check, _revision
from .recorder import Recorder

LIVE_OBJECTIVES: List[Dict[str, Any]] = [
    {"objective": "Forecast next year's revenue and send it to finance@example.com.", "approve": True, "call_limit": None},
    {"objective": "Forecast next year's revenue and send it to partner@example.org.", "approve": True, "call_limit": None},
    {"objective": "What are the top 5 genres by tracks sold?", "approve": True, "call_limit": None},
    {"objective": "Which 5 countries have the most customers?", "approve": True, "call_limit": None},
    {"objective": "Forecast next year's revenue from the yearly totals.", "approve": True, "call_limit": 1},
]


def _import_orchestrator(root: Optional[str]):
    if root:
        path = str(Path(root).resolve())
        if path not in sys.path:
            sys.path.insert(0, path)
    from orchestrator.capabilities import default_adapters
    from orchestrator.catalog import Catalog
    from orchestrator.graph import Orchestrator
    from orchestrator.heuristics import build_heuristic_planner
    from orchestrator.planner import PlannerCascade
    from orchestrator.trace import DecisionTrace
    import orchestrator
    return {"default_adapters": default_adapters, "Catalog": Catalog, "Orchestrator": Orchestrator,
            "build_heuristic_planner": build_heuristic_planner, "PlannerCascade": PlannerCascade,
            "DecisionTrace": DecisionTrace, "root": Path(orchestrator.__file__).resolve().parent.parent}


@dataclass
class LiveRun:
    run_id: str
    objective: str
    status: str
    events: List[Dict[str, Any]]
    wall_ms: float


@dataclass
class LiveReport:
    runs: List[LiveRun]
    checks: List[Check]
    numbers: Dict[str, Any]
    echo_counts: Dict[str, int]
    transitions: Dict[str, int]
    rows: List[Dict[str, Any]]
    environment: Dict[str, Any] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    def to_dict(self) -> Dict[str, Any]:
        return {"passed": self.passed, "checks": [c.to_dict() for c in self.checks], "numbers": self.numbers,
                "echo": {"counts": self.echo_counts, "transitions": self.transitions, "rows": self.rows},
                "runs": [{"run_id": r.run_id, "objective": r.objective, "status": r.status, "events": len(r.events),
                          "wall_ms": r.wall_ms} for r in self.runs],
                "environment": self.environment}

    def render_markdown(self) -> str:
        env, n = self.environment, self.numbers
        lines = ["# Live recording", "",
                 f"**{'PASSED' if self.passed else 'FAILED'}**: {sum(c.passed for c in self.checks)} of {len(self.checks)} checks. "
                 f"Generated {env['generated_at']} at revision `{env['revision']}`; orchestrator revision `{env['orchestrator_revision']}`. "
                 f"Log: {env['bus']}.", "",
                 "The governed-agent-orchestrator ran each objective with its deterministic heuristic planner (no language "
                 "model); a Recorder subscribed to the run's DecisionTrace published every event as it was emitted; the "
                 "harness approved each approval gate.", "",
                 "## Runs", "", "| Run | Objective | Status | Events | Wall time |", "|---|---|---|---|---|"]
        for r in self.runs:
            lines.append(f"| `{r.run_id}` | {r.objective} | {r.status} | {len(r.events)} | {r.wall_ms:.0f} ms |")
        lines += ["", "## Numbers", "", "| Measure | Value |", "|---|---|",
                  f"| Events emitted by the agent | {n['emitted']} |",
                  f"| Events published live | {n['published']} |",
                  f"| Events in the ledger rebuilt from the log | {n['ledger_events']} |",
                  f"| Ledger rebuild time | {n['ingest_s']:.3f} s |",
                  f"| Tool decisions replayed under the new policy | {n['decisions']} |",
                  "", "## Checks", "", "| Check | Result | Detail |", "|---|---|---|"]
        for c in self.checks:
            lines.append(f"| {c.name} | {'pass' if c.passed else 'FAIL'} | {c.detail.replace('|', '/')} |")
        lines += ["", "## Replay of the live runs under the changed policy", "",
                  "| Run | Seq | Level | Capability | Old -> new | Class |", "|---|---|---|---|---|---|"]
        for r in self.rows:
            lines.append(f"| `{r['run_id']}` | {r['seq']} | {r['level']} | {r['capability']} | {r['transition']} | {r['classification']} |")
        return "\n".join(lines) + "\n"


async def _run_all(api: Dict[str, Any], catalog, recorder: Recorder, objectives: List[Dict[str, Any]], prefix: str) -> List[LiveRun]:
    runs: List[LiveRun] = []
    for i, spec in enumerate(objectives, start=1):
        orch = api["Orchestrator"](catalog, api["default_adapters"](),
                                   api["PlannerCascade"]([api["build_heuristic_planner"](catalog)]),
                                   call_limit=spec.get("call_limit"))
        digest = hashlib.sha256(spec["objective"].encode("utf-8")).hexdigest()[:6]
        run_id = f"{prefix}-{i}-{digest}"
        trace = api["DecisionTrace"](run_id)
        trace.subscribe(recorder)            # the one line a runtime needs to stream its decisions
        orch.traces[run_id] = trace
        t0 = time.perf_counter()
        snap = await orch.start(spec["objective"], run_id=run_id)
        while snap["status"] == "awaiting_approval" and spec.get("approve"):
            snap = await orch.resume(run_id, True, by="live-harness")
        runs.append(LiveRun(run_id, spec["objective"], snap["status"], trace.to_list(), (time.perf_counter() - t0) * 1000))
    return runs


def run_live(bus: Bus, topic: str, old: Policy, new: Policy, *, orchestrator_root: Optional[str] = None,
             objectives: Optional[List[Dict[str, Any]]] = None, prefix: str = "live") -> LiveReport:
    api = _import_orchestrator(orchestrator_root)
    catalog = api["Catalog"].load(str(api["root"] / "capabilities.json"))
    recorder = Recorder(bus, topic, policy_id=old.policy_id)
    runs = asyncio.run(_run_all(api, catalog, recorder, objectives or LIVE_OBJECTIVES, prefix))
    bus.flush()

    ledger = WakeLedger()
    t0 = time.perf_counter()
    stats = ledger.ingest(bus, topic)
    ingest_s = time.perf_counter() - t0
    emitted = sum(len(r.events) for r in runs)
    checks: List[Check] = [
        Check("every event the agent emitted was published as it happened", recorder.published == emitted,
              f"emitted {emitted}, published {recorder.published}"),
        Check("the ledger rebuilt from the log holds every live event", stats.inserted == emitted and stats.invalid == 0,
              f"inserted {stats.inserted}, invalid {stats.invalid}"),
    ]
    exact = [r.run_id for r in runs if ledger.events(r.run_id) == r.events]
    checks.append(Check("each run in the ledger equals the agent's own journal, event for event", len(exact) == len(runs),
                        f"{len(exact)} of {len(runs)} runs identical"))
    expected = {r.run_id: digest_envelopes(envelopes_for(r.events, policy_id=old.policy_id)) for r in runs}
    checks.append(Check("per-run digests from the log equal digests of the journal", ledger.digests() == expected,
                        f"{sum(1 for k, v in expected.items() if ledger.digests().get(k) == v)} of {len(expected)} match"))
    checks.append(Check("every live event carries the policy id in force", all(ledger.summary(r.run_id)["policy_ids"] == [old.policy_id] for r in runs),
                        f"policy {old.policy_id}"))
    report = echo(ledger.all_events(), old, new)
    f = report.faithfulness
    checks.append(Check("replay reproduces every live decision under the old policy", f["mismatches"] == 0 and f["checked"] > 0,
                        f"{f['checked']} recomputed, {f['mismatches']} mismatches"))
    checks.append(Check("the policy change is visible in the live replay", report.counts["flipped"] > 0,
                        f"{report.counts['flipped']} flipped, {report.counts['needs-evidence']} need evidence"))
    ledger.close()

    try:
        import subprocess
        orch_rev = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], cwd=api["root"], text=True,
                                           stderr=subprocess.DEVNULL).strip()
    except Exception:  # noqa: BLE001
        orch_rev = "unknown"
    rows = [{"run_id": r.decision.run_id, "seq": r.decision.seq, "level": r.decision.level,
             "capability": r.decision.capability, "transition": r.transition, "classification": r.classification}
            for r in report.rows]
    numbers = {"emitted": emitted, "published": recorder.published, "ledger_events": stats.inserted,
               "ingest_s": ingest_s, "decisions": len(report.rows)}
    environment = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "revision": _revision(),
                   "orchestrator_revision": orch_rev, "bus": bus.describe(), "topic": topic}
    return LiveReport(runs, checks, numbers, report.counts, report.transitions, rows, environment)
