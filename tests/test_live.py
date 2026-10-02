"""The live recorder against the real orchestrator (skipped when it is not importable)."""
import sys

import pytest

if sys.version_info < (3, 11):
    pytest.skip("LangGraph's interrupt() in async nodes needs Python 3.11+", allow_module_level=True)
pytest.importorskip("langgraph")
pytest.importorskip("orchestrator.graph")

from sternwatch.bus import MemoryBus  # noqa: E402
from sternwatch.live import LIVE_OBJECTIVES, run_live  # noqa: E402
from sternwatch.policy import Policy  # noqa: E402


def test_live_runs_stream_into_the_log_and_replay(policies_dir, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # the orchestrator's send_report writes to its own outbox folder, not here
    old, new = Policy.load(str(policies_dir / "v1.json")), Policy.load(str(policies_dir / "v2.json"))
    report = run_live(MemoryBus(), "live", old, new)
    assert report.passed, [c for c in report.checks if not c.passed]
    assert len(report.runs) == len(LIVE_OBJECTIVES)
    statuses = {r.objective: r.status for r in report.runs}
    assert statuses["Forecast next year's revenue and send it to finance@example.com."] == "completed"
    assert statuses["Forecast next year's revenue and send it to partner@example.org."] == "failed"
    transitions = {(r["capability"], r["transition"]) for r in report.rows}
    assert ("send_report", "ALLOWED -> DENIED") in transitions       # finance@example.com under v2
    assert ("send_report", "DENIED -> ALLOWED") in transitions       # partner@example.org under v2
    assert ("top_genres_by_tracks_sold", "ALLOWED -> DENIED") in transitions  # top 5 over the new cap of 2
    text = report.render_markdown()
    assert "**PASSED**" in text and "event for event" in text


def test_two_live_runs_on_one_topic_both_pass(policies_dir, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    old, new = Policy.load(str(policies_dir / "v1.json")), Policy.load(str(policies_dir / "v2.json"))
    bus = MemoryBus()
    first = run_live(bus, "live", old, new)
    second = run_live(bus, "live", old, new)
    assert first.passed and second.passed, [c for r in (first, second) for c in r.checks if not c.passed]
    run_ids = {r.run_id for r in first.runs} | {r.run_id for r in second.runs}
    assert len(run_ids) == 2 * len(LIVE_OBJECTIVES)                          # new run ids for every invocation
    assert second.numbers["ledger_events"] == second.numbers["emitted"]      # the first run's events stayed out
    assert len(bus.consume("live")) == first.numbers["published"] + second.numbers["published"]


def test_budget_refusals_carry_their_arguments_live(policies_dir):
    old = Policy.load(str(policies_dir / "v1.json"))
    one = [{"objective": "Forecast next year's revenue from the yearly totals.", "approve": True, "call_limit": 1}]
    report = run_live(MemoryBus(), "live", old, old, objectives=one)
    denied = [e for r in report.runs for e in r.events if e["type"] == "tool_call_denied"]
    assert denied and all("inputs" in e["data"] for e in denied)
