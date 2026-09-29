"""Live run ids: new for every invocation, and refused up front when the topic already holds them.

None of this needs the orchestrator: the refusal comes before it is imported.
"""
from datetime import datetime, timezone

import pytest

from tracewake import live
from tracewake.bus import MemoryBus
from tracewake.live import LIVE_OBJECTIVES, default_prefix, run_ids_for, run_live, runs_in_topic
from tracewake.policy import Policy


def test_run_ids_keep_their_digests_and_the_default_prefix_is_the_utc_time():
    # The committed live report was recorded under the fixed prefix "live"; the digests are unchanged.
    assert run_ids_for(LIVE_OBJECTIVES, "live") == ["live-1-7015bc", "live-2-afade5", "live-3-fad77a",
                                                     "live-4-f4d5ae", "live-5-ba66fb"]
    at = datetime(2026, 9, 30, 10, 15, 0, 123456, tzinfo=timezone.utc)
    assert default_prefix(at) == "live-20260930T101500.123Z"
    assert default_prefix().startswith("live-") and default_prefix().endswith("Z")


def test_runs_in_topic_reads_the_message_keys_and_leaves_big_topics_unread():
    bus = MemoryBus()
    assert runs_in_topic(bus, "t", ["a"]) == []                      # no topic yet
    bus.publish("t", b"a", b"{}")
    bus.publish("t", b"b", b"{}")
    assert runs_in_topic(bus, "t", ["a", "c"]) == ["a"]
    assert runs_in_topic(bus, "t", ["a"], limit=1) is None           # more messages than the limit: not read


def test_a_topic_that_already_holds_the_run_ids_is_refused_before_any_agent_runs(policies_dir, monkeypatch):
    def no_agent(root):
        raise AssertionError("the orchestrator was imported, so an agent could have run")
    monkeypatch.setattr(live, "_import_orchestrator", no_agent)
    policy = Policy.load(str(policies_dir / "v1.json"))
    bus = MemoryBus()
    taken = run_ids_for(LIVE_OBJECTIVES, "again")[3]
    bus.publish("live", taken.encode("utf-8"), b"an earlier run's event")
    with pytest.raises(ValueError, match=f"topic live already holds events for run ids {taken};"):
        run_live(bus, "live", policy, policy, prefix="again")
