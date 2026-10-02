# Live recording

> Produced by the `ReplayProof and live recording against AutoMQ` job of GitHub Actions run [37001674067](https://github.com/gandhiashutosh14/sternwatch/actions/runs/37001674067) on an `ubuntu-latest` runner, AutoMQ `automqinc/automq:1.7.4` with MinIO-compatible storage from `docker/compose.yaml`, governed-agent-orchestrator at `211971f`. The file is the job's artifact, unedited apart from this note.

**PASSED**: 7 of 7 checks. Generated 2026-10-02T11:35:32+00:00 at revision `0b44a24`; orchestrator revision `211971f`. Log: KafkaBus on localhost:9092 (kafka-python).

The governed-agent-orchestrator ran each objective with its deterministic heuristic planner (no language model); a Recorder subscribed to the run's DecisionTrace published every event as it was emitted; the harness approved each approval gate.

## Runs

| Run | Objective | Status | Events | Wall time |
|---|---|---|---|---|
| `live-20261002T113530.784Z-1-7015bc` | Forecast next year's revenue and send it to finance@example.com. | completed | 25 | 116 ms |
| `live-20261002T113530.784Z-2-afade5` | Forecast next year's revenue and send it to partner@example.org. | failed | 3 | 3 ms |
| `live-20261002T113530.784Z-3-fad77a` | What are the top 5 genres by tracks sold? | completed | 12 | 5 ms |
| `live-20261002T113530.784Z-4-f4d5ae` | Which 5 countries have the most customers? | completed | 12 | 11 ms |
| `live-20261002T113530.784Z-5-ba66fb` | Forecast next year's revenue from the yearly totals. | failed | 11 | 4 ms |

## Numbers

| Measure | Value |
|---|---|
| Events emitted by the agent | 63 |
| Events published live | 63 |
| Events in the ledger rebuilt from the log | 63 |
| Ledger rebuild time | 0.126 s |
| Tool decisions replayed under the new policy | 10 |

## Checks

| Check | Result | Detail |
|---|---|---|
| every event the agent emitted was published as it happened | pass | emitted 63, published 63 |
| the ledger rebuilt from the log holds every live event | pass | inserted 63, invalid 0 |
| each run in the ledger equals the agent's own journal, event for event | pass | 5 of 5 runs identical |
| per-run digests from the log equal digests of the journal | pass | 5 of 5 match |
| every live event carries the policy id in force | pass | policy cc9add63cb2c |
| replay reproduces every live decision under the old policy | pass | 10 recomputed, 0 mismatches |
| the policy change is visible in the live replay | pass | 3 flipped, 0 need evidence |

## Replay of the live runs under the changed policy

| Run | Seq | Level | Capability | Old -> new | Class |
|---|---|---|---|---|---|
| `live-20261002T113530.784Z-1-7015bc` | 5 | call | revenue_by_year | ALLOWED -> ALLOWED | unchanged |
| `live-20261002T113530.784Z-1-7015bc` | 9 | call | forecast_next_year | ALLOWED -> ALLOWED | unchanged |
| `live-20261002T113530.784Z-1-7015bc` | 13 | call | draft_summary | ALLOWED -> ALLOWED | unchanged |
| `live-20261002T113530.784Z-1-7015bc` | 22 | call | send_report | ALLOWED -> DENIED | flipped |
| `live-20261002T113530.784Z-2-afade5` | 2 | plan | send_report | DENIED -> ALLOWED | flipped |
| `live-20261002T113530.784Z-3-fad77a` | 5 | call | top_genres_by_tracks_sold | ALLOWED -> DENIED | flipped |
| `live-20261002T113530.784Z-3-fad77a` | 9 | call | draft_summary | ALLOWED -> ALLOWED | unchanged |
| `live-20261002T113530.784Z-4-f4d5ae` | 5 | call | customer_count_by_country | ALLOWED -> ALLOWED | unchanged |
| `live-20261002T113530.784Z-4-f4d5ae` | 9 | call | draft_summary | ALLOWED -> ALLOWED | unchanged |
| `live-20261002T113530.784Z-5-ba66fb` | 5 | call | revenue_by_year | ALLOWED -> ALLOWED | unchanged |
