"""STERNWATCH: event-sourced governance for AI agents.

Every decision an agent makes leaves a wake. STERNWATCH publishes that wake to a Kafka-compatible
log as versioned TraceEnvelopes, rebuilds the run's ledger from the log alone (WatchLedger), and
re-decides the recorded tool calls under a changed policy without re-executing anything
(PolicyEcho). ReplayProof is the check that all of that actually holds.
"""

__version__ = "0.4.0a1"
