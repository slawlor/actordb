"""Fast discoverable orchestration invariants; no daemon/subprocess required."""
import importlib.util
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("hardening", Path(__file__).with_name("cluster-hardening.py"))
hardening = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hardening)


class Fake:
    def __init__(self):
        self.events, self.boots = [], {}
    def start(self, node):
        self.boots[node] = self.boots.get(node, 0) + 1
        self.events.append(("start", node))
    def stop(self, node, hard=True):
        self.events.append(("stop", node, hard))
    def ready(self, node, old_boot=None):
        if old_boot is not None:
            assert old_boot != self.boots[node]
        self.events.append(("ready", node))
    def status(self, node):
        return {"boot_id": self.boots[node]}
    def checker(self, phase, target="node-a", **kwargs):
        self.events.append(("phase", phase, target, kwargs))
    def partition(self, node, isolated):
        self.events.append(("partition", node, isolated))
    def confirm_isolation(self, node):
        self.events.append(("isolated", node))
    def wait(self, *args):
        self.events.append(("wait", args[0]))


class Tests(unittest.TestCase):
    def test_matrix_orders_persisted_intent_and_missing_candidate_proofs(self):
        backend = Fake()
        hardening.run(backend, SimpleNamespace(cycles=1))
        events = backend.events
        prepare = next(i for i, e in enumerate(events) if e[:2] == ("phase", "prepare-shrink"))
        shrink = next(i for i, e in enumerate(events) if e[:2] == ("phase", "shrink"))
        self.assertTrue(any(e == ("stop", "node-d", True) for e in events[prepare:shrink]))
        self.assertEqual(events[shrink][3]["absent"], "node-d")
        survivors = next(i for i, e in enumerate(events) if e[:2] == ("phase", "survivors-ready"))
        self.assertEqual(events[survivors - 1], ("stop", "node-d", True))
        self.assertEqual(events[survivors][3]["absent"], "node-d")
        self.assertEqual(events[survivors + 1][:2], ("phase", "rolling"))
        first_rolling = next(i for i, e in enumerate(events) if e[:2] == ("phase", "rolling"))
        self.assertEqual(events[first_rolling - 1][:2], ("phase", "delivery-ready"))
        self.assertEqual(events[first_rolling - 1][3]["absent"], "node-a")
        self.assertEqual(events[first_rolling - 2], ("stop", "node-a", True))
        arm = next(i for i, e in enumerate(events) if e[:2] == ("phase", "arm-recovery"))
        blocked = next(i for i, e in enumerate(events) if e[:2] == ("phase", "blocked"))
        self.assertEqual({e[1] for e in events[arm:blocked] if e[0] == "stop"}, set(hardening.NODES))
        self.assertEqual([e[1] for e in events[arm:blocked] if e[0] == "start"], ["node-b"])
        self.assertEqual(events[blocked][2], "node-b")
        self.assertEqual([e[1] for e in events if e[0] == "stop" and not e[2]], list(hardening.NODES))

    def test_network_faults_are_project_scoped_and_restore_peer_alias(self):
        with tempfile.TemporaryDirectory() as directory:
            backend = hardening.Docker(SimpleNamespace(), Path(directory))
            with patch.object(backend, "container", return_value="owned-id"), patch.object(hardening, "command") as run:
                backend.partition("node-c", True)
                run.assert_called_once_with(["docker", "network", "disconnect", "--force", backend.network, "owned-id"])
            with patch.object(backend, "container", return_value="owned-id"), patch.object(hardening, "command") as run:
                backend.partition("node-c", False)
                run.assert_called_once_with(["docker", "network", "connect", "--alias", "peer-node-c", backend.network, "owned-id"])

    def test_success_requires_assertion_marker_not_just_zero_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            backend = hardening.Backend(SimpleNamespace(records_per_fragment=128), Path(directory))
            backend.checker_command = lambda argv: ["checker", *argv]
            with patch.object(hardening, "command", return_value="no assertions completed"):
                with self.assertRaises(RuntimeError):
                    backend.checker("verify")
            with patch.object(hardening, "command", return_value="SCENARIO_OK phase=verify"):
                self.assertIn("SCENARIO_OK", backend.checker("verify"))

    def test_docker_isolation_requires_detached_peer_bridge_and_retained_control(self):
        with tempfile.TemporaryDirectory() as directory:
            backend = hardening.Docker(SimpleNamespace(), Path(directory))
            for networks, valid in [
                ({backend.project + "_control-net": {}}, True),
                ({backend.network: {}, backend.project + "_control-net": {}}, False),
                ({}, False),
            ]:
                with patch.object(backend, "container", return_value="owned-id"), patch.object(
                    hardening, "command", return_value=hardening.json.dumps(networks)
                ) as run:
                    if valid:
                        backend.confirm_isolation("node-c")
                    else:
                        with self.assertRaises(RuntimeError):
                            backend.confirm_isolation("node-c")
                    run.assert_called_once_with([
                        "docker", "inspect", "--format", "{{json .NetworkSettings.Networks}}", "owned-id"
                    ])

    def test_command_checks_exit_status(self):
        with patch.object(hardening.subprocess, "run", return_value=SimpleNamespace(returncode=1, stdout="failure")):
            with self.assertRaises(RuntimeError):
                hardening.command(["checker"])

    def test_failure_status_capture_continues_after_unavailable_node(self):
        with tempfile.TemporaryDirectory() as directory:
            backend = hardening.Backend(SimpleNamespace(), Path(directory))
            with patch.object(backend, "status", side_effect=[{"node":"node-a"}, RuntimeError("stopped"), {"node":"node-c"}, {"node":"node-d"}]):
                backend.capture_statuses()
            self.assertTrue((Path(directory) / "diagnostic-node-a.json").exists())
            self.assertFalse((Path(directory) / "diagnostic-node-b.json").exists())
            self.assertTrue((Path(directory) / "diagnostic-node-c.json").exists())
            self.assertTrue((Path(directory) / "diagnostic-node-d.json").exists())


if __name__ == "__main__":
    unittest.main()

