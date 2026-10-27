#!/usr/bin/env python3
"""Four-process durable cluster matrix; Docker exercises real bridge faults.

Process mode uses a private test-only peer transport gate instead of bridge
disconnect. The external SDK checker never links a Node in either mode.
"""

import argparse
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import time

NODES = ("node-a", "node-b", "node-c", "node-d")


def command(argv, timeout=240):
    result = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, timeout=timeout, check=False)
    if result.returncode:
        raise RuntimeError(f"command failed ({result.returncode}): {argv}\n{result.stdout}")
    return result.stdout


class Backend:
    def __init__(self, args, directory):
        self.args, self.directory = args, directory
        self.phase_number, self.endpoints = 0, []

    def checker(self, phase, target="node-a", **flags):
        self.phase_number += 1
        argv = ["--phase", phase, "--target", target, "--round",
                str(flags.pop("round", self.phase_number)), "--records-per-fragment",
                str(self.args.records_per_fragment)]
        argv += ["--diagnostic-wait-secs", str(getattr(self.args, "diagnostic_wait_secs", 0))]
        for endpoint in self.endpoints:
            argv += ["--node", endpoint]
        for key, value in flags.items():
            argv += ["--" + key.replace("_", "-"),
                     str(value).lower() if isinstance(value, bool) else str(value)]
        print(f"[phase {self.phase_number}] {phase} target={target}", flush=True)
        started = time.monotonic()
        try:
            output = command(self.checker_command(argv))
        except (RuntimeError, subprocess.TimeoutExpired) as error:
            (self.directory / f"{self.phase_number:03d}-{phase}-{target}.log").write_text(str(error))
            raise
        (self.directory / f"{self.phase_number:03d}-{phase}-{target}.log").write_text(output)
        if f"SCENARIO_OK phase={phase}" not in output:
            raise RuntimeError(f"checker omitted completed assertion marker: {output}")
        print(f"  PASS {phase}: {time.monotonic() - started:.1f}s", flush=True)
        return output

    def status(self, node):
        output = self.checker("status", node)
        for line in output.splitlines():
            if line.startswith("RESULT "):
                return json.loads(line.removeprefix("RESULT "))
        raise RuntimeError(f"{node} omitted structured status")

    def wait(self, node, predicate, label, timeout=90):
        deadline, last = time.monotonic() + timeout, None
        while time.monotonic() < deadline:
            try:
                last = self.status(node)
                if predicate(last):
                    return last
            except (RuntimeError, subprocess.TimeoutExpired) as error:
                last = str(error)
            time.sleep(0.5)
        raise RuntimeError(f"{node}: timeout {label}; last={last}")

    def ready(self, node, old_boot=None):
        return self.wait(node, lambda s: s["node"] == node and s["placement_complete"]
                         and (old_boot is None or s["boot_id"] != old_boot),
                         "new process and placement observation")

    def confirm_isolation(self, node):
        # The process-mode transport gate actively closes its peer streams.
        self.wait(node, lambda s: s["peers"] == 0, "closed test peer transport")

    def capture_statuses(self):
        for node in NODES:
            try:
                status = self.status(node)
                (self.directory / f"diagnostic-{node}.json").write_text(json.dumps(status, indent=2))
                print(f"DIAGNOSTIC {node}: {json.dumps(status)}", flush=True)
            except Exception as error:
                print(f"DIAGNOSTIC {node} unavailable: {error}", flush=True)


class Processes(Backend):
    def __init__(self, args, directory):
        super().__init__(args, directory)
        self.processes, self.generations, self.logs = {}, {}, []
        self.reservations = []
        for _ in range(12):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            self.reservations.append(sock)
        self.ports = [sock.getsockname()[1] for sock in self.reservations]
        self.endpoints = [f"{node}@127.0.0.1:{self.ports[i*3+1]}@127.0.0.1:{self.ports[i*3+2]}"
                          for i, node in enumerate(NODES)]

    def start(self, node):
        index = NODES.index(node)
        generation = self.generations.get(node, 0) + 1
        self.generations[node] = generation
        for sock in self.reservations[index*3:index*3+3]:
            sock.close()
        argv = [str(self.args.node_bin), "cluster-node", "--node-id", node,
                "--db-path", str(self.directory / node),
                "--peer-port", str(self.ports[index*3]),
                "--client-port", str(self.ports[index*3+1]),
                "--control-port", str(self.ports[index*3+2])]
        for other in range(4):
            if other != index:
                argv += ["--peer", f"127.0.0.1:{self.ports[other*3]}"]
        log = self.directory / f"{node}-{generation}.log"
        with log.open("w") as output:
            process = subprocess.Popen(argv, stdout=output, stderr=subprocess.STDOUT)
        self.processes[node] = process
        self.logs.append(log)
        print(f"start {node}: pid={process.pid} generation={generation}", flush=True)

    def stop(self, node, hard=True):
        process = self.processes[node]
        if process.poll() is not None:
            raise RuntimeError(f"{node} exited unexpectedly: {process.returncode}")
        process.send_signal(signal.SIGKILL if hard else signal.SIGTERM)
        code = process.wait(timeout=10 if hard else 75)
        expected = -signal.SIGKILL if hard else 0
        if code != expected:
            raise RuntimeError(f"{node} exited {code}, expected {expected}")
        log = self.directory / f"{node}-{self.generations[node]}.log"
        if not hard and "SHUTDOWN_OK" not in log.read_text():
            raise RuntimeError(f"{node} omitted shutdown ownership evidence")

    def checker_command(self, argv):
        return [str(self.args.client_bin), "--ledger", str(self.directory / "ledger.json"), *argv]

    def partition(self, node, isolated):
        self.checker("fault", node, isolated=isolated)

    def diagnostics(self):
        for log in self.logs:
            print(f"--- {log} ---", flush=True)
            print(log.read_text(errors="replace")[-12000:], flush=True)

    def close(self):
        for process in self.processes.values():
            if process.poll() is None:
                process.terminate()
        for process in self.processes.values():
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        for sock in self.reservations:
            sock.close()


class Docker(Backend):
    def __init__(self, args, directory):
        super().__init__(args, directory)
        self.project = f"actordb-hardening-{os.getpid()}-{secrets.token_hex(4)}"
        self.compose = ["docker", "compose", "--project-name", self.project,
                        "--file", "docker-compose.hardening.yml"]
        self.endpoints = [f"{node}@{node}:7449@{node}:7450" for node in NODES]
        self.network, self.started = self.project + "_peer-net", False

    def container(self, node):
        result = command([*self.compose, "ps", "--all", "--quiet", node]).strip()
        if not result or "\n" in result:
            raise RuntimeError(f"missing/ambiguous container for {self.project}/{node}")
        return result

    def start(self, node):
        self.started = True
        command([*self.compose, "up", "--detach", "--no-build", node], timeout=90)

    def stop(self, node, hard=True):
        container = self.container(node)
        command(["docker", "kill", "--signal", "SIGKILL" if hard else "SIGTERM", container])
        deadline = time.monotonic() + (15 if hard else 75)
        while time.monotonic() < deadline:
            state = json.loads(command(["docker", "inspect", "--format", "{{json .State}}", container]))
            if not state["Running"]:
                if state["ExitCode"] != (137 if hard else 0):
                    raise RuntimeError(f"{node} exit state: {state}")
                if not hard and "SHUTDOWN_OK" not in command(["docker", "logs", container]):
                    raise RuntimeError(f"{node} omitted shutdown ownership evidence")
                return
            time.sleep(0.5)
        raise RuntimeError(f"{node} exceeded shutdown budget")

    def checker_command(self, argv):
        return [*self.compose, "run", "--rm", "--no-deps", "-T", "checker", *argv]

    def partition(self, node, isolated):
        argv = ["docker", "network", "disconnect", "--force", self.network, self.container(node)] if isolated else [
            "docker", "network", "connect", "--alias", "peer-" + node, self.network, self.container(node)]
        command(argv)

    def confirm_isolation(self, node):
        networks = json.loads(command([
            "docker", "inspect", "--format", "{{json .NetworkSettings.Networks}}",
            self.container(node)]))
        if self.network in networks or self.project + "_control-net" not in networks:
            raise RuntimeError(f"{node}: peer bridge not isolated with control access retained: {networks}")
        # Bridge removal drops packets, not idle TCP stream registrations.
        # Routed read/query failure and isolated majority timeout below prove
        # the outage at the application boundary without trusting peer_count.

    def diagnostics(self):
        if self.started:
            output = command([*self.compose, "logs", "--no-color", "--timestamps"], timeout=30)
            (self.directory / "containers.log").write_text(output)
            print(output[-20000:], flush=True)
            output = command([*self.compose, "ps", "--all", "--format", "json"])
            (self.directory / "containers-state.json").write_text(output)

    def close(self):
        if self.started:
            # Only this generated owned project, never user containers/volumes.
            command([*self.compose, "down", "--volumes", "--remove-orphans"], timeout=90)


def run(backend, args):
    for node in NODES:
        backend.start(node)
    for node in NODES:
        backend.ready(node)
    backend.checker("genesis")
    backend.checker("seed")
    backend.checker("grow-two")
    backend.checker("grow-three")
    backend.partition("node-d", True)
    backend.confirm_isolation("node-d")
    backend.checker("unavailable", "node-d")
    backend.partition("node-d", False)
    backend.wait("node-d", lambda s: s["peers"] == 3, "routing ingress healing")
    backend.partition("node-c", True)
    backend.confirm_isolation("node-c")
    backend.checker("partition")
    backend.partition("node-c", False)
    backend.wait("node-c", lambda s: s["peers"] == 3, "replica healing")
    backend.checker("heal")
    for cycle in range(args.cycles):
        for node in ("node-a", "node-b", "node-c"):
            old = backend.status(node)["boot_id"]
            backend.stop(node)
            survivor = next(n for n in ("node-a", "node-b", "node-c") if n != node)
            backend.checker("rolling", survivor, round=cycle*3 + NODES.index(node))
            backend.start(node)
            backend.ready(node, old)
            backend.checker("verify")
    backend.checker("storage-fault", "node-b")
    backend.checker("timeout", "node-b")
    old = backend.status("node-d")["boot_id"]
    backend.checker("fault", "node-d", delay_ms=1000)
    backend.checker("grow-four")
    backend.checker("await-bootstrap", "node-d")
    backend.stop("node-d")
    backend.checker("survivors-ready", absent="node-d")
    backend.checker("rolling", "node-b", round=100)
    backend.start("node-d")
    backend.ready("node-d", old)
    backend.checker("verify")
    old = backend.status("node-d")["boot_id"]
    backend.checker("prepare-shrink")  # persist absent D's next boot map
    backend.stop("node-d")
    backend.checker("shrink", absent="node-d")
    backend.checker("shrink-blocked")
    backend.checker("rolling", "node-b", round=101)
    backend.start("node-d")
    backend.ready("node-d", old)
    backend.checker("retired")
    old = backend.status("node-a")["boot_id"]
    backend.stop("node-a")
    backend.start("node-a")
    backend.ready("node-a", old)
    backend.checker("retired")
    backend.checker("arm-recovery", epoch=secrets.token_hex(32))
    before = {node: backend.status(node)["boot_id"] for node in NODES}
    for node in NODES:
        backend.stop(node)
    backend.start("node-b")
    backend.ready("node-b", before["node-b"])
    backend.checker("blocked", "node-b")
    for node in ("node-c", "node-d", "node-a"):
        backend.start(node)
        backend.ready(node, before[node])
    backend.checker("verify")
    for node in NODES:
        backend.stop(node, hard=False)
    print("SCENARIO_OK: durable cluster hardening matrix", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("docker", "process"), default="docker")
    parser.add_argument("--node-bin", type=Path, default=Path("target/debug/actordb-docker-test"))
    parser.add_argument("--client-bin", type=Path, default=Path("target/debug/actordb-docker-cluster-client"))
    parser.add_argument("--artifacts", type=Path, default=Path("test-artifacts/cluster-hardening"))
    parser.add_argument("--cycles", type=int, choices=range(1, 6), default=3)
    parser.add_argument("--records-per-fragment", type=int, choices=range(1, 257), default=128)
    parser.add_argument("--timeout-secs", type=int, choices=range(180, 5401), default=2700)
    parser.add_argument("--diagnostic-wait-secs", type=int, choices=range(46), default=0)
    args = parser.parse_args()
    if args.backend == "process":
        args.node_bin = args.node_bin.resolve(strict=True)
        args.client_bin = args.client_bin.resolve(strict=True)
    args.artifacts.mkdir(parents=True, exist_ok=False)
    backend = (Docker if args.backend == "docker" else Processes)(args, args.artifacts.resolve())
    def interrupted(number, _frame):
        raise RuntimeError(f"hardening interrupted by signal {number}; collecting diagnostics and cleaning up")
    signal.signal(signal.SIGTERM, interrupted)
    if hasattr(signal, "SIGALRM"):
        signal.signal(signal.SIGALRM, interrupted)
        signal.alarm(args.timeout_secs)
    try:
        run(backend, args)
        backend.diagnostics()
    except BaseException:
        try:
            backend.capture_statuses()
            backend.diagnostics()
        except Exception as error:
            print(f"diagnostics failed: {error}", flush=True)
        raise
    finally:
        if hasattr(signal, "SIGALRM"):
            signal.alarm(0)
        backend.close()


if __name__ == "__main__":
    main()

