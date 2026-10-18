#!/usr/bin/env python3
"""Run the external-client scenario as four processes when Docker is unavailable."""

import argparse
from collections import deque
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import threading
import time

MARKERS = {
    "READY", "CLIENT_HEALTHY_OK", "CLIENT_WAITING_FOR_NODE_A_FAILURE",
    "CLIENT_FAILOVER_OK", "CLIENT_RECOVERY_OK", "SCENARIO_OK", "SCENARIO_FAILED",
}


class Process:
    def __init__(self, name, command, log):
        self.name, self.log = name, log
        self.condition = threading.Condition()
        self.lines = deque(maxlen=200)
        self.markers = set()
        self.reader_done = False
        self.process = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1,
        )
        self.reader = threading.Thread(target=self.read, daemon=True)
        self.reader.start()
        print(f"Started {name}: pid={self.process.pid}", flush=True)

    def read(self):
        try:
            with self.log.open("w") as output:
                for line in self.process.stdout:
                    output.write(line)
                    output.flush()
                    with self.condition:
                        self.lines.append(line.rstrip())
                        if line.strip() in MARKERS:
                            self.markers.add(line.strip())
                        self.condition.notify_all()
        finally:
            self.process.stdout.close()
            with self.condition:
                self.reader_done = True
                self.condition.notify_all()

    def wait_marker(self, marker, timeout):
        deadline = time.monotonic() + timeout
        with self.condition:
            while marker not in self.markers:
                if self.reader_done:
                    raise RuntimeError(f"{self.name} closed output before {marker}")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RuntimeError(f"{self.name} timed out waiting for {marker}")
                self.condition.wait(min(remaining, 0.25))
        print(f"{self.name}: {marker}", flush=True)

    def wait_exit(self, timeout, expected=0):
        code = self.process.wait(timeout=timeout)
        self.reader.join(timeout=2)
        if code != expected:
            raise RuntimeError(f"{self.name} exited {code}, expected {expected}")

    def snapshot_lines(self):
        with self.condition:
            return list(self.lines)

    def stop(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.reader.join(timeout=2)


def run(node_bin, client_bin, directory):
    processes, nodes = [], {}
    reservations = []
    try:
        for _ in range(6):
            reservation = socket.socket()
            reservation.bind(("127.0.0.1", 0))
            reservations.append(reservation)
        ports = [reservation.getsockname()[1] for reservation in reservations]
        names = ["node-a", "node-b", "node-c"]
        credential = "0123456789abcdef0123456789abcdef"

        def start(index, generation):
            name = names[index]
            command = [str(node_bin), "client-node", "--node-id", name,
                       "--peer-port", str(ports[index * 2]),
                       "--client-port", str(ports[index * 2 + 1]),
                       "--credential", credential, "--db-path", str(directory / name)]
            for peer in range(3):
                if peer != index:
                    command.extend(["--peer", f"127.0.0.1:{ports[peer * 2]}"])
            process = Process(name, command, directory / f"{name}-{generation}.log")
            processes.append(process)
            nodes[name] = process
            return process

        for index in range(3):
            reservations[index * 2].close()
            reservations[index * 2 + 1].close()
            start(index, 1)
        for node in nodes.values():
            node.wait_marker("READY", 90)
        command = [str(client_bin), "--credential", credential, "--timeout-secs", "180"]
        for index in range(3):
            command.extend(["--endpoint", f"127.0.0.1:{ports[index * 2 + 1]}"])
        client = Process("external-client", command, directory / "client.log")
        processes.append(client)
        assert len({process.process.pid for process in processes}) == 4
        client.wait_marker("CLIENT_WAITING_FOR_NODE_A_FAILURE", 120)
        nodes["node-a"].process.kill()
        nodes["node-a"].wait_exit(10, expected=-signal.SIGKILL)
        client.wait_marker("CLIENT_FAILOVER_OK", 90)
        start(0, 2).wait_marker("READY", 90)
        client.wait_marker("SCENARIO_OK", 90)
        client.wait_exit(20)
        for node in nodes.values():
            node.process.terminate()
        for node in nodes.values():
            node.wait_exit(30)
        print("PASS: separate SDK process, remote routing, SIGKILL, failover, and recovery", flush=True)
    except Exception:
        for process in processes:
            print(f"--- {process.name}: {process.log} ---", flush=True)
            print("\n".join(process.snapshot_lines()), flush=True)
        raise
    finally:
        for reservation in reservations:
            reservation.close()
        for process in processes:
            process.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node-bin", type=Path, default=Path("target/debug/actordb-docker-test"))
    parser.add_argument("--client-bin", type=Path, default=Path("target/debug/actordb-docker-client-test"))
    parser.add_argument("--logs", type=Path)
    args = parser.parse_args()
    binaries = [path.resolve(strict=True) for path in [args.node_bin, args.client_bin]]
    if args.logs:
        args.logs.mkdir(parents=True, exist_ok=False)
        run(*binaries, args.logs.resolve())
    else:
        with tempfile.TemporaryDirectory(prefix="actordb-external-client-") as directory:
            run(*binaries, Path(directory))


if __name__ == "__main__":
    main()
