"""Deterministic tests for exact-version registry dependency readiness."""

import json
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError

import wait_for_crate_dependency as wait


def entry(version="0.4.0", *, name="target", yanked=False):
    return json.dumps({"name": name, "vers": version, "yanked": yanked}).encode()


class Clock:
    def __init__(self):
        self.now = 0
        self.sleeps = []

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class ReadinessTests(unittest.TestCase):
    def run_wait(self, responses, timeout=15):
        clock, requests = Clock(), []
        responses = iter(responses)

        def fetch(url, request_timeout):
            requests.append((clock.now, url, request_timeout))
            response = next(responses)
            if isinstance(response, Exception):
                raise response
            return response

        wait.wait_until_ready("target", "0.4.0", timeout, 5, fetch=fetch,
                              clock=clock.time, sleep=clock.sleep, report=lambda _: None)
        return clock, requests

    def test_all_sparse_index_paths(self):
        for name, suffix in [("a", "1/a"), ("ab", "2/ab"), ("abc", "3/a/abc"), ("Actordb", "ac/to/actordb")]:
            self.assertEqual(wait.crate_index_url(name), "https://index.crates.io/" + suffix)
        with self.assertRaises(wait.WaitError):
            wait.crate_index_url("../target")

    def test_immediate_check_then_missing_to_ready(self):
        clock, requests = self.run_wait([HTTPError("url", 404, "missing", {}, None), entry()])
        self.assertEqual([request[0] for request in requests], [0, 5])
        self.assertEqual(clock.sleeps, [5])

    def test_wrong_version_name_and_yanked_are_not_ready(self):
        body = b"\n".join([entry("0.4.1"), entry(name="other"), entry(yanked=True)])
        self.assertFalse(wait.index_ready(body, "target", "0.4.0"))
        self.run_wait([body, entry()])

    def test_transient_transport_http_and_malformed_index_retry(self):
        for transient in [URLError("offline"), HTTPError("url", 429, "limited", {}, None), HTTPError("url", 503, "down", {}, None), b"not-json", b"\xff", b"[]", b"[" * 2000 + b"]" * 2000]:
            with self.subTest(transient=type(transient).__name__):
                clock, requests = self.run_wait([transient, entry()])
                self.assertEqual(len(requests), 2)
                self.assertEqual(clock.now, 5)

    def test_timeout_is_bounded_and_request_timeout_uses_remaining(self):
        clock, requests = Clock(), []

        def fetch(url, timeout):
            requests.append(timeout)
            return entry("0.4.1")

        with self.assertRaises(wait.WaitError):
            wait.wait_until_ready("target", "0.4.0", 7, 5, fetch=fetch,
                                  clock=clock.time, sleep=clock.sleep, report=lambda _: None)
        self.assertEqual(requests, [7, 2])
        self.assertEqual(clock.sleeps, [5, 2])
        for bad in [0, -1, float("nan"), float("inf")]:
            with self.assertRaises(wait.WaitError):
                wait.wait_until_ready("target", "0.4.0", bad, fetch=fetch)

    def test_reporter_deadline_race_never_sleeps_negative(self):
        clock = Clock()
        def report(_): clock.now = 20
        with self.assertRaises(wait.WaitError):
            wait.wait_until_ready("target", "0.4.0", 7, 5, fetch=lambda *_: b"",
                                  clock=clock.time, sleep=clock.sleep, report=report)
        self.assertEqual(clock.sleeps, [])

    def test_index_body_limit(self):
        with self.assertRaises(ValueError):
            wait.index_ready(b" " * (wait.MAX_INDEX_BYTES + 1), "target", "0.4.0")

    def test_fetch_checks_deadline_during_streaming_body(self):
        clock = Clock()
        class Response:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def read1(self, limit):
                clock.now += 1
                return b" "
        with patch.object(wait, "urlopen", return_value=Response()), patch.object(wait.time, "monotonic", clock.time):
            with self.assertRaises(TimeoutError):
                wait.fetch_index("https://index.crates.io/ta/rg/target", 2)
        self.assertEqual(clock.now, 2)

    def test_dependency_exact_manifest_version_and_local_membership(self):
        dependent = {"id": "dependent-id", "name": "dependent", "dependencies": [{"name": "target", "kind": "dev", "source": None, "path": "/workspace/target", "req": "^0.4"}]}
        target = {"id": "target-id", "name": "target", "version": "0.4.0", "manifest_path": "/workspace/target/Cargo.toml"}
        metadata = {"packages": [dependent, target], "workspace_members": ["dependent-id", "target-id"]}
        self.assertEqual(wait.dependency_version(metadata, "dependent", "target"), "0.4.0")
        for kind in [None, "dev", "build"]:
            dependent["dependencies"][0]["kind"] = kind
            self.assertEqual(wait.dependency_version(metadata, "dependent", "target"), "0.4.0")
        for field, value in [("source", "registry+https://github.com/rust-lang/crates.io-index"), ("path", "/elsewhere/target"), ("kind", "unknown"), ("name", "indirect")]:
            original = dependent["dependencies"][0].copy()
            dependent["dependencies"][0][field] = value
            with self.assertRaises(wait.WaitError):
                wait.dependency_version(metadata, "dependent", "target")
            dependent["dependencies"][0] = original
        metadata["workspace_members"].remove("target-id")
        with self.assertRaises(wait.WaitError):
            wait.dependency_version(metadata, "dependent", "target")


if __name__ == "__main__":
    unittest.main()
