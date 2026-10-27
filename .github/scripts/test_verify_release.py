"""Stdlib tests for backend opt-ins, release provenance, and safe CI output."""

import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock


SPEC = importlib.util.spec_from_file_location(
    "verify_release", Path(__file__).with_name("verify_release.py")
)
guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guard)


def metadata(version="0.2.0"):
    return {
        "packages": [
            {
                "name": "actordb",
                "id": "actordb-id",
                "version": version,
                "features": {
                    "default": ["grpc"],
                    "grpc": ["dep:tonic"],
                    "memory": [],
                    "rocksdb": ["dep:rocksdb"],
                },
                "dependencies": [
                    {
                        "name": "rocksdb",
                        "kind": None,
                        "optional": True,
                        "source": "registry+https://github.com/rust-lang/crates.io-index",
                    }
                ],
            },
            {"name": "xtask", "id": "xtask-id", "version": "0.1.0", "publish": []},
            {"name": "docker-tests", "id": "docker-tests-id", "version": "0.1.0", "publish": []},
            *[
                {"name": name, "id": f"{name}-id", "version": version, "publish": None}
                for name in ["actordb-protocol", "actordb-query", "actordb-client"]
            ],
        ],
        "workspace_members": ["actordb-id", "xtask-id", "docker-tests-id", "actordb-protocol-id", "actordb-query-id", "actordb-client-id"],
    }


def command_result(stdout="", returncode=0, stderr=""):
    return subprocess.CompletedProcess([], returncode, stdout, stderr)


class MetadataTests(unittest.TestCase):
    def resolve(self, value):
        with mock.patch.object(
            guard.subprocess, "run", return_value=command_result(json.dumps(value))
        ):
            return guard.resolve_version()

    def test_reads_only_the_named_workspace_package_with_metadata(self):
        with mock.patch.object(
            guard.subprocess, "run", return_value=command_result(json.dumps(metadata()))
        ) as run:
            self.assertEqual(guard.resolve_version(), "0.2.0")
        run.assert_called_once_with(
            ["cargo", "metadata", "--format-version", "1", "--no-deps"],
            cwd=None,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_metadata_does_not_require_an_existing_workspace_lockfile(self):
        with tempfile.TemporaryDirectory() as directory:
            lockfile = Path(directory) / "Cargo.lock"
            self.assertFalse(lockfile.exists())

            def cargo_metadata(command, **options):
                self.assertEqual(options["cwd"], directory)
                self.assertFalse(lockfile.exists())
                self.assertEqual(
                    command,
                    ["cargo", "metadata", "--format-version", "1", "--no-deps"],
                )
                return command_result(json.dumps(metadata()))

            with mock.patch.object(
                guard.subprocess, "run", side_effect=cargo_metadata
            ) as run:
                self.assertEqual(guard.resolve_version(directory), "0.2.0")
            run.assert_called_once()
            self.assertFalse(lockfile.exists())

    def test_preserves_prerelease_and_build_version(self):
        self.assertEqual(
            self.resolve(metadata("1.2.3-rc.1+build.4")), "1.2.3-rc.1+build.4"
        )

    def test_companions_must_match_the_core_version(self):
        for name in ["actordb-protocol", "actordb-query", "actordb-client"]:
            for version in ["0.1.0", "0.2.0-rc.1", "0.2.0+build", None, "private-token\n"]:
                value = metadata()
                next(package for package in value["packages"] if package["name"] == name)["version"] = version
                with self.subTest(name=name, version=version), self.assertRaisesRegex(guard.ReleaseError, "share") as caught:
                    self.resolve(value)
                self.assertNotIn("private-token", str(caught.exception))

    def test_companions_must_be_present_once_and_in_the_workspace(self):
        for name in ["actordb-protocol", "actordb-query", "actordb-client"]:
            for mutation in ["missing", "duplicate", "outside_workspace"]:
                value = metadata()
                package = next(package for package in value["packages"] if package["name"] == name)
                if mutation == "missing":
                    value["packages"].remove(package)
                elif mutation == "duplicate":
                    value["packages"].append(package.copy())
                else:
                    value["workspace_members"].remove(package["id"])
                with self.subTest(name=name, mutation=mutation), self.assertRaises(guard.ReleaseError):
                    self.resolve(value)

    def test_publish_metadata_uses_cargo_null_or_registry_allowlist(self):
        for name in guard.PUBLISHED_PACKAGES:
            for publish in [None, ["crates-io"], ["private", "crates-io"]]:
                value = metadata()
                next(package for package in value["packages"] if package["name"] == name)["publish"] = publish
                with self.subTest(name=name, publish=publish):
                    self.assertEqual(self.resolve(value), "0.2.0")
            for publish in [[], False, True, ["private"], "crates-io", [1, "crates-io"]]:
                value = metadata()
                next(package for package in value["packages"] if package["name"] == name)["publish"] = publish
                with self.subTest(name=name, publish=publish), self.assertRaisesRegex(guard.ReleaseError, "crates-io"):
                    self.resolve(value)

    def test_internal_tools_and_accidental_workspace_packages_cannot_publish(self):
        for name in ["xtask", "docker-tests"]:
            for publish in [None, ["crates-io"], False]:
                value = metadata()
                next(package for package in value["packages"] if package["name"] == name)["publish"] = publish
                with self.subTest(name=name, publish=publish), self.assertRaisesRegex(guard.ReleaseError, "disable publication"):
                    self.resolve(value)
        value = metadata()
        value["packages"].append({"name": "extra-library", "id": "extra-id", "version": "0.2.0", "publish": None})
        value["workspace_members"].append("extra-id")
        with self.assertRaisesRegex(guard.ReleaseError, "disable publication"):
            self.resolve(value)
        value["packages"][-1]["publish"] = []
        self.assertEqual(self.resolve(value), "0.2.0")

    def test_nonworkspace_dependency_publication_metadata_is_not_a_workspace_policy(self):
        value = metadata()
        value["packages"].append({"name": "third-party", "id": "dependency-id", "version": "999.0.0", "publish": None})
        self.assertEqual(self.resolve(value), "0.2.0")

    def test_workspace_member_without_package_metadata_fails_closed(self):
        value = metadata()
        value["workspace_members"].append("unknown-id")
        with self.assertRaisesRegex(guard.ReleaseError, "one package"):
            self.resolve(value)

    def test_backend_features_remain_independent_and_opt_in(self):
        value = metadata()
        # Extra, unselected features do not require a generic feature-graph audit.
        value["packages"][0]["features"]["other"] = []
        self.assertEqual(self.resolve(value), "0.2.0")

    def test_rejects_wrong_or_missing_default_features(self):
        for defaults in [["memory"], ["rocksdb"], ["other"]]:
            value = metadata()
            value["packages"][0]["features"]["default"] = defaults
            with self.subTest(defaults=defaults), self.assertRaises(guard.ReleaseError):
                self.resolve(value)
        value = metadata()
        del value["packages"][0]["features"]["default"]
        with self.assertRaises(guard.ReleaseError):
            self.resolve(value)

    def test_requires_both_named_backend_opt_in_features(self):
        for feature in ["memory", "rocksdb"]:
            value = metadata()
            del value["packages"][0]["features"][feature]
            with self.subTest(feature=feature), self.assertRaises(guard.ReleaseError):
                self.resolve(value)

    def test_rejects_memory_feature_dependencies(self):
        for entries in [["rocksdb"], ["dep:rocksdb"], ["other"]]:
            value = metadata()
            value["packages"][0]["features"]["memory"] = entries
            with self.subTest(entries=entries), self.assertRaises(guard.ReleaseError):
                self.resolve(value)

    def test_rejects_missing_or_nonruntime_rocksdb_dependency(self):
        for kind in ["dev", "build"]:
            value = metadata()
            value["packages"][0]["dependencies"][0]["kind"] = kind
            with self.subTest(kind=kind), self.assertRaises(guard.ReleaseError):
                self.resolve(value)
        for dependencies in [[], metadata()["packages"][0]["dependencies"] * 2]:
            value = metadata()
            value["packages"][0]["dependencies"] = dependencies
            with self.subTest(dependencies=dependencies), self.assertRaises(
                guard.ReleaseError
            ):
                self.resolve(value)

    def test_rejects_mandatory_or_nonregistry_rocksdb_dependency(self):
        for field, setting in [
            ("optional", False),
            ("optional", None),
            ("optional", 1),
            ("source", None),
            ("source", "git+https://example.invalid/rocksdb"),
        ]:
            value = metadata()
            value["packages"][0]["dependencies"][0][field] = setting
            with self.subTest(field=field, setting=setting), self.assertRaises(
                guard.ReleaseError
            ):
                self.resolve(value)

    def test_requires_explicit_rocksdb_dependency_feature(self):
        for entries in [[], ["memory"], ["rocksdb"], ["rocksdb/default"]]:
            value = metadata()
            value["packages"][0]["features"]["rocksdb"] = entries
            with self.subTest(entries=entries), self.assertRaises(guard.ReleaseError):
                self.resolve(value)

    def test_rocksdb_opt_in_cannot_enable_memory_or_another_feature(self):
        for entries in [["dep:rocksdb", "memory"], ["dep:rocksdb", "another"]]:
            value = metadata()
            value["packages"][0]["features"]["rocksdb"] = entries
            with self.subTest(entries=entries), self.assertRaises(guard.ReleaseError):
                self.resolve(value)

    def test_rejects_malformed_backend_metadata(self):
        for field, setting in [
            ("features", None),
            ("features", []),
            ("features", {"default": [], "memory": [], "rocksdb": "dep:rocksdb"}),
            ("dependencies", None),
            ("dependencies", {}),
            ("dependencies", [None]),
        ]:
            value = metadata()
            value["packages"][0][field] = setting
            with self.subTest(field=field, setting=setting), self.assertRaises(
                guard.ReleaseError
            ):
                self.resolve(value)

    def test_rejects_missing_wrong_duplicate_and_nonworkspace_package(self):
        cases = [
            {"packages": [], "workspace_members": []},
            {
                "packages": [{"name": "other", "id": "other", "version": "0.2.0"}],
                "workspace_members": ["other"],
            },
            {
                "packages": [metadata()["packages"][0]] * 2,
                "workspace_members": ["actordb-id"],
            },
            {
                "packages": metadata()["packages"],
                "workspace_members": ["xtask-id"],
            },
        ]
        for value in cases:
            with self.subTest(value=value), self.assertRaises(guard.ReleaseError):
                self.resolve(value)

    def test_rejects_malformed_metadata_shape(self):
        cases = [
            None, [], {},
            {"packages": [None], "workspace_members": []},
            {"packages": [], "workspace_members": [None]},
        ]
        for value in cases:
            with self.subTest(value=value), self.assertRaises(guard.ReleaseError):
                self.resolve(value)

    def test_rejects_missing_invalid_or_output_injecting_version(self):
        versions = [
            None, 2, "", "v0.2.0", "0.2", "00.2.0", "0.2.0-01",
            "0.2.0\nsecret=value", "0.2.0\n", "0.2.0-rc..1",
        ]
        for version in versions:
            with self.subTest(version=version), self.assertRaises(guard.ReleaseError):
                self.resolve(metadata(version))
        value = metadata()
        del value["packages"][0]["version"]
        with self.assertRaises(guard.ReleaseError):
            self.resolve(value)

    def test_command_and_json_failures_do_not_expose_output(self):
        results = [
            command_result("private-token", 1, "private-token"),
            command_result("private-token"),
        ]
        for result in results:
            with mock.patch.object(guard.subprocess, "run", return_value=result):
                with self.assertRaises(guard.ReleaseError) as caught:
                    guard.resolve_version()
            self.assertNotIn("private-token", str(caught.exception))
        for error in [FileNotFoundError("private-token"), UnicodeError("private-token")]:
            with mock.patch.object(guard.subprocess, "run", side_effect=error):
                with self.assertRaises(guard.ReleaseError) as caught:
                    guard.resolve_version()
            self.assertNotIn("private-token", str(caught.exception))


class OutputAndCliTests(unittest.TestCase):
    def invoke(self, arguments, environment=None):
        stdout, stderr = io.StringIO(), io.StringIO()
        with mock.patch.dict(os.environ, environment or {}, clear=True):
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                result = guard.main(arguments)
        return result, stdout.getvalue(), stderr.getvalue()

    def test_nonrelease_resolves_version_without_git_and_outputs_only_version(self):
        with mock.patch.object(guard, "resolve_version", return_value="0.2.0"):
            with mock.patch.object(guard, "verify_tag") as verify:
                self.assertEqual(
                    self.invoke([], {"UNRELATED_SECRET": "private-token"}),
                    (0, "version=0.2.0\n", ""),
                )
        verify.assert_not_called()

    def test_failed_backend_policy_emits_no_success_or_private_metadata(self):
        value = metadata()
        value["packages"][0]["features"]["default"] = ["private-token"]
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "outputs"
            with mock.patch.object(
                guard.subprocess, "run", return_value=command_result(json.dumps(value))
            ), mock.patch.object(guard, "verify_tag") as verify:
                result, stdout, stderr = self.invoke(
                    ["--tag", "v0.2.0"], {"GITHUB_OUTPUT": str(output)}
                )
            self.assertEqual(result, 1)
            self.assertEqual(stdout, "")
            self.assertNotIn("private-token", stderr)
            self.assertIn("default features must only enable grpc", stderr)
            self.assertFalse(output.exists())
            verify.assert_not_called()

    def test_appends_github_output_and_emits_stdout_after_verification(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "outputs with spaces"
            output.write_text("earlier=value\n", encoding="utf-8")
            with mock.patch.object(guard, "resolve_version", return_value="0.2.0"):
                with mock.patch.object(guard, "verify_tag") as verify:
                    self.assertEqual(
                        self.invoke(["--tag", "v0.2.0"], {"GITHUB_OUTPUT": str(output)}),
                        (0, "version=0.2.0\n", ""),
                    )
            verify.assert_called_once_with("v0.2.0", "0.2.0")
            self.assertEqual(
                output.read_text(encoding="utf-8"), "earlier=value\nversion=0.2.0\n"
            )

    def test_failed_release_emits_no_success_or_github_output(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "outputs"
            with mock.patch.object(guard, "resolve_version", return_value="0.2.0"):
                for tag in ["", "0.2.0", "v0.1.0", "v0.2.0-extra", "v0.2.0\n"]:
                    with self.subTest(tag=tag):
                        result, stdout, stderr = self.invoke(
                            ["--tag", tag], {"GITHUB_OUTPUT": str(output)}
                        )
                        self.assertEqual(result, 1)
                        self.assertEqual(stdout, "")
                        self.assertIn("release verification failed", stderr)
                        self.assertFalse(output.exists())

    def test_missing_tag_argument_is_cli_error(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as caught:
            guard.main(["--tag"])
        self.assertEqual(caught.exception.code, 2)

    def test_metadata_failure_emits_no_success_or_github_output(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "outputs"
            with mock.patch.object(
                guard, "resolve_version", side_effect=guard.ReleaseError("metadata failed")
            ), mock.patch.object(guard, "verify_tag") as verify:
                result, stdout, stderr = self.invoke(
                    ["--tag", "v0.2.0"], {"GITHUB_OUTPUT": str(output)}
                )
            self.assertEqual(result, 1)
            self.assertEqual(stdout, "")
            self.assertIn("metadata failed", stderr)
            self.assertFalse(output.exists())
            verify.assert_not_called()

    def test_output_path_failure_is_safe_and_emits_no_success(self):
        with mock.patch.object(guard, "resolve_version", return_value="0.2.0"):
            with mock.patch("builtins.open", side_effect=OSError("private-token")):
                result, stdout, stderr = self.invoke([], {"GITHUB_OUTPUT": "private-token"})
        self.assertEqual(result, 1)
        self.assertEqual(stdout, "")
        self.assertNotIn("private-token", stderr)

    def test_provenance_failure_emits_no_success_or_github_output(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "outputs"
            with mock.patch.object(guard, "resolve_version", return_value="0.2.0"):
                with mock.patch.object(
                    guard.subprocess, "run", return_value=command_result("private-token", 1)
                ):
                    result, stdout, stderr = self.invoke(
                        ["--tag", "v0.2.0"], {"GITHUB_OUTPUT": str(output)}
                    )
            self.assertEqual(result, 1)
            self.assertEqual(stdout, "")
            self.assertNotIn("private-token", stderr)
            self.assertFalse(output.exists())


class GitAncestryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.repository = self.temporary.name
        self.git("init", "--quiet")
        self.git("config", "user.name", "Release guard test")
        self.git("config", "user.email", "release-test@example.invalid")
        self.git("config", "commit.gpgsign", "false")
        self.git("config", "tag.gpgsign", "false")
        self.git("commit", "--quiet", "--allow-empty", "-m", "first")
        self.first = self.git("rev-parse", "HEAD").strip()

    def git(self, *arguments):
        return subprocess.run(
            ["git", *arguments],
            cwd=self.repository,
            check=True,
            capture_output=True,
            text=True,
        ).stdout

    def test_exact_main_head_and_historical_ancestor_are_allowed(self):
        self.git("commit", "--quiet", "--allow-empty", "-m", "second")
        self.git("update-ref", "refs/remotes/origin/main", "HEAD")
        self.git("tag", "v0.2.0")
        guard.verify_tag("v0.2.0", "0.2.0", self.repository)
        self.git("checkout", "--quiet", "--detach", self.first)
        self.git("tag", "--force", "v0.2.0")
        guard.verify_tag("v0.2.0", "0.2.0", self.repository)

    def test_annotated_tag_is_peeled_to_the_head_commit(self):
        self.git("update-ref", "refs/remotes/origin/main", "HEAD")
        self.git("tag", "--annotate", "v0.2.0", "-m", "release")
        self.assertNotEqual(self.git("rev-parse", "refs/tags/v0.2.0").strip(), self.first)
        guard.verify_tag("v0.2.0", "0.2.0", self.repository)

    def test_head_ahead_of_main_is_rejected(self):
        self.git("update-ref", "refs/remotes/origin/main", self.first)
        self.git("commit", "--quiet", "--allow-empty", "-m", "unmerged")
        self.git("tag", "v0.2.0")
        with self.assertRaisesRegex(guard.ReleaseError, "reachable"):
            guard.verify_tag("v0.2.0", "0.2.0", self.repository)

    def test_unrelated_head_is_rejected(self):
        self.git("update-ref", "refs/remotes/origin/main", self.first)
        self.git("checkout", "--quiet", "--orphan", "unrelated")
        self.git("commit", "--quiet", "--allow-empty", "-m", "unrelated")
        self.git("tag", "v0.2.0")
        with self.assertRaisesRegex(guard.ReleaseError, "reachable"):
            guard.verify_tag("v0.2.0", "0.2.0", self.repository)

    def test_missing_main_ref_is_rejected(self):
        self.git("tag", "v0.2.0")
        with self.assertRaisesRegex(guard.ReleaseError, "reachable"):
            guard.verify_tag("v0.2.0", "0.2.0", self.repository)

    def test_missing_tag_is_rejected_even_when_head_is_on_main(self):
        self.git("update-ref", "refs/remotes/origin/main", self.first)
        with self.assertRaisesRegex(guard.ReleaseError, "verify release HEAD and tag"):
            guard.verify_tag("v0.2.0", "0.2.0", self.repository)

    def test_tag_on_different_or_moved_commit_is_rejected(self):
        self.git("tag", "v0.2.0")
        self.git("commit", "--quiet", "--allow-empty", "-m", "second")
        self.git("update-ref", "refs/remotes/origin/main", "HEAD")
        with self.assertRaisesRegex(guard.ReleaseError, "checked-out HEAD"):
            guard.verify_tag("v0.2.0", "0.2.0", self.repository)
        self.git("tag", "--force", "v0.2.0")
        self.git("checkout", "--quiet", "--detach", self.first)
        with self.assertRaisesRegex(guard.ReleaseError, "checked-out HEAD"):
            guard.verify_tag("v0.2.0", "0.2.0", self.repository)

    def test_release_rejects_prerelease_and_build_versions_before_git(self):
        for version in ["0.2.0-rc.1", "0.2.0+build.4"]:
            with self.subTest(version=version):
                with mock.patch.object(guard.subprocess, "run") as run:
                    with self.assertRaisesRegex(guard.ReleaseError, "stable numeric"):
                        guard.verify_tag(f"v{version}", version, self.repository)
                run.assert_not_called()

    def test_ancestry_uses_the_verified_commit_not_an_uncorrelated_head(self):
        head = "a" * 40
        responses = [command_result(head + "\n"), command_result(head + "\n"), command_result()]
        with mock.patch.object(guard.subprocess, "run", side_effect=responses) as run:
            guard.verify_tag("v0.2.0", "0.2.0", self.repository)
        self.assertEqual(
            [call.args[0] for call in run.call_args_list],
            [
                ["git", "rev-parse", "--verify", "HEAD"],
                ["git", "rev-parse", "--verify", "refs/tags/v0.2.0^{commit}"],
                ["git", "merge-base", "--is-ancestor", head, "refs/remotes/origin/main"],
            ],
        )

    def test_wrong_tag_does_not_even_invoke_git(self):
        with mock.patch.object(guard.subprocess, "run") as run:
            with self.assertRaises(guard.ReleaseError):
                guard.verify_tag("v0.1.0", "0.2.0", self.repository)
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
