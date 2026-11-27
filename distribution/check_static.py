#!/usr/bin/env python3
"""Static provenance regressions; never import or execute product modules.

All adversarial inputs and checkout transforms are in-memory. This script writes
no files, opens no databases or sockets, and installs no dependencies.
"""
from __future__ import annotations

from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "i8_distribution_builder", ROOT / "distribution/build_candidate.py"
)
assert SPEC is not None and SPEC.loader is not None
TOOL = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(TOOL)
INVENTORY = ROOT / "distribution/source-inventory.json"
MANIFEST = ROOT / "distribution/distribution-manifest.1446.json"


class StaticProvenanceChecks(unittest.TestCase):
    def assert_metadata_rejected(self, change_inventory=None, change_manifest=None):
        inventory = json.loads(INVENTORY.read_text(encoding="utf-8"))
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        if change_inventory is not None:
            change_inventory(inventory)
        if change_manifest is not None:
            change_manifest(manifest)
        digest = TOOL.sha256_bytes(TOOL.json_bytes(inventory))
        manifest["source_inventory"]["sha256"] = digest
        original_load = TOOL.load_json
        original_sha = TOOL.sha256_file

        def load(path):
            if path == INVENTORY:
                return deepcopy(inventory)
            if path == MANIFEST:
                return deepcopy(manifest)
            return original_load(path)

        def sha(path, *, canonical=False):
            if path == INVENTORY:
                return digest
            return original_sha(path, canonical=canonical)

        with patch.object(TOOL, "load_json", side_effect=load), patch.object(
            TOOL, "sha256_file", side_effect=sha
        ):
            with self.assertRaises(TOOL.CandidateError):
                TOOL.check(ROOT)

    def test_current_candidate(self):
        result = TOOL.check(ROOT)
        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["baseline_member_count"], 158)
        self.assertEqual(result["publication_status"], "BLOCKED")

    def test_lf_checkout_is_portable_without_regeneration(self):
        original_read = Path.read_bytes
        with patch.object(
            Path, "read_bytes", lambda path: TOOL.canonical_bytes(original_read(path))
        ):
            self.assertEqual(TOOL.check(ROOT)["status"], "passed")

    def test_crlf_checkout_is_portable_without_regeneration(self):
        original_read = Path.read_bytes
        with patch.object(
            Path,
            "read_bytes",
            lambda path: TOOL.canonical_bytes(original_read(path)).replace(b"\n", b"\r\n"),
        ):
            self.assertEqual(TOOL.check(ROOT)["status"], "passed")

    def test_candidate_excludes_geography_and_derived_scenarios(self):
        # Independent oracle: every scenario JSON references the geography pin;
        # named packs additionally embed upstream country/settlement records.
        expected = {
            "app/__init__.py",
            "app/disease.py",
            "app/execution.py",
            "app/scenario.py",
            "app/source_attestation.py",
        }
        self.assertEqual(set(TOOL.CANDIDATE_FILES), expected)
        inventory = json.loads(INVENTORY.read_text(encoding="utf-8"))
        entries = {entry["path"]: entry for entry in inventory["entries"]}
        for relative, entry in entries.items():
            if relative == "app/data/scenario.json" or relative.startswith("app/data/scenarios/"):
                with self.subTest(path=relative):
                    self.assertEqual(entry["distribution"]["status"], "excluded")
                    self.assertEqual(entry["source"]["kind"], "derived_external_geography_scenario")
                    self.assertEqual(entry["license"]["publication"], "blocked")

    def test_missing_excluded_inventory_entry_is_rejected(self):
        self.assert_metadata_rejected(
            lambda value: value.update(
                entries=[e for e in value["entries"] if e["path"] != "app/data/geography.json"]
            )
        )

    def test_duplicate_inventory_entry_is_rejected(self):
        self.assert_metadata_rejected(
            lambda value: value["entries"].append(deepcopy(value["entries"][-1]))
        )

    def test_forged_license_disposition_is_rejected(self):
        def mutate(value):
            value["entries"][-1]["license"] = {"status": "cleared", "publication": "allowed"}
        self.assert_metadata_rejected(mutate)

    def test_forged_publication_status_is_rejected(self):
        self.assert_metadata_rejected(
            change_manifest=lambda value: value.update(publication_status="APPROVED")
        )

    def test_missing_publication_blocker_is_rejected(self):
        self.assert_metadata_rejected(
            change_manifest=lambda value: value.update(publication_blockers=[])
        )

    def test_boolean_metadata_cannot_be_replaced_by_integer(self):
        self.assert_metadata_rejected(
            change_manifest=lambda value: value["checks"].update(allowlist_verified=1)
        )

    def test_missing_excluded_manifest_path_is_rejected(self):
        self.assert_metadata_rejected(
            change_manifest=lambda value: value["excluded_baseline_paths"].pop()
        )

    def test_secret_patterns_report_only_pattern_identifiers(self):
        synthetic = b"test=" + b"ghp_" + b"X" * 30
        with patch.object(Path, "read_bytes", return_value=synthetic):
            self.assertEqual(TOOL.secret_hits(Path("in-memory-fixture")), ["2"])

    def test_forbidden_path_classes(self):
        paths = ["app/private/file.txt", "app/.env.local", "app/world.sqlite", "app/photo.png"]
        self.assertEqual(TOOL.forbidden_path_hits(paths), sorted(paths))

    def test_traversal_and_absolute_paths_are_rejected(self):
        for relative in ("../outside", "/outside", "app/../../outside"):
            with self.subTest(path=relative), self.assertRaises(TOOL.CandidateError):
                TOOL.safe_relative(relative)

    def test_linked_output_parent_is_rejected(self):
        with patch.object(Path, "is_symlink", lambda path: path == ROOT / "distribution"):
            with self.assertRaises(TOOL.CandidateError):
                TOOL.safe_repo_path(ROOT, "distribution/source-inventory.json")

    def test_candidate_source_drift_is_rejected(self):
        target = ROOT / "distribution/candidate-1446/app/disease.py"
        original_read = Path.read_bytes
        with patch.object(
            Path,
            "read_bytes",
            lambda path: original_read(path) + (b"\n# unexpected drift\n" if path == target else b""),
        ):
            with self.assertRaises(TOOL.CandidateError):
                TOOL.check(ROOT)


if __name__ == "__main__":
    unittest.main(verbosity=2)
