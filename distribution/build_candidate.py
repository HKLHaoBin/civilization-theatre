#!/usr/bin/env python3
"""Build and verify the I8 generation-1446 source review candidate.

This tool is intentionally stdlib-only. It copies only an explicit, dependency-
closed allowlist, preserves the exact pinned bytes from the original generation
manifest, and refuses to treat a review candidate as a public release. The
publication check is expected to fail until maintainers provide rights and
dependency evidence.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
BASELINE_MANIFEST = ROOT / "baseline/generation-manifest.1446.original.json"
SOURCES_RECORD = ROOT / "app/data/SOURCES.json"
INVENTORY_PATH = ROOT / "distribution/source-inventory.json"
DISTRIBUTION_MANIFEST_PATH = ROOT / "distribution/distribution-manifest.1446.json"
DEFAULT_CANDIDATE_ROOT = ROOT / "distribution/candidate-1446"
EXPECTED_BASELINE_MEMBERS = 158
EXPECTED_ORIGINAL_MANIFEST_SHA256_LF = (
    "248b31fd3402673e2fed83ec3b6c8ef5c71c3f96c13706d05c6310978e308150"
)
UPSTREAM_LICENSE_RECORDED_SHA256 = "e0eedba615d5cd1b986afb6c5b3a4b1ae33713e7e9dc74d19daec5e3221f9d2e"
UPSTREAM_LICENSE_OBSERVED_SHA256 = "0d96a4ff68ad6d4b6f1f30f713b18d5184912ba8dd389f86aa7710db079abcb0"
UPSTREAM_REPOSITORY = "guaner-334/civ-atlas"
UPSTREAM_COMMIT = "176cc9168a69be30d1e09bfd85db84a5f3fb99da"

# This is a deliberate source-only review subset. It is not a product runtime.
# Every path is relative to the repository root and is copied only if it is
# present in the 1446 original generation manifest.
CANDIDATE_FILES = (
    "app/__init__.py",
    "app/disease.py",
    "app/execution.py",
    "app/scenario.py",
    "app/source_attestation.py",
    "app/data/scenario.json",
    "app/data/scenarios/contact-v2-iron-4.json",
    "app/data/scenarios/contact-v2-iron-5.json",
    "app/data/scenarios/contact-v2-iron-6.json",
    "app/data/scenarios/event-wakeup-v1-iron-4.json",
    "app/data/scenarios/event-wakeup-v1-iron-5.json",
    "app/data/scenarios/event-wakeup-v1-iron-6.json",
    "app/data/scenarios/iron-4.json",
    "app/data/scenarios/iron-5.json",
    "app/data/scenarios/iron-6.json",
    "app/data/scenarios/lifecycle-v1-iron-4.json",
    "app/data/scenarios/military-v1-iron-4.json",
    "app/data/scenarios/staged-projects-v1-iron-4.json",
)
CANDIDATE_SET = frozenset(CANDIDATE_FILES)

FORBIDDEN_COMPONENTS = frozenset(
    {
        ".env",
        "credentials",
        "credential",
        "deliverables",
        "delivery",
        "evidence",
        "private",
        "runtime",
        "secret",
        "secrets",
        "spikes",
    }
)
FORBIDDEN_SUFFIXES = frozenset(
    {
        ".db",
        ".sqlite",
        ".sqlite3",
        ".sqlite-shm",
        ".sqlite-wal",
        ".wal",
        ".shm",
        ".key",
        ".pem",
        ".p12",
        ".pfx",
        ".crt",
        ".cer",
        ".der",
        ".bin",
        ".mp3",
        ".mp4",
        ".mov",
        ".wav",
        ".flac",
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".webp",
        ".woff",
        ".woff2",
    }
)
SECRET_PATTERNS = (
    re.compile(rb"-----BEGIN [A-Z ]+PRIVATE KEY-----"),
    re.compile(rb"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(rb"\bgh[pousr]_[A-Za-z0-9_]{20,}\b"),
    re.compile(rb"\bsk-[A-Za-z0-9_-]{20,}\b"),
    re.compile(
        rb"(?i)(?:api[_-]?key|password|secret|access[_-]?token)\s*[:=]\s*['\"][^'\"]{12,}['\"]"
    ),
)

PUBLICATION_BLOCKERS = (
    {
        "code": "PROJECT_LICENSE_AUTHORIZATION_REQUIRED",
        "reason": "The baseline repository has no tracked license grant; maintainer authorization is required before external publication.",
        "evidence": "The tracked baseline tree contains no LICENSE, COPYING, or equivalent license file.",
    },
    {
        "code": "EXTERNAL_GEOGRAPHY_REUSE_UNRESOLVED",
        "reason": "app/data/geography.json records external upstream reuse, but this repository contains no redistribution grant for that material.",
        "evidence": "app/data/SOURCES.json records an upstream commit and hashes, not redistribution rights.",
    },
    {
        "code": "RECORDED_UPSTREAM_LICENSE_HASH_MISMATCH",
        "reason": "The recorded upstream license bytes do not match the public LICENSE bytes at the recorded commit, so the exact source snapshot license is unresolved.",
        "evidence": f"app/data/SOURCES.json records {UPSTREAM_LICENSE_RECORDED_SHA256}; the public {UPSTREAM_REPOSITORY}@{UPSTREAM_COMMIT} LICENSE was observed as {UPSTREAM_LICENSE_OBSERVED_SHA256}.",
    },
    {
        "code": "OPTIONAL_SIWC_DEPENDENCIES_NOT_CLEARED",
        "reason": "The optional SIWC surface declares PyJWT and cryptography, but they are not part of this source-only candidate and were not installed or executed.",
        "evidence": "app/requirements-siwc.txt; validation boundary forbids dependency installation and product execution.",
    },
    {
        "code": "PI_SDK_DEPENDENCIES_NOT_CLEARED",
        "reason": "The Pi worker refers to external Node packages that are not present in this repository, so the runtime surface is excluded.",
        "evidence": "app/pi_worker.mjs refers to @earendil-works/pi-agent-core, @earendil-works/pi-ai, and typebox; node_modules is not in the baseline tree.",
    },
    {
        "code": "TEST_DEPENDENCY_UNDECLARED",
        "reason": "The excluded test surface imports psutil without a corresponding tracked requirement declaration.",
        "evidence": "app/tests/test_activation_application_kill.py imports psutil; app/requirements-siwc.txt does not declare it.",
    },
    {
        "code": "RUNTIME_AND_EVIDENCE_SURFACES_EXCLUDED",
        "reason": "Runtime, database, credential, private-evidence, integration-test, and verifier surfaces are not distribution payloads for this first candidate.",
        "evidence": ".gitignore and the source paths themselves identify these as research/runtime material.",
    },
)


class CandidateError(RuntimeError):
    """Raised for a failed provenance, allowlist, or static candidate check."""


def canonical_bytes(raw: bytes) -> bytes:
    """Return the LF form used by the original generation manifest."""
    if b"\x00" in raw:
        return raw
    return raw.replace(b"\r\n", b"\n")


def sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def sha256_file(path: Path, *, canonical: bool = False) -> str:
    raw = path.read_bytes()
    return sha256_bytes(canonical_bytes(raw) if canonical else raw)


def pinned_bytes(path: Path, pin: dict[str, Any]) -> tuple[bytes, str]:
    """Return bytes matching a baseline pin, preserving its recorded newline form."""
    raw = path.read_bytes()
    candidates = [(raw, "raw")]
    normalized = canonical_bytes(raw)
    if normalized != raw:
        candidates.append((normalized, "LF"))
    reconstructed_crlf = normalized.replace(b"\n", b"\r\n")
    if reconstructed_crlf not in (raw, normalized):
        candidates.append((reconstructed_crlf, "CRLF"))
    for candidate, style in candidates:
        if len(candidate) == pin.get("bytes") and sha256_bytes(candidate) == pin.get("sha256"):
            return candidate, style
    raise CandidateError(f"baseline source drift: {path}")


def json_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=False).encode("utf-8")
        + b"\n"
    )


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(json_bytes(value))


def load_json(path: Path) -> Any:
    if path.is_symlink() or not path.is_file():
        raise CandidateError(f"missing or linked JSON file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def safe_relative(value: str) -> str:
    normalized = value.replace("\\", "/")
    path = PurePosixPath(normalized)
    if path.is_absolute() or not path.parts or ".." in path.parts:
        raise CandidateError(f"unsafe relative path: {value!r}")
    return path.as_posix()


def safe_repo_path(root: Path, relative: str) -> Path:
    relative = safe_relative(relative)
    path = root.joinpath(*PurePosixPath(relative).parts)
    if path.is_symlink():
        raise CandidateError(f"symlink is not accepted: {relative}")
    resolved_root = root.resolve()
    resolved_path = path.resolve()
    if not resolved_path.is_relative_to(resolved_root):
        raise CandidateError(f"path escapes repository: {relative}")
    return path


def license_files(root: Path) -> list[str]:
    names = ("LICENSE", "LICENSE.txt", "LICENSE.md", "COPYING", "COPYING.txt")
    return [name for name in names if (root / name).is_file()]


def load_baseline(root: Path) -> tuple[dict[str, Any], dict[str, Any], str, str]:
    manifest_path = root / BASELINE_MANIFEST.relative_to(ROOT)
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise CandidateError(f"missing baseline manifest: {manifest_path}")
    raw_manifest = manifest_path.read_bytes()
    canonical_manifest = canonical_bytes(raw_manifest)
    manifest_sha_lf = sha256_bytes(canonical_manifest)
    manifest_sha_raw = sha256_bytes(raw_manifest)
    if manifest_sha_lf != EXPECTED_ORIGINAL_MANIFEST_SHA256_LF:
        raise CandidateError(
            "original generation manifest LF hash mismatch: "
            f"{manifest_sha_lf} != {EXPECTED_ORIGINAL_MANIFEST_SHA256_LF}"
        )
    manifest = json.loads(canonical_manifest.decode("utf-8"))
    files = manifest.get("files")
    if manifest.get("schema") != "parent950-product-v1" or not isinstance(files, dict):
        raise CandidateError("unexpected 1446 generation manifest schema")
    if len(files) != EXPECTED_BASELINE_MEMBERS:
        raise CandidateError(
            f"expected {EXPECTED_BASELINE_MEMBERS} baseline members, found {len(files)}"
        )
    for relative, pin in files.items():
        relative = safe_relative(relative)
        source = safe_repo_path(root, relative)
        if not source.is_file():
            raise CandidateError(f"manifest source is missing: {relative}")
        pinned_bytes(source, pin)
    sources_path = root / SOURCES_RECORD.relative_to(ROOT)
    sources = load_json(sources_path)
    return manifest, sources, manifest_sha_lf, manifest_sha_raw


def candidate_import_report(root: Path, candidate_paths: set[str]) -> dict[str, dict[str, Any]]:
    """Resolve imports for the dependency-closed candidate using AST only."""
    stdlib: set[str] = set(getattr(sys, "stdlib_module_names", ()))
    stdlib.update(
        {
            "__future__",
            "_sqlite3",
            "builtins",
            "msvcrt",
            "_winapi",
        }
    )
    report: dict[str, dict[str, Any]] = {}

    def target_for_relative(source: str, level: int, module: str | None) -> str | None:
        parts = list(PurePosixPath(source).parts[:-1])
        if level > len(parts):
            return None
        package = parts[: len(parts) - level + 1]
        if module:
            package.extend(module.split("."))
        if not package or package[0] != "app":
            return None
        return "/".join(package) + ".py"

    for relative in sorted(candidate_paths):
        if not relative.endswith(".py"):
            continue
        source = safe_repo_path(root, relative)
        try:
            tree = ast.parse(canonical_bytes(source.read_bytes()).decode("utf-8"), relative)
        except (SyntaxError, UnicodeDecodeError) as error:
            raise CandidateError(f"candidate parse failure {relative}: {error}") from error
        internal: set[str] = set()
        external: set[str] = set()
        missing_internal: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
                for name in names:
                    root_name = name.split(".", 1)[0]
                    if root_name == "app":
                        target = name.replace(".", "/") + ".py"
                        internal.add(target)
                        if target not in candidate_paths and target != "app/__init__.py":
                            missing_internal.add(target)
                    elif root_name not in stdlib:
                        external.add(root_name)
            elif isinstance(node, ast.ImportFrom):
                if node.level:
                    target = target_for_relative(relative, node.level, node.module)
                    if target is not None:
                        internal.add(target)
                        if target not in candidate_paths and target != "app/__init__.py":
                            missing_internal.add(target)
                elif node.module:
                    root_name = node.module.split(".", 1)[0]
                    if root_name == "app":
                        target = node.module.replace(".", "/") + ".py"
                        internal.add(target)
                        if target not in candidate_paths and target != "app/__init__.py":
                            missing_internal.add(target)
                    elif root_name not in stdlib:
                        external.add(root_name)
        report[relative] = {
            "status": "closed" if not external and not missing_internal else "blocked",
            "internal": sorted(internal),
            "external": sorted(external),
            "missing_internal": sorted(missing_internal),
        }
    blocked = {
        path: details for path, details in report.items() if details["status"] != "closed"
    }
    if blocked:
        raise CandidateError(
            "candidate dependency closure is not closed: " + json.dumps(blocked, sort_keys=True)
        )
    return report


def candidate_dependency_disposition(relative: str) -> dict[str, Any]:
    if relative in CANDIDATE_SET:
        return {
            "status": "closed",
            "required": ["Python standard library", "candidate-internal imports only"],
        }
    if relative == "app/pi_worker.mjs":
        return {
            "status": "blocked_missing_external",
            "required": [
                "Node.js runtime",
                "@earendil-works/pi-agent-core",
                "@earendil-works/pi-ai",
                "typebox",
            ],
        }
    if relative == "app/requirements-siwc.txt":
        return {
            "status": "blocked_not_verified",
            "required": ["PyJWT==2.10.1", "cryptography==46.0.3"],
        }
    if relative.startswith("app/tests/") or relative.startswith("app/verify_"):
        return {
            "status": "excluded_test_or_evidence_surface",
            "required": ["runtime/evidence fixtures or product execution"],
        }
    if relative in {
        "app/actors.py",
        "app/actor_execution.py",
        "app/coordinator_execution.py",
        "app/coordinator_maintenance.py",
        "app/coordinator_scheduler.py",
        "app/adjudication.py",
        "app/failure_projection.py",
        "app/product_cli.py",
        "app/replay.py",
        "app/server.py",
        "app/scripted_dm.py",
    }:
        return {
            "status": "excluded_runtime_closure",
            "required": ["excluded runtime surfaces and/or Pi worker"],
        }
    if relative in {
        "app/credential_adapter.py",
        "app/real_intent.py",
        "app/real_provider.py",
        "app/real_settings.py",
        "app/siwc_oauth.py",
        "app/siwc_responses.py",
        "app/siwc_storage.py",
        "app/siwc_transactions.py",
    }:
        return {
            "status": "excluded_credential_or_network_surface",
            "required": ["credential authorization, network policy, or optional SIWC dependency"],
        }
    if relative == "app/data/geography.json":
        return {
            "status": "blocked_external_reuse",
            "required": ["redistribution rights for recorded upstream material"],
        }
    if relative == "app/data/SOURCES.json":
        return {
            "status": "excluded_provenance_record",
            "required": ["none; retained only as an inventory input"],
        }
    return {
        "status": "excluded_not_whitelisted",
        "required": ["explicit I8 allowlist decision"],
    }


def license_disposition(relative: str, repository_license: list[str]) -> dict[str, Any]:
    if relative == "app/data/geography.json":
        return {
            "status": "unresolved_external_redistribution",
            "publication": "blocked",
            "evidence": [
                "app/data/SOURCES.json records upstream hashes and an upstream commit, not a redistribution grant.",
                {
                    "upstream_repository": UPSTREAM_REPOSITORY,
                    "upstream_commit": UPSTREAM_COMMIT,
                    "recorded_license_sha256": UPSTREAM_LICENSE_RECORDED_SHA256,
                    "observed_public_license_sha256": UPSTREAM_LICENSE_OBSERVED_SHA256,
                    "status": "hash_mismatch",
                },
            ],
        }
    if repository_license:
        return {
            "status": "repository_license_present_but_not_reviewed_by_I8",
            "publication": "maintainer_review_required",
            "evidence": repository_license,
        }
    return {
        "status": "baseline_project_rights_not_evidenced",
        "publication": "blocked_pending_maintainer_authorization",
        "evidence": [
            "No tracked LICENSE, LICENSE.txt, LICENSE.md, COPYING, or COPYING.txt is present at the baseline root."
        ],
    }


def source_disposition(relative: str, sources: dict[str, Any]) -> dict[str, Any]:
    if relative == "app/data/geography.json":
        original = sources.get("original_source", {})
        return {
            "kind": "recorded_external_snapshot",
            "record": "app/data/SOURCES.json",
            "upstream_repository": UPSTREAM_REPOSITORY,
            "upstream_commit": original.get("upstream_commit"),
            "upstream_file_count": len(original.get("files", {})),
            "recorded_license_sha256": UPSTREAM_LICENSE_RECORDED_SHA256,
            "observed_public_license_sha256": UPSTREAM_LICENSE_OBSERVED_SHA256,
            "license_status": "hash_mismatch_unresolved",
        }
    if relative == "app/data/SOURCES.json":
        return {
            "kind": "baseline_provenance_record",
            "record": "app/data/SOURCES.json",
            "scope": "records the excluded geography source and upstream hashes",
        }
    return {
        "kind": "baseline_1446_import",
        "record": "baseline/generation-manifest.1446.original.json",
        "recorded_root": "D:\\Dev\\src\\civilization-theatre\\runtime\\lifecycle-candidate-1446-review-v01",
    }


def distribution_reason(relative: str) -> str:
    if relative in CANDIDATE_SET:
        return "dependency-closed static review allowlist"
    if relative == "app/data/geography.json":
        return "excluded: external reuse has no redistribution-rights evidence"
    if relative == "app/data/SOURCES.json":
        return "excluded: provenance input, not candidate payload"
    if relative.startswith("app/tests/") or relative.startswith("app/verify_"):
        return "excluded: tests/verifiers may create evidence or require runtime fixtures"
    if relative == "app/pi_worker.mjs":
        return "excluded: external Node SDK packages are not present"
    if relative == "app/requirements-siwc.txt" or relative.startswith("app/siwc_"):
        return "excluded: optional SIWC dependency/authorization surface"
    if relative in {
        "app/credential_adapter.py",
        "app/real_intent.py",
        "app/real_provider.py",
        "app/real_settings.py",
    }:
        return "excluded: credential or network surface requires separate authorization"
    return "excluded: outside the explicit I8 dependency-closed allowlist"


def deliverable_dispositions(repository_license: list[str]) -> list[dict[str, Any]]:
    license_record = {
        "status": "issue_8_authored_no_license_asserted",
        "publication": "blocked_pending_maintainer_authorization",
        "evidence": repository_license
        or ["No repository license evidence was present for the baseline."],
    }
    items = (
        ("distribution/README.md", "authored", "stdlib-free documentation"),
        ("distribution/build_candidate.py", "authored", "Python standard library only"),
        ("distribution/source-inventory.json", "generated", "generated JSON; no runtime dependencies"),
        (
            "distribution/distribution-manifest.1446.json",
            "generated",
            "generated JSON; no runtime dependencies",
        ),
        (
            "distribution/candidate-1446/CANDIDATE.md",
            "generated",
            "stdlib-free documentation",
        ),
        ("WORKER-RECEIPT.md", "authored", "stdlib-free delivery receipt"),
    )
    return [
        {
            "path": path,
            "kind": kind,
            "source": "issue_8_worker",
            "license": license_record,
            "dependencies": {"status": "resolved", "required": dep},
            "distribution": "review_artifact_not_product_release",
        }
        for path, kind, dep in items
    ]


def build_inventory(
    root: Path,
    manifest: dict[str, Any],
    sources: dict[str, Any],
    manifest_sha_lf: str,
    manifest_sha_raw: str,
) -> dict[str, Any]:
    repository_license = license_files(root)
    entries = []
    for relative, pin in sorted(manifest["files"].items()):
        relative = safe_relative(relative)
        is_candidate = relative in CANDIDATE_SET
        source_path = safe_repo_path(root, relative)
        worktree_raw = source_path.read_bytes()
        pinned, line_ending_style = pinned_bytes(source_path, pin)
        entries.append(
            {
                "path": relative,
                "baseline": {
                    "bytes": pin["bytes"],
                    "sha256": pin["sha256"],
                    "pinned_line_endings": line_ending_style,
                    "worktree_bytes": len(worktree_raw),
                    "worktree_sha256": sha256_bytes(worktree_raw),
                    "pinned_bytes": len(pinned),
                },
                "source": source_disposition(relative, sources),
                "license": license_disposition(relative, repository_license),
                "dependencies": candidate_dependency_disposition(relative),
                "distribution": {
                    "status": "included_internal_review_only" if is_candidate else "excluded",
                    "reason": distribution_reason(relative),
                    "candidate_path": (
                        "distribution/candidate-1446/" + relative if is_candidate else None
                    ),
                },
            }
        )
    return {
        "schema": "civilization-theatre-source-license-dependency-inventory-v1",
        "issue": 8,
        "generation": 1446,
        "baseline": {
            "commit": "3126c45",
            "manifest": "baseline/generation-manifest.1446.original.json",
            "manifest_sha256_lf": manifest_sha_lf,
            "manifest_sha256_worktree_bytes": manifest_sha_raw,
            "members": len(entries),
            "flags": {
                key: manifest.get(key)
                for key in (
                    "preparation_only",
                    "product_imported",
                    "tests_executed",
                    "review_status_at_assembly",
                    "real_model_verified",
                    "release_accepted",
                )
            },
        },
        "repository_license": {
            "files": repository_license,
            "status": "present_but_not_reviewed_by_I8" if repository_license else "unresolved",
            "publication": "maintainer_authorization_required",
        },
        "candidate": {
            "root": "distribution/candidate-1446",
            "status": "INTERNAL_REVIEW_ONLY",
            "publication": "BLOCKED",
            "allowlist": list(CANDIDATE_FILES),
            "selection": "dependency_closed_static_review_subset",
            "forbidden_path_components": sorted(FORBIDDEN_COMPONENTS),
            "forbidden_suffixes": sorted(FORBIDDEN_SUFFIXES),
            "blockers": list(PUBLICATION_BLOCKERS),
        },
        "deliverables": deliverable_dispositions(repository_license),
        "entries": entries,
    }


def forbidden_path_hits(relative_paths: list[str]) -> list[str]:
    hits = []
    for relative in relative_paths:
        path = PurePosixPath(relative)
        components = {part.casefold() for part in path.parts}
        suffix = path.suffix.casefold()
        lowered = relative.casefold()
        if components & FORBIDDEN_COMPONENTS or suffix in FORBIDDEN_SUFFIXES:
            hits.append(relative)
        elif lowered.endswith("/.env") or "/.env." in lowered or lowered.startswith(".env"):
            hits.append(relative)
    return sorted(set(hits))


def secret_hits(path: Path) -> list[str]:
    raw = path.read_bytes()
    return [str(index) for index, pattern in enumerate(SECRET_PATTERNS) if pattern.search(raw)]


def scan_candidate_files(output: Path) -> tuple[list[str], dict[str, list[str]]]:
    if output.is_symlink() or not output.is_dir():
        raise CandidateError(f"candidate root is missing or linked: {output}")
    relative_paths = []
    secret_matches: dict[str, list[str]] = {}
    for path in output.rglob("*"):
        relative = path.relative_to(output).as_posix()
        if path.is_symlink():
            raise CandidateError(f"candidate contains symlink: {relative}")
        if path.is_file():
            relative_paths.append(relative)
            hits = secret_hits(path)
            if hits:
                secret_matches[relative] = hits
    return sorted(relative_paths), secret_matches


def write_candidate_note(output: Path, inventory_sha256: str) -> None:
    lines = [
        "1446 source candidate — internal review only",
        "",
        "Status: INTERNAL_REVIEW_ONLY",
        "Publication: BLOCKED",
        "",
        "This directory is a dependency-closed static source review subset, not a product runtime, installer, release, or acceptance receipt.",
        "",
        "Source disposition:",
        "- The copied source bytes come from the 1446 baseline manifest and preserve each entry's exact pinned bytes; most text is LF and the six pinned scenario fixtures retain their recorded CRLF bytes.",
        "- app/data/geography.json is intentionally absent because its external reuse has no redistribution-rights evidence.",
        "",
        "License disposition:",
        "- No license grant is asserted by this candidate. The baseline has no tracked license file; maintainer authorization is required before external publication.",
        "",
        "Dependency disposition:",
        "- Included Python files use only the Python standard library and candidate-internal imports.",
        "- Optional SIWC and Pi/Node surfaces are intentionally absent because their dependencies and authorization were not cleared.",
        "",
        f"Source inventory: distribution/source-inventory.json (sha256 {inventory_sha256})",
        "",
        "Unverified by design: product services, real models, paid calls, browser acceptance, device builds, runtime isolation, databases, and release acceptance.",
    ]
    (output / "CANDIDATE.md").write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")


def build_distribution_manifest(
    root: Path,
    output: Path,
    inventory: dict[str, Any],
    inventory_sha256: str,
    import_report: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    files, secret_matches = scan_candidate_files(output)
    path_hits = forbidden_path_hits(files)
    if path_hits:
        raise CandidateError("forbidden candidate paths: " + ", ".join(path_hits))
    if secret_matches:
        raise CandidateError("candidate secret-pattern hits: " + json.dumps(secret_matches))
    expected_source_entries = {
        entry["path"]: entry for entry in inventory["entries"] if entry["path"] in CANDIDATE_SET
    }
    expected_files = set(CANDIDATE_FILES) | {"CANDIDATE.md"}
    if set(files) != expected_files:
        raise CandidateError(
            "candidate file set mismatch: "
            + json.dumps({"actual": files, "expected": sorted(expected_files)})
        )
    manifest_files: dict[str, Any] = {}
    for relative in sorted(files):
        candidate_path = output / relative
        raw = candidate_path.read_bytes()
        if relative == "CANDIDATE.md":
            manifest_files[relative] = {
                "source_path": "distribution/candidate-1446/CANDIDATE.md",
                "source": "issue_8_worker_generated",
                "license": "issue_8_authored_no_license_asserted",
                "dependency_status": "resolved_stdlib_free_documentation",
                "bytes": len(raw),
                "sha256": sha256_bytes(raw),
                "canonical_bytes": len(canonical_bytes(raw)),
                "canonical_sha256": sha256_bytes(canonical_bytes(raw)),
                }
            continue
        entry = expected_source_entries[relative]
        manifest_files[relative] = {
            "source_path": relative,
            "source": entry["source"],
            "license": entry["license"],
            "dependency_status": entry["dependencies"]["status"],
            "bytes": len(raw),
            "sha256": sha256_bytes(raw),
            "canonical_bytes": len(canonical_bytes(raw)),
            "canonical_sha256": sha256_bytes(canonical_bytes(raw)),
            "baseline_bytes": entry["baseline"]["bytes"],
            "baseline_sha256": entry["baseline"]["sha256"],
        }
        if len(raw) != entry["baseline"]["bytes"] or sha256_bytes(raw) != entry["baseline"]["sha256"]:
            raise CandidateError(f"candidate copy changed baseline bytes: {relative}")
    return {
        "schema": "civilization-theatre-distribution-candidate-v1",
        "issue": 8,
        "generation": 1446,
        "candidate_status": "INTERNAL_REVIEW_ONLY",
        "publication_status": "BLOCKED",
        "candidate_root": "distribution/candidate-1446",
        "source_inventory": {
            "path": "distribution/source-inventory.json",
            "sha256": inventory_sha256,
        },
        "baseline": {
            "commit": "3126c45",
            "manifest": "baseline/generation-manifest.1446.original.json",
            "manifest_sha256_lf": inventory["baseline"]["manifest_sha256_lf"],
            "members": inventory["baseline"]["members"],
        },
        "files": manifest_files,
        "excluded_baseline_paths": sorted(
            entry["path"] for entry in inventory["entries"] if entry["path"] not in CANDIDATE_SET
        ),
        "checks": {
            "baseline_manifest_verified": True,
            "allowlist_verified": True,
            "forbidden_paths": path_hits,
            "secret_pattern_hits": secret_matches,
            "dependency_report": import_report,
            "databases_credentials_private_evidence_media": "absent",
        },
        "publication_blockers": list(PUBLICATION_BLOCKERS),
    }


def build(root: Path, *, force: bool) -> dict[str, Any]:
    manifest, sources, manifest_sha_lf, manifest_sha_raw = load_baseline(root)
    import_report = candidate_import_report(root, set(CANDIDATE_FILES))
    inventory = build_inventory(root, manifest, sources, manifest_sha_lf, manifest_sha_raw)
    inventory_path = root / INVENTORY_PATH.relative_to(ROOT)
    distribution_manifest_path = root / DISTRIBUTION_MANIFEST_PATH.relative_to(ROOT)
    output = root / DEFAULT_CANDIDATE_ROOT.relative_to(ROOT)
    if output.exists() or output.is_symlink():
        if not force:
            raise CandidateError(f"candidate output exists; use --force: {output}")
        if output.is_symlink() or not output.is_dir():
            output.unlink()
        else:
            shutil.rmtree(output)
    output.mkdir(parents=True, exist_ok=False)
    write_json(inventory_path, inventory)
    inventory_sha256 = sha256_file(inventory_path)
    for relative in CANDIDATE_FILES:
        source = safe_repo_path(root, relative)
        pin = manifest["files"][relative]
        destination = output.joinpath(*PurePosixPath(relative).parts)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(pinned_bytes(source, pin)[0])
    write_candidate_note(output, inventory_sha256)
    distribution_manifest = build_distribution_manifest(
        root, output, inventory, inventory_sha256, import_report
    )
    write_json(distribution_manifest_path, distribution_manifest)
    # Read back the generated state through the same verifier used by --check.
    result = check(root)
    result["mode"] = "build"
    return result


def check(root: Path) -> dict[str, Any]:
    manifest, sources, manifest_sha_lf, manifest_sha_raw = load_baseline(root)
    inventory_path = root / INVENTORY_PATH.relative_to(ROOT)
    distribution_manifest_path = root / DISTRIBUTION_MANIFEST_PATH.relative_to(ROOT)
    output = root / DEFAULT_CANDIDATE_ROOT.relative_to(ROOT)
    inventory = load_json(inventory_path)
    distribution_manifest = load_json(distribution_manifest_path)
    if inventory.get("schema") != "civilization-theatre-source-license-dependency-inventory-v1":
        raise CandidateError("unexpected source inventory schema")
    if distribution_manifest.get("schema") != "civilization-theatre-distribution-candidate-v1":
        raise CandidateError("unexpected distribution manifest schema")
    if distribution_manifest.get("source_inventory", {}).get("sha256") != sha256_file(inventory_path):
        raise CandidateError("distribution manifest source-inventory hash mismatch")
    if inventory.get("baseline", {}).get("manifest_sha256_lf") != manifest_sha_lf:
        raise CandidateError("inventory baseline manifest hash mismatch")
    if inventory.get("baseline", {}).get("manifest_sha256_worktree_bytes") != manifest_sha_raw:
        raise CandidateError("inventory raw manifest hash mismatch")
    if inventory.get("candidate", {}).get("allowlist") != list(CANDIDATE_FILES):
        raise CandidateError("inventory allowlist mismatch")
    import_report = candidate_import_report(output, set(CANDIDATE_FILES))
    files, secret_matches = scan_candidate_files(output)
    path_hits = forbidden_path_hits(files)
    if path_hits or secret_matches:
        raise CandidateError(
            "candidate exclusion check failed: "
            + json.dumps({"paths": path_hits, "secrets": secret_matches})
        )
    expected = set(CANDIDATE_FILES) | {"CANDIDATE.md"}
    if set(files) != expected:
        raise CandidateError(
            "candidate file set mismatch: "
            + json.dumps({"actual": files, "expected": sorted(expected)})
        )
    manifest_files = distribution_manifest.get("files", {})
    if set(manifest_files) != expected:
        raise CandidateError("distribution manifest file set mismatch")
    for relative in files:
        path = output / relative
        raw = path.read_bytes()
        record = manifest_files[relative]
        exact = len(raw) == record.get("bytes") and sha256_bytes(raw) == record.get("sha256")
        canonical = len(canonical_bytes(raw)) == record.get("canonical_bytes") and sha256_bytes(
            canonical_bytes(raw)
        ) == record.get("canonical_sha256")
        if not exact and not canonical:
            raise CandidateError(f"candidate hash mismatch: {relative}")
    for relative in CANDIDATE_FILES:
        source = safe_repo_path(root, relative)
        expected_entry = next(entry for entry in inventory["entries"] if entry["path"] == relative)
        raw = (output / relative).read_bytes()
        expected_bytes = pinned_bytes(source, manifest["files"][relative])[0]
        if raw != expected_bytes and canonical_bytes(raw) != canonical_bytes(expected_bytes):
            raise CandidateError(f"candidate/source mismatch: {relative}")
        baseline_exact = (
            len(raw) == expected_entry["baseline"]["bytes"]
            and sha256_bytes(raw) == expected_entry["baseline"]["sha256"]
        )
        baseline_canonical = (
            len(canonical_bytes(raw)) == len(canonical_bytes(expected_bytes))
            and sha256_bytes(canonical_bytes(raw))
            == sha256_bytes(canonical_bytes(expected_bytes))
        )
        if not baseline_exact and not baseline_canonical:
            raise CandidateError(f"candidate baseline pin mismatch: {relative}")
    if distribution_manifest.get("checks", {}).get("databases_credentials_private_evidence_media") != "absent":
        raise CandidateError("candidate exclusion receipt is not explicit")
    return {
        "status": "passed",
        "mode": "static_candidate_check",
        "generation": 1446,
        "candidate_root": "distribution/candidate-1446",
        "candidate_file_count": len(files),
        "baseline_member_count": len(manifest["files"]),
        "baseline_manifest_sha256_lf": manifest_sha_lf,
        "source_inventory_sha256": sha256_file(inventory_path),
        "publication_status": distribution_manifest.get("publication_status"),
        "publication_blocker_count": len(distribution_manifest.get("publication_blockers", [])),
        "dependency_files_checked": len(import_report),
        "forbidden_path_hits": path_hits,
        "secret_pattern_hits": secret_matches,
    }


def publication_check(root: Path) -> dict[str, Any]:
    result = check(root)
    result["mode"] = "publication_check"
    result["status"] = "blocked"
    result["blockers"] = list(PUBLICATION_BLOCKERS)
    return result


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="verify existing generated candidate")
    mode.add_argument(
        "--check-publication",
        action="store_true",
        help="run static checks and report the expected publication blockers",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="replace only the exact generated candidate directory during a build",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    try:
        if args.check_publication:
            result = publication_check(ROOT)
            print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
            return 2
        if args.check:
            result = check(ROOT)
        else:
            result = build(ROOT, force=args.force)
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    except (CandidateError, OSError, json.JSONDecodeError) as error:
        print(json.dumps({"status": "failed", "error": str(error)}, ensure_ascii=False, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
