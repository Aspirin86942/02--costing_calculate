from __future__ import annotations

import argparse
import getpass
import hashlib
import json
import math
import os
import re
import shutil
import socket
import stat
import subprocess
import tempfile
import tomllib
import unicodedata
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Literal, TypeAlias

from tests.rust_oracle.benchmark_protocol import (
    AttemptState,
    ClosedBinaryLabel,
    ComparisonProfile,
    HarnessVerdict,
)
from tests.rust_oracle.repo_paths import repo_root

UPSTREAM_URL = 'https://github.com/jmcnamara/rust_xlsxwriter.git'
UPSTREAM_TAG = 'v0.96.0'
UPSTREAM_BASE_REVISION = '9134de25afadaee955d0f821862338e3d046a338'
CRATES_IO_CHECKSUM = 'dd1746025420e17b5d62528b930e550e016e857038794d74e169018126ef3d14'
FORK_URL = 'https://github.com/Aspirin86942/rust_xlsxwriter.git'
FORK_BRANCH = 'costing-fallible-temp-io-v0.96.0'
CRATE_VERSION = '0.96.0'
DEPENDENCY_MANIFEST_RELATIVE_PATH = Path('docs/performance/dependencies/2026-07-11-rust-xlsxwriter-0.96.0.json')
LOCAL_LOG_ROOT_RELATIVE_PATH = Path('rust/target/perf/local-logs')

MANDATORY_DIFF_FILES = (
    'src/packager.rs',
    'src/workbook.rs',
    'src/workbook/tests.rs',
    'src/worksheet.rs',
    'src/worksheet/tests.rs',
)
XMLWRITER_PATH = 'src/xmlwriter.rs'
FALLBACK_TRIGGER_TESTS = frozenset(
    {
        'row_start_write_failure_returns_original_io_error',
        'cell_body_write_failure_returns_original_io_error',
        'row_end_write_failure_returns_original_io_error',
    }
)

_HEX40 = re.compile(r'^[0-9a-f]{40}$')
_HEX64 = re.compile(r'^[0-9a-f]{64}$')
_DRIVE_PATH = re.compile(r'(?i)(?:^|[^a-z0-9])[a-z]:[\\/]')
_UNC_PATH = re.compile(r'(?<!:)(?:\\\\|//)[a-z0-9_.-]+[\\/]', re.IGNORECASE)
_SENSITIVE_TOKEN = re.compile(r'(?i)user|canary|stdout|stderr')
_VERSIONED_SENSITIVE_MARKER = re.compile(r'(?i)(?:expected\s*=|actual\s*=|stdout\s*:|stderr\s*:|canary)')
_USERS_PATH = re.compile(r'(?i)(?:[\\/]users[\\/])')
_COORDINATE = re.compile(r'^[A-Z]+[1-9][0-9]*$')
_DIMENSION = re.compile(r'^[A-Z]+[1-9][0-9]*:[A-Z]+[1-9][0-9]*$')
_EXPECTED_KEYS = (
    'upstream_url',
    'upstream_tag',
    'upstream_base_revision',
    'crates_io_checksum',
    'pre_pin_costing_commit',
    'fork_url',
    'fork_revision',
    'allowed_diff_files',
    'xmlwriter_fallback_used',
    'xmlwriter_fallback_trigger_test',
    'diff_sha256',
    'local_unversioned_log_sha256',
    'upstream_pr_url',
    'verdict',
)


class EvidenceKind(StrEnum):
    BENCHMARK = 'benchmark'
    COMMAND = 'command'
    SMOKE = 'smoke'
    PE_IMPORTS = 'pe-imports'
    FORK_PROVENANCE = 'fork-provenance'
    CARGO_FEATURE_TREE = 'cargo-feature-tree'
    TEXT_REPORT = 'text-report'


class PathAlias(StrEnum):
    REPO_ROOT = '$REPO_ROOT'
    GB_INPUT = '$GB_INPUT'
    SK_INPUT = '$SK_INPUT'
    REFERENCE_EXE = '$REFERENCE_EXE'
    CANDIDATE_EXE = '$CANDIDATE_EXE'
    ROUND_OUTPUT = '$ROUND_OUTPUT'
    FORK_CHECKOUT = '$FORK_CHECKOUT'


class BenchmarkMetric(StrEnum):
    WALL = 'wall'
    PWS = 'pws'
    WALL_MEDIAN = 'wall_median'
    PWS_MEDIAN = 'pws_median'
    WALL_RATIO = 'wall_ratio'
    PWS_RATIO = 'pws_ratio'
    INGEST = 'ingest'
    NORMALIZE = 'normalize'
    SPLIT = 'split'
    FACT = 'fact'
    PRESENTATION = 'presentation'
    TOTAL = 'total'
    EXPORT = 'export'
    WRITER_POPULATE = 'writer_populate'
    XLSX_SAVE = 'xlsx_save'


class RuntimeCount(StrEnum):
    READER_ROWS = 'reader_rows'
    DETAIL_ROWS = 'detail_rows'
    QTY_ROWS = 'qty_rows'
    QTY_SHEET_ROWS = 'qty_sheet_rows'
    QUALITY_METRIC_COUNT = 'quality_metric_count'
    WORK_ORDER_ROWS = 'work_order_rows'
    ERROR_LOG_COUNT = 'error_log_count'


class ApprovedSheet(StrEnum):
    COST_DETAIL = '成本计算单总表'
    QUANTITY = '成本计算单数量聚合维度'
    ANALYSIS = '成本分析工单维度'


class MismatchKind(StrEnum):
    VALUE_MISMATCH = 'value_mismatch'
    STORAGE_TYPE_MISMATCH = 'storage_type_mismatch'
    STYLE_MISMATCH = 'style_mismatch'
    COLUMN_TOTAL_MISMATCH = 'column_total_mismatch'
    GROUP_TOTAL_MISMATCH = 'group_total_mismatch'
    REQUIRED_HEADER_MISSING = 'required_header_missing'
    UNEXPECTED_NUMERIC_HEADER = 'unexpected_numeric_header'
    NUMERIC_STORAGE_INVALID = 'numeric_storage_invalid'
    BLANK_GROUP_KEY = 'blank_group_key'
    DUPLICATE_GROUP_KEY = 'duplicate_group_key'
    SHARED_STRING_INDEX_OUT_OF_RANGE = 'shared_string_index_out_of_range'
    PACKAGE_RELATIONSHIP_MISMATCH = 'package_relationship_mismatch'


class StorageType(StrEnum):
    BLANK = 'blank'
    NUMBER = 'number'
    STRING = 'string'
    BOOLEAN = 'boolean'
    DATE = 'date'
    ERROR = 'error'
    FORMULA = 'formula'


class CommandId(StrEnum):
    CARGO_BUILD_RELEASE = 'cargo-build-release'
    CARGO_TREE_FEATURES = 'cargo-tree-features'
    PHASE0H_SMOKE = 'phase0h-smoke'
    PHASE0A_CAPTURE = 'phase0a-capture'
    PE_DEPENDENTS = 'pe-dependents'
    PE_IMPORTS = 'pe-imports'
    DEPENDENCY_ATTESTATION = 'dependency-attestation'


class CommandToken(StrEnum):
    CARGO = 'cargo'
    GIT = 'git'
    GH = 'gh'
    PYTHON = 'python'
    POWERSHELL = 'powershell'
    DUMPBIN = 'dumpbin'
    LLVM_READOBJ = 'llvm-readobj'
    BUILD = 'build'
    TREE = 'tree'
    RELEASE = '--release'
    LOCKED = '--locked'
    MANIFEST_PATH = '--manifest-path'
    PACKAGE = '-p'
    TARGET = '--target'
    TARGET_DIR = '--target-dir'
    NO_DEFAULT_FEATURES = '--no-default-features'
    FEATURES = '--features'
    EDGE_FEATURES = '-e=features'
    DEPENDENTS = '/DEPENDENTS'
    IMPORTS = '/IMPORTS'
    COFF_IMPORTS = '--coff-imports'


class ToolName(StrEnum):
    CARGO = 'cargo'
    GIT = 'git'
    GH = 'gh'
    PYTHON = 'python'
    POWERSHELL = 'powershell'
    DUMPBIN = 'dumpbin'
    LLVM_READOBJ = 'llvm-readobj'


class DllBasename(StrEnum):
    KERNEL32 = 'KERNEL32.dll'
    ADVAPI32 = 'ADVAPI32.dll'
    BCRYPT = 'bcrypt.dll'
    NTDLL = 'ntdll.dll'
    OLE32 = 'ole32.dll'
    SHELL32 = 'SHELL32.dll'
    USER32 = 'USER32.dll'
    WS2_32 = 'WS2_32.dll'
    VCRUNTIME140 = 'VCRUNTIME140.dll'
    VCRUNTIME140_1 = 'VCRUNTIME140_1.dll'
    MSVCP140 = 'MSVCP140.dll'
    UCRTBASE = 'ucrtbase.dll'
    ZLIB1 = 'zlib1.dll'
    LIBZ = 'libz.dll'
    PROJECT_PRIVATE = 'costing-private.dll'


class ForkUrl(StrEnum):
    OFFICIAL = UPSTREAM_URL
    COSTING_FORK = FORK_URL


class ForkTag(StrEnum):
    V0_96_0 = UPSTREAM_TAG


class ForkDiffPath(StrEnum):
    PACKAGER = 'src/packager.rs'
    WORKBOOK = 'src/workbook.rs'
    WORKBOOK_TESTS = 'src/workbook/tests.rs'
    WORKSHEET = 'src/worksheet.rs'
    WORKSHEET_TESTS = 'src/worksheet/tests.rs'
    XMLWRITER = 'src/xmlwriter.rs'


class CargoPackage(StrEnum):
    COSTING_CALCULATE = 'costing-calculate'
    COSTING_XLSX = 'costing-xlsx'
    RUST_XLSXWRITER = 'rust_xlsxwriter'
    ZLIB_RS = 'zlib-rs'
    ZMIJ = 'zmij'


class CargoFeature(StrEnum):
    LOW_MEMORY = 'low-memory'
    CONSTANT_MEMORY = 'constant_memory'
    ZLIB = 'zlib'
    ZMIJ = 'zmij'


class ReportKind(StrEnum):
    PHASE_GATE = 'phase-gate'
    RELEASE_GATE = 'release-gate'
    DEPENDENCY_GATE = 'dependency-gate'


class ReportTitle(StrEnum):
    PHASE_GATE_RESULT = 'Phase gate result'
    RELEASE_GATE_RESULT = 'Release gate result'
    DEPENDENCY_GATE_RESULT = 'Dependency gate result'


class ReportCheckId(StrEnum):
    CORRECTNESS = 'correctness'
    WALL = 'wall'
    PWS = 'pws'
    OUTPUT_BYTES = 'output-bytes'
    PE_IMPORTS = 'pe-imports'
    SMOKE = 'smoke'
    PROVENANCE = 'provenance'


@dataclass(frozen=True)
class MachineArtifactEvidence:
    windows_build_sha256: str
    architecture: Literal['x86_64']
    cpu_model_sha256: str
    logical_cpu_count: int
    physical_memory_bytes: int
    system_drive_media_type: Literal['SSD', 'HDD', 'UNKNOWN']
    system_drive_size_bytes: int
    fingerprint_sha256: str


@dataclass(frozen=True)
class BenchmarkRoundEvidence:
    metric: BenchmarkMetric
    global_round: int
    order: tuple[Literal['reference', 'candidate'], Literal['reference', 'candidate']]
    reference_value: Decimal
    candidate_value: Decimal


@dataclass(frozen=True)
class BenchmarkMetricEvidence:
    metric: BenchmarkMetric
    value: Decimal


@dataclass(frozen=True)
class RuntimeCountEvidence:
    name: RuntimeCount
    value: int


@dataclass(frozen=True)
class SheetDimensionEvidence:
    sheet: ApprovedSheet
    dimension: str


@dataclass(frozen=True)
class OutputBytesEvidence:
    role: Literal['reference', 'candidate']
    value: int


@dataclass(frozen=True)
class MismatchEvidence:
    sheet: ApprovedSheet
    coordinate: str
    mismatch_kind: MismatchKind
    expected_storage_type: StorageType
    actual_storage_type: StorageType
    local_log_sha256: str


@dataclass(frozen=True)
class BenchmarkManifestEvidence:
    schema_version: Literal[1]
    profile: ComparisonProfile
    pipeline: Literal['gb', 'sk']
    input_alias: PathAlias
    input_sha256: str
    reference_label: ClosedBinaryLabel
    reference_exe_sha256: str
    candidate_label: ClosedBinaryLabel
    candidate_exe_sha256: str
    machine: MachineArtifactEvidence
    attempt_count: int
    prior_safe_verdicts: tuple[HarnessVerdict, ...]
    ledger_head_sha256: str
    first_group_sha256: str
    expanded_group_sha256: str | None
    rounds: tuple[BenchmarkRoundEvidence, ...]
    metrics: tuple[BenchmarkMetricEvidence, ...]
    runtime_counts: tuple[RuntimeCountEvidence, ...]
    sheet_dimensions: tuple[SheetDimensionEvidence, ...]
    output_bytes: tuple[OutputBytesEvidence, ...]
    mismatches: tuple[MismatchEvidence, ...]
    local_log_sha256: tuple[str, ...]
    verdict: HarnessVerdict


@dataclass(frozen=True)
class SanitizedToolVersion:
    major: int
    minor: int
    patch: int


@dataclass(frozen=True)
class CommandTranscriptEvidence:
    command_id: CommandId
    tokens: tuple[CommandToken | PathAlias, ...]
    tool: ToolName
    tool_version: SanitizedToolVersion
    exit_code: int
    local_log_sha256: str
    verdict: HarnessVerdict


@dataclass(frozen=True)
class SmokeSummaryEvidence:
    candidate_exe_sha256: str
    fixture_sha256: str
    pipeline: Literal['gb', 'sk']
    exit_code: int
    approved_sheets: tuple[ApprovedSheet, ...]
    temp_canary_created: bool
    temp_residue_count: int
    missing_dll: bool
    local_log_sha256: str
    verdict: HarnessVerdict


@dataclass(frozen=True)
class PeImportsEvidence:
    candidate_exe_sha256: str
    baseline_exe_sha256: str
    tools: tuple[ToolName, ...]
    normal_imports: tuple[DllBasename, ...]
    delay_imports: tuple[DllBasename, ...]
    local_log_sha256: str
    verdict: HarnessVerdict


@dataclass(frozen=True)
class ForkProvenanceEvidence:
    official_url: ForkUrl
    fork_url: ForkUrl
    tag: ForkTag
    upstream_base_revision: str
    crates_io_checksum: str
    fork_revision: str
    allowed_diff_files: tuple[ForkDiffPath, ...]
    diff_sha256: str
    no_pr: bool
    local_log_sha256: str
    verdict: HarnessVerdict


@dataclass(frozen=True)
class CargoPackageEvidence:
    package: CargoPackage
    revision: str | None


@dataclass(frozen=True)
class CargoFeatureEdge:
    source_package: CargoPackage
    source_feature: CargoFeature
    target_package: CargoPackage
    target_feature: CargoFeature


@dataclass(frozen=True)
class CargoFeatureTreeEvidence:
    candidate_label: ClosedBinaryLabel
    candidate_exe_sha256: str
    fork_revision: str
    packages: tuple[CargoPackageEvidence, ...]
    feature_edges: tuple[CargoFeatureEdge, ...]
    local_log_sha256: str
    verdict: HarnessVerdict


@dataclass(frozen=True)
class ReportCheckEvidence:
    check_id: ReportCheckId
    verdict: HarnessVerdict
    evidence_sha256: str


@dataclass(frozen=True)
class TextReportEvidence:
    report_kind: ReportKind
    title: ReportTitle
    checks: tuple[ReportCheckEvidence, ...]
    overall_verdict: HarnessVerdict


EvidenceSource: TypeAlias = (
    BenchmarkManifestEvidence
    | CommandTranscriptEvidence
    | SmokeSummaryEvidence
    | PeImportsEvidence
    | ForkProvenanceEvidence
    | CargoFeatureTreeEvidence
    | TextReportEvidence
)


@dataclass(frozen=True)
class _SanitizedArtifact:
    kind: EvidenceKind
    file_name: str
    payload: Mapping[str, object]
    content: str
    numeric_values: tuple[int | float, ...]
    source: EvidenceSource


@dataclass(frozen=True)
class _StagedIndexEntry:
    path: Path
    mode: Literal['100644', '100755']
    blob_sha: str
    content: bytes


def _strict_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f'duplicate JSON key: {key}')
        result[key] = value
    return result


def _strict_json_loads(raw: str) -> object:
    try:
        return json.loads(raw, object_pairs_hook=_strict_json_object)
    except json.JSONDecodeError as exc:
        raise ValueError('invalid JSON evidence') from exc


@dataclass(frozen=True)
class DependencyEvidence:
    upstream_url: Literal['https://github.com/jmcnamara/rust_xlsxwriter.git']
    upstream_tag: Literal['v0.96.0']
    upstream_base_revision: str
    crates_io_checksum: str
    pre_pin_costing_commit: str
    fork_url: Literal['https://github.com/Aspirin86942/rust_xlsxwriter.git']
    fork_revision: str
    allowed_diff_files: tuple[str, ...]
    xmlwriter_fallback_used: bool
    xmlwriter_fallback_trigger_test: str | None
    diff_sha256: str
    local_unversioned_log_sha256: str
    upstream_pr_url: None
    verdict: Literal['VALIDATED']


class EvidenceSanitizer:
    @classmethod
    def closed_policy(cls) -> EvidenceSanitizer:
        return cls()

    def build_benchmark_manifest(self, value: BenchmarkManifestEvidence) -> _SanitizedArtifact:
        if not isinstance(value, BenchmarkManifestEvidence) or value.schema_version != 1:
            raise ValueError('benchmark evidence must use schema version 1')
        profile = _enum_text(value.profile, ComparisonProfile, 'profile')
        pipeline = _pipeline(value.pipeline)
        input_alias = _enum_text(value.input_alias, PathAlias, 'input alias')
        reference_label = _enum_text(value.reference_label, ClosedBinaryLabel, 'reference label')
        candidate_label = _enum_text(value.candidate_label, ClosedBinaryLabel, 'candidate label')
        input_sha = _require_hash(value.input_sha256, 64, 'input_sha256')
        reference_sha = _require_hash(value.reference_exe_sha256, 64, 'reference_exe_sha256')
        candidate_sha = _require_hash(value.candidate_exe_sha256, 64, 'candidate_exe_sha256')
        machine = _machine_payload(value.machine)
        attempt_count = _nonnegative_int(value.attempt_count, 'attempt_count', positive=True)
        safe_verdicts = tuple(
            _enum_text(item, HarnessVerdict, 'prior safe verdict') for item in value.prior_safe_verdicts
        )
        allowed_prior_verdicts = {
            HarnessVerdict.ENVIRONMENT_DRIFT.value,
            HarnessVerdict.REFERENCE_FAILED.value,
        }
        if any(item not in allowed_prior_verdicts for item in safe_verdicts):
            raise ValueError('prior verdicts are closed to safe retry reasons')
        ledger_head = _require_hash(value.ledger_head_sha256, 64, 'ledger_head_sha256')
        first_group = _require_hash(value.first_group_sha256, 64, 'first_group_sha256')
        expanded_group = (
            None
            if value.expanded_group_sha256 is None
            else _require_hash(value.expanded_group_sha256, 64, 'expanded_group_sha256')
        )

        rounds: list[dict[str, object]] = []
        for item in value.rounds:
            if not isinstance(item, BenchmarkRoundEvidence):
                raise ValueError('benchmark rounds must use BenchmarkRoundEvidence')
            metric = _enum_text(item.metric, BenchmarkMetric, 'round metric')
            global_round = _nonnegative_int(item.global_round, 'global_round', positive=True)
            if global_round > 10:
                raise ValueError('global_round must be between 1 and 10')
            expected_order = ('reference', 'candidate') if global_round % 2 else ('candidate', 'reference')
            if item.order != expected_order:
                raise ValueError('round order must follow global AB/BA order')
            reference_value = _finite_decimal(item.reference_value, 'reference_value', positive=True)
            candidate_value = _finite_decimal(item.candidate_value, 'candidate_value', positive=True)
            rounds.append(
                {
                    'metric': metric,
                    'global_round': global_round,
                    'order': list(item.order),
                    'reference_value': reference_value,
                    'candidate_value': candidate_value,
                }
            )

        metrics: list[dict[str, object]] = []
        for item in value.metrics:
            if not isinstance(item, BenchmarkMetricEvidence):
                raise ValueError('benchmark metrics must use BenchmarkMetricEvidence')
            metrics.append(
                {
                    'metric': _enum_text(item.metric, BenchmarkMetric, 'benchmark metric'),
                    'value': _finite_decimal(item.value, 'benchmark metric value'),
                }
            )
        _reject_duplicate_keys(tuple(item['metric'] for item in metrics), 'benchmark metric')

        runtime_counts: list[dict[str, object]] = []
        for item in value.runtime_counts:
            if not isinstance(item, RuntimeCountEvidence):
                raise ValueError('runtime counts must use RuntimeCountEvidence')
            runtime_counts.append(
                {
                    'name': _enum_text(item.name, RuntimeCount, 'runtime count'),
                    'value': _nonnegative_int(item.value, 'runtime count value'),
                }
            )
        _reject_duplicate_keys(tuple(item['name'] for item in runtime_counts), 'runtime count')

        dimensions: list[dict[str, object]] = []
        for item in value.sheet_dimensions:
            if not isinstance(item, SheetDimensionEvidence):
                raise ValueError('sheet dimensions must use SheetDimensionEvidence')
            if not isinstance(item.dimension, str) or _DIMENSION.fullmatch(item.dimension) is None:
                raise ValueError('sheet dimension must be a strict A1 range')
            dimensions.append(
                {
                    'sheet': _enum_text(item.sheet, ApprovedSheet, 'sheet'),
                    'dimension': item.dimension,
                }
            )
        _reject_duplicate_keys(tuple(item['sheet'] for item in dimensions), 'sheet dimension')

        output_bytes: list[dict[str, object]] = []
        for item in value.output_bytes:
            if not isinstance(item, OutputBytesEvidence) or item.role not in ('reference', 'candidate'):
                raise ValueError('output bytes must use a closed binary role')
            output_bytes.append(
                {
                    'role': item.role,
                    'value': _nonnegative_int(item.value, 'output bytes', positive=True),
                }
            )
        _reject_duplicate_keys(tuple(item['role'] for item in output_bytes), 'output bytes role')

        mismatches = [_mismatch_payload(item) for item in value.mismatches]
        local_log_sha = [_require_hash(item, 64, 'local log SHA') for item in value.local_log_sha256]
        verdict = _enum_text(value.verdict, HarnessVerdict, 'benchmark verdict')
        payload: dict[str, object] = {
            'schema_version': 1,
            'profile': profile,
            'pipeline': pipeline,
            'input_alias': input_alias,
            'input_sha256': input_sha,
            'reference_label': reference_label,
            'reference_exe_sha256': reference_sha,
            'candidate_label': candidate_label,
            'candidate_exe_sha256': candidate_sha,
            'machine': machine,
            'attempt_count': attempt_count,
            'prior_safe_verdicts': list(safe_verdicts),
            'ledger_head_sha256': ledger_head,
            'first_group_sha256': first_group,
            'expanded_group_sha256': expanded_group,
            'rounds': rounds,
            'metrics': metrics,
            'runtime_counts': runtime_counts,
            'sheet_dimensions': dimensions,
            'output_bytes': output_bytes,
            'mismatches': mismatches,
            'local_log_sha256': local_log_sha,
            'verdict': verdict,
        }
        return _json_artifact(
            EvidenceKind.BENCHMARK,
            f'benchmark-{_sha256_text(f"{profile}|{pipeline}|{candidate_sha}")[:16]}.json',
            payload,
            value,
        )

    def build_command_transcript(self, value: CommandTranscriptEvidence) -> _SanitizedArtifact:
        if not isinstance(value, CommandTranscriptEvidence):
            raise ValueError('command evidence must use CommandTranscriptEvidence')
        command_id = _enum_text(value.command_id, CommandId, 'command ID')
        tokens = [_closed_command_token(item) for item in value.tokens]
        tool = _enum_text(value.tool, ToolName, 'tool')
        version = _tool_version_payload(value.tool_version)
        payload: dict[str, object] = {
            'command_id': command_id,
            'tokens': tokens,
            'tool': tool,
            'tool_version': version,
            'exit_code': _integer(value.exit_code, 'exit_code'),
            'local_log_sha256': _require_hash(value.local_log_sha256, 64, 'local_log_sha256'),
            'verdict': _enum_text(value.verdict, HarnessVerdict, 'command verdict'),
        }
        return _json_artifact(EvidenceKind.COMMAND, f'command-{command_id}.json', payload, value)

    def build_smoke(self, value: SmokeSummaryEvidence) -> _SanitizedArtifact:
        if not isinstance(value, SmokeSummaryEvidence):
            raise ValueError('smoke evidence must use SmokeSummaryEvidence')
        sheets = [_enum_text(item, ApprovedSheet, 'approved sheet') for item in value.approved_sheets]
        if tuple(sheets) != tuple(item.value for item in ApprovedSheet):
            raise ValueError('smoke evidence must contain the exact approved Sheet tuple')
        candidate_sha = _require_hash(value.candidate_exe_sha256, 64, 'candidate_exe_sha256')
        pipeline = _pipeline(value.pipeline)
        payload: dict[str, object] = {
            'candidate_exe_sha256': candidate_sha,
            'fixture_sha256': _require_hash(value.fixture_sha256, 64, 'fixture_sha256'),
            'pipeline': pipeline,
            'exit_code': _integer(value.exit_code, 'exit_code'),
            'approved_sheets': sheets,
            'temp_canary_created': _boolean(value.temp_canary_created, 'temp_canary_created'),
            'temp_residue_count': _nonnegative_int(value.temp_residue_count, 'temp_residue_count'),
            'missing_dll': _boolean(value.missing_dll, 'missing_dll'),
            'local_log_sha256': _require_hash(value.local_log_sha256, 64, 'local_log_sha256'),
            'verdict': _enum_text(value.verdict, HarnessVerdict, 'smoke verdict'),
        }
        return _json_artifact(EvidenceKind.SMOKE, f'smoke-{pipeline}-{candidate_sha[:12]}.json', payload, value)

    def build_pe_imports(self, value: PeImportsEvidence) -> _SanitizedArtifact:
        if not isinstance(value, PeImportsEvidence):
            raise ValueError('PE evidence must use PeImportsEvidence')
        candidate_sha = _require_hash(value.candidate_exe_sha256, 64, 'candidate_exe_sha256')
        tools = [_enum_text(item, ToolName, 'PE tool') for item in value.tools]
        normal = [_enum_text(item, DllBasename, 'normal import basename') for item in value.normal_imports]
        delayed = [_enum_text(item, DllBasename, 'delay import basename') for item in value.delay_imports]
        _reject_duplicate_keys(tuple(tools), 'PE tool')
        _reject_duplicate_keys(tuple(normal), 'normal import')
        _reject_duplicate_keys(tuple(delayed), 'delay import')
        payload: dict[str, object] = {
            'candidate_exe_sha256': candidate_sha,
            'baseline_exe_sha256': _require_hash(value.baseline_exe_sha256, 64, 'baseline_exe_sha256'),
            'tools': tools,
            'normal_imports': normal,
            'delay_imports': delayed,
            'local_log_sha256': _require_hash(value.local_log_sha256, 64, 'local_log_sha256'),
            'verdict': _enum_text(value.verdict, HarnessVerdict, 'PE verdict'),
        }
        return _json_artifact(EvidenceKind.PE_IMPORTS, f'pe-imports-{candidate_sha[:12]}.json', payload, value)

    def build_fork_provenance(self, value: ForkProvenanceEvidence) -> _SanitizedArtifact:
        if not isinstance(value, ForkProvenanceEvidence):
            raise ValueError('fork evidence must use ForkProvenanceEvidence')
        if value.official_url is not ForkUrl.OFFICIAL or value.fork_url is not ForkUrl.COSTING_FORK:
            raise ValueError('fork evidence uses an unapproved URL')
        if value.tag is not ForkTag.V0_96_0:
            raise ValueError('fork evidence uses an unapproved tag')
        paths = tuple(_enum_text(item, ForkDiffPath, 'fork diff path') for item in value.allowed_diff_files)
        mandatory = tuple(item.value for item in ForkDiffPath if item is not ForkDiffPath.XMLWRITER)
        if paths not in (mandatory, (*mandatory, ForkDiffPath.XMLWRITER.value)):
            raise ValueError('fork diff paths do not match the exact allowlist')
        revision = _require_hash(value.fork_revision, 40, 'fork_revision')
        payload: dict[str, object] = {
            'official_url': value.official_url.value,
            'fork_url': value.fork_url.value,
            'tag': value.tag.value,
            'upstream_base_revision': _require_hash(value.upstream_base_revision, 40, 'upstream_base_revision'),
            'crates_io_checksum': _require_hash(value.crates_io_checksum, 64, 'crates_io_checksum'),
            'fork_revision': revision,
            'allowed_diff_files': list(paths),
            'diff_sha256': _require_hash(value.diff_sha256, 64, 'diff_sha256'),
            'no_pr': _boolean(value.no_pr, 'no_pr'),
            'local_log_sha256': _require_hash(value.local_log_sha256, 64, 'local_log_sha256'),
            'verdict': _enum_text(value.verdict, HarnessVerdict, 'fork verdict'),
        }
        if payload['no_pr'] is not True:
            raise ValueError('fork evidence requires an empty upstream PR result')
        return _json_artifact(
            EvidenceKind.FORK_PROVENANCE,
            f'fork-provenance-{revision[:12]}.json',
            payload,
            value,
        )

    def build_cargo_feature_tree(self, value: CargoFeatureTreeEvidence) -> _SanitizedArtifact:
        if not isinstance(value, CargoFeatureTreeEvidence):
            raise ValueError('Cargo evidence must use CargoFeatureTreeEvidence')
        label = _enum_text(value.candidate_label, ClosedBinaryLabel, 'candidate label')
        candidate_sha = _require_hash(value.candidate_exe_sha256, 64, 'candidate_exe_sha256')
        fork_revision = _require_hash(value.fork_revision, 40, 'fork_revision')
        packages: list[dict[str, object]] = []
        for item in value.packages:
            if not isinstance(item, CargoPackageEvidence):
                raise ValueError('Cargo packages must use CargoPackageEvidence')
            package = _enum_text(item.package, CargoPackage, 'Cargo package')
            revision = None if item.revision is None else _require_hash(item.revision, 40, 'package revision')
            if item.package is CargoPackage.RUST_XLSXWRITER and revision != fork_revision:
                raise ValueError('rust_xlsxwriter package revision must equal the fork revision')
            packages.append({'package': package, 'revision': revision})
        _reject_duplicate_keys(tuple(item['package'] for item in packages), 'Cargo package')
        edges: list[dict[str, str]] = []
        for item in value.feature_edges:
            if not isinstance(item, CargoFeatureEdge):
                raise ValueError('Cargo feature edges must use CargoFeatureEdge')
            edges.append(
                {
                    'source_package': _enum_text(item.source_package, CargoPackage, 'source package'),
                    'source_feature': _enum_text(item.source_feature, CargoFeature, 'source feature'),
                    'target_package': _enum_text(item.target_package, CargoPackage, 'target package'),
                    'target_feature': _enum_text(item.target_feature, CargoFeature, 'target feature'),
                }
            )
        payload: dict[str, object] = {
            'candidate_label': label,
            'candidate_exe_sha256': candidate_sha,
            'fork_revision': fork_revision,
            'packages': packages,
            'feature_edges': edges,
            'local_log_sha256': _require_hash(value.local_log_sha256, 64, 'local_log_sha256'),
            'verdict': _enum_text(value.verdict, HarnessVerdict, 'Cargo feature verdict'),
        }
        return _json_artifact(
            EvidenceKind.CARGO_FEATURE_TREE,
            f'cargo-feature-tree-{label}-{candidate_sha[:12]}.json',
            payload,
            value,
        )

    def build_text_report(self, value: TextReportEvidence) -> _SanitizedArtifact:
        if not isinstance(value, TextReportEvidence):
            raise ValueError('text report must use TextReportEvidence')
        report_kind = _enum_text(value.report_kind, ReportKind, 'report kind')
        title = _enum_text(value.title, ReportTitle, 'report title')
        checks: list[dict[str, str]] = []
        for item in value.checks:
            if not isinstance(item, ReportCheckEvidence):
                raise ValueError('report checks must use ReportCheckEvidence')
            checks.append(
                {
                    'check_id': _enum_text(item.check_id, ReportCheckId, 'report check ID'),
                    'verdict': _enum_text(item.verdict, HarnessVerdict, 'report check verdict'),
                    'evidence_sha256': _require_hash(item.evidence_sha256, 64, 'report evidence SHA'),
                }
            )
        _reject_duplicate_keys(tuple(item['check_id'] for item in checks), 'report check')
        overall = _enum_text(value.overall_verdict, HarnessVerdict, 'overall report verdict')
        payload: dict[str, object] = {
            'report_kind': report_kind,
            'title': title,
            'checks': checks,
            'overall_verdict': overall,
        }
        lines = [f'# {title}', '', f'Overall: {overall}', '']
        lines.extend(f'- {item["check_id"]}: {item["verdict"]} ({item["evidence_sha256"]})' for item in checks)
        return _SanitizedArtifact(
            kind=EvidenceKind.TEXT_REPORT,
            file_name=f'report-{report_kind}.md',
            payload=_deep_freeze(payload),
            content='\n'.join(lines) + '\n',
            numeric_values=(),
            source=value,
        )

    def scan_tree(self, root: Path, *, sensitive_names: tuple[str, ...] = ()) -> None:
        _scan_tree_safely(root, sensitive_names=sensitive_names)

    def scan_staged(self, *, sensitive_names: tuple[str, ...] = ()) -> None:
        entries = _staged_index_entries(repo_root().resolve(strict=True))
        for entry in entries:
            _scan_versioned_bytes(entry.content, suffix=entry.path.suffix, sensitive_names=sensitive_names)
            _scan_versioned_text(entry.path.name, sensitive_names=sensitive_names)
        _validate_staged_batch_markers(entries)

    def validate_local_destination(self, path: Path, *, ignored_roots: tuple[Path, ...]) -> Path:
        if not ignored_roots:
            raise ValueError('at least one exact ignored root is required')
        _reject_parent_traversal(path, 'local destination')
        raw = _strip_windows_extended_prefix(path).expanduser().absolute()
        _reject_reparse_components(raw)
        canonical = raw.resolve(strict=False)
        roots = tuple(
            _strip_windows_extended_prefix(item).expanduser().absolute().resolve(strict=False) for item in ignored_roots
        )
        if not any(_casefold_relative_to(canonical, root) for root in roots):
            raise ValueError('local destination must stay below an exact ignored root')
        return canonical

    def validate_distinct_paths(
        self,
        *,
        input_path: Path,
        output_path: Path,
        raw_log_path: Path,
        evidence_path: Path,
    ) -> None:
        keys = tuple(_normalized_path_key(path) for path in (input_path, output_path, raw_log_path, evidence_path))
        if len(set(keys)) != len(keys):
            raise ValueError('input, output, raw log and evidence path collision')

    def write_batch(
        self,
        *,
        destination_root: Path,
        artifacts: tuple[_SanitizedArtifact, ...],
        cleanup_state: AttemptState,
        scan_staged: bool = True,
        sensitive_names: tuple[str, ...] = (),
    ) -> None:
        if cleanup_state is not AttemptState.CLEANUP_COMPLETE:
            raise ValueError('cleanup must be complete before versioned evidence is written')
        if not artifacts or any(not isinstance(item, _SanitizedArtifact) for item in artifacts):
            raise ValueError('write_batch accepts typed sanitized artifacts only')
        destination_root = destination_root.resolve(strict=False)
        destination_root.mkdir(parents=True, exist_ok=True)
        _reject_reparse_components(destination_root)
        # 短 basename 避免 Windows 深层 pytest/CI 根目录把 staging 子文件推过传统 MAX_PATH。
        staging = destination_root / f'.s-{uuid.uuid4().hex[:12]}'
        staging.mkdir()
        marker_name, marker_content = _batch_commit_marker(artifacts)
        moved: list[tuple[Path, _FileIdentity]] = []
        try:
            for artifact in artifacts:
                if Path(artifact.file_name).name != artifact.file_name or artifact.file_name in {'.', '..'}:
                    raise ValueError('sanitized artifact filename must be one closed basename')
                target = staging / artifact.file_name
                with _evidence_io_path(target).open('x', encoding='utf-8', newline='\n') as stream:
                    stream.write(artifact.content)
                    stream.flush()
                    os.fsync(stream.fileno())
            marker_source = staging / marker_name
            with _evidence_io_path(marker_source).open('x', encoding='utf-8', newline='\n') as stream:
                stream.write(marker_content)
                stream.flush()
                os.fsync(stream.fileno())
            self.scan_tree(staging, sensitive_names=sensitive_names)
            for artifact in artifacts:
                rebuilt = self._rebuild_artifact(artifact)
                if artifact != rebuilt:
                    raise ValueError('sanitized artifact was tampered after its typed source builder')
            self.scan_tree(_performance_tree_root(destination_root), sensitive_names=sensitive_names)
            if scan_staged:
                self.scan_staged(sensitive_names=sensitive_names)
            for artifact in artifacts:
                source = staging / artifact.file_name
                final = destination_root / artifact.file_name
                if _evidence_io_path(final).exists():
                    raise FileExistsError(final)
                source_identity = _file_identity(_evidence_io_path(source))
                os.link(_evidence_io_path(source), _evidence_io_path(final))
                moved.append((final, source_identity))
                if _file_identity(_evidence_io_path(final)) != source_identity:
                    raise OSError('published artifact identity changed before batch commit')
            marker_final = destination_root / marker_name
            if _evidence_io_path(marker_final).exists():
                raise FileExistsError(marker_final)
            marker_identity = _file_identity(_evidence_io_path(marker_source))
            os.link(_evidence_io_path(marker_source), _evidence_io_path(marker_final))
            moved.append((marker_final, marker_identity))
            if _file_identity(_evidence_io_path(marker_final)) != marker_identity:
                raise OSError('batch commit marker identity changed during publication')
            self.scan_tree(_performance_tree_root(destination_root), sensitive_names=sensitive_names)
        except BaseException:
            for path, identity in reversed(moved):
                _unlink_owned_path(_evidence_io_path(path), identity)
            raise
        finally:
            shutil.rmtree(_evidence_io_path(staging), ignore_errors=True)

    def _rebuild_artifact(self, artifact: _SanitizedArtifact) -> _SanitizedArtifact:
        source = artifact.source
        if artifact.kind is EvidenceKind.BENCHMARK and isinstance(source, BenchmarkManifestEvidence):
            return self.build_benchmark_manifest(source)
        if artifact.kind is EvidenceKind.COMMAND and isinstance(source, CommandTranscriptEvidence):
            return self.build_command_transcript(source)
        if artifact.kind is EvidenceKind.SMOKE and isinstance(source, SmokeSummaryEvidence):
            return self.build_smoke(source)
        if artifact.kind is EvidenceKind.PE_IMPORTS and isinstance(source, PeImportsEvidence):
            return self.build_pe_imports(source)
        if artifact.kind is EvidenceKind.FORK_PROVENANCE and isinstance(source, ForkProvenanceEvidence):
            return self.build_fork_provenance(source)
        if artifact.kind is EvidenceKind.CARGO_FEATURE_TREE and isinstance(source, CargoFeatureTreeEvidence):
            return self.build_cargo_feature_tree(source)
        if artifact.kind is EvidenceKind.TEXT_REPORT and isinstance(source, TextReportEvidence):
            return self.build_text_report(source)
        raise ValueError('artifact kind does not match its immutable typed source')

    @staticmethod
    def write_dependency_manifest(output: Path, value: DependencyEvidence) -> None:
        payload = EvidenceSanitizer.dependency_payload(value)
        output.parent.mkdir(parents=True, exist_ok=True)
        temp_path, temp_identity = _write_manifest_temp(output, payload)
        try:
            # 同目录 hard link 是同卷、不会覆盖目标的原子发布；loser 只能清理自己的 unique temp。
            os.link(temp_path, output)
        finally:
            _unlink_owned_path(temp_path, temp_identity)

    @staticmethod
    def dependency_payload(value: DependencyEvidence) -> dict[str, object]:
        EvidenceSanitizer._validate_dependency_evidence(value)
        # 安全边界必须显式列出允许落盘的字段，禁止 asdict 把未来字段意外泄露进 manifest。
        return {
            'upstream_url': value.upstream_url,
            'upstream_tag': value.upstream_tag,
            'upstream_base_revision': value.upstream_base_revision,
            'crates_io_checksum': value.crates_io_checksum,
            'pre_pin_costing_commit': value.pre_pin_costing_commit,
            'fork_url': value.fork_url,
            'fork_revision': value.fork_revision,
            'allowed_diff_files': list(value.allowed_diff_files),
            'xmlwriter_fallback_used': value.xmlwriter_fallback_used,
            'xmlwriter_fallback_trigger_test': value.xmlwriter_fallback_trigger_test,
            'diff_sha256': value.diff_sha256,
            'local_unversioned_log_sha256': value.local_unversioned_log_sha256,
            'upstream_pr_url': None,
            'verdict': value.verdict,
        }

    @staticmethod
    def read_dependency_manifest(path: Path) -> DependencyEvidence:
        payload = _strict_json_loads(path.read_text(encoding='utf-8'))
        if not isinstance(payload, dict) or tuple(payload) != _EXPECTED_KEYS:
            raise ValueError('dependency manifest must use the exact closed schema')
        allowed_diff_files = payload['allowed_diff_files']
        if not isinstance(allowed_diff_files, list) or not all(isinstance(item, str) for item in allowed_diff_files):
            raise ValueError('allowed_diff_files must be a string list')
        value = DependencyEvidence(
            upstream_url=payload['upstream_url'],
            upstream_tag=payload['upstream_tag'],
            upstream_base_revision=payload['upstream_base_revision'],
            crates_io_checksum=payload['crates_io_checksum'],
            pre_pin_costing_commit=payload['pre_pin_costing_commit'],
            fork_url=payload['fork_url'],
            fork_revision=payload['fork_revision'],
            allowed_diff_files=tuple(allowed_diff_files),
            xmlwriter_fallback_used=payload['xmlwriter_fallback_used'],
            xmlwriter_fallback_trigger_test=payload['xmlwriter_fallback_trigger_test'],
            diff_sha256=payload['diff_sha256'],
            local_unversioned_log_sha256=payload['local_unversioned_log_sha256'],
            upstream_pr_url=payload['upstream_pr_url'],
            verdict=payload['verdict'],
        )
        EvidenceSanitizer._validate_dependency_evidence(value)
        return value

    @staticmethod
    def verify_rust_xlsxwriter_revision_consistency(
        *,
        fork_head: str,
        cargo_manifest: Path,
        cargo_metadata: Path,
        cargo_lock: Path,
        dependency_manifest: Path,
    ) -> str:
        return EvidenceSanitizer._verify_revision_contents(
            fork_head=fork_head,
            cargo_manifest_text=cargo_manifest.read_text(encoding='utf-8'),
            cargo_metadata_text=cargo_metadata.read_text(encoding='utf-8'),
            cargo_lock_text=cargo_lock.read_text(encoding='utf-8'),
            dependency_manifest_text=dependency_manifest.read_text(encoding='utf-8'),
        )

    @staticmethod
    def verify_registry_checksum(pre_pin_lock_text: str, archives: tuple[Path, ...]) -> str:
        checksum = _registry_checksum_from_lock(pre_pin_lock_text)
        if checksum != CRATES_IO_CHECKSUM:
            raise ValueError('registry lock checksum does not match the approved crates.io checksum')
        if not archives:
            raise ValueError('no cached rust_xlsxwriter-0.96.0.crate archive found')
        for archive in archives:
            if _sha256_file(archive) != checksum:
                raise ValueError('cached archive checksum does not match the approved crates.io checksum')
        return checksum

    @staticmethod
    def verify_empty_pr_query(raw_json: str) -> None:
        payload = _strict_json_loads(raw_json)
        if not isinstance(payload, list) or payload:
            raise ValueError('an upstream PR exists or the upstream PR query was not an empty list')

    @staticmethod
    def verify_fork_diff_statuses(raw: str) -> tuple[str, ...]:
        paths: list[str] = []
        for row in raw.splitlines():
            fields = row.split('\t')
            status = fields[0] if fields else ''
            if status != 'M':
                raise ValueError('every approved fork diff path must have modified status M')
            if len(fields) != 2 or not fields[1]:
                raise ValueError('fork diff name-status row must contain exactly one path')
            paths.append(fields[1])
        result = tuple(paths)
        expected = (*MANDATORY_DIFF_FILES, XMLWRITER_PATH) if XMLWRITER_PATH in result else MANDATORY_DIFF_FILES
        if result != expected:
            raise ValueError(f'fork diff does not match the closed allowlist: {result!r}')
        return result

    @staticmethod
    def _verify_revision_contents(
        *,
        fork_head: str,
        cargo_manifest_text: str,
        cargo_metadata_text: str,
        cargo_lock_text: str,
        dependency_manifest_text: str,
    ) -> str:
        revisions = {
            'fork HEAD': fork_head.strip(),
            'workspace manifest': _revision_from_cargo_manifest(cargo_manifest_text),
            'Cargo metadata': _revision_from_cargo_metadata(cargo_metadata_text),
            'Cargo.lock': _revision_from_cargo_lock(cargo_lock_text),
            'dependency manifest': _revision_from_dependency_manifest(dependency_manifest_text),
        }
        for name, revision in revisions.items():
            _require_hash(revision, 40, name)
        if len(set(revisions.values())) != 1:
            raise ValueError(f'rust_xlsxwriter revision mismatch: {revisions!r}')
        return fork_head.strip()

    @staticmethod
    def _validate_dependency_evidence(value: DependencyEvidence) -> None:
        if value.upstream_url != UPSTREAM_URL:
            raise ValueError('unexpected upstream URL')
        if value.upstream_tag != UPSTREAM_TAG:
            raise ValueError('unexpected upstream tag')
        if value.upstream_base_revision != UPSTREAM_BASE_REVISION:
            raise ValueError('unexpected upstream base revision')
        if value.crates_io_checksum != CRATES_IO_CHECKSUM:
            raise ValueError('unexpected crates.io checksum')
        if value.fork_url != FORK_URL:
            raise ValueError('unexpected fork URL')
        if value.verdict != 'VALIDATED':
            raise ValueError('dependency verdict must be VALIDATED')
        if value.upstream_pr_url is not None:
            raise ValueError('upstream_pr_url must be null')
        if type(value.xmlwriter_fallback_used) is not bool:
            raise ValueError('xmlwriter_fallback_used must be a boolean')

        _require_hash(value.upstream_base_revision, 40, 'upstream_base_revision')
        _require_hash(value.crates_io_checksum, 64, 'crates_io_checksum')
        _require_hash(value.pre_pin_costing_commit, 40, 'pre_pin_costing_commit')
        _require_hash(value.fork_revision, 40, 'fork_revision')
        _require_hash(value.diff_sha256, 64, 'diff_sha256')
        _require_hash(value.local_unversioned_log_sha256, 64, 'local_unversioned_log_sha256')

        for item in _all_manifest_strings(value):
            _reject_sensitive_string(item)

        expected_files = MANDATORY_DIFF_FILES
        if value.xmlwriter_fallback_used:
            expected_files = (*MANDATORY_DIFF_FILES, XMLWRITER_PATH)
            if value.xmlwriter_fallback_trigger_test not in FALLBACK_TRIGGER_TESTS:
                raise ValueError('xmlwriter fallback requires one of the three named row fault gates')
        elif value.xmlwriter_fallback_trigger_test is not None:
            raise ValueError('xmlwriter fallback trigger must be null when fallback is unused')
        if value.allowed_diff_files != expected_files:
            raise ValueError('allowed_diff_files does not match the closed fork diff allowlist')


def _enum_text(value: object, enum_type: type[StrEnum], name: str) -> str:
    if not isinstance(value, enum_type):
        raise ValueError(f'{name} must use the closed {enum_type.__name__} enum')
    return value.value


def _pipeline(value: object) -> str:
    if value not in ('gb', 'sk') or not isinstance(value, str):
        raise ValueError('pipeline must be gb or sk')
    return value


def _integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f'{name} must be an integer')
    return value


def _nonnegative_int(value: object, name: str, *, positive: bool = False) -> int:
    result = _integer(value, name)
    if result < (1 if positive else 0):
        qualifier = 'positive' if positive else 'non-negative'
        raise ValueError(f'{name} must be {qualifier}')
    return result


def _boolean(value: object, name: str) -> bool:
    if type(value) is not bool:
        raise ValueError(f'{name} must be a boolean')
    return value


def _finite_decimal(value: object, name: str, *, positive: bool = False) -> float:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f'{name} must be a finite Decimal')
    if value < 0 or (positive and value <= 0):
        qualifier = 'positive' if positive else 'non-negative'
        raise ValueError(f'{name} must be {qualifier}')
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f'{name} cannot be represented as a finite JSON number')
    return result


def _reject_duplicate_keys(values: tuple[object, ...], name: str) -> None:
    if len(set(values)) != len(values):
        raise ValueError(f'{name} values must be unique')


def _machine_payload(value: MachineArtifactEvidence) -> dict[str, object]:
    if not isinstance(value, MachineArtifactEvidence):
        raise ValueError('machine evidence must use MachineArtifactEvidence')
    if value.architecture != 'x86_64':
        raise ValueError('machine architecture must be x86_64')
    if value.system_drive_media_type not in ('SSD', 'HDD', 'UNKNOWN'):
        raise ValueError('machine media type is not closed')
    return {
        'windows_build_sha256': _require_hash(value.windows_build_sha256, 64, 'windows_build_sha256'),
        'architecture': value.architecture,
        'cpu_model_sha256': _require_hash(value.cpu_model_sha256, 64, 'cpu_model_sha256'),
        'logical_cpu_count': _nonnegative_int(value.logical_cpu_count, 'logical_cpu_count', positive=True),
        'physical_memory_bytes': _nonnegative_int(value.physical_memory_bytes, 'physical_memory_bytes', positive=True),
        'system_drive_media_type': value.system_drive_media_type,
        'system_drive_size_bytes': _nonnegative_int(
            value.system_drive_size_bytes,
            'system_drive_size_bytes',
            positive=True,
        ),
        'fingerprint_sha256': _require_hash(value.fingerprint_sha256, 64, 'machine fingerprint'),
    }


def _mismatch_payload(value: MismatchEvidence) -> dict[str, object]:
    if not isinstance(value, MismatchEvidence):
        raise ValueError('mismatches must use MismatchEvidence')
    if not isinstance(value.coordinate, str) or _COORDINATE.fullmatch(value.coordinate) is None:
        raise ValueError('mismatch coordinate must be strict A1 syntax')
    return {
        'sheet': _enum_text(value.sheet, ApprovedSheet, 'mismatch sheet'),
        'coordinate': value.coordinate,
        'mismatch_kind': _enum_text(value.mismatch_kind, MismatchKind, 'mismatch kind'),
        'expected_storage_type': _enum_text(
            value.expected_storage_type,
            StorageType,
            'expected storage type',
        ),
        'actual_storage_type': _enum_text(value.actual_storage_type, StorageType, 'actual storage type'),
        'local_unversioned_log_sha256': _require_hash(
            value.local_log_sha256,
            64,
            'mismatch local log SHA',
        ),
    }


def _closed_command_token(value: object) -> str:
    if isinstance(value, (CommandToken, PathAlias)):
        return value.value
    raise ValueError('command transcript tokens must be closed literals or approved aliases')


def _tool_version_payload(value: SanitizedToolVersion) -> dict[str, int]:
    if not isinstance(value, SanitizedToolVersion):
        raise ValueError('tool version must use SanitizedToolVersion')
    return {
        'major': _nonnegative_int(value.major, 'tool version major'),
        'minor': _nonnegative_int(value.minor, 'tool version minor'),
        'patch': _nonnegative_int(value.patch, 'tool version patch'),
    }


def _json_artifact(
    kind: EvidenceKind,
    file_name: str,
    payload: dict[str, object],
    source: EvidenceSource,
) -> _SanitizedArtifact:
    content = json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + '\n'
    return _SanitizedArtifact(
        kind=kind,
        file_name=file_name,
        payload=_deep_freeze(payload),
        content=content,
        numeric_values=_numeric_values(payload),
        source=source,
    )


def _deep_freeze(value: object) -> object:
    if isinstance(value, dict):
        return MappingProxyType({key: _deep_freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_deep_freeze(item) for item in value)
    return value


def _numeric_values(value: object) -> tuple[int | float, ...]:
    if isinstance(value, bool):
        return ()
    if isinstance(value, (int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError('versioned evidence contains a non-finite number')
        return (value,)
    if isinstance(value, Mapping):
        return tuple(item for child in value.values() for item in _numeric_values(child))
    if isinstance(value, (list, tuple)):
        return tuple(item for child in value for item in _numeric_values(child))
    return ()


def _scan_versioned_text(raw: str, *, sensitive_names: tuple[str, ...]) -> None:
    folded = unicodedata.normalize('NFKC', raw).casefold()
    username = unicodedata.normalize('NFKC', getpass.getuser().strip()).casefold()
    hostname = unicodedata.normalize('NFKC', socket.gethostname().strip()).casefold()
    names: set[str] = set()
    for item in sensitive_names:
        if not isinstance(item, str) or not item:
            raise ValueError('sensitive names must be non-empty strings')
        path = Path(item)
        names.update(
            (
                unicodedata.normalize('NFKC', path.name).casefold(),
                unicodedata.normalize('NFKC', path.stem).casefold(),
            )
        )
    identity_hit = any(token and len(token) >= 3 and token in folded for token in (username, hostname))
    name_hit = any(token and token in folded for token in names)
    if (
        _DRIVE_PATH.search(folded)
        or _UNC_PATH.search(folded)
        or _USERS_PATH.search(folded)
        or _VERSIONED_SENSITIVE_MARKER.search(folded)
        or identity_hit
        or name_hit
    ):
        raise ValueError('versioned evidence contains sensitive content')


def _scan_versioned_bytes(raw: bytes, *, suffix: str, sensitive_names: tuple[str, ...]) -> None:
    try:
        text = raw.decode('utf-8', errors='strict')
    except UnicodeDecodeError as exc:
        raise ValueError('versioned evidence is not valid UTF-8') from exc
    _scan_versioned_text(text, sensitive_names=sensitive_names)
    if suffix.casefold() != '.json':
        return
    payload = _strict_json_loads(text)
    _scan_decoded_json_strings(payload, sensitive_names=sensitive_names)


def _scan_decoded_json_strings(value: object, *, sensitive_names: tuple[str, ...]) -> None:
    if isinstance(value, str):
        _scan_versioned_text(value, sensitive_names=sensitive_names)
        return
    if isinstance(value, list):
        for item in value:
            _scan_decoded_json_strings(item, sensitive_names=sensitive_names)
        return
    if not isinstance(value, dict):
        return
    forbidden_keys = {'expected', 'actual', 'stdout', 'stderr', 'expected_value', 'actual_value'}
    for key, item in value.items():
        normalized_key = unicodedata.normalize('NFKC', key).casefold()
        if normalized_key in forbidden_keys:
            raise ValueError('versioned evidence contains a sensitive JSON key')
        _scan_versioned_text(key, sensitive_names=sensitive_names)
        _scan_decoded_json_strings(item, sensitive_names=sensitive_names)


def _run_git_bytes(root: Path, *args: str) -> bytes:
    git = shutil.which('git')
    if git is None:
        raise FileNotFoundError('git executable not found for staged evidence scan')
    completed = subprocess.run(  # noqa: S603 - fixed Git executable and closed arguments.
        [git, '-C', str(root), *args],
        check=True,
        capture_output=True,
    )
    return completed.stdout


def _staged_index_entries(root: Path) -> tuple[_StagedIndexEntry, ...]:
    raw_paths = _run_git_bytes(
        root,
        'diff',
        '--cached',
        '--name-only',
        '-z',
        '--diff-filter=ACMRT',
        '--',
    )
    entries: list[_StagedIndexEntry] = []
    seen: set[Path] = set()
    for raw_path in raw_paths.split(b'\0'):
        if not raw_path:
            continue
        try:
            relative_text = raw_path.decode('utf-8', errors='strict')
        except UnicodeDecodeError as exc:
            raise ValueError('staged evidence path is not valid UTF-8') from exc
        relative = Path(relative_text)
        if '..' in relative.parts:
            raise ValueError('staged evidence path contains parent traversal')
        if len(relative.parts) < 2 or relative.parts[:2] != ('docs', 'performance'):
            continue
        if relative in seen:
            raise ValueError('staged evidence path is duplicated')
        seen.add(relative)
        stage_raw = _run_git_bytes(root, 'ls-files', '--stage', '-z', '--', relative.as_posix())
        rows = tuple(row for row in stage_raw.split(b'\0') if row)
        if len(rows) != 1:
            raise ValueError('staged evidence must have exactly one index entry')
        try:
            metadata, indexed_path = rows[0].split(b'\t', 1)
            mode_raw, blob_raw, stage_number = metadata.split(b' ', 2)
            mode = mode_raw.decode('ascii')
            blob_sha = blob_raw.decode('ascii')
            indexed_text = indexed_path.decode('utf-8', errors='strict')
        except (UnicodeDecodeError, ValueError) as exc:
            raise ValueError('staged evidence index entry is malformed') from exc
        if stage_number != b'0':
            raise ValueError('staged evidence must use the unique stage-0 index entry')
        if mode not in ('100644', '100755'):
            raise ValueError('staged evidence mode rejects symlink, submodule, type-change and special files')
        if indexed_text != relative.as_posix():
            raise ValueError('staged evidence index path changed during lookup')
        _require_hash(blob_sha, 40, 'staged Git blob SHA')
        content = _run_git_bytes(root, 'cat-file', 'blob', blob_sha)
        entries.append(_StagedIndexEntry(relative, mode, blob_sha, content))
    return tuple(entries)


def _validate_staged_batch_markers(entries: tuple[_StagedIndexEntry, ...]) -> None:
    if not entries:
        return
    by_path = {item.path: item for item in entries}
    legacy = DEPENDENCY_MANIFEST_RELATIVE_PATH
    nonlegacy = {path for path in by_path if path != legacy}
    marker_pattern = re.compile(r'^batch-([0-9a-f]{16})\.commit\.json$')
    markers = tuple(item for item in entries if marker_pattern.fullmatch(item.path.name))
    bound: set[Path] = set()
    for marker in markers:
        payload = _strict_json_loads(marker.content.decode('utf-8', errors='strict'))
        records, batch_sha = _validate_batch_marker_payload(payload)
        match = marker_pattern.fullmatch(marker.path.name)
        assert match is not None
        if match.group(1) != batch_sha[:16]:
            raise ValueError('batch marker filename does not match its batch SHA')
        for file_name, expected_sha in records:
            path = marker.path.parent / file_name
            entry = by_path.get(path)
            if entry is None:
                raise ValueError('batch marker references a missing staged artifact')
            if _sha256_bytes(entry.content) != expected_sha:
                raise ValueError('batch marker artifact SHA does not match the staged index blob')
            if path in bound:
                raise ValueError('staged artifact is bound by more than one batch marker')
            bound.add(path)
        bound.add(marker.path)
    if nonlegacy != bound:
        raise ValueError('staged evidence contains an orphan or uncommitted batch artifact')


def _batch_commit_marker(artifacts: tuple[_SanitizedArtifact, ...]) -> tuple[str, str]:
    names = tuple(item.file_name for item in artifacts)
    if len(set(names)) != len(names):
        raise ValueError('batch artifacts must have unique basenames')
    records = [
        {
            'file_name': artifact.file_name,
            'sha256': _sha256_bytes(artifact.content.encode('utf-8')),
        }
        for artifact in artifacts
    ]
    basis = {'schema_version': 1, 'artifacts': records}
    batch_sha = _sha256_bytes(
        json.dumps(basis, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf-8')
    )
    payload = {**basis, 'batch_sha256': batch_sha}
    content = json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + '\n'
    return f'batch-{batch_sha[:16]}.commit.json', content


def _validate_batch_marker_payload(payload: object) -> tuple[tuple[tuple[str, str], ...], str]:
    if not isinstance(payload, dict) or tuple(payload) != ('schema_version', 'artifacts', 'batch_sha256'):
        raise ValueError('batch commit marker must use the exact closed schema')
    if payload['schema_version'] != 1 or isinstance(payload['schema_version'], bool):
        raise ValueError('batch commit marker schema version must be 1')
    raw_records = payload['artifacts']
    if not isinstance(raw_records, list) or not raw_records:
        raise ValueError('batch commit marker must bind at least one artifact')
    records: list[tuple[str, str]] = []
    for raw in raw_records:
        if not isinstance(raw, dict) or tuple(raw) != ('file_name', 'sha256'):
            raise ValueError('batch commit marker artifact record uses an unknown field')
        file_name = raw['file_name']
        if (
            not isinstance(file_name, str)
            or Path(file_name).name != file_name
            or re.fullmatch(r'[a-z0-9][a-z0-9.-]*\.(?:json|md)', file_name) is None
            or file_name.startswith('batch-')
        ):
            raise ValueError('batch commit marker contains an invalid artifact basename')
        records.append((file_name, _require_hash(raw['sha256'], 64, 'batch artifact SHA')))
    if len({name for name, _sha in records}) != len(records):
        raise ValueError('batch commit marker contains a duplicate artifact basename')
    batch_sha = _require_hash(payload['batch_sha256'], 64, 'batch SHA')
    basis = {
        'schema_version': 1,
        'artifacts': [{'file_name': name, 'sha256': sha} for name, sha in records],
    }
    expected = _sha256_bytes(
        json.dumps(basis, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf-8')
    )
    if batch_sha != expected:
        raise ValueError('batch commit marker batch SHA is invalid')
    return tuple(records), batch_sha


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


_TreeIdentity = tuple[int, int, int, int, int]
_ContentIdentity = tuple[int, int, int, int]


def _tree_identity(metadata: os.stat_result) -> _TreeIdentity:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_size,
        metadata.st_mtime_ns,
    )


def _content_identity(metadata: os.stat_result) -> _ContentIdentity:
    return (metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns)


def _lstat_tree_path(path: Path) -> os.stat_result:
    return os.lstat(_evidence_io_path(path))


def _reject_tree_special(path: Path, metadata: os.stat_result) -> None:
    attributes = getattr(metadata, 'st_file_attributes', 0)
    if stat.S_ISLNK(metadata.st_mode) or attributes & 0x400:
        raise ValueError(f'evidence tree contains a symlink or reparse point: {path}')
    if not stat.S_ISREG(metadata.st_mode) and not stat.S_ISDIR(metadata.st_mode):
        raise ValueError(f'evidence tree contains a non-regular entry: {path}')


def _assert_tree_contained(path: Path, root: Path) -> None:
    canonical = _strip_windows_extended_prefix(path).resolve(strict=True)
    if not _casefold_relative_to(canonical, root):
        raise ValueError('evidence tree entry escapes its canonical root')


def _scan_tree_safely(root: Path, *, sensitive_names: tuple[str, ...]) -> None:
    raw_root = _strip_windows_extended_prefix(root).expanduser().absolute()
    root_metadata = _lstat_tree_path(raw_root)
    _reject_tree_special(raw_root, root_metadata)
    canonical_root = raw_root.resolve(strict=True)
    if stat.S_ISREG(root_metadata.st_mode):
        _scan_regular_tree_file(raw_root, root_metadata, canonical_root, sensitive_names=sensitive_names)
        return
    stack: list[tuple[Literal['enter', 'exit'], Path, _TreeIdentity]] = [
        ('enter', raw_root, _tree_identity(root_metadata))
    ]
    while stack:
        action, directory, expected_identity = stack.pop()
        current = _lstat_tree_path(directory)
        _reject_tree_special(directory, current)
        if not stat.S_ISDIR(current.st_mode) or _tree_identity(current) != expected_identity:
            raise ValueError('evidence tree directory identity changed during scan')
        _assert_tree_contained(directory, canonical_root)
        if action == 'exit':
            continue
        _scan_versioned_text(directory.name, sensitive_names=sensitive_names)
        try:
            with os.scandir(_evidence_io_path(directory)) as iterator:
                children = tuple(iterator)
        except OSError as exc:
            raise ValueError('evidence tree directory could not be scanned safely') from exc
        if _tree_identity(_lstat_tree_path(directory)) != expected_identity:
            raise ValueError('evidence tree directory changed while being enumerated')
        stack.append(('exit', directory, expected_identity))
        for child in sorted(children, key=lambda item: item.name, reverse=True):
            child_path = _strip_windows_extended_prefix(Path(child.path))
            metadata = _lstat_tree_path(child_path)
            _reject_tree_special(child_path, metadata)
            _assert_tree_contained(child_path, canonical_root)
            _scan_versioned_text(child.name, sensitive_names=sensitive_names)
            if stat.S_ISDIR(metadata.st_mode):
                stack.append(('enter', child_path, _tree_identity(metadata)))
            else:
                _scan_regular_tree_file(child_path, metadata, canonical_root, sensitive_names=sensitive_names)
    if _tree_identity(_lstat_tree_path(raw_root)) != _tree_identity(root_metadata):
        raise ValueError('evidence tree root changed during scan')


def _scan_regular_tree_file(
    path: Path,
    expected_metadata: os.stat_result,
    canonical_root: Path,
    *,
    sensitive_names: tuple[str, ...],
) -> None:
    _reject_tree_special(path, expected_metadata)
    _assert_tree_contained(path, canonical_root)
    expected_identity = _tree_identity(expected_metadata)
    expected_content_identity = _content_identity(expected_metadata)
    try:
        with _evidence_io_path(path).open('rb') as stream:
            if _content_identity(os.fstat(stream.fileno())) != expected_content_identity:
                raise ValueError('evidence file identity changed before read')
            raw = stream.read()
            if _content_identity(os.fstat(stream.fileno())) != expected_content_identity:
                raise ValueError('evidence file identity changed during read')
    except OSError as exc:
        raise ValueError('evidence file could not be read safely') from exc
    if _tree_identity(_lstat_tree_path(path)) != expected_identity:
        raise ValueError('evidence file identity changed after read')
    _scan_versioned_bytes(raw, suffix=path.suffix, sensitive_names=sensitive_names)


def _strip_windows_extended_prefix(path: Path) -> Path:
    raw = str(path)
    if raw.upper().startswith('\\\\?\\UNC\\'):
        raise ValueError('extended UNC paths are not supported')
    if raw.startswith('\\\\?\\'):
        return Path(raw[4:])
    return path


def _evidence_io_path(path: Path) -> Path:
    normal = _strip_windows_extended_prefix(path).absolute()
    if os.name != 'nt':
        return normal
    return Path(f'\\\\?\\{normal}')


def _reject_reparse_components(path: Path) -> None:
    absolute = path.absolute()
    current = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        current /= part
        if not os.path.lexists(current):
            break
        metadata = os.lstat(current)
        attributes = getattr(metadata, 'st_file_attributes', 0)
        if stat.S_ISLNK(metadata.st_mode) or attributes & 0x400:
            raise ValueError(f'path contains a symlink or reparse point: {current}')


def _normalized_path_key(path: Path) -> str:
    raw = _strip_windows_extended_prefix(path).expanduser().absolute().resolve(strict=False)
    return os.path.normcase(os.path.normpath(str(raw)))


def _casefold_relative_to(path: Path, root: Path) -> bool:
    path_key = _normalized_path_key(path)
    root_key = _normalized_path_key(root)
    try:
        return os.path.commonpath((path_key, root_key)) == root_key
    except ValueError:
        return False


def _performance_tree_root(destination: Path) -> Path:
    current = destination
    while current.parent != current:
        if current.name.casefold() == 'performance' and current.parent.name.casefold() == 'docs':
            return current
        current = current.parent
    return destination


def _all_manifest_strings(value: DependencyEvidence) -> tuple[str, ...]:
    scalar_values = (
        value.upstream_url,
        value.upstream_tag,
        value.upstream_base_revision,
        value.crates_io_checksum,
        value.pre_pin_costing_commit,
        value.fork_url,
        value.fork_revision,
        value.diff_sha256,
        value.local_unversioned_log_sha256,
        value.verdict,
    )
    if not all(isinstance(item, str) for item in scalar_values):
        raise ValueError('dependency manifest string fields must be strings')
    if not isinstance(value.allowed_diff_files, tuple) or not all(
        isinstance(item, str) for item in value.allowed_diff_files
    ):
        raise ValueError('allowed_diff_files must be a string tuple')
    strings = (*scalar_values, *value.allowed_diff_files)
    if value.xmlwriter_fallback_trigger_test is not None:
        if not isinstance(value.xmlwriter_fallback_trigger_test, str):
            raise ValueError('xmlwriter fallback trigger must be a string or null')
        strings = (*strings, value.xmlwriter_fallback_trigger_test)
    return strings


def _reject_sensitive_string(value: str) -> None:
    # 不能用“包含 https 就跳过”的宽松规则；两个 URL 也走同一扫描器，只因不命中模式而通过。
    if _DRIVE_PATH.search(value) or _UNC_PATH.search(value) or _SENSITIVE_TOKEN.search(value):
        raise ValueError('dependency manifest contains a sensitive string')


def _require_hash(value: object, length: Literal[40, 64], name: str) -> str:
    pattern = _HEX40 if length == 40 else _HEX64
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise ValueError(f'{name} must be a {length}-character lowercase hexadecimal hash')
    return value


def _revision_from_cargo_manifest(raw: str) -> str:
    payload = tomllib.loads(raw)
    dependency = payload.get('workspace', {}).get('dependencies', {}).get('rust_xlsxwriter')
    if not isinstance(dependency, dict) or dependency.get('git') != FORK_URL:
        raise ValueError('workspace manifest must use the approved rust_xlsxwriter fork URL')
    return _require_hash(dependency.get('rev'), 40, 'workspace manifest revision')


def _revision_from_cargo_metadata(raw: str) -> str:
    payload = _strict_json_loads(raw)
    packages = payload.get('packages') if isinstance(payload, dict) else None
    matches = [item for item in packages or () if isinstance(item, dict) and item.get('name') == 'rust_xlsxwriter']
    if len(matches) != 1 or matches[0].get('version') != CRATE_VERSION:
        raise ValueError('Cargo metadata must contain exactly one rust_xlsxwriter 0.96.0 package')
    return _revision_from_git_source(matches[0].get('source'), 'Cargo metadata')


def _revision_from_cargo_lock(raw: str) -> str:
    payload = tomllib.loads(raw)
    packages = payload.get('package', [])
    matches = [
        item
        for item in packages
        if isinstance(item, dict) and item.get('name') == 'rust_xlsxwriter' and item.get('version') == CRATE_VERSION
    ]
    if len(matches) != 1:
        raise ValueError('Cargo.lock must contain exactly one rust_xlsxwriter 0.96.0 package')
    return _revision_from_git_source(matches[0].get('source'), 'Cargo.lock')


def _revision_from_git_source(source: object, name: str) -> str:
    if not isinstance(source, str):
        raise ValueError(f'{name} rust_xlsxwriter source must be a string')
    match = re.fullmatch(rf'git\+{re.escape(FORK_URL)}\?rev=([0-9a-f]{{40}})#([0-9a-f]{{40}})', source)
    if match is None or match.group(1) != match.group(2):
        raise ValueError(f'{name} rust_xlsxwriter source must pin one exact fork revision')
    return match.group(1)


def _revision_from_dependency_manifest(raw: str) -> str:
    payload = _strict_json_loads(raw)
    if not isinstance(payload, dict):
        raise ValueError('dependency manifest must be a JSON object')
    return _require_hash(payload.get('fork_revision'), 40, 'dependency manifest revision')


def _registry_checksum_from_lock(raw: str) -> str:
    payload = tomllib.loads(raw)
    matches = [
        item
        for item in payload.get('package', [])
        if isinstance(item, dict) and item.get('name') == 'rust_xlsxwriter' and item.get('version') == CRATE_VERSION
    ]
    if len(matches) != 1 or matches[0].get('source') != 'registry+https://github.com/rust-lang/crates.io-index':
        raise ValueError('pre-pin lock must contain the crates.io rust_xlsxwriter 0.96.0 package')
    return _require_hash(matches[0].get('checksum'), 64, 'registry lock checksum')


def _sha256_file(path: Path) -> str:
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


_FileIdentity = tuple[int, int]


def _file_identity(path: Path) -> _FileIdentity:
    stat_result = path.stat(follow_symlinks=False)
    return stat_result.st_dev, stat_result.st_ino


def _identity_from_fd(file_descriptor: int) -> _FileIdentity:
    stat_result = os.fstat(file_descriptor)
    return stat_result.st_dev, stat_result.st_ino


def _unlink_owned_path(path: Path, identity: _FileIdentity) -> None:
    try:
        current_identity = _file_identity(path)
    except FileNotFoundError:
        return
    if current_identity == identity:
        path.unlink()


def _write_manifest_temp(output: Path, payload: dict[str, object]) -> tuple[Path, _FileIdentity]:
    file_descriptor, raw_path = tempfile.mkstemp(
        prefix=f'.{output.name}.',
        suffix='.tmp',
        dir=output.parent,
    )
    temp_path = Path(raw_path)
    identity = _identity_from_fd(file_descriptor)
    descriptor_owned = True
    try:
        stream = os.fdopen(file_descriptor, 'w', encoding='utf-8', newline='\n')
        descriptor_owned = False
        with stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        if descriptor_owned:
            os.close(file_descriptor)
        _unlink_owned_path(temp_path, identity)
        raise
    return temp_path, identity


def _write_owned_log(path: Path, raw_log: str) -> _FileIdentity:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_BINARY', 0)
    file_descriptor = os.open(path, flags, 0o600)
    identity = _identity_from_fd(file_descriptor)
    descriptor_owned = True
    try:
        stream = os.fdopen(file_descriptor, 'w', encoding='utf-8', newline='\n')
        descriptor_owned = False
        with stream:
            stream.write(raw_log)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        if descriptor_owned:
            os.close(file_descriptor)
        _unlink_owned_path(path, identity)
        raise
    return identity


@dataclass(frozen=True)
class _CommandResult:
    command: tuple[str, ...]
    stdout: str
    stderr: str


def _reject_parent_traversal(path: Path, name: str) -> None:
    if '..' in path.parts:
        raise ValueError(f'{name} path must not contain parent traversal')


def _resolve_exact_repo_path(path: Path, expected: Path, name: str, *, require_file: bool = False) -> Path:
    _reject_parent_traversal(path, name)
    resolved = path.resolve(strict=False)
    if resolved != expected:
        raise ValueError(f'{name} path must equal the approved repository path')
    if require_file and not resolved.is_file():
        raise ValueError(f'{name} path must be an existing file')
    return resolved


def _resolve_fork_checkout(path: Path) -> Path:
    _reject_parent_traversal(path, 'fork checkout')
    try:
        resolved = path.resolve(strict=True)
    except FileNotFoundError as exc:
        raise ValueError('fork checkout path must be an existing directory') from exc
    if not resolved.is_dir():
        raise ValueError('fork checkout path must be an existing directory')
    return resolved


def _validate_generation_paths(
    *,
    fork_checkout: Path,
    cargo_manifest: Path,
    cargo_lock: Path,
    local_log_root: Path,
    output: Path,
) -> tuple[Path, Path, Path, Path, Path, Path]:
    root = repo_root().resolve(strict=True)
    fork = _resolve_fork_checkout(fork_checkout)
    manifest = _resolve_exact_repo_path(
        cargo_manifest,
        root / 'rust' / 'Cargo.toml',
        'cargo manifest',
        require_file=True,
    )
    lock = _resolve_exact_repo_path(cargo_lock, root / 'rust' / 'Cargo.lock', 'cargo lock', require_file=True)
    log_root = _resolve_exact_repo_path(
        local_log_root,
        root / LOCAL_LOG_ROOT_RELATIVE_PATH,
        'local log root',
    )
    output_path = _resolve_exact_repo_path(
        output,
        root / DEPENDENCY_MANIFEST_RELATIVE_PATH,
        'dependency output',
    )
    return root, fork, manifest, lock, log_root, output_path


def _validate_verification_paths(
    *,
    fork_checkout: Path,
    cargo_manifest: Path,
    cargo_lock: Path,
    dependency_manifest: Path,
) -> tuple[Path, Path, Path, Path]:
    root = repo_root().resolve(strict=True)
    fork = _resolve_fork_checkout(fork_checkout)
    manifest = _resolve_exact_repo_path(
        cargo_manifest,
        root / 'rust' / 'Cargo.toml',
        'cargo manifest',
        require_file=True,
    )
    lock = _resolve_exact_repo_path(cargo_lock, root / 'rust' / 'Cargo.lock', 'cargo lock', require_file=True)
    dependency = _resolve_exact_repo_path(
        dependency_manifest,
        root / DEPENDENCY_MANIFEST_RELATIVE_PATH,
        'dependency manifest',
        require_file=True,
    )
    return fork, manifest, lock, dependency


def _run_command(executable: str, *args: str) -> _CommandResult:
    resolved = shutil.which(executable)
    if resolved is None:
        raise FileNotFoundError(f'required executable not found: {executable}')
    command = (resolved, *args)
    completed = subprocess.run(  # noqa: S603 - executable is resolved from a fixed allowlist name.
        command,
        capture_output=True,
        text=True,
        encoding='utf-8',
        check=True,
    )
    return _CommandResult(command, completed.stdout, completed.stderr)


def _collect_live_evidence(
    *,
    fork_checkout: Path,
    cargo_manifest: Path,
    cargo_lock: Path,
    pre_pin_commit: str,
) -> tuple[dict[str, object], tuple[_CommandResult, ...], tuple[Path, ...]]:
    _require_hash(pre_pin_commit, 40, 'pre_pin_commit')
    root = repo_root().resolve()
    fork = fork_checkout.resolve()
    manifest = cargo_manifest.resolve()
    lock = cargo_lock.resolve()
    head_result = _run_command('git', '-C', str(fork), 'rev-parse', 'HEAD')
    head = _require_hash(head_result.stdout.strip(), 40, 'fork HEAD')
    revision_range = f'{UPSTREAM_BASE_REVISION}..{head}'
    commands = (
        head_result,
        _run_command('git', '-C', str(fork), 'rev-parse', f'{UPSTREAM_TAG}^{{commit}}'),
        _run_command('git', '-C', str(fork), 'remote', 'get-url', 'upstream'),
        _run_command('git', '-C', str(fork), 'remote', 'get-url', 'origin'),
        _run_command('git', '-C', str(fork), 'branch', '--show-current'),
        _run_command(
            'git',
            '-C',
            str(fork),
            'diff',
            '--name-status',
            '--no-renames',
            revision_range,
            '--',
        ),
        _run_command(
            'git',
            '-C',
            str(fork),
            'diff',
            '--no-ext-diff',
            '--binary',
            revision_range,
            '--',
        ),
        _run_command(
            'cargo',
            'metadata',
            '--locked',
            '--manifest-path',
            str(manifest),
            '--format-version',
            '1',
        ),
        _run_command('git', '-C', str(root), 'show', f'{pre_pin_commit}:rust/Cargo.lock'),
        _run_command(
            'gh',
            'pr',
            'list',
            '--repo',
            'jmcnamara/rust_xlsxwriter',
            '--state',
            'all',
            '--head',
            f'Aspirin86942:{FORK_BRANCH}',
            '--json',
            'url',
        ),
    )
    (
        head_result,
        tag_result,
        upstream_remote_result,
        origin_remote_result,
        branch_result,
        diff_names_result,
        diff_result,
        metadata_result,
        pre_pin_lock_result,
        pr_result,
    ) = commands

    if tag_result.stdout.strip() != UPSTREAM_BASE_REVISION:
        raise ValueError('upstream tag does not resolve to the approved base revision')
    if upstream_remote_result.stdout.strip() != UPSTREAM_URL or origin_remote_result.stdout.strip() != FORK_URL:
        raise ValueError('fork remotes do not match the approved upstream and origin URLs')
    if branch_result.stdout.strip() != FORK_BRANCH:
        raise ValueError('fork checkout is not on the approved fixed branch')
    EvidenceSanitizer.verify_empty_pr_query(pr_result.stdout)

    diff_files = EvidenceSanitizer.verify_fork_diff_statuses(diff_names_result.stdout)
    fallback_used = XMLWRITER_PATH in diff_files

    cargo_home = Path(os.environ.get('CARGO_HOME', Path.home() / '.cargo')).resolve()
    archive_root = cargo_home / 'registry' / 'cache'
    archives = tuple(sorted(archive_root.rglob(f'rust_xlsxwriter-{CRATE_VERSION}.crate')))
    checksum = EvidenceSanitizer.verify_registry_checksum(pre_pin_lock_result.stdout, archives)
    current_lock_text = lock.read_text(encoding='utf-8')
    live = {
        'fork_head': head,
        'cargo_manifest_text': manifest.read_text(encoding='utf-8'),
        'cargo_metadata_text': metadata_result.stdout,
        'cargo_lock_text': current_lock_text,
        'pre_pin_lock_text': pre_pin_lock_result.stdout,
        'diff_files': diff_files,
        'diff_sha256': _sha256_text(diff_result.stdout),
        'fallback_used': fallback_used,
        'checksum': checksum,
        'pr_query_output': pr_result.stdout,
    }
    return live, commands, archives


def _raw_log_text(commands: tuple[_CommandResult, ...], archives: tuple[Path, ...]) -> str:
    sections: list[str] = []
    for index, result in enumerate(commands, start=1):
        sections.extend(
            (
                f'command[{index}]={json.dumps(result.command)}',
                f'stdout[{index}]:',
                result.stdout,
                f'stderr[{index}]:',
                result.stderr,
            )
        )
    for archive in archives:
        sections.append(f'archive={archive} sha256={_sha256_file(archive)}')
    return '\n'.join(sections)


def _build_dependency_evidence(
    *,
    live: dict[str, object],
    pre_pin_commit: str,
    local_log_sha256: str,
) -> DependencyEvidence:
    fallback_used = live['fallback_used']
    if type(fallback_used) is not bool:
        raise ValueError('internal fallback state must be boolean')
    return DependencyEvidence(
        upstream_url=UPSTREAM_URL,
        upstream_tag=UPSTREAM_TAG,
        upstream_base_revision=UPSTREAM_BASE_REVISION,
        crates_io_checksum=CRATES_IO_CHECKSUM,
        pre_pin_costing_commit=pre_pin_commit,
        fork_url=FORK_URL,
        fork_revision=live['fork_head'],
        allowed_diff_files=live['diff_files'],
        xmlwriter_fallback_used=fallback_used,
        xmlwriter_fallback_trigger_test=None,
        diff_sha256=live['diff_sha256'],
        local_unversioned_log_sha256=local_log_sha256,
        upstream_pr_url=None,
        verdict='VALIDATED',
    )


def generate_dependency_manifest(
    *,
    fork_checkout: Path,
    cargo_manifest: Path,
    cargo_lock: Path,
    pre_pin_commit: str,
    local_log_root: Path,
    output: Path,
) -> None:
    root, fork, manifest, lock, log_root, output_path = _validate_generation_paths(
        fork_checkout=fork_checkout,
        cargo_manifest=cargo_manifest,
        cargo_lock=cargo_lock,
        local_log_root=local_log_root,
        output=output,
    )
    if output_path.exists():
        raise FileExistsError(output_path)
    log_path = log_root / f'rust-xlsxwriter-0.96.0-{uuid.uuid4().hex}.log'
    log_relative_path = log_path.relative_to(root).as_posix()
    ignore_result = _run_command(
        'git',
        '-C',
        str(root),
        'check-ignore',
        '--quiet',
        '--',
        log_relative_path,
    )
    live, commands, archives = _collect_live_evidence(
        fork_checkout=fork,
        cargo_manifest=manifest,
        cargo_lock=lock,
        pre_pin_commit=pre_pin_commit,
    )
    raw_log = _raw_log_text((ignore_result, *commands), archives)
    log_root.mkdir(parents=True, exist_ok=True)
    log_identity = _write_owned_log(log_path, raw_log)
    try:
        log_sha = _sha256_file(log_path)
        value = _build_dependency_evidence(live=live, pre_pin_commit=pre_pin_commit, local_log_sha256=log_sha)
        payload_text = json.dumps(EvidenceSanitizer.dependency_payload(value))
        EvidenceSanitizer._verify_revision_contents(
            fork_head=live['fork_head'],
            cargo_manifest_text=live['cargo_manifest_text'],
            cargo_metadata_text=live['cargo_metadata_text'],
            cargo_lock_text=live['cargo_lock_text'],
            dependency_manifest_text=payload_text,
        )
        EvidenceSanitizer.write_dependency_manifest(output_path, value)
    except BaseException:
        _unlink_owned_path(log_path, log_identity)
        raise


def verify_dependency_manifest(
    *,
    fork_checkout: Path,
    cargo_manifest: Path,
    cargo_lock: Path,
    pre_pin_commit: str,
    dependency_manifest: Path,
) -> None:
    fork, manifest, lock, dependency = _validate_verification_paths(
        fork_checkout=fork_checkout,
        cargo_manifest=cargo_manifest,
        cargo_lock=cargo_lock,
        dependency_manifest=dependency_manifest,
    )
    value = EvidenceSanitizer.read_dependency_manifest(dependency)
    live, _commands, _archives = _collect_live_evidence(
        fork_checkout=fork,
        cargo_manifest=manifest,
        cargo_lock=lock,
        pre_pin_commit=pre_pin_commit,
    )
    EvidenceSanitizer._verify_revision_contents(
        fork_head=live['fork_head'],
        cargo_manifest_text=live['cargo_manifest_text'],
        cargo_metadata_text=live['cargo_metadata_text'],
        cargo_lock_text=live['cargo_lock_text'],
        dependency_manifest_text=dependency.read_text(encoding='utf-8'),
    )
    if value.pre_pin_costing_commit != pre_pin_commit:
        raise ValueError('dependency manifest pre-pin commit mismatch')
    if value.allowed_diff_files != live['diff_files']:
        raise ValueError('dependency manifest diff allowlist mismatch')
    if value.xmlwriter_fallback_used != live['fallback_used']:
        raise ValueError('dependency manifest xmlwriter fallback state mismatch')
    if value.diff_sha256 != live['diff_sha256']:
        raise ValueError('dependency manifest diff SHA-256 mismatch')
    if value.crates_io_checksum != live['checksum']:
        raise ValueError('dependency manifest crates.io checksum mismatch')


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command', required=True)
    dependency = subparsers.add_parser('dependency')
    verify = subparsers.add_parser('verify-dependency')
    for command in (dependency, verify):
        command.add_argument('--fork-checkout', type=Path, required=True)
        command.add_argument('--cargo-manifest', type=Path, required=True)
        command.add_argument('--cargo-lock', type=Path, required=True)
        command.add_argument('--pre-pin-commit', required=True)
    dependency.add_argument('--local-log-root', type=Path, required=True)
    dependency.add_argument('--output', type=Path, required=True)
    verify.add_argument('--dependency-manifest', type=Path, required=True)
    return parser


def main() -> None:
    args = _argument_parser().parse_args()
    if args.command == 'dependency':
        generate_dependency_manifest(
            fork_checkout=args.fork_checkout,
            cargo_manifest=args.cargo_manifest,
            cargo_lock=args.cargo_lock,
            pre_pin_commit=args.pre_pin_commit,
            local_log_root=args.local_log_root,
            output=args.output,
        )
    else:
        verify_dependency_manifest(
            fork_checkout=args.fork_checkout,
            cargo_manifest=args.cargo_manifest,
            cargo_lock=args.cargo_lock,
            pre_pin_commit=args.pre_pin_commit,
            dependency_manifest=args.dependency_manifest,
        )


if __name__ == '__main__':
    main()
