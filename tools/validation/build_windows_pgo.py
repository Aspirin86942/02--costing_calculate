"""Build a Windows x64 release candidate with a profile trained on synthetic GB/SK inputs."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path

from tools.validation.synthetic_benchmark import build_benchmark_fixture, build_full_sk_fixture

ROOT = Path(__file__).resolve().parents[2]
TARGET = 'x86_64-pc-windows-msvc'
SEED = 7781


def _sha256(path: Path) -> str:
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def _build(work: Path, stage: str, flags: list[str]) -> Path:
    target_dir = work / stage
    cargo = shutil.which('cargo')
    if cargo is None:
        raise ValueError('cargo is required on PATH')
    env = {
        **os.environ,
        'CARGO_ENCODED_RUSTFLAGS': '\x1f'.join(flags),
        'CARGO_TARGET_DIR': str(target_dir),
    }
    subprocess.run(  # noqa: S603 - cargo uses the repository-pinned toolchain.
        [
            cargo,
            'build',
            '--release',
            '--locked',
            '--manifest-path',
            'rust/Cargo.toml',
            '--target',
            TARGET,
            '-p',
            'costing-calculate',
        ],
        cwd=ROOT,
        env=env,
        check=True,
    )
    return target_dir / TARGET / 'release/costing-calculate.exe'


def build_windows_pgo(output_dir: Path) -> Path:
    if os.name != 'nt':
        raise ValueError('Windows x64 MSVC is required for this training recipe')
    info = subprocess.check_output(['rustc', '-vV'], cwd=ROOT, text=True)  # noqa: S603, S607
    if f'host: {TARGET}' not in info.splitlines():
        raise ValueError(f'The repository toolchain must run on {TARGET}')
    sysroot = subprocess.check_output(['rustc', '--print', 'sysroot'], cwd=ROOT, text=True).strip()  # noqa: S603, S607
    profdata = Path(sysroot) / 'lib/rustlib' / TARGET / 'bin/llvm-profdata.exe'
    if not profdata.is_file():
        raise ValueError('Install the pinned toolchain component with rustup component add llvm-tools-preview')

    work = output_dir.resolve()
    work.mkdir(parents=True, exist_ok=False)
    raw_dir = work / 'raw'
    raw_dir.mkdir()
    training = _build(work, 'instrumented', [f'-Cprofile-generate={raw_dir}', '--cfg=costing_pgo_training'])
    fixtures = {}
    profiles = []
    for name, pipeline in (('gb-medium', 'gb'), ('sk-medium', 'sk'), ('full-sk', 'sk')):
        case = work / 'training' / name
        spec = (
            build_full_sk_fixture(case, orders=4096, seed=SEED)
            if name == 'full-sk'
            else build_benchmark_fixture(case, pipeline, 'medium', SEED)
        )
        workbook = case / spec['files']['input']
        config = case / spec['files']['config']
        raw = raw_dir / f'{name}.profraw'
        result = subprocess.check_output(  # noqa: S603 - freshly built trainer, synthetic inputs only.
            [
                training,
                pipeline,
                '--input',
                workbook,
                '--config',
                config,
                '--output',
                case / 'out.xlsx',
                '--redact-paths',
            ],
            cwd=ROOT,
            env={**os.environ, 'LLVM_PROFILE_FILE': str(raw)},
            encoding='utf-8',
        )
        if json.loads(result).get('status') != 'succeeded' or raw.stat().st_size == 0:
            raise RuntimeError(f'{name} training did not finish successfully')
        fixtures[name] = {'input_sha256': _sha256(workbook), 'config_sha256': _sha256(config)}
        profiles.append(raw)

    merged = work / 'training.profdata'
    subprocess.run([profdata, 'merge', '-o', merged, *profiles], cwd=ROOT, check=True)  # noqa: S603
    optimized = _build(work, 'optimized', [f'-Cprofile-use={merged}'])
    binary = work / 'costing-calculate.exe'
    shutil.copyfile(optimized, binary)
    identity = json.loads(subprocess.check_output([binary, '--version-json'], cwd=ROOT, encoding='utf-8'))  # noqa: S603
    report = {
        'target': TARGET,
        'build': identity,
        'training_seed': SEED,
        'synthetic_training': fixtures,
        'profile_sha256': _sha256(merged),
        'binary_sha256': _sha256(binary),
    }
    (work / 'pgo-build.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    return binary


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--output-dir', required=True, type=Path, help='New directory for both builds and the candidate'
    )
    try:
        print(build_windows_pgo(parser.parse_args().output_dir))
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        parser.exit(1, f'{error}\n')
