#!/usr/bin/env python3
"""Run finite TLC checks and expected counterexamples without changing the repository."""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import time
import urllib.request

from audit_results import audit_run

HERE = Path(__file__).resolve().parent
TOOL_URL = 'https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar'
TOOL_SHA256 = '936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88'
DEFAULT_JAR = HERE / '.tools/tla2tools.jar'
CASES = [
    ('correct', None),
    ('no_clear', 'NoValidColumnSkipped'),
    ('split_publish', 'NoValidColumnSkipped'),
    ('min_commit', 'NoValidColumnSkipped'),
    ('witness_skip', 'NoHistoricalSkip'),
    ('witness_fallback', 'NoFallback'),
    ('witness_failure', 'NoFailureAfterStore'),
    ('witness_restart', 'NoRestart'),
    ('witness_crash_alter', 'NoCrashDuringAlter'),
    ('witness_crash_decode', 'NoCrashDuringDecode'),
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=HERE / 'results' /
                        datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ'))
    parser.add_argument('--tlc-jar', type=Path, default=DEFAULT_JAR)
    parser.add_argument('--java', default='java')
    parser.add_argument('--download-tools', action='store_true', help='Download the pinned TLC release if the JAR is absent')
    args = parser.parse_args()
    output = args.output.resolve()
    if output.exists():
        parser.error('Result directory already exists: ' + str(output))
    java = shutil.which(args.java)
    if java is None:
        parser.error('Java runtime not found: ' + args.java)
    jar = args.tlc_jar.resolve()
    if not jar.is_file() and args.download_tools:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(TOOL_URL, timeout=60) as response:
            data = response.read()
        if hashlib.sha256(data).hexdigest() != TOOL_SHA256:
            parser.error('Downloaded TLC JAR failed SHA256 verification')
        jar.parent.mkdir(parents=True, exist_ok=True)
        jar.write_bytes(data)
    if not jar.is_file():
        parser.error('TLC JAR not found; use --download-tools or --tlc-jar /path/to/tla2tools.jar')
    jar_sha256 = hashlib.sha256(jar.read_bytes()).hexdigest()
    if jar == DEFAULT_JAR.resolve() and jar_sha256 != TOOL_SHA256:
        parser.error('Cached TLC JAR does not match the pinned SHA256')
    output.mkdir(parents=True, exist_ok=False)
    tool = dict(jar=str(jar), sha256=jar_sha256, tlc_version=None,
                java_version=subprocess.run([java, '-version'], capture_output=True, text=True).stderr,
                pinned_download_url=TOOL_URL if jar_sha256 == TOOL_SHA256 else None)
    tool_file = output / 'tool-manifest.json'
    tool_file.write_text(json.dumps(tool, indent=2) + '\n')
    results = []
    for name, expected_invariant in CASES:
        run = output / name
        run.mkdir()
        shutil.copy2(HERE / 'SchemaWatermark.tla', run / 'SchemaWatermark.tla')
        shutil.copy2(HERE / (name + '.cfg'), run / (name + '.cfg'))
        command = [java, '-Xmx2g', '-XX:+UseParallelGC', '-cp', str(jar),
                   'tlc2.TLC', '-workers', '2' if name == 'correct' else '1', '-fp', '0', '-seed', '1']
        if name == 'correct':
            command += ['-coverage', '1']
        command += ['-config', name + '.cfg', 'SchemaWatermark.tla']
        print('Starting ' + name, flush=True)
        start = time.monotonic()
        with (run / 'tlc.log').open('w') as log:
            completed = subprocess.run(command, cwd=run, stdout=log, stderr=subprocess.STDOUT)
        text = (run / 'tlc.log').read_text()
        if name == 'correct':
            tool['tlc_version'] = text.splitlines()[0] if text else None
            tool_file.write_text(json.dumps(tool, indent=2) + '\n')
        error = re.search(r'Error: Invariant (\w+) is violated', text)
        stats = re.search(r'([\d,]+) states generated, ([\d,]+) distinct states found, ([\d,]+) states left on queue', text)
        depth = re.search(r'The depth of the complete state graph search is (\d+)', text)
        passed = (completed.returncode == 0 and 'Model checking completed. No error has been found.' in text
                  and stats is not None and int(stats.group(3).replace(',', '')) == 0) if expected_invariant is None else (
                      completed.returncode == 12 and error is not None and error.group(1) == expected_invariant)
        entry = dict(name=name, passed=passed, expected_invariant=expected_invariant,
                     violated_invariant=error.group(1) if error else None, command=command,
                     returncode=completed.returncode, seconds=round(time.monotonic() - start, 3),
                     model_sha256=hashlib.sha256((run / 'SchemaWatermark.tla').read_bytes()).hexdigest(),
                     config_sha256=hashlib.sha256((run / (name + '.cfg')).read_bytes()).hexdigest(),
                     generated_states=int(stats.group(1).replace(',', '')) if stats else None,
                     distinct_states=int(stats.group(2).replace(',', '')) if stats else None,
                     remaining_states=int(stats.group(3).replace(',', '')) if stats else None,
                     depth=int(depth.group(1)) if depth else None,
                     trace_states=len(re.findall(r'^State \d+:', text, re.M)))
        (run / 'result.json').write_text(json.dumps(entry, indent=2) + '\n')
        results.append(entry)
        summary = dict(completed_at=datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).isoformat(),
                       output=str(output), passed=len(results) == len(CASES) and all(x['passed'] for x in results), results=results)
        (output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
        print(json.dumps({k: entry[k] for k in ['name', 'passed', 'returncode', 'seconds', 'distinct_states', 'trace_states']}), flush=True)
        if not passed:
            print('\n'.join(text.splitlines()[-35:]), flush=True)
            raise SystemExit(1)
    try:
        audit_run(output)
    except Exception:
        summary['passed'] = False
        (output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
        raise
    print('All 10 TLC checks and independent evidence audit passed: ' + str(output), flush=True)


if __name__ == '__main__':
    main()
