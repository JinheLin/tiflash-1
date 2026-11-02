#!/usr/bin/env python3
"""Check completed TLC evidence, non-vacuity witnesses, and expected counterexamples."""
from pathlib import Path
import hashlib
import json
import re
import argparse

def audit_run(output):
    """Audit saved model/config snapshots independently of the current checkout."""
    output = Path(output).resolve()
    summary = json.loads((output / 'summary.json').read_text())
    tool = json.loads((output / 'tool-manifest.json').read_text())
    digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    assert summary['passed'] and len(summary['results']) == 10
    results = {result['name']: result for result in summary['results']}
    for name, result in results.items():
        directory = output / name
        assert result['passed'] and result['model_sha256'] == digest(output / 'correct/SchemaWatermark.tla')
        assert digest(directory / 'SchemaWatermark.tla') == result['model_sha256']
        assert digest(directory / (name + '.cfg')) == digest(output / name / (name + '.cfg')) == result['config_sha256']
        log = (directory / 'tlc.log').read_text()
        assert '-simulate' not in result['command'] and '-depth' not in result['command']
        assert not re.search(r'Error: (?:Evaluating|Parsing|TLC threw|Attempted)', log)
        if name == 'correct':
            assert result['returncode'] == 0 and result['remaining_states'] == 0
            assert result['distinct_states'] > 0
            assert 'Model checking completed. No error has been found.' in log
            for invariant in ['TypeOK', 'PublishedPair', 'CachedPair', 'DecoderPair', 'NoValidColumnSkipped',
                              'OrdinaryCommitOrder', 'HistoryLifecycle']:
                assert invariant in (output / 'correct/correct.cfg').read_text()
        else:
            assert result['returncode'] == 12 and result['trace_states'] >= 2
            assert 'Invariant ' + result['expected_invariant'] + ' is violated' in log

    counterexamples = []
    for name in ['no_clear', 'split_publish', 'min_commit']:
        log = (output / name / 'tlc.log').read_text()
        last = list(re.finditer(r'^State \d+:', log, re.M))[-1]
        state = log[last.start():]
        assert '/\\ badSkipped = {"b"}' in state
        assert re.search(r'removed = \[a \|-> 0, b \|-> 0\]', state)
        decoder = re.search(r'^/\\ decoder = (.+)$', state, re.M).group(1)
        tso = int(re.search(r'tso \|-> (\d+)', decoder).group(1))
        commit_ts = [int(x) for x in re.findall(r'commit_ts \|-> (\d+)', decoder)]
        assert 'columns |-> {"a"}' in decoder
        if name == 'min_commit':
            assert min(commit_ts) <= tso < max(commit_ts)
        else:
            assert max(commit_ts) <= tso
        counterexamples.append(dict(name=name, trace_states=results[name]['trace_states'],
                                    decode_schema=['a'], schema_read_tso=tso, batch_commit_ts=commit_ts,
                                    incorrectly_skipped=['b'], column_b_removed=False))
    for name in ['witness_skip', 'witness_fallback', 'witness_failure', 'witness_restart',
                 'witness_crash_alter', 'witness_crash_decode']:
        text = (output / name / 'tlc.log').read_text()
        assert not re.search(r'badSkipped = \{[^}\n]+\}', text)
        assert 'Invariant NoValidColumnSkipped is violated' not in text

    audit = dict(passed=True, model_sha256=digest(output / 'correct/SchemaWatermark.tla'), tool_version=tool['tlc_version'],
                 positive_distinct_states=results['correct']['distinct_states'], positive_queue_remaining=0,
                 injected_fault_counterexamples=counterexamples, non_vacuity_witnesses=6,
                 scope='Finite safety model; no TLAPS proof or C++ refinement proof.')
    (output / 'audit.json').write_text(json.dumps(audit, indent=2) + '\n')
    return audit


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path, help='Result directory written by run_models.py')
    args = parser.parse_args()
    print(json.dumps(audit_run(args.output), indent=2))
