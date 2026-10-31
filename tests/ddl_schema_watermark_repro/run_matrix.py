#!/usr/bin/env python3
"""Local TiUP DDL position matrix. Results and failed attempts are retained."""
import argparse
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time
import traceback
import urllib.request

HERE = Path(__file__).resolve().parent
COLS = ['c_first', 'id', 'c_middle', 'c_last']
BUSINESS = ['c_first', 'c_middle', 'c_last']
LABELS = [('tiflash_schema_trigger_count', 'raft_decode'),
          ('tiflash_schema_internal_ddl_count', 'modify_column'),
          ('tiflash_schema_internal_ddl_count', 'drop_column')]
LABELS += [('tiflash_schema_watermark_decode_count', x) for x in
           ['no_watermark', 'beyond_watermark', 'covered', 'skipped_extra_columns']]


def sha(data):
    return hashlib.sha256(data).hexdigest()


def file_sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def make_parser(description, allow_baseline=True):
    parser = argparse.ArgumentParser(description=description)
    for name in ('tiflash', 'tidb', 'tikv', 'pd'):
        parser.add_argument('--' + name, required=True, type=Path,
                            help='Path to the component binary (TiDB/PD/TiKV must be v8.5.7)')
    parser.add_argument('--output', required=True, type=Path, help='New result directory; must not exist')
    parser.add_argument('--tag', required=True, help='Unique TiUP tag for this isolated cluster')
    parser.add_argument('--offset', required=True, type=int)
    parser.add_argument('--debug-port', required=True, type=int)
    parser.add_argument('--expected-commit', help='Optional exact TiFlash build commit to verify')
    if allow_baseline:
        parser.add_argument('--baseline', action='store_true', help='Expect the v8.5.7 TiFlash behavior')
    else:
        parser.set_defaults(baseline=False)
    return parser


def wait(fn, description, timeout=240):
    end = time.monotonic() + timeout
    error = None
    while time.monotonic() < end:
        try:
            value = fn()
            if value:
                return value
        except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
            error = str(exc)
        time.sleep(1)
    raise RuntimeError(f'Timeout: {description}; last error: {error}')


def counter(text, metric, label):
    match = re.search(r'^' + re.escape(metric) + r'\{[^\n]*type="' + label +
                      r'"[^\n]*\} ([0-9.e+\-]+)$', text, re.M)
    return float(match.group(1)) if match else 0.0


def deltas(before, after):
    return {label: counter(after, metric, label) - counter(before, metric, label)
            for metric, label in LABELS}


class Matrix:
    def __init__(self, args):
        self.args = args
        self.binary = args.tiflash.resolve()
        self.components = {name: getattr(args, name).resolve() for name in ('tidb', 'tikv', 'pd')}
        self.output = args.output.resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        self.port = 4000 + args.offset
        self.metric_port = 8234 + args.offset
        self.debug_port = args.debug_port
        self.state = self.output / 'restart-control'
        self.gate = self.state / 'restart.gate'
        self.proc = None
        self.pid = None
        self.old_gc = None
        self.paused = False
        self.failpoint = False
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        self.env = {k: v for k, v in os.environ.items() if 'proxy' not in k.lower()}
        self.env['LD_LIBRARY_PATH'] = str(self.binary.parent) + ':' + self.env.get('LD_LIBRARY_PATH', '')
        self.result = {'baseline': args.baseline,
                       'requested_ref': 'v8.5.7' if args.baseline else 'jinhe/schema-read-watermark-extra-columns',
                       'tag': args.tag, 'offset': args.offset, 'cases': [], 'passed': False}

    def save(self):
        (self.output / 'result.json').write_text(json.dumps(self.result, ensure_ascii=False, indent=2) + '\n')

    def sql(self, query, engine=None):
        prefix = ''
        if engine:
            prefix = (f"SET tidb_isolation_read_engines='{engine}'; SET tidb_allow_mpp=1; "
                      f"SET tidb_enforce_mpp={int(engine == 'tiflash')}; ")
        run = subprocess.run(['mysql', '--protocol=tcp', '-h127.0.0.1', f'-P{self.port}',
                              '-uroot', '-N', '-B', '-e', prefix + query],
                             capture_output=True, text=True, timeout=60)
        if run.returncode:
            raise RuntimeError(run.stderr.strip())
        return run.stdout.strip()

    def debug(self, query):
        run = subprocess.run([str(self.binary), 'client', '--host', '127.0.0.1', '--port',
                              str(self.debug_port), '--query', query], env=self.env,
                             text=True, capture_output=True, timeout=45)
        if run.returncode:
            raise RuntimeError(f'Debug query {query}: {run.stderr.strip()}')
        return run.stdout.strip()

    def get(self, url):
        with self.opener.open(url, timeout=10) as response:
            return response.read().decode()

    def metrics(self):
        return self.get(f'http://127.0.0.1:{self.metric_port}/metrics')

    def events(self):
        return [json.loads(s) for s in (self.state / 'events.jsonl').read_text().splitlines()]

    def find_pid(self):
        for event in reversed(self.events()):
            if event['event'] == 'start' and Path(f"/proc/{event['pid']}/cmdline").exists():
                return event['pid']
        return None

    def store_ids(self):
        data = json.loads(self.get(f'http://127.0.0.1:{2379+self.args.offset}/pd/api/v1/stores'))
        return sorted(s['store']['id'] for s in data['stores'] if
                      any(x['key'] == 'engine' and x['value'] == 'tiflash'
                          for x in s['store'].get('labels', [])))

    def start(self):
        versions = {}
        for component, binary in {'tiflash': self.binary, **self.components}.items():
            flag = 'version' if component == 'tiflash' else ('-V' if component == 'tidb' else '--version')
            version = subprocess.check_output([str(binary), flag], env=self.env, text=True, timeout=30)
            versions[component] = {'path': str(binary), 'sha256': file_sha(binary), 'version': version}
            if component != 'tiflash' or self.args.baseline:
                if not re.search(r'\bv?8\.5\.7\b', version):
                    raise RuntimeError(f'{component} must be v8.5.7; got {version}')
        self.result['versions'] = versions
        match = re.search(r'^Git Commit Hash:\s*(\S+)', versions['tiflash']['version'], re.M)
        actual_commit = match.group(1) if match else None
        self.result['head'] = actual_commit
        self.result['actual_binary_commit'] = actual_commit
        self.result['runtime_sha256'] = {
            p.name: file_sha(p) for p in sorted(self.binary.parent.glob('*.so*')) if p.is_file()
        }
        self.result['harness_sha256'] = {p.name: file_sha(p) for p in sorted(HERE.glob('*.py'))}
        if self.args.expected_commit and actual_commit != self.args.expected_commit:
            raise RuntimeError(f'TiFlash commit mismatch: expected {self.args.expected_commit}, got {actual_commit}')
        self.save()
        kv = self.output / 'tikv.toml'
        kv.write_text('[raftstore]\nraft-log-gc-tick-interval="0s"\n'
                      'raft-log-gc-count-limit=1000000\nraft-log-gc-size-limit="1GB"\n')
        tf = self.output / 'tiflash.toml'
        tf.write_text(f'tcp_port={self.debug_port}\n[logger]\nlevel="debug"\n'
                      '[profiles.default]\nddl_sync_interval_seconds=3600\n')
        wrapper = HERE / 'restartable_tiflash.py'
        command = ['tiup', 'playground', 'v8.5.7', '--tag', self.args.tag, '--db', '1', '--pd', '1',
                   '--kv', '1', '--tiflash', '1', '--without-monitor', '--port-offset', str(self.args.offset),
                   '--kv.config', str(kv), '--tiflash.config', str(tf), '--tiflash.binpath', str(wrapper)]
        for name, flag in [('tidb', 'db'), ('tikv', 'kv'), ('pd', 'pd')]:
            command += [f'--{flag}.binpath', str(self.components[name])]
        self.result['command'] = command
        self.env['SCHEMA_WATERMARK_TIFLASH_BINARY'] = str(self.binary)
        self.env['SCHEMA_WATERMARK_RESTART_STATE'] = str(self.state)
        self.playground_log = (self.output / 'playground.log').open('w')
        self.proc = subprocess.Popen(command, env=self.env, stdout=self.playground_log,
                                     stderr=subprocess.STDOUT, start_new_session=True)
        wait(lambda: self.sql('SELECT 1') == '1', 'TiDB ready')
        self.pid = wait(self.find_pid, 'TiFlash PID')
        wait(lambda: self.debug('SELECT 1') == '1', 'TiFlash debug port')
        args = Path(f'/proc/{self.pid}/cmdline').read_bytes().split(b'\0')
        config = next(a.decode().split('=', 1)[1] for a in args if a.startswith(b'--config-file='))
        overrides = [a.decode() for a in args if a.startswith(b'--logger.log=')]
        self.tf_log = Path(overrides[0].split('=', 1)[1]) if overrides else Path(config).with_name('tiflash.log')
        self.result['tiflash_arguments'] = [a.decode() for a in args if a]
        arguments = self.result['tiflash_arguments']
        for index, argument in enumerate(arguments):
            if argument.startswith('--status.metrics_port='):
                self.metric_port = int(argument.split('=', 1)[1])
                break
            if argument == '--status.metrics_port':
                self.metric_port = int(arguments[index + 1])
                break
        else:
            raise RuntimeError('Cannot determine the actual TiFlash metrics port from process arguments')
        self.result['actual_metrics_port'] = self.metric_port
        wait(self.metrics, 'actual TiFlash metrics endpoint')
        self.result['tiflash_log'] = str(self.tf_log)
        (self.output / 'generated-tiflash.toml').write_text(Path(config).read_text())
        self.old_gc = self.sql("SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME='tikv_gc_life_time'")
        self.sql("UPDATE mysql.tidb SET VARIABLE_VALUE='1h' WHERE VARIABLE_NAME='tikv_gc_life_time';"
                 'CREATE DATABASE schema_matrix; CREATE TABLE schema_matrix.seed(n INT PRIMARY KEY);'
                 'INSERT INTO schema_matrix.seed VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)')
        self.debug("DBGInvoke enable_schema_sync_service('false')")
        self.result['initial_store_ids'] = wait(self.store_ids, 'TiFlash store registration')
        assert len(self.result['initial_store_ids']) == 1
        self.save()
        print(json.dumps({'started': self.args.tag, 'pid': self.pid}, ensure_ascii=False), flush=True)

    def create_table(self, table, directory):
        self.sql(f'CREATE TABLE schema_matrix.{table} ('
                 'c_first DECIMAL(18,2) NULL,id BIGINT NOT NULL PRIMARY KEY CLUSTERED,'
                 'c_middle DECIMAL(18,2) NULL,c_last DECIMAL(18,2) NULL);'
                 f'INSERT INTO schema_matrix.{table} SELECT IF(MOD(i,7)=0,NULL,i/100),i,'
                 'IF(MOD(i,11)=0,NULL,3*i/100),IF(MOD(i,13)=0,NULL,(7*i+11)/100) FROM '
                 '(SELECT 1+a.n+10*b.n+100*c.n+1000*d.n AS i FROM schema_matrix.seed a,'
                 'schema_matrix.seed b,schema_matrix.seed c,schema_matrix.seed d) s;'
                 f'ALTER TABLE schema_matrix.{table} SET TIFLASH REPLICA 1')
        wait(lambda: self.sql("SELECT AVAILABLE FROM information_schema.tiflash_replica WHERE "
                              f"TABLE_SCHEMA='schema_matrix' AND TABLE_NAME='{table}'") == '1', 'replica available')
        wait(lambda: self.sql(f'SELECT COUNT(*) FROM schema_matrix.{table}', 'tiflash') == '10000', 'initial rows')
        self.debug(f'DBGInvoke refresh_table_schema(schema_matrix, {table})')
        directory.mkdir(exist_ok=True)
        evidence = self.compare(table, COLS, directory, 'initial', updated=False)
        evidence['table_id'] = int(self.sql('SELECT TABLE_ID FROM information_schema.tiflash_replica WHERE '
                                          f"TABLE_SCHEMA='schema_matrix' AND TABLE_NAME='{table}'"))
        evidence['explain'] = self.sql(f'EXPLAIN SELECT * FROM schema_matrix.{table}', 'tiflash')
        assert 'tiflash' in evidence['explain'].lower()
        (directory / 'initial-show-create.txt').write_text(self.sql(f'SHOW CREATE TABLE schema_matrix.{table}') + '\n')
        return evidence

    def rows(self, table, columns, engine):
        select = ','.join('id' if x == 'id' else f'CAST({x} AS CHAR)' for x in columns)
        return self.sql(f'SELECT {select} FROM schema_matrix.{table} ORDER BY id', engine)

    def compare(self, table, columns, directory, stage, updated):
        rows = {engine: self.rows(table, columns, engine) for engine in ('tikv', 'tiflash')}
        for engine, data in rows.items():
            (directory / f'{stage}-{engine}-rows.tsv').write_text(data + '\n')
        assert rows['tikv'] == rows['tiflash'], f'Engine rows differ: {table}/{stage}'
        lines = rows['tikv'].splitlines()
        assert len(lines) == 10000, len(lines)
        nulls = {c: 0 for c in columns if c != 'id'}
        for expected_id, line in enumerate(lines, 1):
            fields = dict(zip(columns, line.split('\t')))
            assert int(fields['id']) == expected_id
            for name, multiplier, addend, mod in [('c_first', 1, 0, 7), ('c_middle', 3, 0, 11), ('c_last', 7, 11, 13)]:
                if name not in fields:
                    continue
                value = fields[name]
                if expected_id % mod == 0:
                    assert value == 'NULL', (table, name, expected_id, value)
                    nulls[name] += 1
                else:
                    expected = Decimal(multiplier * expected_id + addend) / 100 + int(updated)
                    assert Decimal(value) == expected, (table, name, expected_id, value, expected)
        return {'rows': 10000, 'columns': columns, 'sha256': sha(rows['tikv'].encode()),
                'mismatches': 0, 'null_counts': nulls}

    def update(self, table):
        self.sql(f'UPDATE schema_matrix.{table} SET c_first=c_first+1,c_middle=c_middle+1,c_last=c_last+1')

    def ddl(self, table, operation, target):
        action = f'MODIFY COLUMN {target} DECIMAL(20,3) NULL DEFAULT NULL' if operation == 'modify' else f'DROP COLUMN {target}'
        statement = f'ALTER TABLE schema_matrix.{table} {action}'
        self.sql(statement)
        return statement

    def stable_metrics(self, directory):
        previous = self.metrics()
        # Counters must stay unchanged for three consecutive one-second polls.
        stable = 0
        end = time.monotonic() + 45
        while time.monotonic() < end:
            time.sleep(1)
            current = self.metrics()
            if all(v == 0 for v in deltas(previous, current).values()):
                stable += 1
                if stable >= 3:
                    (directory / 'metrics-after.txt').write_text(current)
                    return current
            else:
                stable = 0
            previous = current
        raise RuntimeError('Counters did not stabilize')

    def evidence(self, directory, before, offset, table_id):
        after = self.stable_metrics(directory)
        data = self.tf_log.read_bytes()[offset:].decode(errors='replace')
        (directory / 'tiflash-phase.log').write_text(data)
        snapshots = len(re.findall(r'(?:Begin|Finish) apply snapshot', data))
        assert snapshots == 0, 'Snapshot substituted for log replay'
        syncs = len([line for line in data.splitlines() if 'sync schema cost' in line
                     and re.search(r'table_id=' + str(table_id) + r'(?:\D|$)', line)])
        proof = [line for line in data.splitlines() if 'Skip historical extra column' in line]
        return {'counter_deltas': deltas(before, after), 'target_decode_sync_log_count': syncs,
                'snapshot_apply_events': snapshots, 'skip_log_lines': proof}

    def controlled(self, table, operation, target, directory):
        initial = self.create_table(table, directory)
        before = self.metrics()
        (directory / 'metrics-before.txt').write_text(before)
        offset = self.tf_log.stat().st_size
        started = time.monotonic()
        if operation == 'modify':
            os.kill(self.pid, signal.SIGSTOP)
            self.paused = True
            try:
                statement = self.ddl(table, operation, target)
            finally:
                os.kill(self.pid, signal.SIGCONT)
                self.paused = False
        else:
            self.debug('DBGInvoke enable_fail_point(pause_before_apply_raft_cmd)')
            self.failpoint = True
            try:
                self.update(table)
                statement = self.ddl(table, operation, target)
                # Publish the real complete TableInfo while the first apply is gated.
                refreshed = self.debug(f'DBGInvoke refresh_table_schema(schema_matrix, {table})')
                assert 'refreshed' in refreshed
                (directory / 'schema-refresh.txt').write_text(refreshed + '\n')
            finally:
                self.debug('DBGInvoke disable_fail_point(pause_before_apply_raft_cmd)')
                self.failpoint = False
        if self.args.baseline:
            wait(lambda: deltas(before, self.metrics())['raft_decode'] > 1, 'baseline repeated schema sync')
        else:
            wait(lambda: deltas(before, self.metrics())['skipped_extra_columns'] > 0, 'watermark skips')
        columns = [x for x in COLS if operation != 'drop' or x != target]
        expected = self.rows(table, columns, 'tikv')
        wait(lambda: self.rows(table, columns, 'tiflash') == expected, 'full rows catch up')
        final = self.compare(table, columns, directory, 'after-ddl', updated=operation == 'drop')
        result = self.evidence(directory, before, offset, initial['table_id'])
        if not self.args.baseline:
            assert result['counter_deltas']['raft_decode'] <= 1, result
            assert result['counter_deltas']['skipped_extra_columns'] > 0, result
        result.update(initial=initial, final=final, ddl=statement, catchup_seconds=time.monotonic()-started, passed=True)
        (directory / 'show-create.txt').write_text(self.sql(f'SHOW CREATE TABLE schema_matrix.{table}') + '\n')
        (directory / 'ddl-jobs.txt').write_text(self.sql('ADMIN SHOW DDL JOBS 10') + '\n')
        (directory / 'result.json').write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n')
        return result

    def restart_case(self, table, operation, target, mode, directory):
        initial = self.create_table(table, directory)
        old_pid = self.pid
        old_args = Path(f'/proc/{old_pid}/cmdline').read_bytes()
        old_cwd = os.readlink(f'/proc/{old_pid}/cwd')
        old_metrics = self.metrics()
        (directory / 'metrics-before-restart.txt').write_text(old_metrics)
        offset = self.tf_log.stat().st_size
        self.gate.touch()
        wrapper_pid = int((self.state / 'wrapper.pid').read_text())
        requested = signal.SIGUSR1 if mode == 'graceful' else signal.SIGUSR2
        os.kill(wrapper_pid, requested)
        exited = wait(lambda: next((e for e in self.events() if e['event'] == 'exit' and e['pid'] == old_pid), None),
                      'actual TiFlash exit', timeout=65)
        assert exited['returncode'] == (0 if mode == 'graceful' else -signal.SIGKILL), exited
        assert not Path(f'/proc/{old_pid}').exists()
        # The real service is down here. TiKV accepts writes and DDL; retained logs
        # are replayed by the replacement process in the same data directory.
        self.update(table)
        statement = self.ddl(table, operation, target)
        columns = [x for x in COLS if operation != 'drop' or x != target]
        expected = self.rows(table, columns, 'tikv')
        started = time.monotonic()
        self.gate.unlink()
        self.pid = wait(lambda: p if (p := self.find_pid()) and p != old_pid else None, 'replacement PID')
        wait(lambda: self.debug('SELECT 1') == '1', 'replacement debug port')
        wait(lambda: self.rows(table, columns, 'tiflash') == expected, 'restarted full rows', timeout=300)
        self.debug("DBGInvoke enable_schema_sync_service('false')")
        assert Path(f'/proc/{self.pid}/cmdline').read_bytes() == old_args
        assert os.readlink(f'/proc/{self.pid}/cwd') == old_cwd
        assert wait(self.store_ids, 'TiFlash store registration') == self.result['initial_store_ids']
        recovered = self.compare(table, columns, directory, 'recovered', updated=True)
        recovery = self.evidence(directory, '', offset, initial['table_id'])
        assert not any(e['event'] == 'shutdown_timeout' for e in self.events())
        result = {'mode': mode, 'old_pid': old_pid, 'new_pid': self.pid,
                  'old_returncode': exited['returncode'], 'same_arguments': True, 'same_cwd': True,
                  'store_ids': self.store_ids(), 'initial': initial, 'recovered': recovered,
                  'ddl_while_down': statement, 'recovery_seconds': time.monotonic()-started,
                  'recovery_metrics': recovery, 'passed': False}
        # Verify future writes in the recovered process before certifying a fresh
        # DDL backlog. Changing a surviving value guarantees visible progress.
        survivor = next(x for x in BUSINESS if x in columns)
        self.sql(f'UPDATE schema_matrix.{table} SET {survivor}=12345.67 WHERE id=1')
        wait(lambda: self.sql(f'SELECT CAST({survivor} AS CHAR) FROM schema_matrix.{table} WHERE id=1', 'tiflash') ==
             self.sql(f'SELECT CAST({survivor} AS CHAR) FROM schema_matrix.{table} WHERE id=1', 'tikv'), 'post-restart write')
        result['post_restart_write'] = {'id': 1, 'column': survivor, 'value': '12345.67', 'matched': True}
        result['post_restart_ddl'] = self.controlled(table+'_post', operation, target, directory/'post-restart-ddl')
        result['passed'] = True
        (directory / 'result.json').write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n')
        return result

    def reverse_case(self):
        directory = self.output / 'new-column-reverse'
        directory.mkdir()
        self.sql('CREATE TABLE schema_matrix.newcol(id BIGINT PRIMARY KEY CLUSTERED,c0 TINYINT);'
                 'INSERT INTO schema_matrix.newcol VALUES(1,10); ALTER TABLE schema_matrix.newcol SET TIFLASH REPLICA 1')
        wait(lambda: self.sql('SELECT COUNT(*) FROM schema_matrix.newcol', 'tiflash') == '1', 'reverse initial')
        self.debug('DBGInvoke refresh_table_schema(schema_matrix, newcol)')
        before = self.metrics()
        (directory / 'metrics-before.txt').write_text(before)
        os.kill(self.pid, signal.SIGSTOP)
        self.paused = True
        try:
            self.sql('ALTER TABLE schema_matrix.newcol ADD COLUMN c1 INT NULL;'
                     'INSERT INTO schema_matrix.newcol VALUES(2,20,424242)')
        finally:
            os.kill(self.pid, signal.SIGCONT)
            self.paused = False
        wait(lambda: self.sql('SELECT COUNT(*),SUM(c1) FROM schema_matrix.newcol', 'tiflash') == '2\t424242', 'new column value')
        after = self.stable_metrics(directory)
        result = {'aggregate': '2\t424242', 'counter_deltas': deltas(before, after)}
        assert result['counter_deltas']['raft_decode'] >= 1
        if not self.args.baseline:
            assert result['counter_deltas']['beyond_watermark'] >= 1
        for engine in ('tikv', 'tiflash'):
            (directory / f'{engine}-rows.tsv').write_text(self.sql('SELECT * FROM schema_matrix.newcol ORDER BY id', engine)+'\n')
        assert (directory/'tikv-rows.tsv').read_bytes() == (directory/'tiflash-rows.tsv').read_bytes()
        result['passed'] = True
        self.result['reverse_case'] = result
        (directory/'result.json').write_text(json.dumps(result,indent=2)+'\n')

    def run(self):
        try:
            self.start()
            modes = ['normal'] if self.args.baseline else ['normal', 'graceful', 'crash']
            for mode in modes:
                for operation in ['modify', 'drop']:
                    for position, target in zip(['first', 'middle', 'last'], BUSINESS):
                        name = f'{mode}_{operation}_{position}'
                        directory = self.output/name
                        case = {'name': name, 'mode': mode, 'operation': operation, 'position': position, 'target': target}
                        self.result['cases'].append(case)
                        self.save()
                        print(json.dumps({'case_start':name,'pid':self.pid}),flush=True)
                        if mode == 'normal':
                            case['result'] = self.controlled(name,operation,target,directory)
                        else:
                            case['result'] = self.restart_case(name,operation,target,mode,directory)
                        case['passed'] = True
                        self.save()
                        print(json.dumps({'case_pass':name,'result':case['result']},ensure_ascii=False),flush=True)
            self.reverse_case()
            self.result['passed'] = True
        except BaseException:
            self.result['error'] = traceback.format_exc()
            raise
        finally:
            self.cleanup()

    def cleanup(self):
        try:
            if self.paused and self.pid:
                os.kill(self.pid, signal.SIGCONT)
            self.gate.unlink(missing_ok=True)
            if self.failpoint:
                self.debug('DBGInvoke disable_fail_point(pause_before_apply_raft_cmd)')
            if self.old_gc is not None:
                self.sql(f"UPDATE mysql.tidb SET VARIABLE_VALUE='{self.old_gc}' WHERE VARIABLE_NAME='tikv_gc_life_time'")
                self.result['gc_restored'] = self.sql("SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME='tikv_gc_life_time'") == self.old_gc
        except Exception:
            self.result['cleanup_error'] = traceback.format_exc()
        try:
            if self.pid and Path(f'/proc/{self.pid}/cmdline').exists():
                self.debug("DBGInvoke enable_schema_sync_service('true')")
                self.result['schema_sync_reenabled'] = True
        except Exception:
            self.result['schema_sync_restore_error'] = traceback.format_exc()
        # TiUP can leave a nested server behind. Stop this run's wrapper first
        # so it forwards SIGTERM and cannot respawn the real server.
        wrapper_file = self.state / 'wrapper.pid'
        candidates = [int(wrapper_file.read_text())] if wrapper_file.exists() else []
        if self.pid:
            candidates.append(self.pid)
        for pid in candidates:
            cmdline = Path(f'/proc/{pid}/cmdline')
            if cmdline.exists() and self.args.tag.encode() in cmdline.read_bytes():
                try:
                    os.kill(pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        if self.proc is not None:
            if self.proc.poll() is None:
                os.killpg(self.proc.pid, signal.SIGINT)
                try:
                    self.proc.wait(timeout=50)
                except subprocess.TimeoutExpired:
                    os.killpg(self.proc.pid, signal.SIGTERM)
                    self.proc.wait(timeout=15)
            self.result['playground_exit'] = self.proc.returncode
        deadline = time.monotonic() + 45
        for pid in candidates:
            cmdline = Path(f'/proc/{pid}/cmdline')
            while cmdline.exists() and cmdline.read_bytes() and time.monotonic() < deadline:
                time.sleep(0.2)
            if cmdline.exists() and self.args.tag.encode() in cmdline.read_bytes():
                os.kill(pid, signal.SIGKILL)
                self.result.setdefault('forced_cleanup_pids', []).append(pid)
        self.result['tiflash_process_gone'] = all(
            not Path(f'/proc/{pid}/cmdline').exists() or not Path(f'/proc/{pid}/cmdline').read_bytes()
            for pid in candidates)
        self.result['explicit_shutdown_pids'] = candidates
        self.result['finished_at_unix'] = time.time()
        if (self.result.get('cleanup_error') or self.result.get('schema_sync_restore_error')
                or self.result.get('forced_cleanup_pids') or not self.result['tiflash_process_gone']
                or (self.old_gc is not None and not self.result.get('gc_restored'))
                or (self.proc is not None and self.result.get('playground_exit') != 0)):
            self.result['passed'] = False
        self.save()
        print(json.dumps({'finished':self.args.tag,'passed':self.result['passed'],
                          'gc_restored':self.result.get('gc_restored'),'playground_exit':self.result.get('playground_exit')},ensure_ascii=False),flush=True)


if __name__ == '__main__':
    runner = Matrix(make_parser(__doc__).parse_args())
    runner.run()
    raise SystemExit(0 if runner.result['passed'] else 1)
