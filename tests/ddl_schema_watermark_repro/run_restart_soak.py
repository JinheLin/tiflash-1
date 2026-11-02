#!/usr/bin/env python3
"""Execute the restart soak plan on one new TiUP cluster; --mode smoke is a short rehearsal."""
import argparse
from collections import Counter, deque
from contextlib import contextmanager
from decimal import Decimal
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import random
import re
import shutil
import signal
import socket
import stat
import threading
import time
import traceback

import run_matrix as matrix
from soak_workload import Journal, Workload, replay

HERE = Path(__file__).resolve().parent
GIB = 1024 ** 3
COMBINATIONS = [(mode, ddl, column) for mode in ('graceful', 'crash')
                for ddl in ('modify', 'drop') for column in matrix.BUSINESS]
SMOKE_COMBINATIONS = [('crash', 'drop', 'c_last'), ('crash', 'modify', 'c_middle'),
                      ('graceful', 'modify', 'c_last'), ('graceful', 'drop', 'c_last')]
RETIRED_TABLE_ERROR = re.compile(
    r'\[ERROR\] \[TiDBSchemaSyncer\.cpp:\d+\] '
    r'\["Can not find related database_id and logical_table_id from table_id_map, '
    r'sync table schema fail\. physical_table_id=(\d+)"\] \[source="keyspace=4294967295"\]')


class NotCovered(RuntimeError):
    pass


def duration(value):
    match = re.fullmatch(r'(\d+(?:\.\d+)?)([smh]?)', value)
    if not match:
        raise argparse.ArgumentTypeError('Use a duration such as 24h, 30m or 20s')
    return float(match[1]) * {'': 1, 's': 1, 'm': 60, 'h': 3600}[match[2]]


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.tmp')
    with temporary.open('w') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, default=str)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def directory_bytes(root):
    """Sample a live tree while compaction and log rotation remove descendants."""
    root = Path(root)
    root.stat()  # Losing the test root is not an expected compaction race.

    def on_error(error):
        if not isinstance(error, FileNotFoundError) or Path(error.filename) == root:
            raise error

    size = 0
    for directory, _, filenames in os.walk(root, onerror=on_error):
        for filename in filenames:
            try:
                info = (Path(directory) / filename).stat()
            except FileNotFoundError:
                continue
            if stat.S_ISREG(info.st_mode):
                size += info.st_size
    return size


def ephemeral_port_range():
    path = Path('/proc/sys/net/ipv4/ip_local_port_range')
    return list(map(int, path.read_text().split())) if path.exists() else None


def check_listener_ports(ports, ephemeral):
    if any(not 1 <= port <= 65535 for port in ports):
        raise ValueError('Listener port is out of range')
    if len(set(ports)) != len(ports):
        raise ValueError('Listener ports overlap')
    if ephemeral:
        conflicts = [port for port in ports if ephemeral[0] <= port <= ephemeral[1]]
        if conflicts:
            raise ValueError(f'Listener ports {conflicts} overlap ephemeral port range {ephemeral}; '
                             'choose a lower --offset and --debug-port (for example 6000 and 29000)')


def identity(pid):
    proc = Path(f'/proc/{pid}')
    stat = (proc / 'stat').read_text().rsplit(')', 1)[1].split()
    return dict(pid=pid, starttime=stat[19], cwd=os.readlink(proc / 'cwd'),
                arguments=[p.decode() for p in (proc / 'cmdline').read_bytes().split(b'\0') if p])


def signal_checked(process, sig):
    if identity(process['pid']) != process:
        raise RuntimeError('Process identity changed before signal delivery')
    os.kill(process['pid'], sig)


def token(value):
    if value is None:
        return 'NULL'
    if isinstance(value, Decimal):
        return format(value, 'f')
    return str(value)


def row_bytes(row):
    return ('\t'.join(token(value) for value in row) + '\n').encode()


def round_schedule(seed):
    randomizer = random.Random(seed)
    while True:
        batch = list(COMBINATIONS)
        randomizer.shuffle(batch)
        yield from batch


def accepts_24h(mode, elapsed, combinations, reverse_checks, active_fraction, proof_hours):
    return (mode == 'soak' and elapsed >= 86400 and reverse_checks >= 10 and active_fraction >= 0.5
            and all(combinations.get('/'.join(c), 0) >= 10 for c in COMBINATIONS)
            and all(proof_hours.get(hour, 0) > 0 for hour in range(24)))


def summarize_rows(rows, path, expected=None):
    """Save complete canonical values, not just a COUNT/SUM comparison."""
    count, mismatches, examples = 0, 0, []
    digest, partitions = hashlib.sha256(), {}
    null_counts = None
    iterator = iter(expected) if expected is not None else None
    sentinel = object()
    with gzip.open(path, 'wb') as output:
        for row in rows:
            encoded = row_bytes(row)
            output.write(encoded)
            digest.update(encoded)
            key = int(row[0])
            partitions.setdefault(key // 10000, hashlib.sha256()).update(encoded)
            if null_counts is None:
                null_counts = [0] * len(row)
            for index, value in enumerate(row):
                null_counts[index] += value is None
            if iterator is not None:
                wanted = next(iterator, sentinel)
                if wanted is sentinel or encoded != row_bytes(wanted):
                    mismatches += 1
                    if len(examples) < 20:
                        examples.append(dict(actual=list(row), expected=None if wanted is sentinel else list(wanted)))
            count += 1
        if iterator is not None:
            for wanted in iterator:
                mismatches += 1
                if len(examples) < 20:
                    examples.append(dict(actual=None, expected=list(wanted)))
    # Verify the archive and its uncompressed checksum before certifying it.
    verified = hashlib.sha256()
    with gzip.open(path, 'rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            verified.update(chunk)
    if verified.hexdigest() != digest.hexdigest():
        raise RuntimeError('Corrupt row archive')
    return dict(rows=count, sha256=digest.hexdigest(), mismatches=mismatches, examples=examples,
                null_counts=null_counts or [], partitions={k: v.hexdigest() for k, v in partitions.items()})


class LogCapture:
    """Follow rotation by inode, archive all lines, and retain phase evidence."""
    def __init__(self, path, output):
        self.path, self.output = Path(path), Path(output)
        self.output.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.source = None
        self.archive = None
        self.archive_hour = None
        self.phase = None
        self.summary = None
        self.error = None
        self.last_proof = time.monotonic()
        self.proofs = Counter()
        self.started = time.monotonic()
        self.watch_errors = False
        self.retired_tables = {}
        self.retired_error_counts = Counter()
        self.pending_line = b''
        self.thread = threading.Thread(target=self._run, name='soak-log-capture', daemon=True)

    def start(self):
        self.thread.start()

    def register_retired_table(self, record):
        with self.lock:
            # Do not retroactively forgive errors already emitted before DROP.
            self._drain()
            self.retired_tables[record['table_id']] = dict(record)
            write_json(self.output / 'retired-tables.json', self.retired_tables)

    def _line(self, line):
        hour = int((time.monotonic() - self.started) // 3600)
        if hour != self.archive_hour:
            if self.archive:
                self.archive.close()
            self.archive = gzip.open(self.output / f'tiflash-{hour:04d}.log.gz', 'ab')
            self.archive_hour = hour
        self.archive.write(line)
        text = line.decode(errors='replace')
        is_error = re.search(r'\[(?:ERROR|FATAL)\]', text) is not None
        match = RETIRED_TABLE_ERROR.search(text) if '[FATAL]' not in text else None
        retired = self.retired_tables.get(int(match[1])) if match else None
        if retired is not None:
            # Restart can replay leftover Regions of a table this test dropped.
            # This one nonfatal mapping lookup error is cleanup evidence; all
            # other errors, including the same message for live/unknown IDs, fail.
            self.retired_error_counts[retired['table_id']] += 1
            with (self.output / 'retired-table-schema-errors.jsonl').open('a') as output:
                output.write(json.dumps(dict(observed_at=time.time(), dropped=retired,
                                             line=text.rstrip())) + '\n')
        elif self.watch_errors and is_error and self.error is None:
            self.error = 'Unexpected TiFlash ERROR/FATAL: ' + text.rstrip()
        if self.phase:
            self.phase.write(line)
            if retired is not None:
                self.summary['retired_table_errors'].append(text.rstrip())
            elif is_error:
                self.summary['errors'].append(text.rstrip())
            if re.search(r'(?:Begin|Finish) apply snapshot', text, re.I):
                self.summary['snapshots'].append(text.rstrip())
            if 'sync schema cost' in text:
                match = re.search(r'table_id=(\d+)', text)
                if match:
                    self.summary['syncs'][match[1]] += 1
            if 'Skip historical extra column' in text:
                self.summary['proofs'].append(text.rstrip())
                self.last_proof = time.monotonic()
                self.proofs[hour] += 1

    def _drain(self):
        if self.source is None and self.path.exists():
            self.source = self.path.open('rb')
        if self.source:
            for data in self.source:
                line = self.pending_line + data
                if line.endswith(b'\n'):
                    self._line(line)
                    self.pending_line = b''
                else:
                    self.pending_line = line
            if self.path.exists() and os.fstat(self.source.fileno()).st_ino != self.path.stat().st_ino:
                self.source.close()
                self.source = self.path.open('rb')
                for line in self.source:
                    self.pending_line += line
                    if self.pending_line.endswith(b'\n'):
                        self._line(self.pending_line)
                        self.pending_line = b''

    def _run(self):
        try:
            while not self.stop_event.wait(0.2):
                with self.lock:
                    self._drain()
        except Exception:
            self.error = traceback.format_exc()

    def begin(self, path):
        with self.lock:
            self._drain()
            if self.phase:
                raise RuntimeError('Overlapping log phases')
            self.summary = dict(errors=[], retired_table_errors=[], snapshots=[], proofs=[], syncs=Counter())
            self.phase = gzip.open(path, 'wb')

    def end(self):
        with self.lock:
            self._drain()
            self.phase.close()
            self.phase = None
            return self.summary

    def check(self):
        if self.error:
            raise RuntimeError(self.error)
        with self.lock:
            if self.phase and self.summary['errors']:
                raise RuntimeError('Internal TiFlash error: ' + self.summary['errors'][0])

    def stop(self):
        self.stop_event.set()
        self.thread.join(timeout=5)
        with self.lock:
            self._drain()
            for stream in (self.phase, self.archive, self.source):
                if stream:
                    stream.close()
            self.phase = None


class RestartSoak(matrix.Matrix):
    def __init__(self, args):
        super().__init__(args)
        self.journal = Journal(self.output / 'events.jsonl')
        self.phase, self.round_id, self.generation = 'Preparing', 0, 0
        self.round_deadline = None
        self.interrupted = threading.Event()
        self.monitor_stop = threading.Event()
        self.monitor_error = None
        self.monitor_thread = None
        self.workload, self.logs = None, None
        self.server, self.wrapper = None, None
        self.started = None
        self.table_ids = {}
        self.last_round_tables = []
        self.round_results, self.checks = [], []
        self.resources = deque()
        self.stable_resources = []
        self.alerts = []
        self.mode_counts = Counter()
        self.result.update(mode=args.mode, acceptance_24h=False, status='running', seed=args.seed,
                           parameters=vars(args), combinations={}, reverse_checks=0)
        scripts = self.output / 'scripts'
        scripts.mkdir()
        for name in ('run_restart_soak.py', 'soak_workload.py', 'run_matrix.py', 'restartable_tiflash.py'):
            shutil.copy2(HERE / name, scripts / name)
        self.wrapper_path = scripts / 'restartable_tiflash.py'
        for directory in ('metrics', 'rounds', 'hourly', 'failures', 'effective-configs'):
            (self.output / directory).mkdir()
        self.save()

    def save(self):
        write_json(self.output / 'result.json', self.result)

    def transition(self, phase, **fields):
        self.phase = phase
        self.journal.append('transition', phase=phase, round_id=self.round_id, generation=self.generation, **fields)
        write_json(self.output / 'state.json', dict(phase=phase, round_id=self.round_id,
                                                   generation=self.generation, time=time.time(), **fields))
        print(json.dumps(dict(phase=phase, round=self.round_id, **fields), default=str), flush=True)
        self.guard()

    def guard(self):
        if self.interrupted.is_set():
            raise InterruptedError('Controller interrupted; refusing to replay uncertain stages')
        if self.monitor_error:
            raise RuntimeError(self.monitor_error)
        if self.workload:
            self.workload.check()
        if self.logs:
            self.logs.check()
        if self.round_deadline and time.monotonic() > self.round_deadline:
            raise TimeoutError('Round exceeded its time budget')
        if self.proc and self.proc.poll() is not None:
            raise RuntimeError('TiUP exited unexpectedly')

    def wait(self, function, description, timeout):
        deadline = time.monotonic() + timeout
        error = None
        while time.monotonic() < deadline:
            self.guard()
            try:
                value = function()
                if value:
                    return value
            except (OSError, RuntimeError) as caught:
                error = str(caught)
            time.sleep(0.2)
        raise TimeoutError(f'{description}: {error}')

    def idle(self, seconds):
        deadline = time.monotonic() + max(0, seconds)
        while time.monotonic() < deadline:
            self.guard()
            time.sleep(min(0.2, max(0, deadline - time.monotonic())))

    def connect(self, streaming=False):
        import pymysql
        from pymysql.constants import CLIENT
        connection = pymysql.connect(host='127.0.0.1', port=self.port, user='root', charset='utf8mb4',
                                     autocommit=True, connect_timeout=5, read_timeout=30, write_timeout=30,
                                     client_flag=CLIENT.MULTI_STATEMENTS,
                                     cursorclass=pymysql.cursors.SSCursor if streaming else pymysql.cursors.Cursor)
        with connection.cursor() as cursor:
            cursor.execute("SET tidb_isolation_read_engines='tikv'")
        return connection

    def sql(self, query, engine=None):
        self.journal.append('sql', query=query, engine=engine, round_id=self.round_id, phase=self.phase)
        try:
            connection = self.connect()
        except Exception as error:
            raise RuntimeError(str(error)) from error
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"SET tidb_isolation_read_engines='{engine or 'tikv'}'")
                cursor.execute('SET tidb_allow_mpp=1')
                cursor.execute(f'SET tidb_enforce_mpp={int(engine == "tiflash")}')
                cursor.execute(query)
                result = []
                while True:
                    if cursor.description:
                        result.extend(cursor.fetchall())
                    if not cursor.nextset():
                        break
                return '\n'.join('\t'.join(token(v) for v in row) for row in result)
        except Exception as error:
            raise RuntimeError(str(error)) from error
        finally:
            connection.close()

    def debug(self, query):
        self.journal.append('debug', query=query, round_id=self.round_id, phase=self.phase)
        return super().debug(query)

    def signal_process(self, pid, sig):
        expected = next((p for p in (self.server, self.wrapper) if p and p['pid'] == pid), None)
        if expected is None:
            # Startup can fail before _identify(); require both our unique tag
            # and an exact known executable/wrapper path before adopting it.
            expected = identity(pid)
            arguments = expected['arguments']
            if (self.args.tag not in ' '.join(arguments)
                    or not any(path in arguments for path in (str(self.binary), str(self.wrapper_path)))):
                raise RuntimeError('Refusing cleanup signal for an unrecognized process')
        signal_checked(expected, sig)

    def write_configs(self, kv, tf):
        super().write_configs(kv, tf)
        with kv.open('a') as stream:
            stream.write('\n[log.file]\nmax-size=100\nmax-backups=10\n')
        text = tf.read_text().replace('level="debug"', 'level="debug"\nsize="100M"\ncount=10')
        text = text.replace('ddl_sync_interval_seconds=3600', 'ddl_sync_interval_seconds=10')
        tf.write_text(text)
        (self.output / 'tidb.toml').write_text('[log.file]\nmax-size=100\nmax-backups=10\n')
        (self.output / 'pd.toml').write_text('[log.file]\nmax-size=100\nmax-backups=10\n')

    def playground_options(self):
        return ['--db.config', str(self.output / 'tidb.toml'), '--pd.config', str(self.output / 'pd.toml')]

    def _identify(self):
        self.server = identity(self.pid)
        options = dict(a[2:].split('=', 1) for a in self.server['arguments'] if a.startswith('--') and '=' in a)
        self.data_root = Path(options['path']).parent
        self.cluster_root = self.data_root.parent
        self.metric_port = int(options['status.metrics_port'])
        ports = dict(debug=self.debug_port, metrics=self.metric_port)
        for key in ('flash.service_addr', 'flash.proxy.addr', 'flash.proxy.status-addr'):
            ports[key] = int(options[key].rsplit(':', 1)[1])
        # TiUP may pick a different metrics port; validate the actual arguments,
        # not only the requested offset. Listeners must remain outside the range
        # used for outbound connections while a restarted server is offline.
        check_listener_ports(list(ports.values()), self.args.ephemeral_port_range)
        self.result['listener_ports'] = ports
        self.result['ephemeral_port_range'] = self.args.ephemeral_port_range
        self.wrapper = identity(int((self.state / 'wrapper.pid').read_text()))
        if self.args.tag not in ' '.join(self.server['arguments']):
            raise RuntimeError('Server belongs to another TiUP tag')
        return self.server

    def metadata(self, table, directory, stage):
        text = self.get(f'http://127.0.0.1:{10080 + self.args.offset}/schema/schema_matrix/{table}')
        info = json.loads(text)
        write_json(directory / f'{stage}-table-info.json', info)
        if self.table_ids.setdefault(table, info['id']) != info['id']:
            raise RuntimeError('Fixture table ID changed: ' + table)
        return info

    def drop_fixture(self, table):
        table_id = self.table_ids[table]
        self.sql(f'DROP TABLE schema_matrix.{table}')
        # A failed SQL DROP must never authorize the log exception.
        record = dict(table_id=table_id, table=f'schema_matrix.{table}',
                      dropped_at=time.time(), round_id=self.round_id)
        self.logs.register_retired_table(record)
        self.journal.append('table_dropped', **record)

    def _configure_start(self):
        super().start()
        self._identify()
        if self.args.build_manifest:
            receipt = json.loads(self.args.build_manifest.read_text())
            if receipt.get('head') != self.result['actual_binary_commit'] or not receipt.get('source_sha256'):
                raise RuntimeError('Build manifest must contain matching head and source_sha256')
            shutil.copy2(self.args.build_manifest, self.output / 'source-build-manifest.json')
            self.result['build_manifest_sha256'] = matrix.file_sha(self.args.build_manifest)
        self.result['harness_sha256'] = {p.name: matrix.file_sha(p) for p in (self.output / 'scripts').glob('*.py')}
        self.logs = LogCapture(self.tf_log, self.output / 'logs')
        self.logs.start()
        self.binary_stats = {str(path): (path.stat().st_size, path.stat().st_mtime_ns)
                             for path in [self.binary, *self.binary.parent.glob('*.so*')] if path.is_file()}
        for name in ('tikv.toml', 'tiflash.toml', 'tidb.toml', 'pd.toml', 'generated-tiflash.toml'):
            shutil.copy2(self.output / name, self.output / 'effective-configs' / name)
        for path in self.cluster_root.rglob('*.toml'):
            destination = self.output / 'effective-configs' / 'generated' / path.relative_to(self.cluster_root)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, destination)
        self.sql('SET GLOBAL tidb_row_format_version=2; SET GLOBAL tidb_ddl_reorg_worker_cnt=1; '
                 'SET GLOBAL tidb_ddl_reorg_batch_size=256')
        settings = self.sql('SELECT @@GLOBAL.tidb_row_format_version,@@GLOBAL.tidb_ddl_reorg_worker_cnt,'
                            '@@GLOBAL.tidb_ddl_reorg_batch_size')
        if settings != '2\t1\t256':
            raise RuntimeError('Unexpected DDL settings: ' + settings)
        (self.output / 'cluster-info.tsv').write_text(self.sql(
            'SELECT TYPE,INSTANCE,VERSION,GIT_HASH FROM information_schema.cluster_info') + '\n')
        write_json(self.output / 'manifest.json', dict(**self.result, server=self.server,
                   wrapper=self.wrapper, store_ids=self.result['initial_store_ids'],
                   gc_original=self.old_gc, cluster_root=self.cluster_root, settings=settings))
        self.monitor_thread = threading.Thread(target=self._monitor, name='soak-monitor', daemon=True)
        self.monitor_thread.start()

    def _disk_check(self, initial=False):
        paths = [self.output]
        if hasattr(self, 'cluster_root'):
            paths.append(self.cluster_root)
        seen = set()
        snapshots = []
        for path in paths:
            device = path.stat().st_dev
            if device in seen:
                continue
            seen.add(device)
            usage = shutil.disk_usage(path)
            snapshots.append(dict(path=str(path), total=usage.total, free=usage.free))
            minimum = (self.args.start_free_gib if initial else self.args.min_free_gib) * GIB
            if usage.free < minimum or usage.free / usage.total < self.args.min_free_fraction:
                raise RuntimeError('Disk capacity threshold reached: ' + str(snapshots[-1]))
        return snapshots

    def _monitor(self):
        try:
            while not self.monitor_stop.is_set():
                disks = self._disk_check()
                sample = dict(time=time.time(), monotonic=time.monotonic(), generation=self.generation,
                              phase=self.phase, round_id=self.round_id, disks=disks,
                              writers=self.workload.status() if self.workload else None)
                expected_server, expected_generation = self.server, self.generation
                if expected_server:
                    try:
                        current = identity(self.server['pid'])
                        if current != self.server:
                            raise RuntimeError('Server PID identity changed')
                        status = Path(f'/proc/{self.server["pid"]}/status').read_text()
                        sample.update(pid=self.server['pid'], rss_kib=int(re.search(r'VmRSS:\s+(\d+)', status)[1]),
                                      fds=len(list(Path(f'/proc/{self.server["pid"]}/fd').iterdir())),
                                      threads=int(re.search(r'Threads:\s+(\d+)', status)[1]))
                        cpu = Path(f'/proc/{self.server["pid"]}/stat').read_text().rsplit(')', 1)[1].split()
                        sample['cpu_seconds'] = (int(cpu[11]) + int(cpu[12])) / os.sysconf('SC_CLK_TCK')
                        if not self.paused and self.phase not in ('StopRequested', 'ExitedAndGated', 'OfflineWritesAndDDL', 'Starting'):
                            try:
                                raw = self.metrics()
                            except OSError:
                                if not self.paused and self.phase not in ('StopRequested', 'Starting', 'ExitedAndGated', 'OfflineWritesAndDDL'):
                                    raise
                            else:
                                if self.server == expected_server and self.generation == expected_generation:
                                    with gzip.open(self.output / 'metrics' / f'g{expected_generation:04d}.prom.gz', 'ab') as sink:
                                        sink.write((f'# time={time.time()}\n' + raw).encode())
                    except FileNotFoundError:
                        if self.phase not in ('StopRequested', 'ExitedAndGated', 'OfflineWritesAndDDL', 'Starting'):
                            raise RuntimeError('TiFlash exited outside the requested restart window')
                if self.logs:
                    self.logs.check()
                # Account for the actual test directories, not unrelated filesystem activity.
                size = sum(directory_bytes(root) for root in (self.output, self.cluster_root))
                sample['test_directory_bytes'] = size
                with (self.output / 'metrics/resources.jsonl').open('a') as sink:
                    sink.write(json.dumps(sample) + '\n')
                now = time.monotonic()
                self.resources.append((now, size))
                while len(self.resources) > 1 and self.resources[1][0] < now - 3600:
                    self.resources.popleft()
                growth = max(0, size - self.resources[0][1])
                if any(d['free'] < growth * 2 for d in disks):
                    raise RuntimeError('Disk headroom is less than twice this test\'s last-hour growth')
                if (self.output / 'logs').exists():
                    log_bytes = sum(p.stat().st_size for p in (self.output / 'logs').glob('*'))
                    if log_bytes > self.args.max_log_gib * GIB:
                        raise RuntimeError('Captured log archive reached its size limit')
                self.monitor_stop.wait(self.args.sample_interval)
        except Exception:
            self.monitor_error = traceback.format_exc()

    def _seed_load(self):
        self.sql('CREATE TABLE schema_matrix.confirmations(worker INT,seq BIGINT,digest CHAR(64),'
                 'PRIMARY KEY(worker,seq))')
        for name in ('load_a', 'load_b'):
            self.sql(f'CREATE TABLE schema_matrix.{name}(id BIGINT PRIMARY KEY CLUSTERED,last_seq BIGINT,'
                     'n BIGINT,nullable_n BIGINT NULL,payload VARCHAR(80));'
                     f'INSERT INTO schema_matrix.{name} SELECT i,0,i*17,IF(MOD(i,11)=0,NULL,i*3),'
                     "CONCAT('seed-',i) FROM (SELECT 1+a.n+10*b.n+100*c.n+1000*d.n+10000*e.n+100000*f.n AS i "
                     'FROM schema_matrix.seed a,schema_matrix.seed b,schema_matrix.seed c,'
                     f'schema_matrix.seed d,schema_matrix.seed e,schema_matrix.seed f) s WHERE i<={self.args.load_rows};'
                     f'ALTER TABLE schema_matrix.{name} SET TIFLASH REPLICA 1')
            self._available(name)
        self.workload = Workload(self.connect, self.output, self.args.load_rows, self.args.workers,
                                 self.args.rate, self.args.seed)
        self.workload.start()

    def _available(self, table):
        self.wait(lambda: self.sql("SELECT AVAILABLE FROM information_schema.tiflash_replica WHERE "
                                  f"TABLE_SCHEMA='schema_matrix' AND TABLE_NAME='{table}'") == '1',
                  'replica available', 240)

    def _dump(self, query, engine, path, expected):
        connection = self.connect(streaming=True)
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"SET tidb_isolation_read_engines='{engine}'")
                cursor.execute('SET tidb_allow_mpp=1')
                cursor.execute(f'SET tidb_enforce_mpp={int(engine == "tiflash")}')
                cursor.execute(query)
                return summarize_rows(cursor, path, expected)
        finally:
            connection.close()

    def compare_query(self, query, expected, directory, name):
        directory.mkdir(parents=True, exist_ok=True)
        # A frozen independent expectation is also exported in full.
        oracle = summarize_rows(iter(expected), directory / f'{name}-expected.tsv.gz')
        results = {'expected': oracle}
        for engine in ('tikv', 'tiflash'):
            explain = self.sql('EXPLAIN ' + query, engine)
            (directory / f'{name}-{engine}-explain.txt').write_text(explain + '\n')
            if engine == 'tiflash' and 'tiflash' not in explain.lower():
                raise RuntimeError('TiFlash query fell back to another engine')
            deadline = time.monotonic() + self.args.catchup_timeout
            while True:
                self.guard()
                try:
                    result = self._dump(query, engine, directory / f'{name}-{engine}.tsv.gz', iter(expected))
                except Exception as error:
                    if engine != 'tiflash' or time.monotonic() >= deadline:
                        raise
                    self.journal.append('catchup_query_retry', phase=self.phase, error=str(error), query=query)
                    self.idle(1)
                    continue
                if result['mismatches'] == 0:
                    break
                write_json(directory / f'{name}-{engine}-last-difference.json', result)
                if engine == 'tikv' or time.monotonic() >= deadline:
                    raise RuntimeError(f'Value mismatch in {name}/{engine}: {result["examples"]}')
                self.idle(1)
            results[engine] = result
        write_json(directory / f'{name}-result.json', results)
        return results

    def checkpoint(self, directory):
        self.workload.pause()
        self.transition('Checking')
        committed = self.sql('SELECT worker,MAX(seq) FROM schema_matrix.confirmations GROUP BY worker ORDER BY worker')
        actual = {int(row.split('\t')[0]): int(row.split('\t')[1]) for row in committed.splitlines()}
        expected_sequences = self.workload.status()['sequences']
        if any(actual.get(worker, 0) != seq for worker, seq in enumerate(expected_sequences)):
            raise RuntimeError('Commit marker cutoff differs from the confirmed operation log')
        results = {}
        for table, rows in self.workload.oracle.items():
            expected = [rows[key] for key in sorted(rows)]
            results[table] = self.compare_query(
                f'SELECT id,last_seq,n,nullable_n,payload FROM schema_matrix.{table} ORDER BY id',
                expected, directory, table)
        record = dict(time=time.time(), generation=self.generation, round_id=self.round_id,
                      directory=str(directory.relative_to(self.output)), sequences=expected_sequences, tables=results)
        write_json(directory / 'checkpoint.json', record)
        self.checks.append(record)
        return record

    def fixture(self, table, directory):
        directory.mkdir(parents=True, exist_ok=True)
        ddl = (f'CREATE TABLE schema_matrix.{table}(c_first DECIMAL(18,2) NULL,id BIGINT PRIMARY KEY CLUSTERED,'
               'c_middle DECIMAL(18,2) NULL,c_last DECIMAL(18,2) NULL);'
               f'INSERT INTO schema_matrix.{table} SELECT IF(MOD(i,7)=0,NULL,i/100),i,'
               'IF(MOD(i,11)=0,NULL,3*i/100),IF(MOD(i,13)=0,NULL,(7*i+11)/100) FROM '
               '(SELECT 1+a.n+10*b.n+100*c.n+1000*d.n+10000*e.n AS i FROM schema_matrix.seed a,'
               'schema_matrix.seed b,schema_matrix.seed c,schema_matrix.seed d,schema_matrix.seed e) s '
               f'WHERE i<={self.args.ddl_rows};ALTER TABLE schema_matrix.{table} SET TIFLASH REPLICA 1')
        (directory / 'create.sql').write_text(ddl + ';\n')
        self.sql(ddl)
        self._available(table)
        self.fixture_check(table, matrix.BUSINESS, False, directory / 'initial')
        refreshed = self.debug(f'DBGInvoke refresh_table_schema(schema_matrix, {table})')
        if 'refreshed' not in refreshed:
            raise RuntimeError('Initial full-schema refresh failed')
        info = self.metadata(table, directory, 'initial')
        (directory / 'regions.tsv').write_text(self.sql(f'SHOW TABLE schema_matrix.{table} REGIONS') + '\n')
        return info

    def fixture_check(self, table, columns, updated, directory):
        query = 'SELECT id,' + ','.join(f'CAST({column} AS DECIMAL(30,3))' for column in columns)
        query += f' FROM schema_matrix.{table} ORDER BY id'
        expected = []
        rules = dict(c_first=(1, 0, 7), c_middle=(3, 0, 11), c_last=(7, 11, 13))
        for key in range(1, self.args.ddl_rows + 1):
            row = [key]
            for column in columns:
                multiplier, addend, modulus = rules[column]
                value = None if key % modulus == 0 else (Decimal(multiplier * key + addend) / 100 + int(updated))
                row.append(None if value is None else value.quantize(Decimal('0.001')))
            expected.append(row)
        return self.compare_query(query, expected, directory, 'fixture')

    def _stable(self, directory):
        previous, stable = self.metrics(), 0
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            self.idle(1)
            current = self.metrics()
            stable = stable + 1 if not any(matrix.deltas(previous, current).values()) else 0
            if stable >= 3:
                (directory / 'metrics-stable.prom').write_text(current)
                return current
            previous = current
        raise TimeoutError('Metrics did not stabilize')

    def _ids(self, info):
        return {column['name']['L']: column['id'] for column in info['cols']}

    def _phase_end(self, directory, table_id, before=None, removed=None):
        after = self._stable(directory) if before is not None else None
        evidence = self.logs.end()
        if evidence['errors']:
            raise RuntimeError('Internal error during controlled phase')
        if evidence['snapshots']:
            raise NotCovered('Snapshot replaced retained log replay')
        evidence['target_syncs'] = evidence['syncs'].get(str(table_id), 0)
        if before is not None:
            (directory / 'metrics-after.prom').write_text(after)
            evidence['counters'] = matrix.deltas(before, after)
        for line in evidence['proofs']:
            match = re.search(r'table_id=(\d+).*?read_tso=(\d+) max_commit_ts=(\d+) unknown_column_id=(\d+)', line)
            if not match:
                raise RuntimeError('Malformed watermark proof: ' + line)
            target, read_tso, commit_ts, column_id = map(int, match.groups())
            if commit_ts <= 0 or commit_ts > read_tso or column_id <= 0:
                raise RuntimeError('Invalid watermark proof: ' + line)
            if target == table_id and removed is not None and column_id not in removed:
                raise RuntimeError('Skipped ID was not removed from the complete schema')
        write_json(directory / 'evidence.json', evidence)
        return evidence

    def _verify_binary_stats(self):
        for path, expected in self.binary_stats.items():
            stat = Path(path).stat()
            if (stat.st_size, stat.st_mtime_ns) != expected:
                raise RuntimeError('Binary or runtime library changed during the test')

    def restart_round(self, combination, directory):
        mode, operation, column = combination
        table = f'r{self.round_id}_ddl'
        old_info = self.fixture(table, directory / 'schemas')
        self.transition('Ready', combination=combination)
        self.workload.resume()
        self.logs.begin(directory / 'recovery.log.gz')
        old = identity(self.pid)
        write_json(directory / 'process-before.json', old)
        self.gate.touch()
        self.transition('StopRequested', mode=mode)
        signal_checked(self.wrapper, signal.SIGUSR1 if mode == 'graceful' else signal.SIGUSR2)
        exited = self.wait(lambda: next((e for e in self.events() if e['event'] == 'exit' and e['pid'] == old['pid']), None),
                           'real TiFlash exit', 47)
        if exited['returncode'] != (0 if mode == 'graceful' else -signal.SIGKILL):
            raise RuntimeError('Unexpected real server exit code')
        self.transition('ExitedAndGated', exit=exited)
        if Path(f'/proc/{old["pid"]}').exists() or not self.gate.exists():
            raise RuntimeError('Old server did not exit or restart gate disappeared')
        self.transition('OfflineWritesAndDDL')
        statement = (f'ALTER TABLE schema_matrix.{table} MODIFY COLUMN {column} DECIMAL(20,3) NULL DEFAULT NULL'
                     if operation == 'modify' else f'ALTER TABLE schema_matrix.{table} DROP COLUMN {column}')
        self.update(table)
        self.sql(statement)
        (directory / 'ddl.sql').write_text(statement + ';\n')
        new_info = self.metadata(table, directory / 'schemas', 'final')
        old_ids, new_ids = self._ids(old_info), self._ids(new_info)
        if operation == 'modify':
            if old_ids[column] == new_ids[column]:
                raise NotCovered('MODIFY did not replace the target column ID')
        elif column in new_ids:
            raise RuntimeError('DROP did not remove the column')
        if any(new_ids[name] != value for name, value in old_ids.items() if name != column):
            raise RuntimeError('Unrelated column IDs changed')
        self.idle(self.args.offline_seconds)
        if self.find_pid() is not None:
            raise RuntimeError('Server respawned before the offline DDL window ended')
        self.transition('Starting')
        started = time.monotonic()
        self.gate.unlink()
        self.pid = self.wait(lambda: pid if (pid := self.find_pid()) and pid != old['pid'] else None, 'replacement PID', 120)
        self.wait(lambda: self.debug('SELECT 1') == '1', 'replacement debug endpoint', max(0.1, 120-(time.monotonic()-started)))
        self._identify()
        self.generation += 1
        self.wait(self.metrics, 'replacement metrics', max(0.1, 120-(time.monotonic()-started)))
        if self.server['arguments'] != old['arguments'] or self.server['cwd'] != old['cwd']:
            raise RuntimeError('Restart changed arguments, data paths or working directory')
        if self.wait(self.store_ids, 'store registration', 120) != self.result['initial_store_ids']:
            raise RuntimeError('Restart changed TiFlash store ID')
        self._verify_binary_stats()
        write_json(directory / 'process-after.json', self.server)
        self.debug(f"DBGInvoke enable_schema_sync_service('{str(self.round_id > 12).lower()}')")
        self.transition('CatchingUp')
        self.workload.pause()
        columns = [c for c in matrix.BUSINESS if operation != 'drop' or c != column]
        probes = [f'SELECT COUNT(*),SUM(id),{",".join(f"SUM({c})" for c in columns)} FROM schema_matrix.{table}']
        probes += [f'SELECT COUNT(*),SUM(id),SUM(last_seq),SUM(n) FROM schema_matrix.{name}'
                   for name in ('load_a', 'load_b')]
        cutoff = [(query, self.sql(query, 'tikv')) for query in probes]
        ready = time.monotonic()
        self.wait(lambda: all(self.sql(query, 'tiflash') == expected for query, expected in cutoff),
                  'recovery timing probes (full value verification follows)', self.args.catchup_timeout)
        recovery_seconds = time.monotonic() - started
        write_json(directory / 'recovery-timing.json', dict(startup_seconds=ready-started,
                   catchup_seconds=time.monotonic()-ready, timing_probes=cutoff,
                   note='Full export and value checks are recorded separately'))
        verify_started = time.monotonic()
        recovered = self.fixture_check(table, columns, True, directory / 'recovery')
        self.checkpoint(directory / 'checks')
        verification_seconds = time.monotonic() - verify_started
        # Exercise all three post-restart DML forms with visible, independently
        # expected values, even if random writers have not selected them yet.
        survivor, new_id = columns[0], self.args.ddl_rows + 1
        additions = []
        rules = dict(c_first=(1, 0, 7), c_middle=(3, 0, 11), c_last=(7, 11, 13))
        for name in columns:
            multiplier, addend, modulus = rules[name]
            additions.append(None if new_id % modulus == 0 else
                             (Decimal(multiplier * new_id + addend) / 100 + 1).quantize(Decimal('0.001')))
        values = ','.join('NULL' if value is None else str(value) for value in additions)
        self.sql(f'UPDATE schema_matrix.{table} SET {survivor}=12345.67 WHERE id=1;'
                 f'INSERT INTO schema_matrix.{table}(id,{",".join(columns)}) VALUES({new_id},{values});'
                 f'DELETE FROM schema_matrix.{table} WHERE id={self.args.ddl_rows}')
        expected = []
        for key in range(1, new_id + 1):
            if key == self.args.ddl_rows:
                continue
            row = [key]
            for name in columns:
                multiplier, addend, modulus = rules[name]
                value = None if key % modulus == 0 else (Decimal(multiplier * key + addend) / 100 + 1).quantize(Decimal('0.001'))
                row.append(Decimal('12345.670') if key == 1 and name == survivor else value)
            expected.append(row)
        query = 'SELECT id,' + ','.join(f'CAST({name} AS DECIMAL(30,3))' for name in columns)
        self.compare_query(query + f' FROM schema_matrix.{table} ORDER BY id', expected,
                           directory / 'post-restart-dml', 'fixture')
        evidence = self._phase_end(directory, old_info['id'])
        self.transition('OptimizationProbe')
        probe = self.optimization_probe(column, directory / 'post-restart-probe')
        reverse = None
        every = 2 if self.args.mode == 'smoke' else 12
        if self.round_id % every == 0:
            reverse = self.reverse_probe(directory / 'new-column-reverse')
        tables = [table, f'r{self.round_id}_probe']
        if reverse:
            tables.append(f'r{self.round_id}_reverse')
        self.last_round_tables.append(tables)
        if len(self.last_round_tables) > 2:
            for expired in self.last_round_tables.pop(0):
                self.drop_fixture(expired)
        result = dict(combination=combination, generation=self.generation, passed=True, recovered=recovered,
                      recovery_seconds=recovery_seconds, verification_seconds=verification_seconds,
                      evidence=evidence, probe=probe, reverse=reverse,
                      old_column_ids=old_ids, new_column_ids=new_ids, old_exit=exited)
        write_json(directory / 'result.json', result)
        self.mode_counts['/'.join(combination)] += 1
        status = Path(f'/proc/{self.pid}/status').read_text()
        stable = dict(generation=self.generation, rss_kib=int(re.search(r'VmRSS:\s+(\d+)', status)[1]),
                      fds=len(list(Path(f'/proc/{self.pid}/fd').iterdir())))
        self.stable_resources.append(stable)
        recent = self.stable_resources[-5:]
        for key in ('rss_kib', 'fds'):
            if len(recent) == 5 and all(a[key] < b[key] for a, b in zip(recent, recent[1:])) and recent[-1][key] > recent[0][key] * 1.5:
                self.alerts.append(dict(kind='resource_growth_needs_investigation', metric=key, samples=recent))
        write_json(self.output / 'metrics/stable-resources.json', dict(samples=self.stable_resources, alerts=self.alerts))
        self.round_results.append(result)
        self.result.update(combinations=dict(self.mode_counts), completed_rounds=len(self.round_results))
        self.save()
        self.debug(f"DBGInvoke enable_schema_sync_service('{str(self.round_id > 12).lower()}')")
        self.transition('Ready')
        self.workload.resume()

    def optimization_probe(self, column, directory):
        self.workload.pause()
        self.debug("DBGInvoke enable_schema_sync_service('false')")
        self.checkpoint(directory / 'drained')
        table = f'r{self.round_id}_probe'
        old_info = self.fixture(table, directory)
        before = self._stable(directory)
        (directory / 'metrics-before.prom').write_text(before)
        self.logs.begin(directory / 'tiflash-phase.log.gz')
        with self.pause_server(directory):
            self.ddl(table, 'modify', column)
            if 'State:\tT' not in Path(f'/proc/{self.pid}/status').read_text():
                raise RuntimeError('TiFlash resumed during DDL')
            new_info = self.metadata(table, directory, 'final')
            removed = set(self._ids(old_info).values()) - set(self._ids(new_info).values())
            if self._ids(old_info)[column] not in removed:
                raise NotCovered('Probe MODIFY did not replace the column ID')
        self.fixture_check(table, matrix.BUSINESS, False, directory / 'checks')
        self._stable(directory)
        evidence = self._phase_end(directory, old_info['id'], before, removed)
        counts = evidence['counters']
        if counts['raft_decode'] != 1 or evidence['target_syncs'] != 1:
            raise RuntimeError('Optimization probe did not synchronize exactly once')
        if counts['covered'] <= 0 or counts['skipped_extra_columns'] <= 0:
            raise NotCovered('Probe did not produce multiple covered batches with actual skipped columns')
        return evidence

    def reverse_probe(self, directory):
        directory.mkdir(parents=True, exist_ok=True)
        table = f'r{self.round_id}_reverse'
        self.sql(f'CREATE TABLE schema_matrix.{table}(id BIGINT PRIMARY KEY CLUSTERED,c0 INT);'
                 f'INSERT INTO schema_matrix.{table} VALUES(1,10);ALTER TABLE schema_matrix.{table} SET TIFLASH REPLICA 1')
        self._available(table)
        self.compare_query(f'SELECT id,c0 FROM schema_matrix.{table} ORDER BY id', [[1, 10]], directory / 'initial', 'reverse')
        self.debug(f'DBGInvoke refresh_table_schema(schema_matrix, {table})')
        info = self.metadata(table, directory, 'initial')
        before = self._stable(directory)
        self.logs.begin(directory / 'tiflash-phase.log.gz')
        value = 424242 + self.round_id
        with self.pause_server(directory):
            self.sql(f'ALTER TABLE schema_matrix.{table} ADD COLUMN c1 INT NULL;'
                     f'INSERT INTO schema_matrix.{table} VALUES(2,20,{value})')
            self.metadata(table, directory, 'final')
        self.compare_query(f'SELECT id,c0,c1 FROM schema_matrix.{table} ORDER BY id',
                           [[1, 10, None], [2, 20, value]], directory / 'checks', 'reverse')
        self._stable(directory)
        evidence = self._phase_end(directory, info['id'], before)
        if evidence['counters']['beyond_watermark'] <= 0 or evidence['target_syncs'] < 1:
            raise RuntimeError('New column did not use the schema synchronization fallback')
        self.result['reverse_checks'] += 1
        return evidence

    @contextmanager
    def pause_server(self, directory):
        process = identity(self.pid)
        self.journal.append('process_signal', process=process, signal='SIGSTOP', round_id=self.round_id)
        signal_checked(process, signal.SIGSTOP)
        self.paused = True
        try:
            self.wait(lambda: 'State:\tT' in Path(f'/proc/{self.pid}/status').read_text(), 'process SIGSTOP', 5)
            write_json(directory / 'stopped-process.json', process)
            (directory / 'stopped-before-ddl.status').write_text(Path(f'/proc/{self.pid}/status').read_text())
            yield
            status = Path(f'/proc/{self.pid}/status').read_text()
            (directory / 'stopped-after-ddl.status').write_text(status)
            if 'State:\tT' not in status:
                raise RuntimeError('Process did not remain stopped throughout DDL')
        finally:
            self.journal.append('process_signal', process=process, signal='SIGCONT', round_id=self.round_id)
            signal_checked(process, signal.SIGCONT)
            self.paused = False

    def audit(self):
        tables, sequences = replay(self.output / 'operations.jsonl', self.args.load_rows)
        if tables != self.workload.oracle or any(sequences.get(w, 0) != s for w, s in enumerate(self.workload.sequences)):
            raise RuntimeError('Independent journal replay disagrees with final workload oracle')
        archives = 0
        for path in self.output.rglob('*-result.json'):
            record = json.loads(path.read_text())
            if not all(engine in record for engine in ('expected', 'tikv', 'tiflash')):
                continue
            for engine in ('expected', 'tikv', 'tiflash'):
                data = record[engine]
                archive = path.with_name(path.name.removesuffix('-result.json') + f'-{engine}.tsv.gz')
                digest = hashlib.sha256()
                with gzip.open(archive, 'rb') as source:
                    for chunk in iter(lambda: source.read(1024 * 1024), b''):
                        digest.update(chunk)
                if digest.hexdigest() != data['sha256'] or data['mismatches']:
                    raise RuntimeError('Archive audit failed: ' + str(archive))
                archives += 1
        write_json(self.output / 'audit.json', dict(passed=True, replayed_sequences=sequences, checked_archives=archives))

    def run(self):
        previous_signals = {}
        for sig in (signal.SIGINT, signal.SIGTERM):
            previous_signals[sig] = signal.signal(sig, lambda *_: self.interrupted.set())
        error = None
        try:
            self._disk_check(initial=True)
            self._configure_start()
            self._seed_load()
            self.checkpoint(self.output / 'warmup-initial')
            self.started = time.monotonic()
            self.logs.started = self.started
            self.logs.watch_errors = True
            self.result['started_at'] = time.time()
            end = self.started + self.args.duration
            loop_end = end - self.args.final_window
            self.transition('Warmup')
            self.workload.resume()
            self.idle(self.args.warmup)
            main_started = time.monotonic()
            main_enabled = self.workload.status()['enabled_seconds']
            schedule = (iter(SMOKE_COMBINATIONS)
                        if self.args.mode == 'smoke' else round_schedule(self.args.seed))
            next_round, next_hour = main_started, self.started + 3600
            while time.monotonic() + self.args.round_timeout < loop_end:
                if self.args.mode == 'smoke' and self.round_id >= len(SMOKE_COMBINATIONS):
                    break
                self.idle(next_round - time.monotonic())
                if time.monotonic() + self.args.round_timeout >= loop_end:
                    break
                round_started = time.monotonic()
                self.round_id += 1
                self.round_deadline = min(time.monotonic() + self.args.round_timeout, loop_end)
                directory = self.output / 'rounds' / f'{self.round_id:06d}'
                directory.mkdir()
                self.restart_round(next(schedule), directory)
                self.round_deadline = None
                next_round = round_started + self.args.interval
                if time.monotonic() >= next_hour:
                    self.workload.pause()
                    self.checkpoint(self.output / 'hourly' / f'{int((time.monotonic()-self.started)//3600):04d}')
                    self.workload.resume()
                    next_hour += 3600
            self.round_deadline = None
            if self.args.mode == 'smoke' and self.round_id != len(SMOKE_COMBINATIONS):
                raise NotCovered('Smoke rehearsal did not complete cleanup followed by restart')
            if self.args.mode != 'smoke':
                self.idle(loop_end - time.monotonic())
            main_seconds = time.monotonic() - main_started
            main_fraction = (self.workload.status()['enabled_seconds'] - main_enabled) / max(0.001, main_seconds)
            self.workload.pause()
            self.transition('FinalChecking')
            self.checkpoint(self.output / 'final')
            self.audit()
            if self.args.mode != 'smoke':
                self.idle(end - time.monotonic())
            self._verify_binary_stats()
            if matrix.file_sha(self.binary) != self.result['versions']['tiflash']['sha256']:
                raise RuntimeError('TiFlash binary changed during the run')
            elapsed = time.monotonic() - self.started
            sufficient = accepts_24h(self.args.mode, elapsed, self.mode_counts, self.result['reverse_checks'],
                                     main_fraction, self.logs.proofs) and not self.alerts
            self.result.update(elapsed_seconds=elapsed, writer_active_fraction=main_fraction,
                               writers=self.workload.status(), acceptance_24h=sufficient,
                               proof_hours=dict(self.logs.proofs), resource_alerts=self.alerts,
                               status=('smoke_passed' if self.args.mode == 'smoke' else
                                       'passed' if sufficient else 'coverage_incomplete'),
                               passed=self.args.mode == 'smoke' or sufficient)
        except BaseException as caught:
            error = caught
            self.result.update(passed=False, status=('not_covered' if isinstance(caught, NotCovered) else
                                                    'interrupted' if isinstance(caught, InterruptedError) else 'failed'),
                               error=traceback.format_exc())
            write_json(self.output / 'failures' / f'round-{self.round_id:06d}.json',
                       dict(error=self.result['error'], phase=self.phase, server=self.server, round_id=self.round_id))
        finally:
            self.round_deadline = None
            self.monitor_stop.set()
            if self.monitor_thread:
                self.monitor_thread.join(timeout=40)
            if self.workload:
                try:
                    self.workload.stop()
                except Exception:
                    self.result.update(passed=False, writer_cleanup_error=traceback.format_exc())
            if self.logs:
                try:
                    self.logs.stop()
                    self.result['retired_table_schema_errors'] = dict(self.logs.retired_error_counts)
                    self.result['retired_tables'] = self.logs.retired_tables
                    self.logs.check()
                except Exception:
                    self.result.update(passed=False, log_cleanup_error=traceback.format_exc())
            # Restore the original process only after validating its identity.
            if self.paused and self.server:
                try:
                    signal_checked(self.server, signal.SIGCONT)
                    self.paused = False
                except Exception:
                    self.result.update(passed=False, resume_error=traceback.format_exc())
            super().cleanup()
            if not self.result['passed'] and self.result['status'] in ('passed', 'smoke_passed'):
                self.result['status'] = 'cleanup_failed'
            self.result['acceptance_24h'] = self.result.get('acceptance_24h', False) and self.result['passed']
            if self.workload:
                self.result['writers'] = self.workload.status()
                self.result['actual_write_rate_while_enabled'] = self.result['writers']['committed'] / max(
                    0.001, self.result['writers']['enabled_seconds'])
            self.journal.append('finished', status=self.result['status'], passed=self.result['passed'])
            write_json(self.output / 'state.json', dict(phase='Complete' if self.result['passed'] else 'Stopped',
                       status=self.result['status'], round_id=self.round_id, generation=self.generation, time=time.time()))
            self.save()
            (self.output / 'FINAL_REPORT.md').write_text(
                '# TiFlash restart soak result\n\n' +
                f'- Status: {self.result.get("status")}\n- Passed: {self.result["passed"]}\n' +
                f'- 24-hour acceptance: {self.result["acceptance_24h"]}\n' +
                f'- Actual TiFlash commit: {self.result.get("actual_binary_commit")}\n' +
                f'- Completed rounds: {len(self.round_results)}\n' +
                f'- Reverse checks: {self.result["reverse_checks"]}\n\n' +
                'Full configuration, counters, commit evidence and limits: [result.json](result.json).\n')
            for sig, handler in previous_signals.items():
                signal.signal(sig, handler)
        if error:
            print(self.result['error'], flush=True)


def make_parser():
    parser = matrix.make_parser(__doc__, allow_baseline=False)
    parser.set_defaults(component_version=None)
    parser.add_argument('--mode', choices=('soak', 'smoke'), default='soak')
    parser.add_argument('--duration', type=duration, help='Total budget, default 24h (smoke: 20m)')
    parser.add_argument('--warmup', type=duration, help='Default 30m (smoke: 5s)')
    parser.add_argument('--final-window', type=duration, help='Default 30m (smoke: 30s)')
    parser.add_argument('--interval', type=duration, help='Minimum spacing between round starts, default 10m (smoke: 0s)')
    parser.add_argument('--build-manifest', type=Path, help='Build receipt JSON with head/source_sha256, required for soak')
    parser.add_argument('--round-timeout', type=duration, default=480)
    parser.add_argument('--catchup-timeout', type=duration, default=180)
    parser.add_argument('--offline-seconds', type=float, help='Default 30 (smoke: 2)')
    parser.add_argument('--load-rows', type=int, help='Rows per load table, default 200000 (smoke: 1000)')
    parser.add_argument('--ddl-rows', type=int, default=10000)
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--rate', type=float, help='Row changes/second, default 200 (smoke: 20)')
    parser.add_argument('--seed', type=int, default=20261031)
    parser.add_argument('--sample-interval', type=float, default=10)
    parser.add_argument('--start-free-gib', type=float, default=300)
    parser.add_argument('--min-free-gib', type=float, default=100)
    parser.add_argument('--min-free-fraction', type=float, default=0.1)
    parser.add_argument('--max-log-gib', type=float, default=64)
    return parser


def parse_args(argv=None):
    parser = make_parser()
    args = parser.parse_args(argv)
    smoke = args.mode == 'smoke'
    if not smoke and not args.build_manifest:
        parser.error('Formal soak requires --build-manifest; use --mode smoke for rehearsal without a build receipt')
    if args.build_manifest and not args.build_manifest.is_file():
        parser.error('--build-manifest must be an existing JSON build receipt')
    if importlib.util.find_spec('pymysql') is None:
        parser.error('Install requirements.txt (PyMySQL) before starting the cluster')
    for name, default in dict(duration=1200 if smoke else 86400, warmup=5 if smoke else 1800,
                              final_window=30 if smoke else 1800, interval=0 if smoke else 600,
                              offline_seconds=2 if smoke else 30, load_rows=1000 if smoke else 200000,
                              rate=20 if smoke else 200).items():
        if getattr(args, name) is None:
            setattr(args, name, default)
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]+', args.tag):
        parser.error('Use a unique tag containing only letters, digits, dot, dash and underscore')
    if not (1 <= args.workers <= args.load_rows <= 1000000 and 1000 <= args.ddl_rows <= 100000):
        parser.error('Require 1 <= workers <= load-rows <= 1000000 and 1000 <= ddl-rows <= 100000')
    if min(args.rate, args.sample_interval, args.round_timeout, args.catchup_timeout, args.max_log_gib) <= 0:
        parser.error('Rates, timeouts and archive limit must be positive')
    if min(args.warmup, args.final_window, args.interval, args.offline_seconds) < 0:
        parser.error('Durations cannot be negative')
    if args.duration <= args.warmup + args.final_window + args.round_timeout:
        parser.error('Duration must allow warmup, at least one round, and final checking')
    tiup_home = Path(os.environ.get('TIUP_HOME', Path.home() / '.tiup'))
    if (tiup_home / 'data' / args.tag).exists():
        parser.error('TiUP tag already has a data directory; choose a new tag')
    ports = [4000+args.offset, 10080+args.offset, 2379+args.offset, 2380+args.offset,
             20160+args.offset, 20180+args.offset, 3930+args.offset, 20170+args.offset,
             20292+args.offset, 8234+args.offset, args.debug_port]
    args.ephemeral_port_range = ephemeral_port_range()
    try:
        check_listener_ports(ports, args.ephemeral_port_range)
    except ValueError as error:
        parser.error(str(error))
    for port in ports:
        try:
            with socket.socket() as listener:
                listener.bind(('127.0.0.1', port))
        except OSError:
            parser.error(f'Port {port} is already in use; choose a free offset/debug port')
    return args


if __name__ == '__main__':
    runner = RestartSoak(parse_args())
    runner.run()
    raise SystemExit(0 if runner.result['passed'] else 2)
