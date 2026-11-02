"""Controller regressions; no TiUP cluster or TiFlash binary is needed."""
from collections import Counter
from contextlib import redirect_stderr
from decimal import Decimal
import gzip
import itertools
import io
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch

import run_restart_soak as soak
import soak_workload as workload


class FakeConnection:
    def __init__(self, database, lose_reply=False, commit_before_error=True):
        self.database = database
        self.lose_reply = lose_reply
        self.commit_before_error = commit_before_error
        self.pending = None
        self.rowcount = 1

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def begin(self):
        self.pending = None

    def execute(self, query, parameters):
        if query.startswith('SELECT'):
            self.selected = self.database.get(tuple(parameters))
        elif 'confirmations' in query:
            worker, seq, digest = parameters
            self.pending = ((worker, seq), digest)
        else:
            self.database.setdefault('executed', []).append((query, parameters))

    def fetchone(self):
        return (self.selected,) if self.selected else None

    def commit(self):
        if not self.lose_reply or self.commit_before_error:
            self.database[self.pending[0]] = self.pending[1]
            self.database['commits'] = self.database.get('commits', 0) + 1
        if self.lose_reply:
            self.lose_reply = False
            raise Exception(2013, 'Lost connection during COMMIT')

    def rollback(self):
        self.pending = None

    def close(self):
        pass


class RestartSoakTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def operation(self):
        operation = dict(worker=0, seq=1, table='load_a', kind='update', key=1, row=[1, 1, 101, None, 'sentinel'])
        operation['digest'] = workload.digest_operation(operation)
        return operation

    def test_schedule_covers_every_combination_once_per_group(self):
        schedule = list(itertools.islice(soak.round_schedule(23), 24))
        self.assertEqual(set(schedule[:12]), set(soak.COMBINATIONS))
        self.assertEqual(set(schedule[12:]), set(soak.COMBINATIONS))
        self.assertEqual(schedule, list(itertools.islice(soak.round_schedule(23), 24)))

    def test_acceptance_rejects_duration_only_and_smoke(self):
        counts = Counter({'/'.join(c): 10 for c in soak.COMBINATIONS})
        hours = Counter({hour: 1 for hour in range(24)})
        self.assertTrue(soak.accepts_24h('soak', 86400, counts, 10, 0.5, hours))
        self.assertFalse(soak.accepts_24h('smoke', 86400, counts, 10, 1, hours))
        self.assertFalse(soak.accepts_24h('soak', 86399, counts, 10, 1, hours))
        counts['/'.join(soak.COMBINATIONS[0])] = 9
        self.assertFalse(soak.accepts_24h('soak', 86400, counts, 10, 1, hours))
        counts['/'.join(soak.COMBINATIONS[0])] = 10
        del hours[12]
        self.assertFalse(soak.accepts_24h('soak', 86400, counts, 10, 1, hours))

    def test_value_and_null_differences_are_not_hidden_by_equal_count(self):
        expected = [[1, Decimal('1.000'), None], [2, Decimal('2.000'), 9]]
        actual = [[1, Decimal('1.000'), 0], [2, Decimal('3.000'), 9]]
        result = soak.summarize_rows(iter(actual), self.root / 'rows.gz', iter(expected))
        self.assertEqual(result['rows'], 2)
        self.assertEqual(result['mismatches'], 2)
        self.assertEqual(result['null_counts'], [0, 0, 0])
        self.assertEqual(len(result['examples']), 2)

    def test_missing_trailing_row_is_detected(self):
        result = soak.summarize_rows(iter([[1, 'x']]), self.root / 'rows.gz', iter([[1, 'x'], [2, 'y']]))
        self.assertEqual(result['mismatches'], 1)
        with gzip.open(self.root / 'rows.gz', 'rt') as source:
            self.assertEqual(source.read(), '1\tx\n')

    def test_unknown_commit_uses_marker_without_reapplying_dml(self):
        database = {}
        journal = workload.Journal(self.root / 'operations.jsonl')
        operation = self.operation()
        original = FakeConnection(database, lose_reply=True)
        workload.commit_operation(lambda: FakeConnection(database), original, operation, journal)
        self.assertEqual(database['commits'], 1)
        self.assertEqual(len(database['executed']), 1)

    def test_unknown_commit_before_publication_retries_one_transaction(self):
        database = {}
        journal = workload.Journal(self.root / 'operations.jsonl')
        original = FakeConnection(database, lose_reply=True, commit_before_error=False)
        workload.commit_operation(lambda: FakeConnection(database), original, self.operation(), journal)
        self.assertEqual(database['commits'], 1)
        self.assertEqual(len(database['executed']), 2)

    def test_wrong_marker_digest_stops_the_workload(self):
        database = {}
        operation = self.operation()
        journal = workload.Journal(self.root / 'operations.jsonl')
        original = FakeConnection(database, lose_reply=True)
        def reconnect():
            database[(0, 1)] = 'wrong-digest'
            return FakeConnection(database)
        with self.assertRaisesRegex(ValueError, 'different operation'):
            workload.commit_operation(reconnect, original, operation, journal)

    def test_journal_replay_requires_confirmed_matching_operations(self):
        journal = workload.Journal(self.root / 'operations.jsonl')
        operation = self.operation()
        journal.append('intent', **operation)
        with self.assertRaisesRegex(RuntimeError, 'Unresolved'):
            workload.replay(journal.path, 3)
        journal.append('confirmed', worker=0, seq=1, digest=operation['digest'])
        tables, sequences = workload.replay(journal.path, 3)
        self.assertEqual(tables['load_a'][1], operation['row'])
        self.assertEqual(tables['load_b'][1], workload.initial_row(1))
        self.assertEqual(sequences, {0: 1})

    def test_pause_drains_inflight_writers_and_freezes_the_oracle(self):
        entered, release, drained = threading.Event(), threading.Event(), threading.Event()
        load = workload.Workload(lambda: None, self.root, 10, 1, 100, 7)
        def commit(*_):
            entered.set()
            release.wait(timeout=3)
            return None
        with patch.object(workload, 'commit_operation', side_effect=commit):
            load.start()
            load.resume()
            self.assertTrue(entered.wait(timeout=2))
            waiter = threading.Thread(target=lambda: (load.pause(), drained.set()))
            waiter.start()
            self.assertFalse(drained.wait(timeout=0.05))
            release.set()
            self.assertTrue(drained.wait(timeout=2))
            before = load.status()['committed']
            time.sleep(0.05)
            self.assertEqual(before, load.status()['committed'])
            load.stop()
            waiter.join()
        tables, _ = workload.replay(load.journal.path, 10)
        self.assertEqual(tables, load.oracle)

    def test_log_rotation_preserves_phase_evidence_and_complete_lines(self):
        path = self.root / 'tiflash.log'
        path.write_text('initial\n')
        capture = soak.LogCapture(path, self.root / 'archive')
        capture.begin(self.root / 'phase.gz')
        with path.open('a') as sink:
            sink.write('sync schema cost 1 ms, table_id=123')
        capture._drain()
        self.assertEqual(capture.summary['syncs'], {})
        with path.open('a') as sink:
            sink.write('\n')
        path.rename(self.root / 'tiflash.log.1')
        path.write_text('Skip historical extra column, table_id=123 read_tso=9 max_commit_ts=8 unknown_column_id=4\n')
        evidence = capture.end()
        self.assertEqual(evidence['syncs']['123'], 1)
        self.assertEqual(len(evidence['proofs']), 1)
        capture._drain()
        for stream in (capture.source, capture.archive):
            if stream:
                stream.close()

    def test_unexpected_internal_error_is_detected_outside_a_probe(self):
        path = self.root / 'tiflash.log'
        path.write_text('')
        capture = soak.LogCapture(path, self.root / 'archive')
        capture.watch_errors = True
        capture._line(b'[ERROR] metadata snapshot construction failed\n')
        with self.assertRaisesRegex(RuntimeError, 'Unexpected TiFlash'):
            capture.check()
        capture.archive.close()

    @staticmethod
    def retired_error(table_id=133):
        return (f'[ERROR] [TiDBSchemaSyncer.cpp:325] ["Can not find related database_id and '
                f'logical_table_id from table_id_map, sync table schema fail. physical_table_id={table_id}"] '
                '[source="keyspace=4294967295"] [thread_id=195]\n').encode()

    def test_dropped_table_mapping_error_is_archived_separately(self):
        capture = soak.LogCapture(self.root / 'tiflash.log', self.root / 'archive')
        capture.watch_errors = True
        dropped = dict(table_id=133, table='schema_matrix.r1_probe', dropped_at=123, round_id=3)
        capture.register_retired_table(dropped)
        capture._line(self.retired_error())  # Outside the controlled phase.
        capture.begin(self.root / 'phase.gz')
        capture._line(self.retired_error())
        evidence = capture.end()
        capture.check()
        self.assertEqual(evidence['errors'], [])
        self.assertEqual(len(evidence['retired_table_errors']), 1)
        self.assertEqual(capture.retired_error_counts, {133: 2})
        records = [json.loads(line) for line in (capture.output / 'retired-table-schema-errors.jsonl').read_text().splitlines()]
        self.assertEqual([record['dropped'] for record in records], [dropped, dropped])
        capture.archive.close()
        with gzip.open(capture.output / 'tiflash-0000.log.gz', 'rb') as archive:
            self.assertEqual(archive.read(), self.retired_error() * 2)
        # A later known cleanup message must not erase an earlier fatal error.
        capture.error = 'previous internal error'
        capture.archive = None
        capture.archive_hour = None
        capture._line(self.retired_error())
        with self.assertRaisesRegex(RuntimeError, 'previous internal error'):
            capture.check()
        capture.archive.close()

    def test_dropped_table_exception_does_not_hide_other_internal_errors(self):
        original = self.retired_error()
        cases = [self.retired_error(134),  # An active or unknown table.
                 original.replace(b'[ERROR]', b'[FATAL]'),
                 original.replace(b'TiDBSchemaSyncer.cpp', b'Other.cpp'),
                 original.replace(b'keyspace=4294967295', b'keyspace=42'),
                 original.replace(b'sync table schema fail.', b'metadata snapshot failed.')]
        for index, line in enumerate(cases):
            with self.subTest(line=line):
                capture = soak.LogCapture(self.root / 'tiflash.log', self.root / str(index))
                capture.watch_errors = True
                capture.register_retired_table(dict(table_id=133, table='schema_matrix.r1_probe'))
                capture.begin(self.root / f'phase-{index}.gz')
                capture._line(line)
                self.assertEqual(len(capture.end()['errors']), 1)
                self.assertEqual(capture.retired_error_counts, {})
                with self.assertRaisesRegex(RuntimeError, 'Unexpected TiFlash'):
                    capture.check()
                capture.archive.close()

    def test_only_successful_drop_registers_an_exception(self):
        runner = object.__new__(soak.RestartSoak)
        runner.table_ids, runner.round_id = {'r1_probe': 133}, 3
        runner.logs, runner.journal = Mock(), Mock()
        runner.sql = Mock(side_effect=RuntimeError('SQL DROP failed'))
        with self.assertRaisesRegex(RuntimeError, 'SQL DROP failed'):
            runner.drop_fixture('r1_probe')
        runner.logs.register_retired_table.assert_not_called()
        runner.journal.append.assert_not_called()
        runner.sql = Mock()
        runner.drop_fixture('r1_probe')
        runner.sql.assert_called_once_with('DROP TABLE schema_matrix.r1_probe')
        record = runner.logs.register_retired_table.call_args.args[0]
        self.assertEqual(record['table_id'], 133)
        self.assertEqual(record['table'], 'schema_matrix.r1_probe')
        runner.journal.append.assert_called_once_with('table_dropped', **record)

    def test_retirement_does_not_reclassify_earlier_errors(self):
        path = self.root / 'tiflash.log'
        path.write_bytes(self.retired_error())
        capture = soak.LogCapture(path, self.root / 'archive')
        capture.watch_errors = True
        capture.register_retired_table(dict(table_id=133, table='schema_matrix.r1_probe'))
        with self.assertRaisesRegex(RuntimeError, 'Unexpected TiFlash'):
            capture.check()
        self.assertEqual(capture.retired_error_counts, {})
        capture.archive.close()
        capture.source.close()

    def test_signal_refuses_a_reused_pid_identity(self):
        expected = dict(pid=123, starttime='old', arguments=['server'], cwd='/tmp')
        with patch.object(soak, 'identity', return_value=dict(expected, starttime='new')), \
                patch.object(soak.os, 'kill') as kill:
            with self.assertRaisesRegex(RuntimeError, 'identity changed'):
                soak.signal_checked(expected, 15)
            kill.assert_not_called()

    def test_disk_sample_survives_compaction_directory_removal(self):
        disappearing = self.root / '.del.dmf_884'
        disappearing.mkdir()
        (disappearing / 'data').write_bytes(b'old data')
        (self.root / 'live').mkdir()
        (self.root / 'live/data').write_bytes(b'12345')
        (self.root / 'log').write_bytes(b'abc')
        scandir = soak.os.scandir
        removed = []

        def remove_before_descent(path):
            if Path(path) == disappearing:
                (disappearing / 'data').unlink()
                disappearing.rmdir()
                removed.append(path)
            return scandir(path)

        with patch.object(soak.os, 'scandir', side_effect=remove_before_descent):
            self.assertEqual(soak.directory_bytes(self.root), 8)
        self.assertEqual(len(removed), 1)

    def test_disk_sample_survives_file_rotation(self):
        (self.root / 'live').write_bytes(b'12345')
        rotated = self.root / 'rotated.log'
        rotated.write_bytes(b'old log')
        original_stat = Path.stat

        def remove_before_stat(path, *args, **kwargs):
            if path == rotated:
                rotated.unlink()
            return original_stat(path, *args, **kwargs)

        with patch.object(Path, 'stat', remove_before_stat):
            self.assertEqual(soak.directory_bytes(self.root), 5)

    def test_disk_sample_does_not_hide_permission_or_io_errors(self):
        for error in (PermissionError(13, 'Permission denied', str(self.root)),
                      OSError(5, 'Input/output error', str(self.root))):
            with self.subTest(error=error), patch.object(soak.os, 'scandir', side_effect=error):
                with self.assertRaises(type(error)):
                    soak.directory_bytes(self.root)

    def test_disk_sample_rejects_a_disappeared_test_root(self):
        with self.assertRaises(FileNotFoundError):
            soak.directory_bytes(self.root / 'missing-test-root')
        # Also reject a root removed between stat and directory traversal.
        with patch.object(soak.os, 'scandir', side_effect=FileNotFoundError(2, 'Gone', str(self.root))):
            with self.assertRaises(FileNotFoundError):
                soak.directory_bytes(self.root)

    def test_failed_ddl_still_resumes_the_same_process(self):
        runner = object.__new__(soak.RestartSoak)
        runner.pid, runner.round_id, runner.paused = 123, 1, False
        runner.journal = workload.Journal(self.root / 'events.jsonl')
        runner.wait = lambda *_: True
        process = dict(pid=123, starttime='original', arguments=['server'], cwd='/tmp')
        with patch.object(soak, 'identity', return_value=process), \
                patch.object(soak, 'signal_checked') as send, \
                patch.object(Path, 'read_text', return_value='State:\tT\n'):
            with self.assertRaisesRegex(RuntimeError, 'Injected DDL failure'):
                with runner.pause_server(self.root):
                    raise RuntimeError('Injected DDL failure')
        self.assertEqual([call.args[1] for call in send.call_args_list], [soak.signal.SIGSTOP, soak.signal.SIGCONT])
        self.assertFalse(runner.paused)

    def test_round_timeout_prevents_advancing_to_the_next_stage(self):
        runner = object.__new__(soak.RestartSoak)
        runner.interrupted = threading.Event()
        runner.monitor_error = runner.workload = runner.logs = runner.proc = None
        runner.round_deadline = 10
        with patch.object(soak.time, 'monotonic', return_value=11):
            with self.assertRaisesRegex(TimeoutError, 'time budget'):
                runner.guard()

    def cli_args(self):
        return ['--mode', 'smoke', '--tiflash', '/unused/tiflash', '--tidb', '/unused/tidb',
                '--tikv', '/unused/tikv', '--pd', '/unused/pd', '--output', str(self.root / 'run'),
                '--tag', 'isolated-cli-test', '--offset', '6000', '--debug-port', '29000']

    def test_listener_ports_reject_ephemeral_range_including_boundaries(self):
        for port in (32768, 42930, 60999):
            with self.subTest(port=port), self.assertRaisesRegex(ValueError, 'ephemeral'):
                soak.check_listener_ports([port], [32768, 60999])
        soak.check_listener_ports([9930, 14234, 26170, 26292, 29000], [32768, 60999])
        with self.assertRaisesRegex(ValueError, 'overlap'):
            soak.check_listener_ports([29000, 29000], [32768, 60999])

    def test_ephemeral_listener_is_rejected_before_touching_ports(self):
        args = self.cli_args()
        args[args.index('--offset') + 1] = '39000'
        with patch.dict(soak.os.environ, {'TIUP_HOME': str(self.root)}), \
                patch.object(soak.importlib.util, 'find_spec', return_value=object()), \
                patch.object(soak, 'ephemeral_port_range', return_value=[32768, 60999]), \
                patch.object(soak.socket, 'socket') as listen, redirect_stderr(io.StringIO()) as errors:
            with self.assertRaises(SystemExit):
                soak.parse_args(args)
            self.assertIn('ephemeral port range', errors.getvalue())
            listen.assert_not_called()

    def test_actual_metrics_port_is_validated_after_tiup_allocation(self):
        runner = object.__new__(soak.RestartSoak)
        runner.pid, runner.debug_port, runner.result = 123, 29000, {}
        runner.args = soak.argparse.Namespace(tag='test-tag', ephemeral_port_range=[32768, 60999])
        runner.state = self.root
        (self.root / 'wrapper.pid').write_text('456')
        arguments = ['tiflash', '--path=/test-tag/tiflash-0/data', '--status.metrics_port=47234',
                     '--flash.service_addr=127.0.0.1:9930', '--flash.proxy.addr=127.0.0.1:26170',
                     '--flash.proxy.status-addr=127.0.0.1:26292']
        with patch.object(soak, 'identity', return_value=dict(pid=123, arguments=arguments)):
            with self.assertRaisesRegex(ValueError, '47234'):
                runner._identify()
            arguments[2] = '--status.metrics_port=14235'
            runner._identify()
        self.assertEqual(runner.metric_port, 14235)
        self.assertEqual(runner.result['listener_ports']['metrics'], 14235)

    def test_existing_tiup_tag_is_rejected_before_touching_ports(self):
        (self.root / 'data/isolated-cli-test').mkdir(parents=True)
        with patch.dict(soak.os.environ, {'TIUP_HOME': str(self.root)}), \
                patch.object(soak.importlib.util, 'find_spec', return_value=object()), \
                patch.object(soak.socket, 'socket') as listen, redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                soak.parse_args(self.cli_args())
            listen.assert_not_called()

    def test_occupied_port_is_rejected_before_cluster_start(self):
        with patch.dict(soak.os.environ, {'TIUP_HOME': str(self.root)}), \
                patch.object(soak.importlib.util, 'find_spec', return_value=object()), \
                patch.object(soak.socket, 'socket', side_effect=OSError('occupied')), redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                soak.parse_args(self.cli_args())


if __name__ == '__main__':
    unittest.main()
