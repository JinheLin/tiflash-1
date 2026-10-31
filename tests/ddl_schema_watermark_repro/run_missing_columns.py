#!/usr/bin/env python3
"""Run PLAN_MISSING_COLUMNS.md on the schema-watermark TiFlash branch."""
import datetime
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import time
import traceback

import run_matrix as matrix

HERE = Path(__file__).resolve().parent


def now():
    return datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).isoformat()


class MissingColumns(matrix.Matrix):
    def __init__(self, args):
        super().__init__(args)
        self.env = {k: v for k, v in self.env.items() if 'proxy' not in k.lower()}
        self.result.update(started_at=now(), plan=str(HERE / 'PLAN_MISSING_COLUMNS.md'))
        self.result['harness_sha256'] = {
            str(p): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in [Path(__file__), HERE / 'run_matrix.py', HERE / 'restartable_tiflash.py']
        }

    def sql(self, query, engine=None):
        with (self.output / 'sql-transcript.sql').open('a') as log:
            log.write(f'-- time={time.time()} engine={engine}\n{query};\n\n')
        return super().sql(query, engine)

    def row_values(self, table, engine):
        return self.sql(f'SELECT c_first,id,c_middle,c_last FROM schema_matrix.{table} ORDER BY id', engine)

    def compare_rows(self, table, directory, stage):
        rows = {engine: self.row_values(table, engine) for engine in ('tikv', 'tiflash')}
        for engine, data in rows.items():
            (directory / f'{stage}-{engine}-rows.tsv').write_text(data + '\n')
        assert rows['tikv'] == rows['tiflash'], f'Engine mismatch: {table}/{stage}'
        lines = rows['tikv'].splitlines()
        assert len(lines) == 10000, len(lines)
        for index, line in enumerate(lines, 1):
            first, handle, middle, last = line.split('\t')
            assert int(handle) == index
            assert Decimal(first) == Decimal(index) / 100
            assert Decimal(middle) == Decimal(3*index) / 100
            assert Decimal(last) == Decimal(7*index+11) / 100
        return {'rows': len(lines), 'sha256': matrix.sha(rows['tikv'].encode()), 'mismatches': 0}

    def _run_case(self, not_null, explicit_default=False):
        name = 'not_null_default_zero' if explicit_default else ('not_null' if not_null else 'nullable_control')
        table = 't_' + name
        directory = self.output / name
        directory.mkdir()
        nullability = 'NOT NULL' if not_null else 'NULL DEFAULT NULL'
        if explicit_default:
            nullability = 'NOT NULL DEFAULT 0'
        self.sql(f'CREATE TABLE schema_matrix.{table} ('
                 f'c_first DECIMAL(18,2) {nullability},id BIGINT NOT NULL PRIMARY KEY CLUSTERED,'
                 f'c_middle DECIMAL(18,2) {nullability},c_last DECIMAL(18,2) {nullability});'
                 f'INSERT INTO schema_matrix.{table} SELECT i/100,i,3*i/100,(7*i+11)/100 FROM '
                 '(SELECT 1+a.n+10*b.n+100*c.n+1000*d.n AS i FROM schema_matrix.seed a,'
                 'schema_matrix.seed b,schema_matrix.seed c,schema_matrix.seed d) s;'
                 f'ALTER TABLE schema_matrix.{table} SET TIFLASH REPLICA 1')
        matrix.wait(lambda: self.sql("SELECT AVAILABLE FROM information_schema.tiflash_replica WHERE "
                                    f"TABLE_SCHEMA='schema_matrix' AND TABLE_NAME='{table}'") == '1', 'replica ready')
        matrix.wait(lambda: self.row_values(table, 'tikv') == self.row_values(table, 'tiflash'), 'initial rows ready')
        initial = self.compare_rows(table, directory, 'initial')
        self.stable_metrics(directory)
        original = self.metadata(table, directory, 'initial')
        table_id = original['id']
        before = self.metrics()
        (directory / 'metrics-before.txt').write_text(before)
        os.kill(self.pid, signal.SIGSTOP)
        self.paused = True
        matrix.wait(lambda: 'State:\tT' in Path(f'/proc/{self.pid}/status').read_text(), 'TiFlash actually stopped')
        (directory / 'stopped-process-status.txt').write_text(Path(f'/proc/{self.pid}/status').read_text())
        ddls = []
        for column in matrix.BUSINESS:
            statement = f'ALTER TABLE schema_matrix.{table} MODIFY COLUMN {column} DECIMAL(20,3) {nullability}'
            begin = time.monotonic()
            self.sql(statement)
            ddls.append({'sql': statement, 'seconds': time.monotonic()-begin})
            self.metadata(table, directory, 'after_' + column)
            print(json.dumps({'ddl_done': table, 'column': column}, ensure_ascii=False), flush=True)
        final_info = self.metadata(table, directory, 'final')
        column_ids = {column['name']['O']: column['id'] for column in final_info['cols']}
        for column in final_info['cols']:
            if column['name']['O'] in matrix.BUSINESS:
                assert bool(column['type']['Flag'] & 1) == not_null
                assert column.get('origin_default') is None, column
                if explicit_default:
                    assert not (column['type']['Flag'] & 4096), column
                    assert column.get('default') is not None, column
        (directory / 'show-create.txt').write_text(self.sql(f'SHOW CREATE TABLE schema_matrix.{table}') + '\n')
        (directory / 'ddl-jobs.txt').write_text(self.sql('ADMIN SHOW DDL JOBS 12') + '\n')
        offset = self.tf_log.stat().st_size
        start = time.monotonic()
        os.kill(self.pid, signal.SIGCONT)
        self.paused = False
        expected = self.row_values(table, 'tikv')
        matrix.wait(lambda: self.row_values(table, 'tiflash') == expected, 'backlog catches up')
        final = self.compare_rows(table, directory, 'after-ddl')
        evidence = self.evidence(directory, before, offset, table_id)
        publications = self.publication_evidence((directory / 'tiflash-phase.log').read_text(), table_id)
        result = {'table': table, 'table_id': table_id, 'not_null': not_null, 'ddls': ddls,
                  'explicit_default_zero': explicit_default,
                  'initial': initial, 'final': final, 'final_column_ids': column_ids,
                  'first_sync': 'Natural decode failure refreshes the final complete schema after SIGCONT',
                  'backlog_control': 'Actual TiFlash process SIGSTOP/SIGCONT', 'schema_publications': publications,
                  'catchup_seconds': time.monotonic()-start, **evidence}
        (directory / 'result.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
        self.result['cases'].append(result)
        self.save()
        counts = evidence['counter_deltas']
        assert counts['covered'] > 0, result
        assert counts['raft_decode'] == evidence['target_decode_sync_log_count'], result
        assert len(publications['distinct_schema_hashes']) <= 1, result
        if not_null:
            assert counts['raft_decode'] > 1, result
            assert publications['count'] > 1, result
        else:
            assert counts['raft_decode'] <= 1 and counts['skipped_extra_columns'] > 0, result
        print(json.dumps({'case_pass': name, 'counter_deltas': counts,
                          'same_schema_publications': publications['count'],
                          'final_rows': final['rows']}, ensure_ascii=False), flush=True)

    def debug(self, query):
        with (self.output / 'debug-transcript.jsonl').open('a') as log:
            log.write(json.dumps(dict(time=now(), query=query)) + '\n')
        return super().debug(query)

    def start(self):
        super().start()
        self.result['playground_pid'] = self.proc.pid
        self.result['gc_original'] = self.old_gc
        (self.output / 'server-cmdline.txt').write_bytes(Path(f'/proc/{self.pid}/cmdline').read_bytes().replace(b'\0', b' '))
        self.save()

    def metadata(self, table, directory, stage):
        if self.paused:
            status = Path(f'/proc/{self.pid}/status').read_text()
            assert 'State:\tT' in status
            assert self.args.tag.encode() in Path(f'/proc/{self.pid}/cmdline').read_bytes()
            (directory / f'{stage}-stopped-process-status.txt').write_text(status)
        info = json.loads(self.get(f'http://127.0.0.1:{10080+self.args.offset}/schema/schema_matrix/{table}'))
        (directory / f'{stage}-table-info.json').write_text(json.dumps(info, indent=2) + '\n')
        if self.paused:
            assert 'State:\tT' in Path(f'/proc/{self.pid}/status').read_text()
        return info

    def publication_evidence(self, data, table_id):
        infos = []
        decoder = json.JSONDecoder()
        for line in data.splitlines():
            if 'Update table_info:' not in line:
                continue
            for match in re.finditer(r'\[("(?:[^"\\]|\\.)*")\]', line):
                message = json.loads(match.group(1))
                if not message.startswith('Update table_info: '):
                    continue
                content = message.removeprefix('Update table_info: ')
                _, old_end = decoder.raw_decode(content)
                suffix = content[old_end:].lstrip()
                assert suffix.startswith('=>'), message
                new_info, _ = decoder.raw_decode(suffix[2:].lstrip())
                if new_info['id'] == table_id:
                    infos.append(new_info)
        with (self.active_directory / 'schema-publications.jsonl').open('w') as output:
            for info in infos:
                output.write(json.dumps(info, sort_keys=True) + '\n')
        hashes = sorted({matrix.sha(json.dumps(info, sort_keys=True).encode()) for info in infos})
        return dict(count=len(infos), distinct_schema_hashes=hashes,
                    sample_table_info=infos[0] if infos else None, inspected_side='new TableInfo after =>')

    def case(self, not_null, explicit_default=False):
        name = 'not_null_default_zero' if explicit_default else ('not_null' if not_null else 'nullable_control')
        self.active_directory = self.output / name
        print(json.dumps(dict(case_start=name)), flush=True)
        self._run_case(not_null, explicit_default)
        directory = self.active_directory
        result = self.result['cases'][-1]
        stages = ['initial', 'after_c_first', 'after_c_middle', 'after_c_last', 'final']
        metadata = {stage: json.loads((directory / f'{stage}-table-info.json').read_text()) for stage in stages}
        mappings = {stage: {c['name']['O']: c['id'] for c in info['cols']} for stage, info in metadata.items()}
        previous = mappings['initial']
        for column in matrix.BUSINESS:
            current = mappings['after_' + column]
            assert current[column] != previous[column]
            assert current[column] not in previous.values()
            assert all(current[c] == previous[c] for c in previous if c != column)
            previous = current
        assert previous == mappings['final']
        for column in metadata['final']['cols']:
            if column['name']['O'] not in matrix.BUSINESS:
                continue
            assert column['state'] == 5 and bool(column['type']['Flag'] & 1) == not_null
            assert column.get('origin_default') is None
            if explicit_default:
                assert Decimal(str(column.get('default'))) == 0 and not column['type']['Flag'] & 4096
        publications = [json.loads(line) for line in (directory / 'schema-publications.jsonl').read_text().splitlines()]
        assert len(publications) == result['schema_publications']['count']
        assert publications

        def schema_signature(info):
            return [(c['name']['O'], c['id'], c['state'], c['type']['Tp'], c['type']['Flag'],
                     c['type']['Flen'], c['type']['Decimal']) for c in info['cols']]

        assert all(schema_signature(info) == schema_signature(metadata['final']) for info in publications)
        assert len(result['schema_publications']['distinct_schema_hashes']) == 1
        counts = result['counter_deltas']
        covered_sync_lower_bound = int(counts['raft_decode'] - counts['no_watermark'] - counts['beyond_watermark'])
        if not_null:
            assert covered_sync_lower_bound > 0
        else:
            assert counts['raft_decode'] == 1 and counts['skipped_extra_columns'] > 0
        phase = (directory / 'tiflash-phase.log').read_text()
        assert not re.search(r'\[(?:ERROR|FATAL)\]', phase)
        assert not any('snapshot' in line.lower() and re.search(r'apply|applying|ingest', line, re.I)
                       for line in phase.splitlines())
        result.update(passed=True, column_id_transitions=mappings, same_final_schema_published=True,
                      covered_sync_lower_bound=covered_sync_lower_bound,
                      whole_process_stop_verified_for_all_ddls=True, phase_errors=0)
        (directory / 'result.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
        self.save()

    def run(self):
        try:
            self.start()
            self.sql('SET GLOBAL tidb_row_format_version=2; SET GLOBAL tidb_ddl_reorg_worker_cnt=1;'
                     'SET GLOBAL tidb_ddl_reorg_batch_size=256')
            self.result['actual_reorg_settings'] = self.sql(
                'SELECT @@GLOBAL.tidb_row_format_version,@@GLOBAL.tidb_ddl_reorg_worker_cnt,@@GLOBAL.tidb_ddl_reorg_batch_size')
            assert self.result['actual_reorg_settings'] == '2\t1\t256'
            (self.output / 'cluster-info.tsv').write_text(self.sql(
                'SELECT TYPE,INSTANCE,VERSION,GIT_HASH FROM information_schema.cluster_info') + '\n')
            self.case(True)
            self.case(False)
            self.case(True, explicit_default=True)
            self.result['passed'] = all(case['passed'] for case in self.result['cases'])
        except BaseException:
            self.result['error'] = traceback.format_exc()
            self.save()
            raise
        finally:
            self.cleanup()



if __name__ == '__main__':
    runner = MissingColumns(matrix.make_parser(__doc__, allow_baseline=False).parse_args())
    runner.run()
    raise SystemExit(0 if runner.result['passed'] else 1)
