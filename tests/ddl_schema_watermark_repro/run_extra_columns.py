#!/usr/bin/env python3
"""Execute PLAN_EXTRA_COLUMNS.md against an isolated TiUP cluster."""
import datetime
import hashlib
import json
from pathlib import Path
import re
import time
import traceback

import run_matrix as matrix

HERE = Path(__file__).resolve().parent


def now():
    return datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).isoformat()


class ExtraColumns(matrix.Matrix):
    def __init__(self, args):
        super().__init__(args)
        self.env = {k: v for k, v in self.env.items() if 'proxy' not in k.lower()}
        self.result['started_at'] = now()
        self.result['plan'] = str(HERE / 'PLAN_EXTRA_COLUMNS.md')
        self.result['harness_sha256'] = {
            str(p): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in [Path(__file__), HERE / 'run_matrix.py', HERE / 'restartable_tiflash.py']
        }

    def transcript(self, kind, query, **fields):
        with (self.output / 'commands.jsonl').open('a') as output:
            output.write(json.dumps(dict(time=now(), kind=kind, query=query, **fields)) + '\n')

    def sql(self, query, engine=None):
        self.transcript('sql', query, engine=engine)
        return super().sql(query, engine)

    def debug(self, query):
        self.transcript('tiflash-debug', query)
        return super().debug(query)

    def store_ids(self):
        return matrix.wait(lambda: super(ExtraColumns, self).store_ids(), 'TiFlash store registration')

    def metadata(self, table, directory, stage):
        text = self.get(f'http://127.0.0.1:{10080 + self.args.offset}/schema/schema_matrix/{table}')
        (directory / f'schema-{stage}.json').write_text(text)
        info = json.loads(text)
        return {c['name']['L']: int(c['id']) for c in info['cols']}

    def controlled_drop(self, position, target):
        name = 'drop_' + position
        directory = self.output / name
        initial = self.create_table(name, directory)
        old_ids = self.metadata(name, directory, 'before')
        assert list(old_ids) == matrix.COLS, old_ids
        (directory / 'regions-before.tsv').write_text(self.sql(f'SHOW TABLE schema_matrix.{name} REGIONS') + '\n')
        before = self.stable_metrics(directory)
        (directory / 'metrics-before.txt').write_text(before)
        offset = self.tf_log.stat().st_size
        started = time.monotonic()
        self.debug('DBGInvoke enable_fail_point(pause_before_apply_raft_cmd)')
        self.failpoint = True
        try:
            self.update(name)
            committed = now()
            statement = self.ddl(name, 'drop', target)
            refreshed = self.debug(f'DBGInvoke refresh_table_schema(schema_matrix, {name})')
            assert 'refreshed' in refreshed, refreshed
            (directory / 'schema-refresh.txt').write_text(refreshed + '\n')
            new_ids = self.metadata(name, directory, 'after')
            assert old_ids[target] not in new_ids.values(), (old_ids, new_ids)
            assert new_ids == {k: v for k, v in old_ids.items() if k != target}, (old_ids, new_ids)
            refreshed_at = now()
        finally:
            self.debug('DBGInvoke disable_fail_point(pause_before_apply_raft_cmd)')
            self.failpoint = False
        if self.args.baseline:
            matrix.wait(lambda: matrix.deltas(before, self.metrics())['raft_decode'] > 1,
                        'baseline repeats decode-triggered schema synchronization')
        else:
            matrix.wait(lambda: matrix.deltas(before, self.metrics())['skipped_extra_columns'] > 0,
                        'current binary actually skips an extra column')
        columns = [c for c in matrix.COLS if c != target]
        expected = self.rows(name, columns, 'tikv')
        matrix.wait(lambda: self.rows(name, columns, 'tiflash') == expected, 'all 10000 rows catch up')
        final = self.compare(name, columns, directory, 'after-ddl', updated=True)
        evidence = self.evidence(directory, before, offset, initial['table_id'])
        counts = evidence['counter_deltas']
        assert counts['raft_decode'] == evidence['target_decode_sync_log_count'], evidence
        if self.args.baseline:
            assert counts['raft_decode'] > 1, evidence
        else:
            assert counts['raft_decode'] == 0 and counts['covered'] > 0 and counts['skipped_extra_columns'] > 0, evidence
            for line in evidence['skip_log_lines']:
                match = re.search(r'table_id=(\d+).*?read_tso=(\d+) max_commit_ts=(\d+) unknown_column_id=(\d+)', line)
                assert match, line
                table_id, read_tso, commit_ts, column_id = map(int, match.groups())
                assert table_id == initial['table_id'] and 0 < commit_ts <= read_tso, line
                assert column_id == old_ids[target] > 0, line
        result = dict(name=name, position=position, target=target, initial=initial, final=final,
                      old_column_ids=old_ids, new_column_ids=new_ids, removed_column_id=old_ids[target],
                      update_committed_at=committed, schema_refreshed_at=refreshed_at, ddl=statement,
                      catchup_seconds=round(time.monotonic() - started, 3), passed=True, **evidence)
        (directory / 'show-create.txt').write_text(self.sql(f'SHOW CREATE TABLE schema_matrix.{name}') + '\n')
        (directory / 'ddl-jobs.txt').write_text(self.sql('ADMIN SHOW DDL JOBS 10') + '\n')
        (directory / 'result.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
        self.result['cases'].append(result)
        self.save()
        print(json.dumps(dict(case_pass=name, counter_deltas=counts, rows=final['rows'],
                              target_sync_logs=evidence['target_decode_sync_log_count'],
                              removed_column_id=old_ids[target])), flush=True)

    def run(self):
        try:
            self.start()
            self.sql('SET GLOBAL tidb_row_format_version=2')
            (self.output / 'cluster-info.tsv').write_text(self.sql(
                'SELECT TYPE,INSTANCE,VERSION,GIT_HASH FROM information_schema.cluster_info') + '\n')
            self.result['row_format_version'] = self.sql('SELECT @@GLOBAL.tidb_row_format_version')
            assert self.result['row_format_version'] == '2'
            self.save()
            for position, target in zip(['first', 'middle', 'last'], matrix.BUSINESS):
                print(json.dumps(dict(case_start='drop_' + position)), flush=True)
                self.controlled_drop(position, target)
            self.reverse_case()
            if not self.args.baseline:
                assert any(case['skip_log_lines'] for case in self.result['cases']), 'No sampled skip proof'
            self.result['passed'] = True
        except BaseException:
            self.result['error'] = traceback.format_exc()
            self.save()
            raise
        finally:
            self.cleanup()



if __name__ == '__main__':
    runner = ExtraColumns(matrix.make_parser(__doc__).parse_args())
    runner.run()
    raise SystemExit(0 if runner.result['passed'] else 1)
