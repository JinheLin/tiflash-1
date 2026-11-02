"""Deterministic workload with transactional commit markers and a replayable oracle."""
import hashlib
import json
import os
from pathlib import Path
import random
import threading
import time


def initial_row(key):
    return [key, 0, key * 17, None if key % 11 == 0 else key * 3, f'seed-{key}']


def digest_operation(operation):
    body = {k: operation[k] for k in ('worker', 'seq', 'table', 'kind', 'key', 'row')}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


class Journal:
    def __init__(self, path):
        self.path = Path(path)
        self.lock = threading.Lock()
        self.path.touch(exist_ok=True)

    def append(self, event, **fields):
        record = dict(event=event, time=time.time(), **fields)
        with self.lock, self.path.open('a') as stream:
            stream.write(json.dumps(record, sort_keys=True) + '\n')
            stream.flush()
            os.fsync(stream.fileno())


def apply_operation(tables, operation):
    rows = tables[operation['table']]
    key = operation['key']
    if operation['kind'] == 'delete':
        del rows[key]
    else:
        rows[key] = operation['row']


def replay(path, rows):
    tables = {name: {key: initial_row(key) for key in range(1, rows + 1)} for name in ('load_a', 'load_b')}
    pending, sequences = {}, {}
    with Path(path).open() as stream:
        for line in stream:
            event = json.loads(line)
            token = (event.get('worker'), event.get('seq'))
            if event['event'] == 'intent':
                if token in pending or event['seq'] != sequences.get(token[0], 0) + 1:
                    raise RuntimeError(f'Duplicate or out-of-order intent: {token}')
                if event['digest'] != digest_operation(event):
                    raise RuntimeError(f'Intent digest mismatch: {token}')
                pending[token] = event
            elif event['event'] == 'confirmed':
                operation = pending.pop(token)
                if operation['digest'] != event['digest']:
                    raise RuntimeError(f'Confirmation digest mismatch: {token}')
                apply_operation(tables, operation)
                sequences[token[0]] = token[1]
    if pending:
        raise RuntimeError(f'Unresolved transaction intents: {list(pending)}')
    return tables, sequences


def commit_operation(connect, connection, operation, journal, timeout=30):
    """A unique marker is inserted before DML, in the same transaction.

    After an ambiguous commit, a matching marker proves success. If the marker
    is not yet visible, retrying its unique key serializes against the original
    transaction; no second DML can commit under the same operation identity.
    """
    deadline = time.monotonic() + timeout
    uncertain = False
    while True:
        try:
            if connection is None:
                connection = connect()
            if uncertain:
                with connection.cursor() as cursor:
                    cursor.execute('SELECT digest FROM schema_matrix.confirmations WHERE worker=%s AND seq=%s',
                                   (operation['worker'], operation['seq']))
                    marker = cursor.fetchone()
                if marker:
                    if marker[0] != operation['digest']:
                        raise ValueError('Commit marker belongs to a different operation')
                    return connection
            connection.begin()
            with connection.cursor() as cursor:
                cursor.execute('INSERT INTO schema_matrix.confirmations VALUES(%s,%s,%s)',
                               (operation['worker'], operation['seq'], operation['digest']))
                table = operation['table']
                if table not in ('load_a', 'load_b'):
                    raise ValueError('Unexpected workload table')
                if operation['kind'] == 'delete':
                    cursor.execute(f'DELETE FROM schema_matrix.{table} WHERE id=%s', (operation['key'],))
                elif operation['kind'] == 'insert':
                    cursor.execute(f'INSERT INTO schema_matrix.{table} VALUES(%s,%s,%s,%s,%s)', operation['row'])
                else:
                    row = operation['row']
                    cursor.execute(f'UPDATE schema_matrix.{table} SET last_seq=%s,n=%s,nullable_n=%s,payload=%s '
                                   'WHERE id=%s', (*row[1:], row[0]))
                if cursor.rowcount != 1:
                    raise ValueError(f'Unexpected affected rows for {operation}')
            connection.commit()
            return connection
        except ValueError:
            if connection:
                connection.rollback()
            raise
        except Exception as error:
            # Syntax, privilege, or schema errors are failures, not transient retries.
            code = error.args[0] if error.args else None
            retryable = (1062, 1205, 1213, 2003, 2006, 2013, 9005)
            if code not in retryable:
                raise
            journal.append('uncertain', worker=operation['worker'], seq=operation['seq'], error=str(error))
            if connection:
                connection.close()
            connection = None
            uncertain = True
            if time.monotonic() >= deadline:
                raise RuntimeError('Could not resolve transaction commit state') from error
            time.sleep(0.1)


class Workload:
    def __init__(self, connect, output, rows, workers, rate, seed):
        self.connect, self.rows, self.workers, self.rate, self.seed = connect, rows, workers, rate, seed
        self.journal = Journal(Path(output) / 'operations.jsonl')
        self.oracle = {name: {key: initial_row(key) for key in range(1, rows + 1)}
                       for name in ('load_a', 'load_b')}
        self.condition = threading.Condition()
        self.paused, self.stopped, self.active = True, False, 0
        self.error = None
        self.threads = []
        self.sequences = [0] * workers
        self.counts = {'insert': 0, 'update': 0, 'delete': 0}
        self.enabled_since = None
        self.enabled_seconds = 0.0

    def start(self):
        for worker in range(self.workers):
            thread = threading.Thread(target=self._worker, args=(worker,), name=f'soak-writer-{worker}', daemon=True)
            self.threads.append(thread)
            thread.start()

    def check(self):
        if self.error:
            raise RuntimeError('Writer failed: ' + self.error)

    def resume(self):
        with self.condition:
            self.check()
            if self.paused:
                self.enabled_since = time.monotonic()
            self.paused = False
            self.condition.notify_all()

    def pause(self, timeout=40):
        with self.condition:
            if not self.paused and self.enabled_since is not None:
                self.enabled_seconds += time.monotonic() - self.enabled_since
                self.enabled_since = None
            self.paused = True
            self.condition.notify_all()
            if not self.condition.wait_for(lambda: self.active == 0, timeout=timeout):
                raise RuntimeError('Writers did not drain before checkpoint')
            self.check()

    def status(self):
        with self.condition:
            enabled = self.enabled_seconds
            if self.enabled_since is not None:
                enabled += time.monotonic() - self.enabled_since
            return dict(sequences=list(self.sequences), committed=sum(self.sequences), kinds=dict(self.counts),
                        enabled_seconds=enabled, paused=self.paused, active=self.active)

    def stop(self):
        with self.condition:
            if self.enabled_since is not None:
                self.enabled_seconds += time.monotonic() - self.enabled_since
                self.enabled_since = None
            self.paused = True
            self.stopped = True
            self.condition.notify_all()
        for thread in self.threads:
            thread.join(timeout=40)
        if any(thread.is_alive() for thread in self.threads):
            raise RuntimeError('Writer remains active during cleanup')

    def _worker(self, worker):
        randomizer = random.Random(self.seed + worker)
        holes = {name: [] for name in self.oracle}
        connection = None
        next_write = time.monotonic()
        try:
            while True:
                with self.condition:
                    self.condition.wait_for(lambda: self.stopped or not self.paused)
                    if self.stopped:
                        break
                    delay = next_write - time.monotonic()
                    if delay > 0:
                        self.condition.wait(timeout=min(delay, 0.1))
                        continue
                    self.active += 1
                try:
                    table = randomizer.choice(('load_a', 'load_b'))
                    kind = randomizer.choices(('insert', 'update', 'delete'), weights=(20, 60, 20))[0]
                    if kind == 'insert' and holes[table]:
                        index = randomizer.randrange(len(holes[table]))
                        key = holes[table][index]
                        holes[table][index] = holes[table][-1]
                        holes[table].pop()
                    else:
                        if kind == 'insert':
                            kind = 'update'
                        owned = range(worker + 1, self.rows + 1, self.workers)
                        present = None
                        for _ in range(100):
                            key = randomizer.choice(owned)
                            if key in self.oracle[table]:
                                present = key
                                break
                        if present is None:
                            # This worker's partition is empty; reinsert one of its deleted keys.
                            kind, key = 'insert', holes[table].pop()
                    seq = self.sequences[worker] + 1
                    row = None if kind == 'delete' else [key, seq, seq * 101 + worker,
                                                         None if seq % 11 == 0 else seq * 17,
                                                         f'w{worker}-s{seq}']
                    operation = dict(worker=worker, seq=seq, table=table, kind=kind, key=key, row=row)
                    operation['digest'] = digest_operation(operation)
                    self.journal.append('intent', **operation)
                    connection = commit_operation(self.connect, connection, operation, self.journal)
                    self.journal.append('confirmed', worker=worker, seq=seq, digest=operation['digest'])
                    apply_operation(self.oracle, operation)
                    if kind == 'delete':
                        holes[table].append(key)
                    with self.condition:
                        self.sequences[worker] = seq
                        self.counts[kind] += 1
                    next_write = max(next_write + self.workers / self.rate, time.monotonic())
                finally:
                    with self.condition:
                        self.active -= 1
                        self.condition.notify_all()
        except Exception as error:
            with self.condition:
                self.error = repr(error)
                self.stopped = True
                self.condition.notify_all()
        finally:
            if connection:
                connection.close()
