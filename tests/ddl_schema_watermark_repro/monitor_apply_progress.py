#!/usr/bin/env python3
"""Sample TiKV/proxy/C++ apply progress without writing data or consuming Ready."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import threading
import time
import urllib.request


def classify(tikv, proxy, cpp_applied):
    """Describe the observed pipeline, not a claim about the root cause."""
    if not all(isinstance(value, dict) and 'raft_status' in value for value in (tikv, proxy)):
        return dict(stage='endpoint_unavailable')
    leader, follower = tikv['raft_status'], proxy['raft_status']
    committed = follower['hard_state']['commit']
    applied = follower['applied']
    diag = proxy.get('apply_diagnostics') or {}
    result = dict(leader_committed=leader['hard_state']['commit'], committed=committed, applied=applied,
                  persisted=follower['persisted_index'], last_index=follower['last_index'],
                  cpp_applied=cpp_applied, last_applying=diag.get('last_applying_index'), diagnostics=diag)
    if committed < result['leader_committed']:
        stage = 'receiving'
    elif result['persisted'] < committed:
        stage = 'persisting'
    elif applied < committed:
        last_applying = result['last_applying']
        if last_applying is None:
            stage = 'apply_backlog_without_diagnostics'
        elif last_applying > applied:
            stage = 'scheduled_not_completed'
        elif diag.get('raw_node_has_ready') and not diag.get('fsm_has_ready'):
            stage = 'raw_ready_not_polled'
        else:
            stage = 'committed_not_scheduled'
    elif cpp_applied is None:
        stage = 'cpp_unavailable'
    elif cpp_applied < committed:
        stage = 'cpp_behind_proxy'
    else:
        stage = 'caught_up'
    return dict(result, stage=stage)


class ProgressTracker:
    def __init__(self, stall_seconds):
        self.stall_seconds = stall_seconds
        self.previous = {}

    def update(self, region, observation, generation, now):
        stage = observation['stage']
        if stage in ('endpoint_unavailable', 'cpp_unavailable', 'caught_up'):
            self.previous.pop(region, None)
            return dict(observation, stalled=False)
        # Track the counter relevant to the current stage, not commit growth.
        # New commits alone must not reset an apply stall timer.
        if stage == 'receiving':
            phase = 'receiving'
            progress = observation['last_index']
        elif stage == 'persisting':
            phase = 'persisting'
            progress = observation['persisted']
        elif stage == 'cpp_behind_proxy':
            phase = 'cpp'
            progress = observation['cpp_applied']
        else:
            phase = 'applying'
            progress = (observation['applied'], observation['cpp_applied'])
        key = (generation, phase, progress)
        previous = self.previous.get(region)
        since = previous[1] if previous and previous[0] == key else now
        self.previous[region] = (key, since)
        return dict(observation, stagnant_seconds=now-since,
                    stalled=now-since >= self.stall_seconds)


def main(args):
    args.output.mkdir(parents=True, exist_ok=False)
    stop = threading.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stop.set())
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    env = {k: v for k, v in os.environ.items() if 'proxy' not in k.lower()}
    env['LD_LIBRARY_PATH'] = str(args.tiflash.parent) + ':' + env.get('LD_LIBRARY_PATH', '')
    tracker = ProgressTracker(args.stall_seconds)
    warned = {}
    finished = time.monotonic() + args.duration

    def fetch(url):
        try:
            with opener.open(url, timeout=args.timeout) as response:
                return json.load(response)
        except Exception as error:
            return dict(error=str(error))

    def cpp_regions():
        try:
            run = subprocess.run([str(args.tiflash), 'client', '--host', '127.0.0.1',
                                  '--port', str(args.debug_port), '--query',
                                  'DBGInvoke dump_all_region(0, false, true)'],
                                 env=env, capture_output=True, text=True, timeout=args.timeout)
            return dict(returncode=run.returncode, stdout=run.stdout, stderr=run.stderr[-2000:])
        except Exception as error:
            return dict(error=str(error))

    with ThreadPoolExecutor(max_workers=2*len(args.regions)+1) as pool, \
            (args.output / 'samples.jsonl').open('a') as samples, \
            (args.output / 'alerts.jsonl').open('a') as alerts:
        while not stop.is_set() and time.monotonic() < finished:
            started = time.monotonic()
            requests = {}
            for region in args.regions:
                requests[f'tikv_{region}'] = pool.submit(fetch, f'{args.tikv}/region/{region}')
                requests[f'proxy_{region}'] = pool.submit(fetch, f'{args.proxy}/region/{region}')
            requests['cpp'] = pool.submit(cpp_regions)
            raw = {key: future.result() for key, future in requests.items()}
            cpp = {int(region): int(index) for region, index in re.findall(
                r'region_id=(\d+).*?applied_index=(\d+)', raw['cpp'].get('stdout', ''))}
            # The restart wrapper's actual child PID changes on each restart.
            generation = args.pid_file.read_text().strip() if args.pid_file and args.pid_file.exists() else None
            observations = {}
            for region in args.regions:
                observation = tracker.update(region, classify(raw[f'tikv_{region}'], raw[f'proxy_{region}'],
                                             cpp.get(region)), generation, started)
                observations[str(region)] = observation
                alert_key = (generation, observation['stage'], observation.get('applied'))
                if observation['stalled'] and warned.get(region) != alert_key:
                    record = dict(time=time.time(), region_id=region, generation=generation, **observation)
                    alerts.write(json.dumps(record)+'\n')
                    alerts.flush()
                    print(json.dumps(record), flush=True)
                    warned[region] = alert_key
            samples.write(json.dumps(dict(time=time.time(), monotonic=started, generation=generation,
                                          regions=observations, raw=raw))+'\n')
            samples.flush()
            stop.wait(max(0, args.interval-(time.monotonic()-started)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument('--tiflash', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--tikv', default='http://127.0.0.1:26180')
    parser.add_argument('--proxy', default='http://127.0.0.1:26292')
    parser.add_argument('--debug-port', type=int, default=29000)
    parser.add_argument('--regions', type=int, nargs='+', default=[143, 150])
    parser.add_argument('--pid-file', type=Path)
    parser.add_argument('--interval', type=float, default=2)
    parser.add_argument('--timeout', type=float, default=2)
    parser.add_argument('--stall-seconds', type=float, default=20)
    parser.add_argument('--duration', type=float, default=1800)
    args = parser.parse_args()
    if min(args.interval, args.timeout, args.stall_seconds, args.duration) <= 0:
        parser.error('Intervals and timeouts must be positive')
    main(args)
