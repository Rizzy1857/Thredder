#!/usr/bin/env python3
"""Validate raw benchmark accounting and generate median-per-configuration summaries."""
import csv
import math
import pathlib
import statistics
import sys
from collections import defaultdict

root = pathlib.Path(sys.argv[1])
with (root / 'trials.csv').open() as stream:
    trials = list(csv.DictReader(stream))
with (root / 'samples.csv').open() as stream:
    samples = list(csv.DictReader(stream))
assert len(trials) == 128 and len(samples) == 128 * 256, 'Incomplete run'
by_trial = defaultdict(list)
for row in samples:
    by_trial[row['trial']].append(row)
assert len({t['trial'] for t in trials}) == 128 and set(by_trial) == {t['trial'] for t in trials}
for trial in trials:
    rows = by_trial[trial['trial']]
    assert len(rows) == 256 and {int(r['job_index']) for r in rows} == set(range(256))
    assert all(r['phase'] == trial['phase'] for r in rows)
    accepted = [r for r in rows if r['accepted'] == 'true']
    assert len(accepted) == int(trial['accepted'])
    assert 256 - len(accepted) == int(trial['rejected'])
    assert all(int(r['admission_call_ns']) >= 0 for r in rows)
    for row in rows:
        if row['accepted'] == 'false':
            assert row['queue_wait_ns'] == row['submit_to_completion_ns'] == ''
        else:
            assert 0 <= int(row['queue_wait_ns']) <= int(row['submit_to_completion_ns'])
    for raw, column in [('queue_wait_ns', 'queue'), ('submit_to_completion_ns', 'latency')]:
        values = sorted(int(r[raw]) for r in accepted)
        for percentile in (50, 95, 99):
            computed = values[math.ceil(len(values) * percentile / 100) - 1] / 1e6
            assert abs(computed - float(trial[f'{column}_p{percentile}_ms'])) <= 0.000001
    rate = len(accepted) * 1000 / float(trial['elapsed_ms'])
    assert math.isclose(rate, float(trial['completed_per_s']), rel_tol=1e-5)
keys = ['workload', 'workers', 'capacity', 'policy', 'offer_ms']
metrics = ['accepted', 'rejected', 'completed_per_s', 'queue_p50_ms', 'queue_p95_ms', 'queue_p99_ms',
           'latency_p50_ms', 'latency_p95_ms', 'latency_p99_ms']
groups = defaultdict(list)
for trial in trials:
    groups[tuple(trial[k] for k in keys)].append(trial)
assert len(groups) == 32
summary = []
for key, rows in sorted(groups.items()):
    assert len(rows) == 4 and sum(r['phase'] == 'warmup' for r in rows) == 1
    measured = [r for r in rows if r['phase'] == 'measured']
    assert len(measured) == 3
    result = dict(zip(keys, key))
    result.update({m: f'{statistics.median(float(r[m]) for r in measured):.3f}' for m in metrics})
    summary.append(result)
with (root / 'summary.csv').open('w', newline='') as stream:
    writer = csv.DictWriter(stream, fieldnames=keys + metrics, lineterminator='\n')
    writer.writeheader()
    writer.writerows(summary)
print('Validated 128 trials and 32768 submissions; saved 32 median configuration summaries')
for r in summary:
    if r['policy'] == 'FIFO' and r['capacity'] == '8':
        print(r)
