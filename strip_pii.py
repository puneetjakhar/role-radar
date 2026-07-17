#!/usr/bin/env python3
"""Remove PII fields from JSON data files before they're committed.

Runs in the workflow between the scrape/notify steps and the commit step,
so no PII ever ships in a public commit even if a crawler harvests it.
"""
import json
import os

PII_FIELDS = {'recruiter_email'}

TARGETS = [
    'crawled_jobs.json',
    'linkedin_jobs.json',
]


def scrub(path):
    if not os.path.exists(path):
        print(f'{path}: skip (missing)')
        return 0
    with open(path) as f:
        try:
            data = json.load(f)
        except Exception as e:
            print(f'{path}: parse error: {e}')
            return 0
    if not isinstance(data, list):
        print(f'{path}: skip (not a list)')
        return 0
    removed_values = 0
    removed_keys = 0
    for row in data:
        if not isinstance(row, dict):
            continue
        for field in PII_FIELDS:
            if field in row:
                val = row.pop(field)
                removed_keys += 1
                if val is not None:
                    removed_values += 1
    if removed_keys:
        with open(path, 'w') as f:
            json.dump(data, f, ensure_ascii=True, indent=2)
        print(f'{path}: dropped {removed_keys} field keys ({removed_values} with real values)')
    else:
        print(f'{path}: clean')
    return removed_values


if __name__ == '__main__':
    total = sum(scrub(t) for t in TARGETS)
    print(f'Total PII fields removed: {total}')
