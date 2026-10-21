"""Fail if benchmark throughput falls below the SPEC targets (with CI headroom)."""

import re
import sys

# name substring -> minimum elements/s (targets: core 1M/s, e2e 250k/s; 30% headroom for CI noise)
TARGETS = {
    "core/cycle/w64/backlog10000": 700_000,
    "runtime/e2e/noop/bulk": 175_000,
}

UNITS = {"elem/s": 1, "Kelem/s": 1e3, "Melem/s": 1e6, "Gelem/s": 1e9}


def parse(paths):
    results, name = {}, None
    for path in paths:
        for line in open(path, encoding="utf-8"):
            m = re.match(r"^(\S+/\S+)", line)
            if m:
                name = m.group(1)
            m = re.search(r"thrpt:\s+\[\S+ \S+ (\S+) (\S+) ", line)
            if m and name:
                results[name] = float(m.group(1)) * UNITS[m.group(2)]
    return results


def main():
    results = parse(sys.argv[1:])
    failed = False
    for key, minimum in TARGETS.items():
        got = next((v for k, v in results.items() if key in k), None)
        status = "MISSING" if got is None else ("ok" if got >= minimum else "BELOW TARGET")
        print(f"{key:40} {got or 0:>14,.0f}/s  (min {minimum:,})  {status}")
        failed |= status != "ok"
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
