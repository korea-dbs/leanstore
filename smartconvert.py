#!/usr/bin/env python3
"""
Robust OCP+SMART log → CSV converter.
Usage:
    python smartconvert.py inputfile.log outputfile.csv
"""

import json, csv, pathlib, sys, re

# ── parse command line ─────────────────────────────────────────────
if len(sys.argv) != 3:
    print(f"Usage: {sys.argv[0]} inputfile.log outputfile.csv", file=sys.stderr)
    sys.exit(1)

infile  = pathlib.Path(sys.argv[1])
outfile = pathlib.Path(sys.argv[2])

# ── helper to read one full JSON block ─────────────────────────────────────────────

def read_one_json(lines_iter):
    buf, level = [], 0
    for line in lines_iter:
        if '{' in line:
            first_curly = line.index('{')
            level += line.count('{', first_curly) - line.count('}', first_curly)
            buf.append(line.rstrip())
            break
    else:
        raise ValueError("Unexpected end of file while looking for JSON start")

    for line in lines_iter:
        level += line.count('{') - line.count('}')
        buf.append(line.rstrip())
        if level == 0:
            break
    return "\n".join(buf)

# ── main conversion ─────────────────────────────────────────────
rows = []
with infile.open(encoding="utf-8") as f:
    line_iter = iter(f)
    for line in line_iter:
        if line.startswith("==="):
            # extract fields using regex
            match = re.match(r"=== ([^=]+) ===(?: ([^=]+) === ([^=]+) === ([^=]+) ===)?", line)
            if not match:
                raise ValueError(f"Unrecognized header format: {line.strip()}")

            ts = match.group(1).strip()
            blk = match.group(2).strip() if match.group(2) else ""
            dev = match.group(3).strip() if match.group(3) else ""
            prefix = match.group(4).strip() if match.group(4) else ""

            ocp_raw   = read_one_json(line_iter)
            smart_raw = read_one_json(line_iter)

            ocp   = json.dumps(json.loads(ocp_raw),   separators=(",", ":"))
            smart = json.dumps(json.loads(smart_raw), separators=(",", ":"))

            rows.append((ts, blk, dev, prefix, ocp, smart))

# ── write CSV ────────────────────────────────────────────
with outfile.open("w", newline="", encoding="utf-8") as f:
    w = csv.writer(f, quoting=csv.QUOTE_ALL)
    w.writerow(["timestamp", "blk", "dev", "prefix", "ocp_json", "smart_json"])
    w.writerows(rows)

print(f"✓  Wrote {len(rows)} rows ➔ {outfile}")
