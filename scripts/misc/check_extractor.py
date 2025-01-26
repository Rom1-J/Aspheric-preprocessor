from pathlib import Path
import sys

success = {}
failed = {}

def verify(p):
    failed[p.parent.name] = []
    success[p.parent.name] = []
    with p.open() as f:
        for l in f:
            success[p.parent.name].append(stat(p.parent.name, l.strip()))

def stat(uuid, line):
    try:
        _id, _emails, _ips, _domains = line.split(",")
        return {"id": _id, "emails": len(_emails.split("|")), "ips": len(_ips.split("|")), "domains": len(_domains.split("|"))}
    except Exception as e:
        failed[uuid].append(line)


paths = list(Path(sys.argv[1]).glob("*/_metadata.csv"))

print([verify(a) for a in paths])

print("success:", sum(len(v) for k, v in success.items()))

print([(k, set(i.split(",")[0] for i in v)) for k, v in failed.items()])
