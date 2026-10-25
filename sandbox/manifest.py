"""Writes the manifest of a built-in sandbox profile (FTR.NAB.CMN-0008 tech §2.3).

python3 manifest.py <profile> > <profile>.json — runs inside the profile
image at build time: the tools of R2 and R3 with their versions, the Python
packages of /opt/py, the system packages and the commands of PATH.
"""

import importlib.metadata
import json
import os
import shutil
import subprocess
import sys

# Group (a key: the panel translates it), tool, command printing the version (None — no version), profile.
TOOLS = [
    ("archives", "zip", ["zip", "-v"], "base"),
    ("archives", "unzip", ["unzip", "-v"], "base"),
    ("archives", "xz", ["xz", "--version"], "base"),
    ("archives", "bzip2", None, "base"),
    ("utilities", "jq", ["jq", "--version"], "base"),
    ("utilities", "rg", ["rg", "--version"], "base"),
    ("utilities", "file", ["file", "--version"], "base"),
    ("utilities", "tree", ["tree", "--version"], "base"),
    ("utilities", "patch", ["patch", "--version"], "base"),
    ("utilities", "less", ["less", "--version"], "base"),
    ("utilities", "bc", ["bc", "--version"], "base"),
    ("utilities", "sqlite3", ["sqlite3", "--version"], "base"),
    ("python", "python3", ["python3", "--version"], "base"),
    ("node-git", "node", ["node", "--version"], "base"),
    ("node-git", "git", ["git", "--version"], "base"),
    ("pdf", "typst", ["typst", "--version"], "documents"),
    ("pdf", "pdftotext", ["pdftotext", "-v"], "documents"),
    ("pdf", "pdftoppm", ["pdftoppm", "-v"], "documents"),
    ("pdf", "qpdf", ["qpdf", "--version"], "documents"),
]

PROFILES = {"base": {"base"}, "documents": {"base", "documents"}}


def version_of(cmd):
    if cmd is None:
        return ""
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.TimeoutExpired):
        return ""
    text = (out.stdout or out.stderr).strip().splitlines()
    first = text[0] if text else ""
    for word in first.replace(",", " ").split():
        w = word.lstrip("v")
        if w[:1].isdigit():
            return w
    return first[:40]


def python_packages():
    paths = [p for p in ("/opt/py/lib/python3.13/site-packages",) if os.path.isdir(p)]
    seen = {}
    for d in importlib.metadata.distributions(path=paths or None):
        name = d.metadata["Name"]
        if name:
            seen[name] = d.version
    return [{"name": n, "version": v} for n, v in sorted(seen.items(), key=lambda kv: kv[0].lower())]


def system_packages():
    try:
        out = subprocess.run(["dpkg-query", "-W", "-f", "${Package}\t${Version}\n"], capture_output=True, text=True, check=True)
    except (OSError, subprocess.CalledProcessError):
        return []
    rows = [line.split("\t", 1) for line in out.stdout.splitlines() if "\t" in line]
    return [{"name": n, "version": v} for n, v in sorted(rows)]


def commands():
    seen = set()
    for d in os.environ.get("PATH", "").split(os.pathsep):
        try:
            for e in os.scandir(d):
                if e.is_file() and os.access(e.path, os.X_OK):
                    seen.add(e.name)
        except OSError:
            pass
    return sorted(seen)


def assets():
    out = []
    try:
        with open("/opt/nabu/assets.versions") as f:
            for line in f:
                name, _, version = line.strip().partition("\t")
                if name and name != "typst":
                    out.append({"group": "html-assets", "name": name, "version": version})
    except OSError:
        pass
    return out


def main():
    profile = sys.argv[1]
    groups = PROFILES[profile]
    tools = [{"group": g, "name": n, "version": version_of(c)} for g, n, c, p in TOOLS if p in groups and shutil.which(n)]
    m = {
        "name": profile,
        "kind": "builtin",
        "version": os.environ.get("NABU_BASE_VERSION", ""),
        "tools": tools + assets(),
        "python": python_packages(),
        "system": system_packages(),
        "commands": commands(),
    }
    if profile != "base":
        m["includes"] = "base"
    json.dump(m, sys.stdout, ensure_ascii=False)


if __name__ == "__main__":
    main()
