"""Sets one version in every manifest and lockfile, or checks that they all carry it.

    python3 dev/set-version.py 0.2.0-beta.1
    python3 dev/set-version.py --check 0.2.0-beta.1
    python3 dev/set-version.py --bump

The version is semver, as Cargo, npm and JSR want it. Hatchling normalises pyproject.toml's to
PEP 440 (0.2.0b1) at build time, but uv.lock records the normalised form, so it is written so.
Plain regex edits, so neither cargo, uv nor npm is needed.
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SEMVER = re.compile(r"(\d+)\.(\d+)\.(\d+)(?:-(alpha|beta|rc)\.(\d+))?")
PEP440_PRE = {"alpha": "a", "beta": "b", "rc": "rc"}

# (file, pattern capturing the text before the quoted version, occurrences, PEP 440 form)
LOCATIONS = [
    ("rust/Cargo.toml", r"(?m)^(version = )", 1, False),
    ("rust/Cargo.toml", r"(substantial-macros = \{ version = )", 1, False),
    ("rust/macros/Cargo.toml", r"(?m)^(version = )", 1, False),
    ("Cargo.lock", r'(name = "substantial(?:-macros)?"\nversion = )', 2, False),
    ("pyproject.toml", r"(?m)^(version = )", 1, False),
    ("uv.lock", r'(\[\[package\]\]\nname = "substantial"\nversion = )', 1, True),
    ("package.json", r'(?m)^(  "version": )', 1, False),
    ("package-lock.json", r'("name": "substantial",\n\s*"version": )', 2, False),
    ("jsr.json", r'(?m)^(  "version": )', 1, False),
]


def parse(version):
    match = SEMVER.fullmatch(version)
    if not match:
        sys.exit(f"{version}: expected X.Y.Z or X.Y.Z-(alpha|beta|rc).N")
    return match


def pep440(version):
    major, minor, patch, pre, number = parse(version).groups()
    return f"{major}.{minor}.{patch}" + (f"{PEP440_PRE[pre]}{number}" if pre else "")


def bumped(version):
    major, minor, patch, pre, number = parse(version).groups()
    if pre:
        return f"{major}.{minor}.{patch}-{pre}.{int(number) + 1}"
    return f"{major}.{minor}.{int(patch) + 1}"


def versions(text, path, pattern, expected):
    found = re.findall(pattern + r'"([^"]+)"', text)
    if len(found) != expected:
        sys.exit(f"{path}: expected {expected} versions of substantial, found {len(found)}")
    return [groups[1] for groups in found]


def check(version):
    wrong = []
    for path, pattern, expected, is_pep440 in LOCATIONS:
        want = pep440(version) if is_pep440 else version
        found = versions((ROOT / path).read_text(), path, pattern, expected)
        wrong += [f"{path}: {v}, expected {want}" for v in found if v != want]
    if wrong:
        sys.exit("\n".join(wrong))


def set_version(version):
    parse(version)
    texts = {}
    # Every file is checked before any is written, so a mismatch leaves the tree untouched.
    for path, pattern, expected, is_pep440 in LOCATIONS:
        want = pep440(version) if is_pep440 else version
        text = texts.get(path) or (ROOT / path).read_text()
        versions(text, path, pattern, expected)
        texts[path] = re.sub(pattern + r'"[^"]+"', rf'\g<1>"{want}"', text)
    for path, text in texts.items():
        (ROOT / path).write_text(text)


match sys.argv[1:]:
    case ["--check", version]:
        check(version)
    case ["--bump"]:
        path, pattern, expected, _ = LOCATIONS[0]
        [current] = versions((ROOT / path).read_text(), path, pattern, expected)
        set_version(bumped(current))
        print(bumped(current))
    case [version] if not version.startswith("-"):
        set_version(version)
    case _:
        sys.exit(__doc__)
