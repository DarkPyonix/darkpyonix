"""One version, three places (SPEC FR-C3, INTENT D22).

The source of truth is ``__version__`` in darkpyonix/kernel/__init__.py. The Cargo workspace
version (darkpyonix/manager/Cargo.toml) and, with ``--binary``, the output of
``darkpyonix --version`` must equal it. Exits 1 and says which differ. Standard library only.

    python packaging/check_versions.py [--binary PATH]
"""
import argparse
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def package_version():
    with open(os.path.join(ROOT, "darkpyonix", "kernel", "__init__.py"), encoding="utf-8") as f:
        return re.search(r'^__version__ = "([^"]+)"', f.read(), re.M).group(1)


def cargo_version():
    with open(os.path.join(ROOT, "darkpyonix", "manager", "Cargo.toml"), encoding="utf-8") as f:
        ws = f.read().split("[workspace.package]", 1)[1]
    return re.search(r'^version = "([^"]+)"', ws, re.M).group(1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--binary")
    args = ap.parse_args()
    found = {
        "darkpyonix/kernel/__init__.py": package_version(),
        "darkpyonix/manager/Cargo.toml [workspace.package]": cargo_version(),
    }
    if args.binary:
        out = subprocess.run([args.binary, "--version"], stdout=subprocess.PIPE,
                             universal_newlines=True, check=True).stdout
        found["darkpyonix --version"] = out.split()[-1]
    for where, v in found.items():
        print("%-52s %s" % (where, v))
    if len(set(found.values())) != 1:
        print("::error::versions differ; darkpyonix/kernel/__init__.py is the source of truth",
              file=sys.stderr)
        return 1
    version = package_version()
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write("version=%s\n" % version)
    return 0


if __name__ == "__main__":
    sys.exit(main())
