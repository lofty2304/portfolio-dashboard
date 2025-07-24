"""
test_mf_setup.py
Quick sanity check that required packages are installed
and the data folders exist for the Indian MF pipeline.
"""

import importlib
import os
import sys
import pathlib

REQUIRED_PACKAGES = [
    "pandas",
    "requests",
    "numpy",
    "pytz",
]

def check_packages():
    missing = []
    for pkg in REQUIRED_PACKAGES:
        if importlib.util.find_spec(pkg) is None:
            missing.append(pkg)
    return missing

def check_folders():
    root = pathlib.Path(__file__).resolve().parent
    required_dirs = [
        root / "data",
        root / "data" / "funds",
        root / "data" / "funds" / "daily",
    ]
    missing = [str(p) for p in required_dirs if not p.exists()]
    return missing

def main():
    print("=== Mutual-Fund Pipeline Environment Check ===")
    pkg_missing = check_packages()
    if pkg_missing:
        print("Missing packages:", ", ".join(pkg_missing))
        print("Run: pip install " + " ".join(pkg_missing))
    else:
        print("All required packages are installed ✔")

    dir_missing = check_folders()
    if dir_missing:
        print("Missing folders:")
        for d in dir_missing:
            print("  -", d)
        print("Creating missing folders...")
        for d in dir_missing:
            pathlib.Path(d).mkdir(parents=True, exist_ok=True)
        print("Folders created ✔")
    else:
        print("All required folders exist ✔")

    print("Environment check complete.")

if __name__ == "__main__":
    main()
