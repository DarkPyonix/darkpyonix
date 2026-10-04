"""NFR-V1: no version segment in any REST path of the OpenAPI files (SPEC §NFR-V1).

The served-path half (`/api/v1/...` answers 404) belongs to the Rust manager and the hub
worker tests; this test only checks the contract files.
"""
from __future__ import annotations

import os
import re

import yaml

from conftest import REPO


def test_nfr_v1_no_version_segment_in_any_rest_path():
    version = re.compile(r"^v[0-9]+$")
    for name in ("manager.openapi.yaml", "hub.openapi.yaml"):
        with open(os.path.join(REPO, "docs", "api", name)) as f:
            spec = yaml.safe_load(f)
        for path in spec["paths"]:
            assert not any(version.match(seg) for seg in path.split("/")), (name, path)
