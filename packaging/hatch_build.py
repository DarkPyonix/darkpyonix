"""Wheel build hook (SPEC FR-C3, INTENT D22).

Two kinds of wheel come out of the same pyproject.toml:

* Platform wheel: ``DARKPYONIX_BINARY`` names a compiled Rust ``darkpyonix`` binary and
  ``DARKPYONIX_WHEEL_TAG`` the platform tag (``manylinux_2_28_x86_64``, ``win_amd64``, ...).
  The binary becomes the wheel's ``darkpyonix`` script and the wheel is tagged
  ``py3-none-<platform tag>``.
* Fallback wheel (nothing set; also what building from the sdist gives): ``py3-none-any``,
  kernel and runtime API only, plus a tiny ``darkpyonix`` script that says the manager is
  not available on this platform.

This file only places a file that was compiled elsewhere. It never compiles anything and is
never imported by the kernel.
"""

import os
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

FALLBACK_SCRIPT = "packaging/darkpyonix-unsupported"


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version, build_data):
        binary = os.environ.get("DARKPYONIX_BINARY", "").strip()
        tag = os.environ.get("DARKPYONIX_WHEEL_TAG", "").strip()
        if bool(binary) != bool(tag):
            raise RuntimeError(
                "DARKPYONIX_BINARY and DARKPYONIX_WHEEL_TAG must be set together "
                "(platform wheel) or both left unset (py3-none-any fallback wheel)"
            )
        if not binary:
            build_data["shared_scripts"][FALLBACK_SCRIPT] = "darkpyonix"
            return
        path = Path(binary).resolve()
        if not path.is_file():
            raise RuntimeError("DARKPYONIX_BINARY is not a file: %s" % path)
        if "-" in tag or tag in ("any", "linux_x86_64", "linux_aarch64"):
            raise RuntimeError(
                "DARKPYONIX_WHEEL_TAG %r must be one platform tag such as "
                "manylinux_2_28_x86_64 (PyPI rejects bare linux_* tags)" % tag
            )
        # Artifact transfer between CI jobs drops the executable bit; the wheel must keep it.
        path.chmod(path.stat().st_mode | 0o111)
        build_data["pure_python"] = False
        build_data["infer_tag"] = False
        build_data["tag"] = "py3-none-" + tag
        build_data["shared_scripts"][str(path)] = "darkpyonix" + path.suffix
