"""This package's version, sent to the control plane as Surfra-Client so errors can be traced to a release."""

from __future__ import annotations

from surfra.update import RUNNING

HEADERS = {"Surfra-Client": f"surfra-python/{RUNNING if RUNNING.count('.') >= 2 else '0.0.0'}"}
