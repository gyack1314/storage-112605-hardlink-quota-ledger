"""Compatibility entry point: shared volume service."""

from volume_service import *  # noqa: F401,F403
import volume_service as _implementation

VolumeService = _implementation.VolumeService
Volume = _implementation.Volume
QuotaService = _implementation.QuotaService
QuotaExceeded = _implementation.QuotaExceeded
run_scenario = _implementation.run_scenario
main = _implementation.main
