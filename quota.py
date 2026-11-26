"""Compatibility entry point: quota domain service."""

from volume_service import *  # noqa: F401,F403
import volume_service as _implementation

VolumeService = _implementation.VolumeService
QuotaService = _implementation.QuotaService
Volume = _implementation.Volume
QuotaManager = _implementation.QuotaManager
QuotaExceeded = _implementation.QuotaExceeded
run_scenario = _implementation.run_scenario
main = _implementation.main
