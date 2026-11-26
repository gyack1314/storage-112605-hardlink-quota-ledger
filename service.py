"""Compatibility entry point: service module."""

from volume_service import *  # noqa: F401,F403
import volume_service as _implementation

VolumeService = _implementation.VolumeService
Service = _implementation.Service
Engine = _implementation.Engine
QuotaExceeded = _implementation.QuotaExceeded
run_scenario = _implementation.run_scenario
main = _implementation.main
