"""Scenario runner entry point."""

from volume_service import main, run_scenario

__all__ = ["main", "run_scenario"]

if __name__ == "__main__":
    raise SystemExit(main())
