"""项目入口：等价于 python3 -m volume_service。

这里同时导出常用类型和运行函数，方便测试使用 ``from main import ...``。
"""

from volume_service import (
    AppendNotFound,
    QuotaExceeded,
    VolumeService,
    format_text_response,
    iter_scenario_rows,
    main,
    process_file,
    run_scenario,
)

__all__ = [
    "AppendNotFound",
    "QuotaExceeded",
    "VolumeService",
    "format_text_response",
    "iter_scenario_rows",
    "main",
    "process_file",
    "run_scenario",
]


if __name__ == "__main__":
    raise SystemExit(main())
