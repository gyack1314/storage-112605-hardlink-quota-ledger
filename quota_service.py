"""兼容命名：配额服务模块。"""

from volume_service import *  # noqa: F401,F403
from volume_service import (  # noqa: F401
    AppendNotFound,
    AlreadyExists,
    DirectoryNotEmpty,
    HandleAlreadyOpen,
    HandleNotFound,
    InvalidOperation,
    InvalidPath,
    NamespaceService,
    NotFound,
    QuotaExceeded,
    QuotaManager,
    QuotaService,
    QuotaVolume,
    RequestConflict,
    Volume,
    VolumeError,
    VolumeService,
    format_text_response,
    run_scenario,
)
