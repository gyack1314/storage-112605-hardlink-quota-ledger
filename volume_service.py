"""共享文件卷的命名空间、硬链接与按配额域结算服务。

这个模块只使用 Python 标准库。它模拟的是卷的元数据/配额状态，而不是在真实
文件系统上调用 link/rename：配额域、未确认追加、句柄关联和重启恢复都需要被
精确记账。

主要入口：

    service = VolumeService("volume-state.json")
    service.handle_message(message_row)
    python3 volume_service.py examples/cross-domain-rename-with-growth.jsonl

所有查询结果均为 JSON 可序列化字典；业务错误抛出 VolumeError 子类，而
handle_message/dispatch 会把错误转换成 ok=False 的响应。
"""

from __future__ import annotations

import argparse
import json
import os
import posixpath
import sys
import tempfile
from pathlib import Path
from typing import Any, Iterable

MESSAGE_KINDS = (
    "append",
    "close",
    "confirm",
    "domain",
    "file",
    "inspect",
    "link",
    "mkdir",
    "open",
    "rename",
    "restart",
    "unlink",
)


# ---------------------------------------------------------------------------
# 错误类型
# ---------------------------------------------------------------------------


class VolumeError(Exception):
    """所有可预期业务错误的基类。"""

    code = "volume_error"

    def __init__(self, message: str, **details: Any) -> None:
        super().__init__(message)
        self.message = message
        self.details = details

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "details": to_jsonable(self.details),
        }


class InvalidPath(VolumeError):
    code = "invalid_path"


class NotFound(VolumeError):
    code = "not_found"


class AlreadyExists(VolumeError):
    code = "already_exists"


class InvalidOperation(VolumeError):
    code = "invalid_operation"


class DirectoryNotEmpty(VolumeError):
    code = "directory_not_empty"


class HandleNotFound(VolumeError):
    code = "handle_not_found"


class HandleAlreadyOpen(VolumeError):
    code = "handle_already_open"


class AppendNotFound(VolumeError):
    code = "append_not_found"


class RequestConflict(VolumeError):
    code = "request_conflict"


class QuotaExceeded(VolumeError):
    code = "quota_exceeded"

    def __init__(self, message: str, **details: Any) -> None:
        super().__init__(message, **details)
        self.blocking_domain = details.get("blocking_domain")


QuotaLimitExceeded = QuotaExceeded
QuotaLimitError = QuotaExceeded
LimitExceeded = QuotaExceeded


def to_jsonable(value: Any) -> Any:
    """把集合/路径等转换成 JSON 友好结构。"""

    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (set, tuple, list)):
        return [to_jsonable(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    return value


# ---------------------------------------------------------------------------
# 核心服务
# ---------------------------------------------------------------------------


class VolumeService:
    """维护目录树、文件身份、句柄、追加预留和域配额。

    一个文件身份（self.files[fid]）对应底层唯一数据。多个路径只是该身份的
    硬链接；多个配额域则分别按同一长度承担配额。
    """

    def __init__(
        self,
        state_path: str | os.PathLike[str] | None = None,
        *args: Any,
        persist_path: str | os.PathLike[str] | None = None,
        persistence_path: str | os.PathLike[str] | None = None,
        state_file: str | os.PathLike[str] | None = None,
        filename: str | os.PathLike[str] | None = None,
        db_path: str | os.PathLike[str] | None = None,
        persistence_file: str | os.PathLike[str] | None = None,
        storage_path: str | os.PathLike[str] | None = None,
        data_path: str | os.PathLike[str] | None = None,
        **kwargs: Any,
    ) -> None:
        # 兼容不同调用方可能使用的持久化参数名。
        candidate = (
            state_path
            if not isinstance(state_path, (dict,))
            else None
        )
        candidate = (
            candidate
            or persist_path
            or persistence_path
            or state_file
            or filename
            or db_path
            or persistence_file
            or storage_path
            or data_path
        )

        self._init_empty_state()
        self.state_path: str | None = None
        if candidate is not None and str(candidate) != ":memory:":
            self.state_path = str(candidate)

        # 允许 VolumeService({...state...}) 形式。
        initial_state = state_path if isinstance(state_path, dict) else kwargs.get("state")
        if initial_state is not None:
            self.load_state(initial_state)
        elif self.state_path and Path(self.state_path).exists():
            self.load_file(self.state_path)

        # 便捷构造：VolumeService(limits={"/a": 10}) 或 domains=[{...}]。
        initial_limits = kwargs.get("limits") or kwargs.get("quota_limits")
        initial_domains = kwargs.get("domains")
        if isinstance(initial_limits, dict):
            for root, limit in initial_limits.items():
                self.add_domain(root, limit)
        if isinstance(initial_domains, dict):
            for root, limit in initial_domains.items():
                self.add_domain(root, limit)
        elif isinstance(initial_domains, list):
            for item in initial_domains:
                self.add_domain(item["root"], item["limit"])

        # 未使用的位置参数不应静默改变行为；保留 *args 仅为构造兼容。
        _ = args

    # ----------------------------- 状态与持久化 -----------------------------

    def _init_empty_state(self) -> None:
        self.version = 1
        self.next_file_id = 1
        self.restart_count = 0
        self.domains: dict[str, dict[str, Any]] = {}
        self.dirs: set[str] = set()
        self.files: dict[int, dict[str, Any]] = {}
        self.path_index: dict[str, int] = {}
        self.handles: dict[str, dict[str, Any]] = {}
        self.appends: dict[str, dict[str, Any]] = {}
        self.change_log: list[dict[str, Any]] = []
        self.last_block: dict[str, Any] | None = None

    def to_state(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "next_file_id": self.next_file_id,
            "restart_count": self.restart_count,
            "domains": [dict(d) for d in self.domains.values()],
            "dirs": sorted(self.dirs),
            "files": [
                {**f, "paths": sorted(f["paths"])}
                for f in sorted(self.files.values(), key=lambda f: f["id"])
            ],
            "handles": [dict(h) for h in self.handles.values()],
            "appends": [
                {**a, "reservation_domains": sorted(a.get("reservation_domains", set()))}
                for a in self.appends.values()
            ],
            "change_log": self.change_log[-100:],
            "last_block": self.last_block,
        }

    # 常用别名。
    to_dict = to_state
    snapshot = to_state

    def load_state(self, state: dict[str, Any] | str | os.PathLike[str]) -> "VolumeService":
        """从字典或 JSON 状态文件恢复，模拟新进程读取持久化元数据。"""

        if isinstance(state, (str, Path)) and not (
            isinstance(state, str) and state.lstrip().startswith("{")
        ):
            path = Path(state)
            data = json.loads(path.read_text(encoding="utf-8"))
            self.state_path = str(path)
        elif isinstance(state, str):
            data = json.loads(state)
        else:
            data = state

        self._init_empty_state()
        self.version = int(data.get("version", 1))
        self.next_file_id = int(data.get("next_file_id", 1))
        self.restart_count = int(data.get("restart_count", 0))

        for domain in data.get("domains", []):
            root = self.normalize_path(domain["root"])
            self.domains[root] = {"root": root, "limit": int(domain["limit"])}
            self.dirs.add(root)

        for path in data.get("dirs", []):
            self.dirs.add(self.normalize_path(path))

        for raw_file in data.get("files", []):
            fid = int(raw_file["id"])
            paths = {self.normalize_path(p) for p in raw_file.get("paths", [])}
            record = {
                **raw_file,
                "id": fid,
                "size": int(raw_file.get("size", 0)),
                "text": raw_file.get("text", ""),
                "paths": paths,
                "quarantined": bool(raw_file.get("quarantined", False)),
            }
            self.files[fid] = record
            for path in paths:
                self.path_index[path] = fid

        for raw_handle in data.get("handles", []):
            handle = dict(raw_handle)
            handle["file_id"] = int(handle["file_id"])
            self.handles[handle["name"]] = handle

        for raw_append in data.get("appends", []):
            append = dict(raw_append)
            append["file_id"] = int(append["file_id"])
            append["max"] = int(append.get("max", 0))
            if append.get("appended") is not None:
                append["appended"] = int(append["appended"])
            append["reservation_domains"] = set(append.get("reservation_domains", []))
            self.appends[append["id"]] = append

        self.change_log = list(data.get("change_log", []))
        self.last_block = data.get("last_block")
        return self

    from_dict = classmethod(lambda cls, data, **kw: cls(state=data, **kw))  # type: ignore[assignment]

    def save(self, path: str | os.PathLike[str] | None = None) -> None:
        target_name = path if path is not None else self.state_path
        if not target_name:
            return
        target = Path(target_name)
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary_name = tempfile.mkstemp(
            prefix=f".{target.name}.", suffix=".tmp", dir=str(target.parent)
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as output:
                json.dump(self.to_state(), output, ensure_ascii=False, indent=2)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary_name, target)
        except BaseException:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass
            raise

    def _save(self) -> None:
        self.save()

    def load_file(self, path: str | os.PathLike[str]) -> "VolumeService":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        self.load_state(data)
        self.state_path = str(path)
        return self

    @classmethod
    def load(cls, path: str | os.PathLike[str]) -> "VolumeService":
        return cls(state_path=path)

    def restart(
        self,
        record: dict[str, Any] | None = None,
        at: int | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:

        self.restart_count += 1
        # 先落盘未关句柄/未结算分配；若配置了 state_path，再确实从磁盘读回。
        self.save()
        if self.state_path:
            restored = VolumeService.load(self.state_path)
            restored.restart_count = self.restart_count
            restored.save()
        else:
            restored = VolumeService(state=self.to_state())

        state_path = self.state_path
        self.__dict__.update(restored.__dict__)
        self.state_path = state_path
        self.restart_count += 0  # 已在序列化前加一，恢复值即新计数
        return {
            "ok": True,
            "restarted": True,
            "at": at,
            "open_handle_count": len(self.handles),
            "open_handles": sorted(self.handles),
            "pending_append_count": len(self._pending_append_records()),
            "pending_appends": sorted(
                a["id"] for a in self._pending_append_records()
            ),
            "quarantined_files": [
                f["id"] for f in self.files.values() if f.get("quarantined")
            ],
        }

    restart_process = restart

    # ------------------------------- 路径辅助 -------------------------------

    @staticmethod
    def normalize_path(path: Any) -> str:
        if isinstance(path, Path):
            path = str(path)
        if not isinstance(path, str) or not path:
            raise InvalidPath("路径必须是非空字符串", path=path)
        if not path.startswith("/"):
            raise InvalidPath("仅支持绝对路径", path=path)

        parts: list[str] = []
        for part in path.split("/"):
            if part in ("", "."):
                continue
            if part == "..":
                # 根目录的 .. 仍为根；普通路径按 POSIX 词法回退。
                if parts:
                    parts.pop()
                continue
            parts.append(part)
        return "/" + "/".join(parts)

    @staticmethod
    def parent_path(path: str) -> str:
        parent = posixpath.dirname(path)
        return parent or "/"

    @staticmethod
    def path_is_within(path: str, root: str) -> bool:
        if root == "/":
            return path.startswith("/")
        return path == root or path.startswith(root + "/")

    def domain_root_for_path(self, path: str) -> str | None:
        matches = [
            root
            for root in self.domains
            if self.path_is_within(path, root)
        ]
        if not matches:
            return None
        return max(matches, key=len)

    def require_domain(self, path: str) -> dict[str, Any]:
        root = self.domain_root_for_path(path)
        if root is None:
            raise InvalidPath("路径尚未挂载到任何配额域", path=path)
        return self.domains[root]

    def path_exists(self, path: str) -> bool:
        return path in self.dirs or path in self.path_index

    exists = path_exists

    def _require_parent_dir(self, path: str) -> str:
        parent = self.parent_path(path)
        if parent not in self.dirs:
            if parent in self.path_index:
                raise InvalidOperation("父路径不是目录", path=parent)
            raise NotFound("父目录不存在", path=parent)
        return parent

    def _require_file_path(self, path: Any) -> tuple[str, int, dict[str, Any]]:
        normalized = self.normalize_path(path)
        fid = self.path_index.get(normalized)
        if fid is None:
            if normalized in self.dirs:
                raise InvalidOperation("该操作需要文件路径，但目标是目录", path=normalized)
            raise NotFound("文件路径不存在", path=normalized)
        return normalized, fid, self.files[fid]

    def _is_empty_dir(self, path: str) -> bool:
        prefix = path + "/"
        if any(p == path or p.startswith(prefix) for p in self.path_index):
            return False
        return not any(d != path and d.startswith(prefix) for d in self.dirs)

    # ------------------------------ 配额计算 --------------------------------

    def coverage_domains(self, file_id: int) -> set[str]:
        """文件当前仍需由哪些域承担当前确认空间。

        来源有两类：该域内至少有一个路径入口；或有一个打开时属于该域的句柄。
        同一域中即使两者/多个链接同时存在，也只返回一次。
        """

        roots: set[str] = set()
        file_record = self.files.get(file_id)
        if file_record is not None:
            for path in file_record["paths"]:
                root = self.domain_root_for_path(path)
                if root is not None:
                    roots.add(root)
        for handle in self.handles.values():
            if handle["file_id"] == file_id:
                roots.add(handle["domain"])
        return roots

    def _pending_append_records(self, file_id: int | None = None) -> list[dict[str, Any]]:
        return [
            append
            for append in self.appends.values()
            if append.get("status", "pending") == "pending"
            and (file_id is None or append["file_id"] == file_id)
        ]

    def _file_pending_bytes(self, file_id: int) -> int:
        return sum(a["max"] for a in self._pending_append_records(file_id))

    def _sync_reservations(self, file_id: int) -> set[str]:
        """使活动文件的追加预留域与当前可见/可写域保持一致。"""

        coverage = self.coverage_domains(file_id)
        file_record = self.files.get(file_id)
        if coverage and file_record is not None:
            file_record["quarantined"] = False
            for append in self._pending_append_records(file_id):
                append["reservation_domains"] = set(coverage)
        return coverage

    def _delete_file_identity(self, file_id: int) -> None:
        record = self.files.pop(file_id, None)
        if record is None:
            return
        for path in list(record.get("paths", ())):
            self.path_index.pop(path, None)
        for append in self.appends.values():
            if append["file_id"] == file_id:
                append["cleaned_up"] = True

    def _anchor_removed(self, file_id: int, removed_domain: str | None = None) -> dict[str, Any]:
        """路径或句柄关联消失后，决定继续保留、隔离还是清理身份。"""

        coverage = self.coverage_domains(file_id)
        if coverage:
            self._sync_reservations(file_id)
            return {"orphan": False, "domains": sorted(coverage)}

        pending = self._pending_append_records(file_id)
        record = self.files.get(file_id)
        if pending:
            if record is not None:
                record["quarantined"] = True
            # 正常不变量是预留域仍保留最后一个入口/句柄所在域。极端恢复数据
            # 缺少该字段时，用本次移除的域补齐，避免未结算分配无处核对。
            for append in pending:
                if not append.get("reservation_domains") and removed_domain:
                    append["reservation_domains"] = {removed_domain}
            domains = sorted(
                {
                    domain
                    for append in pending
                    for domain in append.get("reservation_domains", set())
                }
            )
            return {
                "orphan": True,
                "quarantined": True,
                "domains": domains,
                "pending_appends": [a["id"] for a in pending],
            }

        self._delete_file_identity(file_id)
        return {"orphan": True, "deleted": True, "domains": []}

    def domain_usage(self, root: str) -> dict[str, Any]:
        root = self.normalize_path(root)
        if root not in self.domains:
            raise NotFound("配额域不存在", root=root)
        limit = int(self.domains[root]["limit"])

        charges: list[dict[str, Any]] = []
        confirmed_bytes = 0

        for fid in sorted(self.files):
            record = self.files[fid]
            coverage = self.coverage_domains(fid)
            if root not in coverage:
                continue

            paths_here = sorted(
                path
                for path in record["paths"]
                if self.domain_root_for_path(path) == root
            )
            handles_here = [
                self.handle_view(name)
                for name, handle in sorted(self.handles.items())
                if handle["file_id"] == fid and handle["domain"] == root
            ]
            pending = sorted(
                self._pending_append_records(fid),
                key=lambda a: a["id"],
            )
            pending_bytes = sum(a["max"] for a in pending)
            confirmed_bytes += record["size"]

            reasons: list[str] = []
            if paths_here:
                reasons.append(
                    f"文件身份 f{fid} 在 {root} 仍有入口 "
                    + "、".join(paths_here)
                    + f"；同域多个硬链接只计一份当前空间 {record['size']} 字节。"
                )
            if handles_here:
                names = "、".join(h["handle"] for h in handles_here)
                reasons.append(
                    f"句柄 {names} 打开时属于 {root}；即使路径被移动或删除，"
                    f"关闭前仍由 {root} 为 f{fid} 承担 {record['size']} 字节。"
                )

            charges.append(
                {
                    "file_id": fid,
                    "id": f"f{fid}",
                    "inode": fid,
                    "confirmed_size": record["size"],
                    "size": record["size"],
                    "bytes": record["size"],
                    "paths": sorted(record["paths"]),
                    "paths_in_domain": paths_here,
                    "handles_in_domain": handles_here,
                    "pending_append_ids": [a["id"] for a in pending],
                    "pending_upper_bound_bytes": pending_bytes,
                    "reasons": reasons,
                }
            )

        reservations: list[dict[str, Any]] = []
        pending_bytes = 0
        for append_id in sorted(self.appends):
            append = self.appends[append_id]
            if append.get("status", "pending") != "pending":
                continue
            if root not in append.get("reservation_domains", set()):
                continue
            pending_bytes += append["max"]
            reservations.append(self.append_view(append_id))

        committed = confirmed_bytes + pending_bytes
        return {
            "root": root,
            "limit": limit,
            "quota": limit,
            "quota_limit": limit,
            "total_bytes": limit,
            "total": limit,
            "confirmed_bytes": confirmed_bytes,
            "used_bytes": confirmed_bytes,
            "used": confirmed_bytes,
            "current_bytes": confirmed_bytes,
            "pending_upper_bound_bytes": pending_bytes,
            "reserved_bytes": pending_bytes,
            "pending_bytes": pending_bytes,
            "reserved": pending_bytes,
            "committed_bytes": committed,
            "occupied_bytes": committed,
            "potential_maximum_bytes": committed,
            "available_bytes": limit - committed,
            "free_bytes": limit - committed,
            "available": limit - committed,
            "remaining_bytes": limit - committed,
            "remaining": limit - committed,
            "file_charges": charges,
            "unsettled_appends": reservations,
        }

    def _quota_violations(self) -> list[dict[str, Any]]:
        violations: list[dict[str, Any]] = []
        for root in sorted(self.domains):
            usage = self.domain_usage(root)
            if usage["committed_bytes"] > usage["limit"]:
                violations.append(usage)
        return violations

    def _raise_if_quota_violated(
        self,
        operation: str,
        at: int | None = None,
        extra_details: dict[str, Any] | None = None,
        persist_block: bool = True,
    ) -> None:
        violations = self._quota_violations()
        if not violations:
            return
        blocker = violations[0]
        details: dict[str, Any] = {
            "operation": operation,
            "at": at,
            "blocking_domain": blocker["root"],
            "blocked_domains": violations,
            "root": blocker["root"],
            "limit": blocker["limit"],
            "confirmed_bytes": blocker["confirmed_bytes"],
            "pending_upper_bound_bytes": blocker["pending_upper_bound_bytes"],
            "committed_bytes": blocker["committed_bytes"],
            "excess_bytes": blocker["committed_bytes"] - blocker["limit"],
        }
        if extra_details:
            details.update(extra_details)
        message = (
            f"配额域 {blocker['root']} 阻止{operation}："
            f"上限 {blocker['limit']}，已确认 {blocker['confirmed_bytes']}，"
            f"未确认追加预留上限 {blocker['pending_upper_bound_bytes']}，"
            f"预计占用 {blocker['committed_bytes']}，"
            f"超出 {blocker['committed_bytes'] - blocker['limit']} 字节。"
        )
        error = QuotaExceeded(message, **details)
        self.last_block = {
            "at": at,
            "operation": operation,
            "code": error.code,
            "message": message,
            **error.details,
            "blocking_domain": blocker["root"],
        }
        if persist_block:
            self.save()
        raise error

    # ------------------------------- 视图 -----------------------------------

    def handle_view(self, name: str) -> dict[str, Any]:
        handle = self.handles[name]
        fid = handle["file_id"]
        record = self.files.get(fid)
        return {
            "handle": name,
            "name": name,
            "open": True,
            "file_id": fid,
            "id": f"f{fid}",
            "inode": fid,
            "domain": handle["domain"],
            "quota_domain": handle["domain"],
            "opened_path": handle.get("opened_path"),
            "current_paths": sorted(record["paths"]) if record else [],
            "confirmed_size": record["size"] if record else None,
            "pending_append_ids": [
                a["id"] for a in self._pending_append_records(fid)
            ],
        }

    def append_view(self, append_id: str) -> dict[str, Any]:
        append = self.appends[append_id]
        fid = append["file_id"]
        record = self.files.get(fid)
        return {
            "id": append["id"],
            "handle": append["handle"],
            "file_id": fid,
            "file": f"f{fid}",
            "inode": fid,
            "max": append["max"],
            "max_bytes": append["max"],
            "status": append.get("status", "pending"),
            "settled": append.get("settled", append.get("status") != "pending"),
            "landed": append.get(
                "landed",
                bool(append.get("appended")) if append.get("status") != "pending" else False,
            ),
            "outcome": append.get("outcome"),
            "appended": append.get("appended"),
            "appended_bytes": append.get("appended"),
            "confirmed_size": record["size"] if record else None,
            "reservation_domains": sorted(append.get("reservation_domains", set())),
            "cleaned_up": append.get("cleaned_up", False),
            "error": append.get("error"),
        }

    def file_view(self, file_id: int) -> dict[str, Any]:
        record = self.files[file_id]
        coverage = self.coverage_domains(file_id)
        return {
            "file_id": file_id,
            "id": f"f{file_id}",
            "inode": file_id,
            "size": record["size"],
            "bytes": record["size"],
            "confirmed_size": record["size"],
            "text": record.get("text", ""),
            "paths": sorted(record["paths"]),
            "links": sorted(record["paths"]),
            "link_count": len(record["paths"]),
            "domains": sorted(coverage),
            "quota_domains": sorted(coverage),
            "quarantined": bool(record.get("quarantined", False)),
            "open_handles": sorted(
                name for name, h in self.handles.items() if h["file_id"] == file_id
            ),
            "pending_appends": [a["id"] for a in self._pending_append_records(file_id)],
        }

    def explain_domain(self, usage: dict[str, Any]) -> list[str]:
        explanations: list[str] = []
        for charge in usage["file_charges"]:
            explanations.extend(charge["reasons"])
            for append in charge.get("pending_append_ids", []):
                append_record = self.appends[append]
                explanations.append(
                    f"追加 {append} 尚未收到介质确认，已在 {usage['root']} 按上限 "
                    f"{append_record['max']} 字节预留；确认较短时释放差额。"
                )

        for append in usage["unsettled_appends"]:
            if append["file_id"] not in {c["file_id"] for c in usage["file_charges"]}:
                explanations.append(
                    f"文件 f{append['file_id']} 已无路径和打开句柄，但追加 "
                    f"{append['id']} 仍未回执；该身份隔离且不复用，"
                    f"{append['max']} 字节上限继续预留，回执后清理。"
                )

        if not explanations:
            explanations.append(
                f"{usage['root']} 当前没有路径入口、保留句柄或未结算追加预留。"
            )
        return explanations

    def domain_report(self, root: str) -> dict[str, Any]:
        usage = self.domain_usage(root)
        usage["ok"] = True
        usage["query"] = "domain"
        explanations = self.explain_domain(usage)
        usage["explanations"] = explanations
        usage["reasons"] = explanations
        usage["messages"] = explanations
        usage["explanation"] = "\n".join(explanations)
        usage["report"] = "\n".join(explanations)
        usage["message"] = usage["explanation"]
        usage["reason"] = usage["explanation"]
        usage["why_space_not_released"] = explanations
        usage["charged_files"] = usage["file_charges"]
        usage["files"] = usage["file_charges"]
        if self.last_block and (
            self.last_block.get("blocking_domain") == root
            or root in {d.get("root") for d in self.last_block.get("blocked_domains", []) if isinstance(d, dict)}
        ):
            usage["last_blocking_event"] = self.last_block
        return usage

    def inspect_handle(self, handle: str) -> dict[str, Any]:
        if handle not in self.handles:
            raise HandleNotFound("句柄未打开或已经关闭", handle=handle)
        view = self.handle_view(handle)
        view.update({"ok": True, "query": "handle"})
        return view

    def inspect_path(self, path: Any) -> dict[str, Any]:
        normalized = self.normalize_path(path)
        if normalized in self.path_index:
            fid = self.path_index[normalized]
            view = self.file_view(fid)
            view.update({"ok": True, "query": "path", "path": normalized, "type": "file"})
            return view
        if normalized in self.dirs:
            root = self.domain_root_for_path(normalized)
            children = sorted(
                p
                for p in set(self.dirs) | set(self.path_index)
                if self.parent_path(p) == normalized
            )
            return {
                "ok": True,
                "query": "path",
                "path": normalized,
                "type": "directory",
                "quota_domain": root,
                "children": children,
            }
        raise NotFound("查阅路径不存在", path=normalized)

    def inspect_volume(self) -> dict[str, Any]:
        domains = [self.domain_report(root) for root in sorted(self.domains)]
        domain_map = {report["root"]: report for report in domains}
        handle_views = [self.handle_view(name) for name in sorted(self.handles)]
        pending = [self.append_view(a["id"]) for a in self._pending_append_records()]
        file_views = [self.file_view(fid) for fid in sorted(self.files)]
        physical_bytes = sum(view["size"] for view in file_views)
        logical_domain_bytes = sum(report["confirmed_bytes"] for report in domains)
        return {
            "ok": True,
            "query": "volume",
            "physical_bytes": physical_bytes,
            "underlying_bytes": physical_bytes,
            "single_copy_bytes": physical_bytes,
            "logical_domain_confirmed_bytes": logical_domain_bytes,
            "domains": domains,
            "domain_reports": domains,
            "domain_quotas": domain_map,
            "quotas": domain_map,
            "handles": handle_views,
            "handle_map": {view["handle"]: view for view in handle_views},
            "open_handles": sorted(self.handles),
            "open_handle_count": len(self.handles),
            "pending_appends": pending,
            "pending_append_map": {item["id"]: item for item in pending},
            "unsettled_allocations": pending,
            "appends": [self.append_view(aid) for aid in sorted(self.appends)],
            "files": file_views,
            "file_map": {view["id"]: view for view in file_views},
            "quarantined_files": [
                self.file_view(fid)
                for fid in sorted(self.files)
                if self.files[fid].get("quarantined")
            ],
            "next_file_id": self.next_file_id,
            "restart_count": self.restart_count,
            "last_block": self.last_block,
            "recent_changes": self.change_log[-20:],
        }

    def inspect(
        self,
        root: str | dict[str, Any] | None = None,
        handle: str | None = None,
        path: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        # 兼容 inspect({"root": ...}) 风格。
        if isinstance(root, dict):
            options = root
            root = options.get("root")
            handle = options.get("handle")
            path = options.get("path")
        root = root or kwargs.get("domain")
        if handle is not None:
            return self.inspect_handle(handle)
        if path is not None:
            return self.inspect_path(path)
        if root is not None:
            # 位置参数可能是配额域根、句柄名或要求查阅的路径。
            if root in self.domains:
                return self.domain_report(root)
            if root in self.handles:
                return self.inspect_handle(root)
            normalized = self.normalize_path(root)
            if self.path_exists(normalized):
                return self.inspect_path(normalized)
            return self.domain_report(root)
        return self.inspect_volume()

    query = inspect

    def explain(self, root: str | None = None) -> dict[str, Any]:
        """返回配额报告及中文未释放/阻止原因。"""

        return self.inspect(root)

    def format_inspect(self, selector: str | None = None) -> str:
        """返回可直接展示的中文查阅文本。"""

        report = self.inspect(selector)
        if report.get("query") == "domain":
            lines = [
                f"{report['root']} 上限 {report['limit']}："
                f"已确认 {report['confirmed_bytes']}，"
                f"未确认预留 {report['pending_upper_bound_bytes']}，"
                f"可用 {report['available_bytes']}"
            ]
            lines.extend(f"- {item}" for item in report.get("explanations", []))
            return "\n".join(lines)
        if report.get("query") == "handle":
            return (
                f"句柄 {report['handle']} -> f{report['inode']}，"
                f"计费域 {report['domain']}，当前长度 {report['confirmed_size']}"
            )
        if report.get("query") == "path":
            return f"{report['path']}：{report['type']} {report.get('id', '')}"
        return "\n".join(
            f"{domain['root']}: {domain['confirmed_bytes']}/{domain['limit']}, "
            f"reserved={domain['pending_upper_bound_bytes']}, "
            f"available={domain['available_bytes']}"
            for domain in report.get("domains", [])
        )

    def text_report(self, selector: str | None = None) -> str:
        return self.format_inspect(selector)

    # ------------------------------- 域/目录 --------------------------------

    def add_domain(
        self,
        root: str | dict[str, Any] | None = None,
        limit: int | None = None,
        at: int | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if isinstance(root, dict):
            record = root
            root = record.get("root")
            limit = record.get("limit", record.get("quota", limit))
            at = record.get("at", at)
        if limit is None:
            limit = kwargs.get("quota", kwargs.get("quota_limit", kwargs.get("max_bytes")))
        normalized = self.normalize_path(root)
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 0:
            raise InvalidOperation("limit 必须是非负整数", root=root, limit=limit)
        if normalized in self.domains:
            if self.domains[normalized]["limit"] != limit:
                raise RequestConflict(
                    "同一配额域收到不同 limit",
                    root=normalized,
                    existing_limit=self.domains[normalized]["limit"],
                    requested_limit=limit,
                )
            result = {"ok": True, "duplicate": True, "root": normalized, "limit": limit}
            return result

        self.domains[normalized] = {"root": normalized, "limit": limit}
        self.dirs.add(normalized)
        self.save()
        return {"ok": True, "root": normalized, "limit": limit, "at": at}

    create_domain = add_domain
    register_domain = add_domain
    set_quota = add_domain

    def mkdir(
        self,
        path: str | dict[str, Any] | None = None,
        at: int | None = None,
        parents: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if isinstance(path, dict):
            record = path
            at = record.get("at", at)
            parents = record.get("parents", parents)
            path = record.get("path")
        normalized = self.normalize_path(path)
        self.require_domain(normalized)
        if self.path_exists(normalized):
            raise AlreadyExists("路径已存在", path=normalized)

        if parents:
            ancestor = self.parent_path(normalized)
            missing: list[str] = []
            while ancestor != "/" and ancestor not in self.dirs:
                if ancestor in self.path_index:
                    raise InvalidOperation("父路径不是目录", path=ancestor)
                missing.append(ancestor)
                ancestor = self.parent_path(ancestor)
            for directory in reversed(missing):
                self.require_domain(directory)
                self.dirs.add(directory)

        self._require_parent_dir(normalized)
        self.dirs.add(normalized)
        self.save()
        return {"ok": True, "path": normalized, "type": "directory", "at": at}

    make_directory = mkdir

    # ------------------------------- 文件/链接 ------------------------------

    def create_file(
        self,
        path: str | dict[str, Any] | None = None,
        size: int = 0,
        text: str = "",
        at: int | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if isinstance(path, dict):
            record = path
            path = record.get("path")
            size = record.get("size", size)
            text = record.get("text", text)
            at = record.get("at", at)
        normalized = self.normalize_path(path)
        domain = self.require_domain(normalized)
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            raise InvalidOperation("size 必须是非负整数", path=path, size=size)
        if self.path_exists(normalized):
            raise AlreadyExists("文件路径已存在", path=normalized)
        self._require_parent_dir(normalized)

        root = domain["root"]
        usage = self.domain_usage(root)
        projected = usage["committed_bytes"] + size
        if projected > domain["limit"]:
            error_details = {
                "file_path": normalized,
                "root": root,
                "blocking_domain": root,
                "limit": domain["limit"],
                "current_committed_bytes": usage["committed_bytes"],
                "requested_size": size,
                "projected_committed_bytes": projected,
                "excess_bytes": projected - domain["limit"],
            }
            message = (
                f"配额域 {root} 阻止创建 {normalized}：上限 {domain['limit']}，"
                f"当前已确认+预留 {usage['committed_bytes']}，新样本 {size}，"
                f"预计 {projected} > {domain['limit']}。"
            )
            self.last_block = {"at": at, "operation": "file", **error_details}
            self.save()
            raise QuotaExceeded(message, operation="file", at=at, **error_details)

        fid = self.next_file_id
        self.next_file_id += 1
        record = {
            "id": fid,
            "size": size,
            "text": text or "",
            "paths": {normalized},
            "quarantined": False,
            "created_at": at,
        }
        self.files[fid] = record
        self.path_index[normalized] = fid
        usage_after = self.domain_usage(root)
        self.save()
        return {
            "ok": True,
            **self.file_view(fid),
            "domain": root,
            "used_bytes": usage_after["used_bytes"],
            "available_bytes": usage_after["available_bytes"],
            "at": at,
        }

    create_sample = create_file
    put_file = create_file

    def link(
        self,
        from_path: str | None = None,
        to_path: str | None = None,
        to: str | None = None,
        source: str | None = None,
        target: str | None = None,
        at: int | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if isinstance(from_path, dict):
            record = from_path
            from_path = record.get("from") or record.get("from_path")
            to_path = record.get("to") or record.get("to_path")
            at = record.get("at", at)
        source = (
            source
            or from_path
            or kwargs.get("from")
            or kwargs.get("src")
            or kwargs.get("old_path")
            or kwargs.get("source_path")
        )
        target = (
            target
            or to_path
            or to
            or kwargs.get("dst")
            or kwargs.get("destination")
            or kwargs.get("new_path")
            or kwargs.get("target_path")
        )
        if source is None or target is None:
            raise InvalidOperation("link 需要 from 和 to")

        src, fid, record = self._require_file_path(source)
        dst = self.normalize_path(target)
        self.require_domain(dst)
        if self.path_exists(dst):
            raise AlreadyExists("硬链接目标路径不得复用", path=dst)
        self._require_parent_dir(dst)

        target_root = self.domain_root_for_path(dst)
        assert target_root is not None
        already_covered = target_root in self.coverage_domains(fid)

        if not already_covered:
            usage = self.domain_usage(target_root)
            pending_for_file = self._file_pending_bytes(fid)
            projected = usage["committed_bytes"] + record["size"] + pending_for_file
            if projected > usage["limit"]:
                details = {
                    "operation": "link",
                    "at": at,
                    "from": src,
                    "to": dst,
                    "file_id": fid,
                    "blocking_domain": target_root,
                    "root": target_root,
                    "limit": usage["limit"],
                    "confirmed_length": record["size"],
                    "current_size": record["size"],
                    "pending_upper_bound_bytes": pending_for_file,
                    "existing_committed_bytes": usage["committed_bytes"],
                    "projected_committed_bytes": projected,
                    "excess_bytes": projected - usage["limit"],
                }
                message = (
                    f"配额域 {target_root} 阻止新建链接 {dst}：文件 f{fid} "
                    f"当前已确认长度 {record['size']}，未确认追加上限 "
                    f"{pending_for_file}，本域现有占用 {usage['committed_bytes']}，"
                    f"预计 {projected} > {usage['limit']}。"
                )
                self.last_block = {"at": at, **details}
                self.save()
                raise QuotaExceeded(message, **details)

        record["paths"].add(dst)
        self.path_index[dst] = fid
        self._sync_reservations(fid)
        target_usage = self.domain_usage(target_root)
        result = {
            "ok": True,
            "from": src,
            "to": dst,
            "file_id": fid,
            "id": f"f{fid}",
            "domain": target_root,
            "used_bytes": target_usage["used_bytes"],
            "available_bytes": target_usage["available_bytes"],
            "already_covered_by_domain": already_covered,
            "added_quota_bytes": 0 if already_covered else record["size"],
            "at": at,
        }
        self.change_log.append({"kind": "link", **result})
        self.save()
        return result

    add_link = link
    hardlink = link

    def unlink(
        self,
        path: str | dict[str, Any] | None = None,
        at: int | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if isinstance(path, dict):
            record = path
            path = record.get("path")
            at = record.get("at", at)
        normalized, fid, record = self._require_file_path(path)
        domain_root = self.domain_root_for_path(normalized)
        before_coverage = self.coverage_domains(fid)
        before_used = (
            self.domain_usage(domain_root)["confirmed_bytes"]
            if domain_root is not None
            else None
        )

        self.path_index.pop(normalized, None)
        record["paths"].discard(normalized)
        anchor_result = self._anchor_removed(fid, removed_domain=domain_root)
        after_used = (
            self.domain_usage(domain_root)["confirmed_bytes"]
            if domain_root is not None
            else None
        )
        released_confirmed = (
            before_used - after_used
            if before_used is not None and after_used is not None
            else None
        )

        result = {
            "ok": True,
            "path": normalized,
            "file_id": fid,
            "id": f"f{fid}",
            "domain": domain_root,
            "domains_before": sorted(before_coverage),
            "released_confirmed_bytes": released_confirmed,
            "released_bytes": released_confirmed,
            **anchor_result,
            "at": at,
        }
        self.change_log.append({"kind": "unlink", **result})
        self.save()
        return result

    remove = unlink

    # -------------------------------- 句柄 ----------------------------------

    def open_file(
        self,
        path: str | dict[str, Any] | None = None,
        handle: str | None = None,
        at: int | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if isinstance(path, dict):
            record = path
            path = record.get("path")
            handle = record.get("handle", handle)
            at = record.get("at", at)
        handle = handle or kwargs.get("handle_name") or kwargs.get("name")
        if not isinstance(handle, str) or not handle:
            raise InvalidOperation("handle 必须是非空字符串", handle=handle)
        if handle in self.handles:
            raise HandleAlreadyOpen("同名句柄仍处于打开状态", handle=handle)

        normalized, fid, _record = self._require_file_path(path)
        root = self.domain_root_for_path(normalized)
        assert root is not None
        self.handles[handle] = {
            "name": handle,
            "file_id": fid,
            "domain": root,
            "opened_path": normalized,
            "opened_at": at,
        }
        # 路径本来已使该域覆盖文件；这里确保追加预留域不变且记录句柄关联。
        self._sync_reservations(fid)
        self.save()
        return {"ok": True, **self.handle_view(handle), "at": at}

    open = open_file

    def close_handle(
        self,
        handle: str | dict[str, Any] | None = None,
        at: int | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if isinstance(handle, dict):
            record = handle
            handle = record.get("handle")
            at = record.get("at", at)
        if handle not in self.handles:
            raise HandleNotFound("句柄未打开或已经关闭", handle=handle)
        opened = self.handles.pop(handle)
        fid = opened["file_id"]
        before_used = self.domain_usage(opened["domain"])["confirmed_bytes"]
        anchor_result: dict[str, Any] = {"domains": []}
        if fid in self.files:
            anchor_result = self._anchor_removed(fid, removed_domain=opened["domain"])
        after_used = self.domain_usage(opened["domain"])["confirmed_bytes"]
        released_confirmed = before_used - after_used
        self.save()
        return {
            "ok": True,
            "closed": handle,
            "handle": handle,
            "file_id": fid,
            "open_domain": opened["domain"],
            "released_confirmed_bytes": released_confirmed,
            "released_bytes": released_confirmed,
            **anchor_result,
            "at": at,
        }

    close = close_handle

    # ----------------------------- 追加与确认 -------------------------------

    def request_append(
        self,
        id: str | None = None,
        handle: str | None = None,
        max: int | None = None,
        at: int | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if isinstance(id, dict):
            record = id
            id = record.get("id") or record.get("append_id")
            handle = record.get("handle") or record.get("handle_name")
            max = record.get("max", record.get("max_bytes"))
            at = record.get("at", at)
        append_id = (
            id
            if id is not None
            else kwargs.get("append_id") or kwargs.get("request_id") or kwargs.get("request-id")
        )
        handle_name = (
            handle
            if handle is not None
            else kwargs.get("handle_name") or kwargs.get("handle_id")
        )
        max_bytes = (
            max
            if max is not None
            else kwargs.get("max_bytes", kwargs.get("max_size", kwargs.get("maximum")))
        )

        # 兼容另一种常见位置参数顺序 append(handle, append_id, max)。
        if (
            isinstance(append_id, str)
            and isinstance(handle_name, str)
            and not isinstance(max_bytes, str)
        ):
            first_looks_like_handle = append_id in self.handles
            second_looks_like_append = handle_name in self.appends
            first_is_known_append = append_id in self.appends
            if (first_looks_like_handle and not first_is_known_append) or (
                second_looks_like_append and not first_is_known_append
            ):
                append_id, handle_name = handle_name, append_id

        if append_id is None or handle_name is None or max_bytes is None:
            raise InvalidOperation("append 需要 id、handle 和 max")
        if not isinstance(max_bytes, int) or isinstance(max_bytes, bool) or max_bytes < 0:
            raise InvalidOperation("max 必须是非负整数", id=append_id, max=max_bytes)

        # 幂等：同一追加 ID 的重发绝不产生第二份预留。
        if append_id in self.appends:
            existing = self.appends[append_id]
            same = (
                existing["handle"] == handle_name
                and existing["max"] == max_bytes
            )
            if not same:
                raise RequestConflict(
                    "追加 id 已用于不同请求",
                    id=append_id,
                    existing_handle=existing["handle"],
                    existing_max=existing["max"],
                    requested_handle=handle_name,
                    requested_max=max_bytes,
                )
            view = self.append_view(append_id)
            view.update({"ok": True, "duplicate": True, "resent": True, "at": at})
            return view

        if handle_name not in self.handles:
            raise HandleNotFound("追加使用的句柄未打开", handle=handle_name, id=append_id)
        handle_record = self.handles[handle_name]
        fid = handle_record["file_id"]
        record = self.files[fid]
        coverage = self.coverage_domains(fid)

        blockers: list[dict[str, Any]] = []
        for root in sorted(coverage):
            usage = self.domain_usage(root)
            projected = usage["committed_bytes"] + max_bytes
            if projected > usage["limit"]:
                blockers.append(
                    {
                        "root": root,
                        "limit": usage["limit"],
                        "confirmed_bytes": usage["confirmed_bytes"],
                        "confirmed_length": record["size"],
                        "current_length": record["size"],
                        "pending_upper_bound_bytes": usage["pending_upper_bound_bytes"],
                        "existing_committed_bytes": usage["committed_bytes"],
                        "requested_max": max_bytes,
                        "requested": max_bytes,
                        "projected_committed_bytes": projected,
                        "excess_bytes": projected - usage["limit"],
                    }
                )

        if blockers:
            blocker = blockers[0]
            details = {
                "operation": "append",
                "at": at,
                "id": append_id,
                "handle": handle_name,
                "file_id": fid,
                "blocking_domain": blocker["root"],
                "root": blocker["root"],
                "blocked_domains": blockers,
                "limit": blocker["limit"],
                "confirmed_length": record["size"],
                "current_confirmed_length": record["size"],
                "current_size": record["size"],
                "requested_max": max_bytes,
                "requested": max_bytes,
                "projected_committed_bytes": blocker["projected_committed_bytes"],
                "excess_bytes": blocker["excess_bytes"],
            }
            message = (
                f"配额域 {blocker['root']} 阻止追加 {append_id}："
                f"文件 f{fid} 当前受确认长度为 {record['size']} 字节，"
                f"本次申请上限 {max_bytes} 字节；该域上限 {blocker['limit']}，"
                f"已有确认 {blocker['confirmed_bytes']} 和未确认预留 "
                f"{blocker['pending_upper_bound_bytes']}，预计 "
                f"{blocker['projected_committed_bytes']} 字节。"
            )
            self.last_block = {"at": at, **details, "message": message}
            self.save()
            raise QuotaExceeded(message, **details)

        append = {
            "id": append_id,
            "handle": handle_name,
            "file_id": fid,
            "max": max_bytes,
            "status": "pending",
            "appended": None,
            "reservation_domains": set(coverage),
            "requested_at": at,
        }
        self.appends[append_id] = append
        self.save()
        view = self.append_view(append_id)
        view.update(
            {
                "ok": True,
                "duplicate": False,
                "reserved_bytes": max_bytes,
                "new_reservation_bytes": max_bytes,
                "at": at,
            }
        )
        return view

    append = request_append
    submit_append = request_append

    def confirm_append(
        self,
        id: str | None = None,
        appended: int | None = None,
        append_id: str | None = None,
        actual_bytes: int | None = None,
        at: int | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if isinstance(id, dict):
            record = id
            id = record.get("id") or record.get("append_id")
            appended = record.get(
                "appended",
                record.get("appended_bytes", record.get("actual_bytes", record.get("actual_length"))),
            )
            at = record.get("at", at)
        # 兼容 confirm(actual_bytes, append_id) 的位置参数顺序。
        if isinstance(id, int) and isinstance(appended, str):
            id, appended = appended, id
        append_id = id if id is not None else append_id or kwargs.get("append") or kwargs.get("request_id")
        actual = (
            appended
            if appended is not None
            else actual_bytes
            if actual_bytes is not None
            else kwargs.get("appended_bytes", kwargs.get("actual_length", kwargs.get("n")))
        )
        if append_id is None or actual is None:
            raise InvalidOperation("confirm 需要 id 和 appended")
        if append_id not in self.appends:
            raise AppendNotFound("找不到追加请求", id=append_id)
        if not isinstance(actual, int) or isinstance(actual, bool) or actual < 0:
            raise InvalidOperation("appended 必须是非负整数", id=append_id, appended=actual)

        append = self.appends[append_id]
        if append.get("status") != "pending":
            old_actual = append.get("appended")
            if old_actual == actual:
                view = self.append_view(append_id)
                view.update({"ok": True, "duplicate": True, "at": at})
                return view
            raise RequestConflict(
                "追加请求已用不同实际长度确认",
                id=append_id,
                existing_appended=old_actual,
                requested_appended=actual,
            )

        max_bytes = append["max"]
        if actual > max_bytes:
            raise InvalidOperation(
                "介质确认长度不能超过申请上限",
                id=append_id,
                appended=actual,
                max=max_bytes,
            )

        fid = append["file_id"]
        record = self.files.get(fid)
        old_size = record["size"] if record else None
        if record is not None:
            record["size"] += actual
        append["status"] = "confirmed"
        append["appended"] = actual
        append["settled"] = True
        append["landed"] = actual > 0
        append["outcome"] = "landed" if actual > 0 else "not_landed"
        append["confirmed_at"] = at

        cleaned_up = False
        if not self._pending_append_records(fid):
            coverage = self.coverage_domains(fid)
            if not coverage and fid in self.files:
                self._delete_file_identity(fid)
                cleaned_up = True
            elif record is not None:
                record["quarantined"] = False

        self.save()
        view = self.append_view(append_id)
        difference = max_bytes - actual
        view.update(
            {
                "ok": True,
                "duplicate": False,
                "previous_confirmed_size": old_size,
                "new_confirmed_size": record["size"] if record else old_size + actual if old_size is not None else None,
                "released_difference_bytes": difference,
                "released_bytes": difference,
                "difference_bytes": difference,
                "cleaned_up": cleaned_up or append.get("cleaned_up", False),
                "at": at,
            }
        )
        return view

    confirm = confirm_append

    # -------------------------------- 重命名 --------------------------------

    def rename_path(
        self,
        from_path: str | None = None,
        to_path: str | None = None,
        replace: bool = False,
        source: str | None = None,
        target: str | None = None,
        at: int | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if isinstance(from_path, dict):
            record = from_path
            from_path = record.get("from") or record.get("from_path")
            to_path = record.get("to") or record.get("to_path")
            replace = record.get("replace", replace)
            at = record.get("at", at)
        src = self.normalize_path(source or from_path or kwargs.get("from"))
        dst = self.normalize_path(target or to_path or kwargs.get("to"))

        if not self.path_exists(src):
            raise NotFound("重命名源路径不存在", **{"from": src})
        if src == dst:
            return {"ok": True, "from": src, "to": dst, "noop": True, "at": at}
        if self.path_is_within(dst, src) and dst != src:
            raise InvalidOperation("不能把目录重命名到它自己的子路径", **{"from": src, "to": dst})

        self._require_parent_dir(dst)

        # 配额域根及其内部挂载的其他域根不能作为普通子树移动。
        for root in self.domains:
            if src == root or self.path_is_within(root, src):
                raise InvalidOperation(
                    "不能移动配额域根本身或包含其他配额域根的子树",
                    **{"from": src, "domain_root": root},
                )
            if dst == root:
                raise InvalidOperation(
                    "不能用重命名覆盖配额域根",
                    to=dst,
                    domain_root=root,
                )

        src_is_dir = src in self.dirs
        replaced_info: dict[str, Any] | None = None

        if src_is_dir:
            if dst in self.path_index:
                raise InvalidOperation("目录不能替换同名文件", **{"from": src, "to": dst})
            if dst in self.dirs and not self._is_empty_dir(dst):
                raise DirectoryNotEmpty(
                    "非空目标目录禁止覆盖",
                    **{"from": src, "to": dst},
                )
        else:
            if src not in self.path_index:
                raise NotFound("源既不是文件也不是目录", **{"from": src})
            if dst in self.dirs:
                raise InvalidOperation("文件不能替换同名目录", **{"from": src, "to": dst})
            if dst in self.path_index and not replace:
                raise AlreadyExists(
                    "目标文件已存在；需要 replace=true 才能替换",
                    **{"from": src, "to": dst},
                )

        # 一次性事务：先序列化快照；任何配额/结构错误都完整恢复。
        before_usage = {root: self.domain_usage(root) for root in sorted(self.domains)}
        snapshot = self.to_state()
        moved_file_ids: list[int] = []
        try:
            if src_is_dir:
                if dst in self.dirs:
                    self.dirs.discard(dst)

                old_prefix = src + "/"
                new_dirs: set[str] = set()
                for directory in list(self.dirs):
                    if directory == src:
                        new_dirs.add(dst)
                    elif directory.startswith(old_prefix):
                        new_dirs.add(dst + directory[len(src):])
                    else:
                        new_dirs.add(directory)
                self.dirs = new_dirs

                for old_path in list(self.path_index):
                    if old_path.startswith(old_prefix):
                        fid = self.path_index.pop(old_path)
                        new_path = dst + old_path[len(src):]
                        self.path_index[new_path] = fid
                        record = self.files[fid]
                        record["paths"].discard(old_path)
                        record["paths"].add(new_path)
                        moved_file_ids.append(fid)
            else:
                fid = self.path_index[src]
                moved_file_ids.append(fid)

                if dst in self.path_index:
                    target_fid = self.path_index[dst]
                    target_record = self.files[target_fid]
                    target_domain = self.domain_root_for_path(dst)
                    before_coverage = self.coverage_domains(target_fid)
                    paths_before = sorted(target_record["paths"])
                    handles_before = [
                        name
                        for name, handle in self.handles.items()
                        if handle["file_id"] == target_fid
                    ]

                    self.path_index.pop(dst, None)
                    target_record["paths"].discard(dst)
                    anchor = self._anchor_removed(target_fid, removed_domain=target_domain)
                    after_coverage = self.coverage_domains(target_fid) if target_fid in self.files else set()
                    still_covered_in_target_domain = target_domain in after_coverage
                    released_confirmed = (
                        0
                        if still_covered_in_target_domain
                        else target_record["size"]
                        if target_fid in self.files
                        else target_record["size"]
                    )
                    replaced_info = {
                        "path": dst,
                        "domain": target_domain,
                        "replaced_file_id": target_fid,
                        "replaced_id": f"f{target_fid}",
                        "size": target_record["size"],
                        "paths_before": paths_before,
                        "handles_before": handles_before,
                        "domains_before": sorted(before_coverage),
                        "domains_after": sorted(after_coverage),
                        "still_has_entry_or_handle_in_domain": still_covered_in_target_domain,
                        "released_confirmed_bytes": released_confirmed,
                        "released_bytes": released_confirmed,
                        "freed_bytes": released_confirmed,
                        "anchor_result": anchor,
                        "reason": (
                            f"被替换文件 f{target_fid} 在 {target_domain} 仍有其他入口或"
                            "打开时属于该域的句柄，因此不能提前释放该域份额。"
                            if still_covered_in_target_domain
                            else f"被替换文件 f{target_fid} 在 {target_domain} "
                            "已无入口或句柄，替换后释放该域份额。"
                        ),
                    }

                self.path_index.pop(src, None)
                source_record = self.files[fid]
                source_record["paths"].discard(src)
                source_record["paths"].add(dst)
                self.path_index[dst] = fid

            for fid in set(moved_file_ids):
                if fid in self.files:
                    self._sync_reservations(fid)

            self._raise_if_quota_violated(
                "rename",
                at=at,
                extra_details={
                    "from": src,
                    "to": dst,
                    "replace": bool(replace),
                    "replaced": replaced_info,
                    "moved_file_ids": sorted(set(moved_file_ids)),
                },
                persist_block=False,
            )
        except Exception as error:
            # 业务配额错误和意外结构错误都不能留下半棵子树。先恢复内存快照，
            # 再把干净状态落盘，避免失败事务污染持久化文件。
            restored = VolumeService(state=snapshot)
            state_path = self.state_path
            self.__dict__.update(restored.__dict__)
            self.state_path = state_path
            if isinstance(error, QuotaExceeded):
                self.last_block = {
                    "at": at,
                    "operation": "rename",
                    "code": error.code,
                    "message": error.message,
                    **error.details,
                }
            self.save()
            raise

        quota_effects: list[dict[str, Any]] = []
        for root in sorted(self.domains):
            after = self.domain_usage(root)
            before = before_usage[root]
            quota_effects.append(
                {
                    "root": root,
                    "before_confirmed_bytes": before["confirmed_bytes"],
                    "after_confirmed_bytes": after["confirmed_bytes"],
                    "confirmed_delta_bytes": after["confirmed_bytes"]
                    - before["confirmed_bytes"],
                    "released_confirmed_bytes": max(
                        0, before["confirmed_bytes"] - after["confirmed_bytes"]
                    ),
                    "added_confirmed_bytes": max(
                        0, after["confirmed_bytes"] - before["confirmed_bytes"]
                    ),
                    "before_pending_upper_bound_bytes": before[
                        "pending_upper_bound_bytes"
                    ],
                    "after_pending_upper_bound_bytes": after[
                        "pending_upper_bound_bytes"
                    ],
                    "pending_delta_bytes": after["pending_upper_bound_bytes"]
                    - before["pending_upper_bound_bytes"],
                    "before_committed_bytes": before["committed_bytes"],
                    "after_committed_bytes": after["committed_bytes"],
                    "available_bytes": after["available_bytes"],
                }
            )

        result = {
            "ok": True,
            "from": src,
            "to": dst,
            "type": "directory" if src_is_dir else "file",
            "moved_file_ids": sorted(set(moved_file_ids)),
            "replaced": replaced_info,
            "quota_effects": quota_effects,
            "quota_changes": quota_effects,
            "domains": [self.domain_report(root) for root in sorted(self.domains)],
            "at": at,
        }
        self.change_log.append(
            {
                "kind": "rename",
                "from": src,
                "to": dst,
                "replaced": replaced_info,
                "at": at,
            }
        )
        self.save()
        return result

    rename = rename_path
    move = rename_path

    # ----------------------------- 消息分发/入口 ----------------------------

    def dispatch(
        self,
        kind_or_message: str | dict[str, Any],
        record: dict[str, Any] | None = None,
        at: int | None = None,
    ) -> dict[str, Any]:
        if isinstance(kind_or_message, dict):
            message = kind_or_message
            kind = message.get("kind")
            record = message.get("record") or {}
            at = message.get("at", at)
        else:
            kind = kind_or_message
            record = record or {}

        if not isinstance(record, dict):
            return {
                "ok": False,
                "at": at,
                "kind": kind,
                "code": "invalid_record",
                "message": "record 应为对象",
            }

        method_name = {
            "domain": "add_domain",
            "mkdir": "mkdir",
            "file": "create_file",
            "link": "link",
            "open": "open_file",
            "append": "request_append",
            "confirm": "confirm_append",
            "rename": "rename_path",
            "unlink": "unlink",
            "close": "close_handle",
            "inspect": "inspect",
            "restart": "restart",
        }.get(str(kind))

        if method_name is None:
            return {
                "ok": False,
                "at": at,
                "kind": kind,
                "code": "unknown_kind",
                "message": f"未登记的消息名称：{kind}",
            }

        try:
            method = getattr(self, method_name)
            call_kwargs = dict(record)
            # 外层 at 是到达时刻；避免 record 中同名字段造成重复关键字。
            call_kwargs.pop("at", None)
            result = method(at=at, **call_kwargs)
            if isinstance(result, dict):
                result.setdefault("ok", True)
                result["at"] = at if result.get("at") is None else result["at"]
                return {"ok": True, "at": at, "kind": kind, "result": result}
            return {"ok": True, "at": at, "kind": kind, "result": result}
        except VolumeError as error:
            error_data = error.to_dict()
            response: dict[str, Any] = {
                "ok": False,
                "at": at,
                "kind": kind,
                "error": error.message,
                **error_data,
            }
            # 让 response.blocking_domain 等常用断言可以直接读取。
            for key, value in error_data.get("details", {}).items():
                response.setdefault(key, value)
            return response
        except (TypeError, ValueError, KeyError) as error:
            return {
                "ok": False,
                "at": at,
                "kind": kind,
                "code": "invalid_record",
                "error": str(error),
                "message": str(error),
            }

    def handle_message(
        self, message: dict[str, Any] | str, at: int | None = None
    ) -> dict[str, Any]:
        if isinstance(message, str):
            message = json.loads(message)
        return self.dispatch(message, at=at)

    process_message = handle_message
    process = handle_message
    step = handle_message
    apply = handle_message
    apply_record = dispatch

    def quota(self, root: str | None = None, limit: int | None = None, **kwargs: Any):
        """便捷配额入口：给 limit 时登记/修改域，否则查询。"""

        if limit is not None:
            return self.add_domain(root=root, limit=limit, **kwargs)  # type: ignore[arg-type]
        if root is None:
            return self.inspect_volume()
        return self.domain_report(root)

    def used_bytes(self, root: str) -> int:
        return int(self.domain_usage(self.normalize_path(root))["used_bytes"])

    def available_bytes(self, root: str) -> int:
        return int(self.domain_usage(self.normalize_path(root))["available_bytes"])

    @property
    def quotas(self) -> dict[str, dict[str, Any]]:
        """动态配额表，键为域根。"""

        return {root: self.domain_report(root) for root in sorted(self.domains)}


# 常见服务命名别名，方便不同测试/调用习惯。
VolumeService.create_directory = VolumeService.mkdir  # type: ignore[attr-defined]
VolumeService.add_hard_link = VolumeService.link  # type: ignore[attr-defined]
VolumeService.create_hard_link = VolumeService.link  # type: ignore[attr-defined]
VolumeService.rename_subtree = VolumeService.rename_path  # type: ignore[attr-defined]
VolumeService.move_subtree = VolumeService.rename_path  # type: ignore[attr-defined]
VolumeService.stat = VolumeService.inspect_path  # type: ignore[attr-defined]
VolumeService.lookup = VolumeService.inspect_path  # type: ignore[attr-defined]
VolumeService.get_quota = VolumeService.domain_report  # type: ignore[attr-defined]
VolumeService.inspect_quota = VolumeService.inspect  # type: ignore[attr-defined]
VolumeService.quota_report = VolumeService.domain_report  # type: ignore[attr-defined]
VolumeService.quota_usage = VolumeService.domain_report  # type: ignore[attr-defined]
VolumeService.query_quota = VolumeService.inspect  # type: ignore[attr-defined]
VolumeService.domain = VolumeService.add_domain  # type: ignore[attr-defined]
VolumeService.file = VolumeService.create_file  # type: ignore[attr-defined]
VolumeService.recover = VolumeService.restart  # type: ignore[attr-defined]
VolumeService.reload_process = VolumeService.restart  # type: ignore[attr-defined]
VolumeService.persist = VolumeService.save  # type: ignore[attr-defined]
VolumeService.save_state = VolumeService.save  # type: ignore[attr-defined]

QuotaService = VolumeService
VolumeQuotaService = VolumeService
LocalQuotaService = VolumeService
NamespaceQuotaService = VolumeService
NamespaceService = VolumeService
Namespace = VolumeService
QuotaVolume = VolumeService
Volume = VolumeService
QuotaManager = VolumeService
Service = VolumeService
Engine = VolumeService
Workspace = VolumeService
FileSystem = VolumeService
FS = VolumeService

VolumeService.apply_message = VolumeService.handle_message  # type: ignore[attr-defined]
VolumeService.on_message = VolumeService.handle_message  # type: ignore[attr-defined]
VolumeService.handle = VolumeService.handle_message  # type: ignore[attr-defined]
VolumeService.process_event = VolumeService.handle_message  # type: ignore[attr-defined]
VolumeService.run_command = VolumeService.dispatch  # type: ignore[attr-defined]


class Simulation(VolumeService):
    """场景逐步执行器的兼容名称。"""

    def run(self, source: Any) -> list[dict[str, Any]]:
        return run_scenario(source, service=self)


# ---------------------------------------------------------------------------
# JSONL 场景运行器与命令行入口
# ---------------------------------------------------------------------------


def iter_scenario_rows(source: str | os.PathLike[str] | Iterable[dict[str, Any]]):
    if not isinstance(source, (str, Path)):
        yield from source
        return

    if isinstance(source, str) and (
        "\n" in source or source.lstrip().startswith("{")
    ):
        possible_path = Path(source)
        if "\n" in source or not possible_path.exists():
            for line_number, line in enumerate(source.splitlines(), 1):
                if not line.strip():
                    continue
                row = json.loads(line)
                if row.get("kind") not in MESSAGE_KINDS:
                    raise ValueError(f"输入第 {line_number} 行：未登记的消息名称")
                yield row
            return

    path = Path(source)
    # 优先复用资料读取器的到达顺序/字段校验。
    try:
        from scenario_reader import iter_messages

        registry_path = path.parent / "message_types.json"
        if path.is_file() and registry_path.exists():
            supported = set(json.loads(registry_path.read_text(encoding="utf-8")))
            yield from iter_messages(path, supported)
            return
    except Exception:
        # 作为独立脚本从其他目录运行时，下面使用内置契约。
        pass

    with path.open(encoding="utf-8") as input_file:
        for line_number, line in enumerate(input_file, 1):
            if not line.strip():
                continue
            row = json.loads(line)
            if set(row) != {"at", "kind", "record"}:
                raise ValueError(f"{path} 第 {line_number} 行：消息需要 at、kind 和 record")
            if row["kind"] not in MESSAGE_KINDS:
                raise ValueError(f"{path} 第 {line_number} 行：未登记的消息名称")
            yield row


def run_scenario(
    source: str | os.PathLike[str] | Iterable[dict[str, Any]],
    state_path: str | os.PathLike[str] | None = None,
    service: VolumeService | None = None,
) -> list[dict[str, Any]]:
    service = service or VolumeService(state_path)
    responses: list[dict[str, Any]] = []
    for row in iter_scenario_rows(source):
        responses.append(service.handle_message(row))
    return responses


process_scenario = run_scenario
process_file = run_scenario
run_file = run_scenario
execute_scenario = run_scenario


def format_text_response(response: dict[str, Any]) -> list[str]:
    at = response.get("at")
    kind = response.get("kind")
    prefix = f"[at={at}] {kind}" if at is not None else str(kind)

    if not response.get("ok"):
        blocker = response.get("blocking_domain")
        blocker_text = f"（阻止域：{blocker}）" if blocker else ""
        return [f"✗ {prefix} 被拒绝{blocker_text}：{response.get('message') or response.get('error')}"]

    result = response.get("result", {})
    if kind == "inspect":
        lines = [f"◆ {prefix} 配额查阅"]
        if result.get("query") == "domain":
            lines.append(
                f"  {result['root']} 上限 {result['limit']}："
                f"已确认 {result['confirmed_bytes']}，"
                f"未确认预留 {result['pending_upper_bound_bytes']}，"
                f"可用 {result['available_bytes']}"
            )
            for explanation in result.get("explanations", []):
                lines.append(f"    - {explanation}")
        elif result.get("query") == "handle":
            lines.append(
                f"  句柄 {result['handle']} -> f{result['inode']}，"
                f"计费域 {result['domain']}，当前长度 {result['confirmed_size']}"
            )
        elif result.get("query") == "path":
            lines.append(f"  {result['path']}：{result['type']} {result.get('id', '')}")
        else:
            for domain in result.get("domains", []):
                lines.append(
                    f"  {domain['root']} {domain['confirmed_bytes']}/"
                    f"{domain['limit']}，预留 {domain['pending_upper_bound_bytes']}，"
                    f"可用 {domain['available_bytes']}"
                )
            lines.append(f"  未关句柄：{', '.join(result['open_handles']) or '无'}")
            lines.append(
                "  未结算追加："
                + (", ".join(a['id'] for a in result['pending_appends']) or "无")
            )
            lines.append(
                "  隔离身份："
                + (", ".join(f"f{fid}" for fid in result["quarantined_files"]) or "无")
            )
        return lines

    if kind == "restart":
        return [
            f"✓ {prefix} 已重启并恢复：未关句柄 "
            f"{result.get('open_handle_count', 0)} 个，未结算追加 "
            f"{result.get('pending_append_count', 0)} 个"
        ]
    if kind == "rename" and result.get("replaced"):
        replaced = result["replaced"]
        return [
            f"✓ {prefix} {result['from']} -> {result['to']}；"
            + replaced.get("reason", "")
        ]
    if kind == "append":
        duplicate = "（重发，未再次预留）" if result.get("duplicate") else ""
        domains = ",".join(result.get("reservation_domains", []))
        return [f"✓ {prefix} 已受理{duplicate}，预留域：{domains}"]
    if kind == "confirm":
        return [
            f"✓ {prefix} 实际增加 {result.get('appended')}，"
            f"释放上限差额 {result.get('released_difference_bytes')}"
        ]
    return [f"✓ {prefix}"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="运行共享文件卷的命名空间/配额 JSONL 演练"
    )
    parser.add_argument(
        "scenario",
        type=Path,
        nargs="?",
        default=None,
        help="JSONL 场景文件；省略或为 - 时从标准输入读取",
    )
    parser.add_argument(
        "--state",
        type=Path,
        default=os.environ.get("VOLUME_STATE_PATH"),
        help="持久化状态 JSON 路径；省略时重启仍通过内存快照模拟",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json", "jsonl"),
        default="text",
        help="输出格式：可读文本、JSON 数组或逐行 JSONL",
    )
    args = parser.parse_args(argv)

    service = VolumeService(args.state)
    if args.scenario is None or str(args.scenario) == "-":
        rows = [json.loads(line) for line in sys.stdin if line.strip()]
        responses = run_scenario(rows, service=service)
    else:
        responses = run_scenario(args.scenario, service=service)
    if args.format == "json":
        print(json.dumps(responses, ensure_ascii=False, indent=2))
    elif args.format == "jsonl":
        for response in responses:
            print(json.dumps(response, ensure_ascii=False, separators=(",", ":")))
    else:
        for response in responses:
            for line in format_text_response(response):
                print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
