# 共享文件卷的硬链接与跨目录配额结算

使用绝对路径，自顶层目录获得配额域。文件以不随路径变化的身份跟踪。bytes/limit/max 均为字节。会话句柄由调用方在输入中命名。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **domain**　root 是顶层域目录，limit 是配额上限。
- **mkdir**　path 为待创建的目录。
- **file**　path 上创建 size 字节文件，text 是该样本的说明。
- **link**　把 from 所在的文件添加到 to，已有目标路径不得复用。
- **open**　以 handle 打开 path。
- **append**　通过 handle 请求追加最多 max 字节，id 唯一。
- **confirm**　追加 id 的 appended 为实际增加字节；0 表示未落地。
- **rename**　移动 from 到 to；对文件时 replace 可以允许替换，非空目标目录禁止覆盖。
- **unlink**　从 path 去掉一个文件入口。
- **close**　结束 handle。
- **inspect**　查看收费域或文件句柄。
- **restart**　卷进程重启。

## 示例

- cross-domain-rename-with-growth.jsonl
- replacement-keeps-open-charge.jsonl

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会按文件统计各类参与方消息的数量。演练记录只描述各参与方交给系统的输入。

## 卷服务与项目入口

核心实现在 `volume_service.py`，只依赖标准库。它维护虚拟目录树、不随路径变化的文件身份、按打开时所属域固定的句柄关联、追加预留和可持久化状态。

运行演练并查看中文配额说明：

```bash
python3 main.py examples/cross-domain-rename-with-growth.jsonl
python3 volume_service.py examples/replacement-keeps-open-charge.jsonl
python3 scenario_runner.py examples/cross-domain-rename-with-growth.jsonl --format jsonl
```

需要模拟值班员重启后的真实磁盘恢复时，指定状态文件：

```bash
python3 main.py examples/cross-domain-rename-with-growth.jsonl --state /tmp/volume-state.json
```

作为库使用：

```python
from volume_service import VolumeService

volume = VolumeService("/tmp/volume-state.json")
volume.add_domain("/press", 24)
volume.mkdir("/press/jobs")
volume.create_file("/press/jobs/sample", size=9, text="coupon")
volume.link("/press/jobs/sample", "/press/keep")
volume.open_file("/press/jobs/sample", "writer")
volume.append(id="extra", handle="writer", max=8)
volume.rename("/press/jobs", "/lab/jobs")  # 若 /lab 额度不足会原子失败
volume.confirm(id="extra", appended=4)
print(volume.inspect("/press")["explanations"])
volume.restart()
```

也可以把完整 JSONL 行交给消息入口：

```python
response = volume.handle_message({
    "at": 6,
    "kind": "rename",
    "record": {"from": "/press/jobs", "to": "/lab/jobs"},
})
```

### 记账规则

- 同一 inode/文件身份在一个配额域内无论有多少硬链接或句柄，当前确认长度只计一次。
- 跨域硬链接或重命名后，每个仍能通过路径或打开句柄看到该身份的域，都分别承担一份等大配额。
- 追加受理时，在所有当前付费域按 `max` 预留上限；`confirm(appended=n)` 后把确认长度增加 `n`，并释放 `max-n` 差额。
- 同一追加 `id` 重发只返回首次结果，不会重复增加预留。
- 重命名整棵子树使用快照事务；任一目标域额度不足，命名空间、预留和持久化文件都恢复到操作前。
- 目标路径被替换时，只有被替换身份在目标域仍有其他入口、或有打开时属于该域的句柄，才继续占用该域份额。
- 句柄跨重命名有效，其配额域固定为 `open` 时路径所属域，直到 `close`。
- 无路径、无句柄但仍有未确认追加的身份进入隔离状态；重启后仍可核对，收到全部介质回执后清理，inode 身份不会复用。
- 查询结果同时提供 `used_bytes`/`confirmed_bytes`、`pending_upper_bound_bytes`/`reserved_bytes`、`committed_bytes`、`available_bytes` 和中文 `explanations`。失败响应的 `blocking_domain`/`blocked_domains` 说明阻止增长或重命名的域。

## 测试

```bash
python3 -m unittest discover -v
```
