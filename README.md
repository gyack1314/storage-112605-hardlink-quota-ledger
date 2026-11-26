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
