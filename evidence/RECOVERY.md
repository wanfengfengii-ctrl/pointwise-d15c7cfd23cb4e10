# 门户采集上线与恢复说明

本目录是开发副本，只用于构建与出证；线上运行只依赖安装产物
`ROOT/app.pyz`（`ROOT` 为安装及任务数据目录，`CONTROL` 为钩子与服务日志目录，
两者都可含空格）。解释器为系统 Python 3.11+，无第三方依赖，无需管理员权限。

## 1. 构建与校验发布包

```sh
python3 -B ops.py build release.zip
sha256sum release.zip            # 与 artifacts/release.sha256 对照
```

同一源码重复构建逐字节相同；包内只有 `app.pyz` 与 `manifest.json`，
`manifest.json` 记录 `app.pyz` 的 SHA-256，`ops.py install` 在改动
目标目录之前先校验。也可用 `python3 -B lab.py setup` 直接生成
`artifacts/release.zip` 与 `artifacts/release.sha256`。

## 2. 安装（ROOT 必须不存在或为空）

```sh
python3 ROOT/app.pyz install release.zip ROOT --control CONTROL
python3 ROOT/app.pyz version ROOT     # 复核安装内容摘要与 CONTROL 路径
```

坏包会在创建或修改 ROOT 之前失败，不留下半装状态。

## 3. 注册用户级常驻服务

macOS 13+ 已登录用户会话：

```sh
python3 ROOT/app.pyz start ROOT       # 写 LaunchAgent 并 kickstart
python3 ROOT/app.pyz stop ROOT        # 卸载并删除 LaunchAgent
```

无 `launchctl` 的宿主（如 CI 容器）自动改用常驻监督进程：
`start` 启动一个持有 `ROOT/service.lock` 的监督者，并拉起 `worker`
子进程；worker 被杀（含 SIGKILL）后监督者以新 PID 重开，等价于
LaunchAgent 的 `KeepAlive`。两种机制的 worker PID 都写在
`ROOT/worker.pid`，监督者 PID 在 `ROOT/service.pid`；服务日志在
`CONTROL/service.stdout`、`CONTROL/service.stderr`。

## 4. 接纳与查看进度

```sh
python3 ROOT/app.pyz enqueue ROOT TASK URL    # 单源、单活动下载
python3 ROOT/app.pyz downloads ROOT           # 状态数组
python3 ROOT/app.pyz status  ROOT             # 业务任务状态
python3 ROOT/app.pyz read    ROOT sample      # 分析结果与原业务回执
```

`downloads` 每项含 `task_id`、`url`、`state`、`etag`、`length`、`sha256`、
`received`、`error`、`receipt`。`received` 是该 ETag 下**已 fsync 落盘并
确认**的连续原始字节前缀长度，不是仅显示过的字节；`receipt` 仅在
`completed` 时非空。`error` 给出失败原因：网络断开/超时为
`SHORT_RESPONSE` 等，整文件摘要不符为 `DIGEST`，长度不符为 `LENGTH`。

状态机：`queued → downloading → completed` 或 `failed`。传输类故障回到
`queued` 由 worker 自动重试；输入非法或摘要错误置 `failed`，不交给分析核。
同号同 URL 重复 `enqueue` 返回原接纳值，同号异 URL 报 `TASK_CONFLICT`，
已有活动下载时新任务报 `ACTIVE_DOWNLOAD`。

## 5. 断点与恢复语义

- 每收到一块即 `flush`+`fsync` 文件，然后才把 `received` 写入 SQLite；
  崩溃不会把未落盘字节算作确认。
- 同版重开：先 `HEAD` 核对 ETag/长度/摘要未变，再发
  `Range: bytes=received-` + `If-Range: <强ETag>`，只取 `received` 以后的
  字节；`HEAD` 不算重取载荷。
- 未完成时换版：新一轮的表示元数据与检查点不同即丢弃旧 `.part` 前缀，
  从字节 0 完整取新版本，绝不把旧前缀拼到换版 200 响应后。
- 截短：源提前断开时本轮为 `SHORT_RESPONSE`，确认前缀保留，下次自动续传。
- 摘要必须整文件核验：核对长度与 `X-Content-SHA256` 后才原子移为
  `TASK.raw`；只凭 ETag 或长度不会跳过摘要核验。
- 完成交接：完整校验后 `runtime.submit` → 加工 → ledger/回执原子提交，
  再写下载完成状态。任一窗口（文件就绪、业务接纳、入账后）被杀进程，
  重开都从已确认字节或已完成文件继续，最终只产生一笔 ledger。

## 6. 结果核对与停止

读取 `read ROOT sample` 的结果和回执；`receipt` 的
`input_sha256` 是 `pipeline.encode(batch)` 的摘要（不是原始下载摘要），
`result_sha256` 是结果文件摘要，并应等于 `downloads` 中同任务的 `receipt`。
`sample` 的期望结果为
`[{"station":"east","minute":0,"count":2,"sum":0},{"station":"west","minute":1,"count":1,"sum":70}]`。

停止本代理：

```sh
python3 ROOT/app.pyz stop ROOT
```

停止后 `worker.pid` 与监督者退出；重复 `stop` 幂等。

## 7. 交接窗口钩子（仅测试编排）

`CONTROL/hook.json` 为 `{"phase":"chunk"}` 等时，运行到该边界会写
`CONTROL/<phase>.ready`（内含 PID）并等待 `CONTROL/<phase>.go`，通过后写
`<phase>.spent`。`verified` 在完整文件校验保存后、业务接纳前；`accepted`
在业务接纳后；`accounted` 在入账后、下载完成状态前。钩子只控制暂停，
不提供恢复行为；`<phase>.spent` 保证已通过的钩子不重复阻塞。

## 8. 回归与证据

```sh
python3 -B checks.py verify      # 冻结业务断言 + 录制源协议回归（不联网）
python3 -B lab.py verify         # 从安装产物跑正常采集、续传、换版、坏摘要、
                                 # 文件就绪/业务接纳/入账三窗口重启等真实场景
python3 -B lab.py demo           # 从安装产物采集并展示结果、来源摘要与回执
python3 -B lab.py evidence RELEASE DIR
                                 # 上述 verify+demo 加一次真实 live 重启运行，
                                 # 把 evidence.json/发布包/说明写入 DIR
```

`lab.py` 的执行顺序是：解出包内源码 → 用包内 `ops.py build` 重新构建并
与发布包逐字节比较 → 把开发副本移走 → 只经 `ROOT/app.pyz` 安装、启动源与
用户代理、采集、杀进程重启、核账，`evidence.json` 留在指定目录。
`lab.py verify` 与 `demo` 均在临时新目录内运行，不写开发副本。
