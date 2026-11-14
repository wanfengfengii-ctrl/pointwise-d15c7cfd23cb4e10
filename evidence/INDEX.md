# 上线证据索引

本目录可独立核验，不需要开发副本。

- `release.zip` / `release.sha256`：交付包及摘要。在本目录运行
  `sha256sum -c release.sha256` 校验。包内 `manifest.json` 记录 `app.pyz`
  摘要，`ops.py install` 在校验通过前不改动目标。
- `RECOVERY.md`：启动、查看进度与失败原因、核对结果、停止代理的简短恢复说明。
- `evidence.json`：完整可复核记录，键为：
  - `release`：交付包路径与 ZIP 的 SHA-256，应与 `release.sha256` 一致。
  - `verify.normal`：安装产物上的正常采集，含 `app_sha256`、`download`、
    `commands`、`installed_regression`、唯一 `ledger_rows`。
  - `verify.recovery.scenarios`：截短续传（`truncation`）、下载中杀进程同版前缀复用
    （`kill_resume`）、未完成换版（`version_change`）、坏摘要不发布（`bad_digest`），
    以及 `verified`/`accepted`/`accounted` 三个交接窗口的 SIGKILL 重启（`handoff_*`）。
    每项含 `commands`、源 `requests`（method/range/status/version/body_bytes）、
    `paused` 持久进度、原始 `raw` 摘要、业务 `business` 结果与 `receipt`、`ledger_rows`；
    发生杀进程的场景另含 `pid_before_kill`/`pid_after_restart`，截短场景另含 `resumed_at`。
  - `live`：只经交付包安装后的一次真实运行（路径含空格），含 `installed_version`、
    重启前后 PID、暂停时 `received`、最终 `download`、源请求、原始摘要、业务结果与回执。
  - `demo`：安装产物的采集展示（来源 ETag/长度/摘要、结果、回执、请求、ledger 行数）。

核对要点：

1. `kill_resume`/`live` 的 206 `range` 起点等于 `paused` 的 `received`，且
   `body_bytes = length - received`，说明未重取确认前缀。
2. `kill_resume`/`handoff_*`/`live` 的 `pid_before_kill != pid_after_restart`，说明真实重开。
3. `version_change` 无 206，只有新版本完整 200，最终 ETag 为 `"portal-2"`，说明未混版。
4. `bad_digest` 为 `failed`、`error` 含 `DIGEST`、无 `.raw`、ledger 为 0。
5. 所有完成的场景 `ledger_rows == 1`，`receipt_equals_business` 为真。
6. `live` 最终 `received == length`，`jobs` 中 `sample` 为 `completed` 且只有一行。

重新生成：

```sh
python3 -B lab.py setup
python3 -B lab.py evidence artifacts/release.zip evidence
```
