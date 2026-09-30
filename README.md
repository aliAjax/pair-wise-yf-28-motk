# 临床试验随机分配与盲法服务

仅使用 Python 3.11+ 标准库的独立随机化服务。支持分层区组随机、试验方案锁定、隐藏分组、外部编号并发幂等、中心隔离、双人揭盲和审计，并内置**补号段托管**：中心断网时由协调员预领连续号段离线入组，网络恢复后统一核销，避免多中心撞号。

## 运行

```bash
python3 app.py --init --seed
python3 app.py
```

访问 <http://127.0.0.1:8104>，默认数据库 `randomization.db`。测试：

```bash
python3 -m unittest -v
```

演示用户：`site1`、`site2`（研究中心），`coord`（协调员），`monitor1`、`monitor2`（监查员）。

## 主要接口

- `POST /api/trials`：创建草稿试验，指定分组、分层因素、区组长度和随机种子。
- `POST /api/trials/{id}/protocol`：入组前修改方案；一旦入组即锁定。
- `POST /api/trials/{id}/start`：开始入组。
- `POST /api/trials/{id}/enroll`：按当前用户中心入组；响应只返回分配编号，不返回分组。
- `GET /api/trials/{id}/participants`：分中心返回数据，中心用户看不到其他中心。
- `POST /api/participants/{id}/unblinding-requests`：发起揭盲。
- `POST /api/unblinding-requests/{id}/approve`：两人独立审批；同一人不能审批两次。
- `GET /api/trials/{id}/summary`：中心级汇总和审计记录。

### 补号段托管（离线入组）

- `POST /api/trials/{id}/segments`（协调员）：按 `site_id` + `factors`（分层）预占一段**连续号**。各中心共享同一分层的高水位序号，号段互不重叠；中心停用时不能领取。
- `GET /api/trials/{id}/segments`：列出号段；中心用户只见本中心。
- `GET /api/segments/{id}`：号段详情与**差异清单**（已核销 / 未核销 / 未用 / 已释放，逐条列出外部编号、序号和失败原因）。
- `POST /api/trials/{id}/segments/{sid}/offline-items`（中心）：上传离线清单核销，每条 `{external_id, sequence}`。
  - 同一外部编号全试验只核销一次（`UNIQUE(trial_id,external_id)`），重复上传幂等返回原记录；其他中心重复提交报 `external_id_consumed`。
  - 单条失败不影响同批其他条目，**原号段保留**并把失败项落入持久台账（`offline_upload_items`），重启后仍可通过下面的接口或页面继续传。
  - 已核销（used）编号不回收。
- `POST /api/segments/{id}/retry`：重试号段内所有 `pending/failed` 台账条目。
- `POST /api/segments/{id}/release`（协调员）：确认少用后释放未用名额；存在未核销条目时拒绝释放（`pending_items`）。已核销编号和参与者保留，释放出的连续区在后续预占时优先复用。
- `POST /api/sites/{id}`（协调员）：`{"active": false}` 停用中心，自动释放其没有未核销条目的活动号段；已发编号、参与者和全部审计记录保留。

页面（根路径）支持：发起号段、上传核销、重试、释放、停用/启用中心，以及查看号段状态和逐条差异。

## 设计要点

随机表按“试验种子 + 中心 + 分层因素”确定性生成，每个区组为分组数的整数倍并打乱；在线分配在 SQLite `BEGIN IMMEDIATE` 事务中原子占用。补号段走独立的全局分层序列（`segment_numbers` + `segment_highwater`，与在线 `allocations` 序号以 `OFFLINE_SEQ_OFFSET` 隔开），分组按同一随机种子对 `GLOBAL|分层:区组` 确定性生成，保证各中心号段不撞号且区组均衡。所有号段操作写 `audit_log`。实现适合作为流程原型，不替代经认证的临床试验随机化系统。
