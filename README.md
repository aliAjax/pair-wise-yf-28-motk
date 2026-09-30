# 临床试验随机分配与盲法服务

仅使用 Python 3.11+ 标准库的独立随机化服务。支持分层区组随机、试验方案锁定、隐藏分组、外部编号并发幂等、中心隔离、双人揭盲、审计，以及**断网离线入组的号段托管**。

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

## 号段托管（离线断网入组）

断网时中心先预领一批随机号离线入组，网络恢复后上传核销。为避免多中心同时提交撞号，由协调员按**中心 + 分层**预占**连续号段**，各中心号段全局不相交。

- `POST /api/trials/{id}/segments`：协调员按中心和分层预占连续号段，其他中心不能领取同一序列。
- `GET /api/trials/{id}/segments`：列出号段（中心用户仅见本中心）。
- `GET /api/segments/{id}`：号段明细，含每个编号的核销状态与差异。
- `POST /api/segments/{id}/write-off`：单笔核销并入组；同一外部编号只核销一次，重复提交幂等。
- `POST /api/segments/{id}/release`：协调员确认后释放未用名额；已核销编号不回收。
- `POST /api/trials/{id}/upload-batches`：批量上传离线核销清单，持久化后逐笔核销；失败保留原号段并可重试。
- `POST /api/upload-batches/{id}/process`：继续/重试未核销清单；服务重启后自动接着传。
- `GET /api/upload-batches/{id}`：批次明细，含每笔核销状态与失败原因。
- `POST /api/sites/{id}/deactivate`：中心停用，释放其未用号段；已发编号与审计保留可查。
- `GET /api/trials/{id}/audit`：审计记录（含中心停用等全局动作）。

页面（`GET /`）可发起号段、单笔/批量核销、查看差异、重试批次、停用中心和查审计。

## 主要接口

- `POST /api/trials`：创建草稿试验，指定分组、分层因素、区组长度和随机种子。
- `POST /api/trials/{id}/protocol`：入组前修改方案；一旦入组即锁定。
- `POST /api/trials/{id}/start`：开始入组。
- `POST /api/trials/{id}/enroll`：按当前用户中心入组；响应只返回分配编号，不返回分组。
- `GET /api/trials/{id}/participants`：分中心返回数据，中心用户看不到其他中心。
- `POST /api/participants/{id}/unblinding-requests`：发起揭盲。
- `POST /api/unblinding-requests/{id}/approve`：两人独立审批；同一人不能审批两次。
- `GET /api/trials/{id}/summary`：中心级汇总和审计记录。

随机表按“试验种子 + 中心 + 分层因素”确定性生成，每个区组为分组数的整数倍并打乱；分配在 SQLite `BEGIN IMMEDIATE` 事务中原子占用。号段编号在事务中扫描最小可用连续区间，保证多中心并发预占不撞号。实现适合作为流程原型，不替代经认证的临床试验随机化系统。
