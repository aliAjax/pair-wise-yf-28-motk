"""临床试验分层区组随机分配与盲法服务。"""
from __future__ import annotations

import argparse
import hashlib
import json
import random
import sqlite3
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB = BASE_DIR / "randomization.db"
MAX_ARM_LENGTH = 40
# 离线补号在在线随机表（allocations）中的序号偏移，两个号段宇宙互不干扰
OFFLINE_SEQ_OFFSET = 10_000_000


class BusinessError(Exception):
    def __init__(self, message, status=400, code="bad_request"):
        super().__init__(message)
        self.message, self.status, self.code = message, status, code


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class RandomizationStore:
    def __init__(self, db_path=DEFAULT_DB):
        self.db_path = str(db_path)
        self._lock = threading.Lock()

    def connect(self):
        conn = sqlite3.connect(self.db_path, timeout=15)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=15000")
        return conn

    def init_schema(self):
        with self._lock, self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users(
                    id TEXT PRIMARY KEY, name TEXT NOT NULL,
                    role TEXT NOT NULL CHECK(role IN ('site','coordinator','monitor')),
                    site_id TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1))
                );
                CREATE TABLE IF NOT EXISTS trials(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE,
                    protocol_version TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'draft'
                        CHECK(status IN ('draft','running','stopped')),
                    arms_json TEXT NOT NULL, strata_factors_json TEXT NOT NULL,
                    block_size INTEGER NOT NULL CHECK(block_size >= 2),
                    seed TEXT NOT NULL, created_by TEXT NOT NULL REFERENCES users(id),
                    created_at TEXT NOT NULL, started_at TEXT
                );
                CREATE TABLE IF NOT EXISTS strata(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trial_id INTEGER NOT NULL REFERENCES trials(id),
                    stratum_key TEXT NOT NULL, factors_json TEXT NOT NULL, created_at TEXT NOT NULL,
                    UNIQUE(trial_id,stratum_key)
                );
                CREATE TABLE IF NOT EXISTS allocations(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trial_id INTEGER NOT NULL REFERENCES trials(id),
                    stratum_id INTEGER NOT NULL REFERENCES strata(id),
                    sequence INTEGER NOT NULL, block_no INTEGER NOT NULL,
                    arm TEXT NOT NULL, used_by INTEGER, used_at TEXT,
                    UNIQUE(stratum_id,sequence)
                );
                CREATE TABLE IF NOT EXISTS participants(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trial_id INTEGER NOT NULL REFERENCES trials(id),
                    site_id TEXT NOT NULL, external_id TEXT NOT NULL,
                    stratum_id INTEGER NOT NULL REFERENCES strata(id),
                    allocation_id INTEGER NOT NULL UNIQUE REFERENCES allocations(id),
                    allocation_code TEXT NOT NULL UNIQUE, status TEXT NOT NULL DEFAULT 'enrolled'
                        CHECK(status IN ('enrolled','withdrawn','completed')),
                    enrolled_by TEXT NOT NULL REFERENCES users(id), created_at TEXT NOT NULL,
                    UNIQUE(trial_id,external_id)
                );
                CREATE TABLE IF NOT EXISTS unblinding_requests(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    participant_id INTEGER NOT NULL REFERENCES participants(id),
                    requester_id TEXT NOT NULL REFERENCES users(id), reason TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','approved','rejected')),
                    first_approver TEXT REFERENCES users(id), second_approver TEXT REFERENCES users(id),
                    decided_at TEXT, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS audit_log(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, trial_id INTEGER REFERENCES trials(id),
                    actor_id TEXT NOT NULL REFERENCES users(id), action TEXT NOT NULL,
                    detail TEXT NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sites(
                    id TEXT PRIMARY KEY, name TEXT NOT NULL,
                    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS hosted_segments(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trial_id INTEGER NOT NULL REFERENCES trials(id),
                    site_id TEXT NOT NULL REFERENCES sites(id),
                    stratum_key TEXT NOT NULL,
                    factors_json TEXT NOT NULL,
                    seq_start INTEGER NOT NULL, seq_end INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active','released')),
                    created_by TEXT NOT NULL REFERENCES users(id),
                    created_at TEXT NOT NULL, released_at TEXT,
                    UNIQUE(trial_id,stratum_key,seq_start)
                );
                CREATE TABLE IF NOT EXISTS segment_highwater(
                    trial_id INTEGER NOT NULL, stratum_key TEXT NOT NULL, next_seq INTEGER NOT NULL,
                    PRIMARY KEY(trial_id,stratum_key)
                );
                CREATE TABLE IF NOT EXISTS segment_numbers(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trial_id INTEGER NOT NULL, stratum_key TEXT NOT NULL,
                    sequence INTEGER NOT NULL, arm TEXT NOT NULL,
                    segment_id INTEGER NOT NULL REFERENCES hosted_segments(id),
                    site_id TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'reserved' CHECK(state IN ('reserved','used','released')),
                    participant_id INTEGER REFERENCES participants(id),
                    updated_at TEXT NOT NULL,
                    UNIQUE(trial_id,stratum_key,sequence)
                );
                CREATE TABLE IF NOT EXISTS offline_upload_items(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trial_id INTEGER NOT NULL, segment_id INTEGER NOT NULL REFERENCES hosted_segments(id),
                    site_id TEXT NOT NULL, external_id TEXT NOT NULL, sequence INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','reconciled','failed')),
                    error TEXT, attempts INTEGER NOT NULL DEFAULT 0,
                    participant_id INTEGER REFERENCES participants(id),
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL, reconciled_at TEXT,
                    UNIQUE(trial_id,external_id)
                );
                """
            )

    def seed(self):
        self.init_schema()
        with self.connect() as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO users(id,name,role,site_id) VALUES(?,?,?,?)",
                [
                    ("site1", "中心一协调员", "site", "S001"),
                    ("site2", "中心二协调员", "site", "S002"),
                    ("coord", "项目协调员", "coordinator", "CENTER"),
                    ("monitor1", "独立监查员甲", "monitor", "CENTER"),
                    ("monitor2", "独立监查员乙", "monitor", "CENTER"),
                ],
            )
            conn.executemany(
                "INSERT OR IGNORE INTO sites(id,name,created_at) VALUES(?,?,?)",
                [("S001", "中心一", now()), ("S002", "中心二", now())],
            )

    def _user(self, conn, user_id, roles=None):
        if not user_id:
            raise BusinessError("缺少 X-User-Id", 401, "authentication_required")
        user = conn.execute("SELECT * FROM users WHERE id=? AND active=1", (user_id,)).fetchone()
        if not user:
            raise BusinessError("用户不存在或已停用", 401, "unknown_user")
        if roles and user["role"] not in roles:
            raise BusinessError("当前角色无权执行此操作", 403, "forbidden")
        return user

    def _trial(self, conn, trial_id):
        row = conn.execute("SELECT * FROM trials WHERE id=?", (trial_id,)).fetchone()
        if not row:
            raise BusinessError("试验不存在", 404, "not_found")
        return row

    def _audit(self, conn, trial_id, actor, action, detail):
        conn.execute(
            "INSERT INTO audit_log(trial_id,actor_id,action,detail,created_at) VALUES(?,?,?,?,?)",
            (trial_id, actor, action, json.dumps(detail, ensure_ascii=False, sort_keys=True), now()),
        )

    def create_trial(self, user_id, name, protocol_version, arms, strata_factors, block_size, seed):
        name = name.strip()
        if len(name) < 3 or not protocol_version.strip() or len(seed.strip()) < 8:
            raise BusinessError("试验名称、方案版本和至少 8 位随机种子不能为空", 422, "invalid_trial")
        if not isinstance(arms, list) or len(arms) < 2:
            raise BusinessError("至少需要两个试验组", 422, "invalid_arms")
        arms = [str(a).strip() for a in arms]
        if any(not a or len(a) > MAX_ARM_LENGTH for a in arms) or len(set(arms)) != len(arms):
            raise BusinessError("试验组名称必须非空、唯一且不过长", 422, "invalid_arms")
        if not isinstance(strata_factors, list) or any(not str(x).strip() for x in strata_factors) or len(set(strata_factors)) != len(strata_factors):
            raise BusinessError("分层因素必须是非空且不重复的数组", 422, "invalid_strata")
        if isinstance(block_size, bool) or not isinstance(block_size, int) or block_size < len(arms) or block_size % len(arms) != 0:
            raise BusinessError("区组长度必须为试验组数的正整数倍", 422, "invalid_block_size")
        with self.connect() as conn:
            actor = self._user(conn, user_id, {"coordinator"})
            try:
                cur = conn.execute(
                    """INSERT INTO trials(name,protocol_version,arms_json,strata_factors_json,block_size,seed,created_by,created_at)
                       VALUES(?,?,?,?,?,?,?,?)""",
                    (name, protocol_version.strip(), json.dumps(arms), json.dumps([str(x).strip() for x in strata_factors]), block_size, seed.strip(), user_id, now()),
                )
            except sqlite3.IntegrityError:
                raise BusinessError("试验名称已存在", 409, "trial_exists")
            trial_id = cur.lastrowid
            self._audit(conn, trial_id, user_id, "trial.create", {"protocol_version": protocol_version, "arms": len(arms), "block_size": block_size})
            return {"id": trial_id, "name": name, "status": "draft", "arms": arms, "strata_factors": strata_factors, "block_size": block_size}

    def update_protocol(self, user_id, trial_id, protocol_version, arms=None, strata_factors=None, block_size=None, seed=None):
        with self.connect() as conn:
            actor = self._user(conn, user_id, {"coordinator"})
            trial = self._trial(conn, trial_id)
            enrolled = conn.execute("SELECT COUNT(*) FROM participants WHERE trial_id=?", (trial_id,)).fetchone()[0]
            if enrolled or trial["status"] != "draft":
                raise BusinessError("入组开始后不能修改随机方案", 409, "protocol_locked")
            new_arms = arms if arms is not None else json.loads(trial["arms_json"])
            new_strata = strata_factors if strata_factors is not None else json.loads(trial["strata_factors_json"])
            new_block = block_size if block_size is not None else trial["block_size"]
            new_seed = str(seed) if seed is not None else trial["seed"]
            self.create_trial_validation_only(new_arms, new_strata, new_block, new_seed)
            conn.execute(
                """UPDATE trials SET protocol_version=?,arms_json=?,strata_factors_json=?,block_size=?,seed=? WHERE id=?""",
                (protocol_version.strip(), json.dumps(new_arms), json.dumps(new_strata), new_block, new_seed, trial_id),
            )
            self._audit(conn, trial_id, user_id, "protocol.update", {"protocol_version": protocol_version})
            return {"id": trial_id, "protocol_version": protocol_version, "arms": new_arms, "block_size": new_block}

    @staticmethod
    def create_trial_validation_only(arms, strata_factors, block_size, seed):
        if not isinstance(arms, list) or len(arms) < 2 or len(set(arms)) != len(arms):
            raise BusinessError("试验组配置无效", 422, "invalid_arms")
        if not isinstance(strata_factors, list) or not strata_factors or len(set(strata_factors)) != len(strata_factors):
            raise BusinessError("分层因素配置无效", 422, "invalid_strata")
        if isinstance(block_size, bool) or not isinstance(block_size, int) or block_size < len(arms) or block_size % len(arms):
            raise BusinessError("区组长度无效", 422, "invalid_block_size")
        if len(str(seed)) < 8:
            raise BusinessError("随机种子至少 8 位", 422, "invalid_seed")

    def start_trial(self, user_id, trial_id):
        with self.connect() as conn:
            self._user(conn, user_id, {"coordinator"})
            trial = self._trial(conn, trial_id)
            if trial["status"] != "draft":
                raise BusinessError("只有草稿试验可以开始", 409, "invalid_status")
            conn.execute("UPDATE trials SET status='running',started_at=? WHERE id=?", (now(), trial_id))
            self._audit(conn, trial_id, user_id, "trial.start", {})
            return {"id": trial_id, "status": "running"}

    def _normalize_factors(self, trial, factors):
        expected = json.loads(trial["strata_factors_json"])
        if not isinstance(factors, dict) or set(factors) != set(expected):
            raise BusinessError(f"必须提供分层因素: {', '.join(expected)}", 422, "invalid_factors")
        normalized = {k: str(factors[k]).strip() for k in sorted(expected)}
        if any(not v for v in normalized.values()):
            raise BusinessError("分层因素值不能为空", 422, "invalid_factors")
        return normalized

    @staticmethod
    def _factors_key(normalized):
        return json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def _hosted_arm(self, trial, stratum_key, sequence):
        arms = json.loads(trial["arms_json"])
        block_size = trial["block_size"]
        block_no = (sequence - 1) // block_size + 1
        offset = (sequence - 1) % block_size
        rng = random.Random(f"{trial['seed']}:GLOBAL|{stratum_key}:{block_no}")
        plan = arms * (block_size // len(arms))
        rng.shuffle(plan)
        return plan[offset]

    def _stratum(self, conn, trial, factors, site_id):
        normalized = self._normalize_factors(trial, factors)
        key = f"{site_id}|" + self._factors_key(normalized)
        row = conn.execute("SELECT * FROM strata WHERE trial_id=? AND stratum_key=?", (trial["id"], key)).fetchone()
        if row:
            return row
        cur = conn.execute(
            "INSERT INTO strata(trial_id,stratum_key,factors_json,created_at) VALUES(?,?,?,?)",
            (trial["id"], key, json.dumps({"site_id": site_id, **normalized}, ensure_ascii=False, sort_keys=True), now()),
        )
        return conn.execute("SELECT * FROM strata WHERE id=?", (cur.lastrowid,)).fetchone()

    def _next_allocation(self, conn, trial, stratum):
        for block_no in range(1, 101):
            count = conn.execute(
                "SELECT COUNT(*) FROM allocations WHERE stratum_id=? AND block_no=?", (stratum["id"], block_no)
            ).fetchone()[0]
            if count == 0:
                rng = random.Random(f"{trial['seed']}:{stratum['stratum_key']}:{block_no}")
                arms = json.loads(trial["arms_json"])
                plan = []
                blocks = len(arms) if trial["block_size"] > len(arms) else 1
                for _ in range(blocks * (trial["block_size"] // len(arms))):
                    plan.extend(arms)
                rng.shuffle(plan)
                start = conn.execute(
                    "SELECT COALESCE(MAX(sequence),0) FROM allocations WHERE stratum_id=?", (stratum["id"],)
                ).fetchone()[0]
                for offset, arm in enumerate(plan, 1):
                    conn.execute(
                        "INSERT INTO allocations(trial_id,stratum_id,sequence,block_no,arm) VALUES(?,?,?,?,?)",
                        (trial["id"], stratum["id"], start + offset, block_no, arm),
                    )
            free = conn.execute(
                "SELECT * FROM allocations WHERE stratum_id=? AND used_by IS NULL ORDER BY sequence LIMIT 1", (stratum["id"],)
            ).fetchone()
            if free:
                return free
        raise BusinessError("随机分配表已耗尽，请由统计人员扩展方案", 409, "allocation_exhausted")

    def enroll(self, user_id, trial_id, external_id, factors):
        external_id = str(external_id).strip()
        if not external_id:
            raise BusinessError("外部受试者编号不能为空", 422, "invalid_external_id")
        with self.connect() as conn:
            actor = self._user(conn, user_id, {"site"})
            try:
                conn.execute("BEGIN IMMEDIATE")
                trial = self._trial(conn, trial_id)
                if trial["status"] != "running":
                    raise BusinessError("试验尚未开始或已经停止", 409, "trial_not_running")
                existing = conn.execute(
                    "SELECT * FROM participants WHERE trial_id=? AND external_id=?", (trial_id, external_id)
                ).fetchone()
                if existing:
                    if existing["site_id"] != actor["site_id"]:
                        raise BusinessError("不能在当前中心查看其他中心的受试者", 403, "site_isolation")
                    conn.commit()
                    return self._blinded_participant(conn, existing, actor, allow_arm=False, idempotent=True)
                stratum = self._stratum(conn, trial, factors, actor["site_id"])
                allocation = self._next_allocation(conn, trial, stratum)
                allocation_code = hashlib.sha256(f"{trial_id}:{external_id}".encode()).hexdigest()[:12].upper()
                cur = conn.execute(
                    """INSERT INTO participants(trial_id,site_id,external_id,stratum_id,allocation_id,allocation_code,enrolled_by,created_at)
                       VALUES(?,?,?,?,?,?,?,?)""",
                    (trial_id, actor["site_id"], external_id, stratum["id"], allocation["id"], allocation_code, user_id, now()),
                )
                participant_id = cur.lastrowid
                conn.execute("UPDATE allocations SET used_by=?,used_at=? WHERE id=?", (participant_id, now(), allocation["id"]))
                self._audit(conn, trial_id, user_id, "participant.enroll", {"participant_id": participant_id, "external_id": external_id, "allocation_id": allocation["id"], "site_id": actor["site_id"]})
                participant = conn.execute("SELECT * FROM participants WHERE id=?", (participant_id,)).fetchone()
                return self._blinded_participant(conn, participant, actor, allow_arm=False, idempotent=False)
            except sqlite3.IntegrityError as exc:
                conn.rollback()
                if "participants.trial_id, participants.external_id" in str(exc):
                    with self.connect() as retry:
                        row = retry.execute("SELECT * FROM participants WHERE trial_id=? AND external_id=?", (trial_id, external_id)).fetchone()
                        if row and row["site_id"] == actor["site_id"]:
                            return self._blinded_participant(retry, row, actor, False, True)
                raise BusinessError("并发入组冲突，请重新提交", 409, "enrollment_conflict")
            except Exception:
                conn.rollback()
                raise

    def _blinded_participant(self, conn, participant, viewer, allow_arm=False, idempotent=False):
        result = {
            "id": participant["id"], "trial_id": participant["trial_id"],
            "external_id": participant["external_id"], "site_id": participant["site_id"],
            "allocation_code": participant["allocation_code"], "status": participant["status"],
            "created_at": participant["created_at"], "idempotent": idempotent,
        }
        if allow_arm:
            result["arm"] = conn.execute("SELECT arm FROM allocations WHERE id=?", (participant["allocation_id"],)).fetchone()["arm"]
        return result

    def list_participants(self, user_id, trial_id):
        with self.connect() as conn:
            actor = self._user(conn, user_id, {"site", "coordinator", "monitor"})
            self._trial(conn, trial_id)
            if actor["role"] == "site":
                rows = conn.execute("SELECT * FROM participants WHERE trial_id=? AND site_id=? ORDER BY id", (trial_id, actor["site_id"])).fetchall()
            else:
                rows = conn.execute("SELECT * FROM participants WHERE trial_id=? ORDER BY id", (trial_id,)).fetchall()
            return [self._blinded_participant(conn, row, actor) for row in rows]

    def get_participant(self, user_id, participant_id):
        with self.connect() as conn:
            actor = self._user(conn, user_id, {"site", "coordinator", "monitor"})
            row = conn.execute("SELECT * FROM participants WHERE id=?", (participant_id,)).fetchone()
            if not row:
                raise BusinessError("受试者不存在", 404, "not_found")
            if actor["role"] == "site" and row["site_id"] != actor["site_id"]:
                raise BusinessError("只能查看本中心受试者", 403, "site_isolation")
            approved = conn.execute(
                "SELECT 1 FROM unblinding_requests WHERE participant_id=? AND status='approved'", (participant_id,)
            ).fetchone() is not None
            return self._blinded_participant(conn, row, actor, allow_arm=approved)

    def request_unblinding(self, user_id, participant_id, reason):
        if len(reason.strip()) < 8:
            raise BusinessError("揭盲原因至少 8 字", 422, "reason_required")
        with self.connect() as conn:
            actor = self._user(conn, user_id, {"site", "coordinator"})
            participant = conn.execute("SELECT * FROM participants WHERE id=?", (participant_id,)).fetchone()
            if not participant:
                raise BusinessError("受试者不存在", 404, "not_found")
            if actor["role"] == "site" and participant["site_id"] != actor["site_id"]:
                raise BusinessError("不能申请其他中心的揭盲", 403, "site_isolation")
            open_request = conn.execute(
                "SELECT id FROM unblinding_requests WHERE participant_id=? AND status='pending'", (participant_id,)
            ).fetchone()
            if open_request:
                raise BusinessError("该受试者已有待审批的揭盲申请", 409, "request_exists")
            cur = conn.execute(
                "INSERT INTO unblinding_requests(participant_id,requester_id,reason,created_at) VALUES(?,?,?,?)",
                (participant_id, user_id, reason.strip(), now()),
            )
            self._audit(conn, participant["trial_id"], user_id, "unblinding.request", {"request_id": cur.lastrowid, "participant_id": participant_id})
            return {"id": cur.lastrowid, "status": "pending"}

    def approve_unblinding(self, user_id, request_id):
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                approver = self._user(conn, user_id, {"monitor", "coordinator"})
                request = conn.execute("SELECT * FROM unblinding_requests WHERE id=?", (request_id,)).fetchone()
                if not request:
                    raise BusinessError("揭盲申请不存在", 404, "not_found")
                if request["status"] != "pending":
                    raise BusinessError("揭盲申请已经完成", 409, "already_decided")
                if request["first_approver"] is None:
                    conn.execute("UPDATE unblinding_requests SET first_approver=? WHERE id=?", (user_id, request_id))
                    participant = conn.execute("SELECT * FROM participants WHERE id=?", (request["participant_id"],)).fetchone()
                    self._audit(conn, participant["trial_id"], user_id, "unblinding.approve.first", {"request_id": request_id})
                    return {"id": request_id, "status": "pending", "first_approver": user_id, "second_approval_required": True}
                if request["first_approver"] == user_id:
                    raise BusinessError("两次揭盲审批必须由不同人员完成", 409, "distinct_approver_required")
                conn.execute(
                    "UPDATE unblinding_requests SET second_approver=?,status='approved',decided_at=? WHERE id=?",
                    (user_id, now(), request_id),
                )
                participant = conn.execute("SELECT * FROM participants WHERE id=?", (request["participant_id"],)).fetchone()
                arm = conn.execute("SELECT arm FROM allocations WHERE id=?", (participant["allocation_id"],)).fetchone()["arm"]
                self._audit(conn, participant["trial_id"], user_id, "unblinding.approve.second", {"request_id": request_id, "participant_id": participant["id"]})
                return {"id": request_id, "status": "approved", "first_approver": request["first_approver"], "second_approver": user_id, "arm": arm}
            except Exception:
                conn.rollback()
                raise

    def trial_summary(self, user_id, trial_id):
        with self.connect() as conn:
            actor = self._user(conn, user_id, {"site", "coordinator", "monitor"})
            trial = self._trial(conn, trial_id)
            where, params = "", [trial_id]
            if actor["role"] == "site":
                where, params = " AND site_id=?", [trial_id, actor["site_id"]]
            total = conn.execute(f"SELECT COUNT(*) FROM participants WHERE trial_id=?" + where, params).fetchone()[0]
            by_site = conn.execute(
                f"SELECT site_id,COUNT(*) AS count FROM participants WHERE trial_id=?" + where + " GROUP BY site_id", params
            ).fetchall()
            audit = conn.execute("SELECT * FROM audit_log WHERE trial_id=? ORDER BY id", (trial_id,)).fetchall()
            return {
                "trial": {"id": trial["id"], "name": trial["name"], "protocol_version": trial["protocol_version"], "status": trial["status"]},
                "participants_visible": total, "by_site": [dict(x) for x in by_site],
                "audit": [dict(x) | {"detail": json.loads(x["detail"])} for x in audit],
            }

    # ------------------------------------------------------------------
    # 补号段托管（离线入组）
    # ------------------------------------------------------------------

    @staticmethod
    def _contiguous_run(states):
        """在有序的 state 序列中找出第一段长度足够复用的连续 'released' 起点。"""
        run_start, run_len = None, 0
        for idx, state in enumerate(states):
            if state == "released":
                if run_len == 0:
                    run_start = idx
                run_len += 1
            else:
                run_start, run_len = None, 0
        return run_start, run_len

    def list_sites(self, user_id):
        with self.connect() as conn:
            self._user(conn, user_id)
            rows = conn.execute("SELECT * FROM sites ORDER BY id").fetchall()
            return [{"id": r["id"], "name": r["name"], "active": bool(r["active"])} for r in rows]

    def reserve_segment(self, user_id, trial_id, site_id, factors, count):
        site_id = str(site_id).strip()
        if isinstance(count, bool) or not isinstance(count, int) or not 1 <= count <= 200:
            raise BusinessError("号段长度必须是 1-200 的整数", 422, "invalid_count")
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._user(conn, user_id, {"coordinator"})
                trial = self._trial(conn, trial_id)
                if trial["status"] != "running":
                    raise BusinessError("只有进行中的试验可以托管号段", 409, "trial_not_running")
                site = conn.execute("SELECT * FROM sites WHERE id=?", (site_id,)).fetchone()
                if not site or not site["active"]:
                    raise BusinessError("中心不存在或已停用", 422, "invalid_site")
                normalized = self._normalize_factors(trial, factors)
                stratum_key = self._factors_key(normalized)
                start = None
                reused = 0
                existing = conn.execute(
                    "SELECT sequence,state FROM segment_numbers WHERE trial_id=? AND stratum_key=? ORDER BY sequence",
                    (trial_id, stratum_key),
                ).fetchall()
                if existing:
                    run_idx, run_len = self._contiguous_run([r["state"] for r in existing])
                    if run_len >= count:
                        start = existing[run_idx]["sequence"]
                        reused = count
                if start is None:
                    row = conn.execute(
                        "SELECT next_seq FROM segment_highwater WHERE trial_id=? AND stratum_key=?",
                        (trial_id, stratum_key),
                    ).fetchone()
                    start = row["next_seq"] if row else 1
                end = start + count - 1
                cur = conn.execute(
                    "INSERT INTO hosted_segments(trial_id,site_id,stratum_key,factors_json,seq_start,seq_end,created_by,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (trial_id, site_id, stratum_key, json.dumps(normalized, ensure_ascii=False, sort_keys=True), start, end, user_id, now()),
                )
                segment_id = cur.lastrowid
                ts = now()
                if reused:
                    conn.executemany(
                        """UPDATE segment_numbers SET state='reserved',segment_id=?,site_id=?,participant_id=NULL,updated_at=?
                           WHERE trial_id=? AND stratum_key=? AND sequence=?""",
                        [(segment_id, site_id, ts, trial_id, stratum_key, s) for s in range(start, end + 1)],
                    )
                else:
                    rows = [(trial_id, stratum_key, start + i, self._hosted_arm(trial, stratum_key, start + i),
                             segment_id, site_id, "reserved", ts) for i in range(count)]
                    conn.executemany(
                        "INSERT INTO segment_numbers(trial_id,stratum_key,sequence,arm,segment_id,site_id,state,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                        rows,
                    )
                high = conn.execute(
                    "SELECT next_seq FROM segment_highwater WHERE trial_id=? AND stratum_key=?",
                    (trial_id, stratum_key),
                ).fetchone()
                if high and end >= high["next_seq"]:
                    conn.execute(
                        "UPDATE segment_highwater SET next_seq=? WHERE trial_id=? AND stratum_key=?",
                        (end + 1, trial_id, stratum_key),
                    )
                elif not high:
                    conn.execute(
                        "INSERT INTO segment_highwater(trial_id,stratum_key,next_seq) VALUES(?,?,?)",
                        (trial_id, stratum_key, end + 1),
                    )
                self._audit(conn, trial_id, user_id, "segment.reserve", {
                    "segment_id": segment_id, "site_id": site_id, "stratum_key": stratum_key,
                    "seq_start": start, "seq_end": end, "count": count, "reused": reused,
                })
                conn.commit()
                return self._segment_view(conn, segment_id)
            except Exception:
                conn.rollback()
                raise

    def list_segments(self, user_id, trial_id, site_id=None):
        with self.connect() as conn:
            actor = self._user(conn, user_id, {"site", "coordinator", "monitor"})
            self._trial(conn, trial_id)
            sql, params = "SELECT * FROM hosted_segments WHERE trial_id=?", [trial_id]
            if site_id:
                sql += " AND site_id=?"; params.append(site_id)
            if actor["role"] == "site":
                sql += " AND site_id=?"; params.append(actor["site_id"])
            sql += " ORDER BY id"
            return [self._segment_view(conn, r["id"]) for r in conn.execute(sql, params).fetchall()]

    def _segment_view(self, conn, segment_id):
        seg = conn.execute("SELECT * FROM hosted_segments WHERE id=?", (segment_id,)).fetchone()
        if not seg:
            raise BusinessError("号段不存在", 404, "not_found")
        stats = conn.execute(
            "SELECT state,COUNT(*) AS c FROM segment_numbers WHERE segment_id=? GROUP BY state",
            (segment_id,),
        ).fetchall()
        counts = {r["state"]: r["c"] for r in stats}
        up = conn.execute(
            "SELECT status,COUNT(*) AS c FROM offline_upload_items WHERE segment_id=? GROUP BY status",
            (segment_id,),
        ).fetchall()
        uploads = {r["status"]: r["c"] for r in up}
        return {
            "id": seg["id"], "trial_id": seg["trial_id"], "site_id": seg["site_id"],
            "stratum_key": seg["stratum_key"], "factors": json.loads(seg["factors_json"]),
            "seq_start": seg["seq_start"], "seq_end": seg["seq_end"], "status": seg["status"],
            "created_by": seg["created_by"], "created_at": seg["created_at"], "released_at": seg["released_at"],
            "counts": {"reserved": counts.get("reserved", 0), "used": counts.get("used", 0), "released": counts.get("released", 0)},
            "uploads": {"pending": uploads.get("pending", 0), "reconciled": uploads.get("reconciled", 0), "failed": uploads.get("failed", 0)},
        }

    def _segment_detail(self, conn, segment_id):
        view = self._segment_view(conn, segment_id)
        numbers = conn.execute(
            """SELECT n.sequence,n.arm,n.state,n.participant_id,u.external_id AS upload_external_id
                 FROM segment_numbers n LEFT JOIN offline_upload_items u
                   ON u.trial_id=n.trial_id AND u.sequence=n.sequence AND u.status='reconciled'
                WHERE n.segment_id=? ORDER BY n.sequence""",
            (segment_id,),
        ).fetchall()
        items = conn.execute(
            "SELECT * FROM offline_upload_items WHERE segment_id=? ORDER BY id", (segment_id,)
        ).fetchall()
        item_by_seq = {i["sequence"]: i for i in items}
        diffs = []
        covered_item_ids = set()
        for row in numbers:
            item = item_by_seq.get(row["sequence"])
            if item:
                covered_item_ids.add(item["id"])
            if row["state"] == "used":
                kind = "reconciled"
            elif row["state"] == "released":
                kind = "released"
            elif item and item["status"] in ("pending", "failed"):
                kind = "unreconciled"
            else:
                kind = "unused"
            diffs.append({"sequence": row["sequence"], "arm": row["arm"], "state": row["state"],
                          "external_id": item["external_id"] if item else None,
                          "upload_status": item["status"] if item else None,
                          "error": item["error"] if item else None, "kind": kind})
        # 落在号段范围外的未核销项（例如上传时序号写错）同样计入差异
        for item in items:
            if item["id"] in covered_item_ids or item["status"] not in ("pending", "failed"):
                continue
            diffs.append({"sequence": item["sequence"], "arm": None, "state": None,
                          "external_id": item["external_id"], "upload_status": item["status"],
                          "error": item["error"], "kind": "unreconciled"})
        view["numbers"] = [dict(r) for r in numbers]
        view["upload_items"] = [{"id": i["id"], "external_id": i["external_id"], "sequence": i["sequence"],
                                 "status": i["status"], "error": i["error"], "attempts": i["attempts"],
                                 "participant_id": i["participant_id"], "updated_at": i["updated_at"]} for i in items]
        view["diff"] = {"reconciled": sum(d["kind"] == "reconciled" for d in diffs),
                        "unreconciled": sum(d["kind"] == "unreconciled" for d in diffs),
                        "unused": sum(d["kind"] == "unused" for d in diffs),
                        "released": sum(d["kind"] == "released" for d in diffs),
                        "items": diffs}
        return view

    def get_segment(self, user_id, segment_id):
        with self.connect() as conn:
            actor = self._user(conn, user_id, {"site", "coordinator", "monitor"})
            seg = conn.execute("SELECT * FROM hosted_segments WHERE id=?", (segment_id,)).fetchone()
            if not seg:
                raise BusinessError("号段不存在", 404, "not_found")
            if actor["role"] == "site" and seg["site_id"] != actor["site_id"]:
                raise BusinessError("只能查看本中心的号段", 403, "site_isolation")
            return self._segment_detail(conn, segment_id)

    def upload_offline(self, user_id, trial_id, segment_id, items):
        if not isinstance(items, list) or not items:
            raise BusinessError("离线清单不能为空", 422, "invalid_items")
        if len(items) > 500:
            raise BusinessError("单次最多上传 500 条", 422, "invalid_items")
        parsed = []
        seen = set()
        for idx, raw in enumerate(items):
            if not isinstance(raw, dict):
                raise BusinessError(f"第 {idx + 1} 条记录格式错误", 422, "invalid_item")
            ext = str(raw.get("external_id", "")).strip()
            seq = raw.get("sequence")
            if not ext or isinstance(seq, bool) or not isinstance(seq, int):
                raise BusinessError(f"第 {idx + 1} 条缺少 external_id 或 sequence", 422, "invalid_item")
            if ext in seen:
                raise BusinessError(f"清单内外部编号重复: {ext}", 422, "duplicate_in_batch")
            seen.add(ext)
            parsed.append((ext, seq))
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                actor = self._user(conn, user_id, {"site"})
                seg = conn.execute("SELECT * FROM hosted_segments WHERE id=? AND trial_id=?", (segment_id, trial_id)).fetchone()
                if not seg:
                    raise BusinessError("号段不存在", 404, "not_found")
                if seg["site_id"] != actor["site_id"]:
                    raise BusinessError("只能核销本中心的号段", 403, "site_isolation")
                if seg["status"] != "active":
                    raise BusinessError("号段已释放，不能继续核销", 409, "segment_released")
                trial = self._trial(conn, trial_id)
                if trial["status"] != "running":
                    raise BusinessError("只有进行中的试验可以核销离线清单", 409, "trial_not_running")
                results, reconciled = [], 0
                for ext, seq in parsed:
                    sp = conn.execute("SAVEPOINT item")
                    try:
                        row = conn.execute(
                            "SELECT * FROM offline_upload_items WHERE trial_id=? AND external_id=?",
                            (trial_id, ext),
                        ).fetchone()
                        if row and row["status"] == "reconciled":
                            if row["segment_id"] != seg["id"]:
                                raise BusinessError("外部编号已被其他中心/号段核销", 409, "external_id_consumed")
                            result = {"external_id": ext, "sequence": row["sequence"], "status": "reconciled",
                                      "error": None, "attempts": row["attempts"], "idempotent": True,
                                      "participant_id": row["participant_id"]}
                            reconciled += 1
                        else:
                            result = self._reconcile_item(conn, trial, seg, actor, ext, seq)
                            if result["status"] == "reconciled":
                                reconciled += 1
                        results.append(result)
                        conn.execute("RELEASE SAVEPOINT item")
                    except Exception as exc:
                        conn.execute("ROLLBACK TO SAVEPOINT item")
                        conn.execute("RELEASE SAVEPOINT item")
                        self._persist_ledger(conn, seg, ext, seq, exc)
                        results.append({"external_id": ext, "sequence": seq, "status": "failed",
                                        "error": getattr(exc, "message", str(exc)),
                                        "code": getattr(exc, "code", "reconcile_failed")})
                self._audit(conn, trial_id, user_id, "offline.upload", {
                    "segment_id": segment_id, "received": len(parsed),
                    "reconciled_now": sum(r["status"] == "reconciled" and not r.get("idempotent") for r in results),
                    "idempotent": sum(1 for r in results if r.get("idempotent")),
                    "failed": sum(r["status"] == "failed" for r in results),
                })
                conn.commit()
                detail = self._segment_detail(conn, segment_id)
                return {"segment_id": segment_id, "results": results,
                        "summary": {"received": len(parsed), "reconciled": reconciled,
                                    "failed": sum(r["status"] == "failed" for r in results)},
                        "segment": detail}
            except Exception:
                conn.rollback()
                raise

    def _persist_ledger(self, conn, seg, ext, seq, exc):
        ts = now()
        msg = getattr(exc, "message", str(exc))
        existing = conn.execute(
            "SELECT id,segment_id FROM offline_upload_items WHERE trial_id=? AND external_id=?",
            (seg["trial_id"], ext),
        ).fetchone()
        if existing:
            # 外部编号已属于其他中心/号段：不迁移台账，只返回冲突
            if existing["segment_id"] != seg["id"]:
                return
            conn.execute(
                "UPDATE offline_upload_items SET status='failed',error=?,attempts=attempts+1,updated_at=?,sequence=? WHERE id=?",
                (msg, ts, seq, existing["id"]),
            )
        else:
            conn.execute(
                """INSERT INTO offline_upload_items(trial_id,segment_id,site_id,external_id,sequence,status,error,attempts,created_at,updated_at)
                   VALUES(?,?,?,?,?,'failed',?,1,?,?)""",
                (seg["trial_id"], seg["id"], seg["site_id"], ext, seq, msg, ts, ts),
            )

    def _reconcile_item(self, conn, trial, seg, actor, external_id, sequence):
        if isinstance(sequence, bool) or not isinstance(sequence, int) or not seg["seq_start"] <= sequence <= seg["seq_end"]:
            raise BusinessError(f"编号 {sequence} 不在号段 {seg['seq_start']}-{seg['seq_end']} 内", 422, "out_of_segment")
        number = conn.execute(
            "SELECT * FROM segment_numbers WHERE trial_id=? AND stratum_key=? AND sequence=?",
            (trial["id"], seg["stratum_key"], sequence),
        ).fetchone()
        if not number or number["segment_id"] != seg["id"]:
            raise BusinessError(f"编号 {sequence} 不属于本号段", 422, "out_of_segment")
        if number["state"] == "released":
            raise BusinessError(f"编号 {sequence} 已释放，不能核销", 409, "number_released")
        if number["state"] == "used":
            raise BusinessError(f"编号 {sequence} 已被其他受试者核销", 409, "number_used")
        participant = conn.execute(
            "SELECT * FROM participants WHERE trial_id=? AND external_id=?",
            (trial["id"], external_id),
        ).fetchone()
        if participant:
            raise BusinessError("外部编号已核销过", 409, "external_id_consumed")
        factors = json.loads(seg["factors_json"])
        stratum = self._stratum(conn, trial, factors, seg["site_id"])
        ts = now()
        allocation_code = hashlib.sha256(f"{trial['id']}:{external_id}".encode()).hexdigest()[:12].upper()
        cur = conn.execute(
            """INSERT INTO allocations(trial_id,stratum_id,sequence,block_no,arm) VALUES(?,?,?,?,?)""",
            (trial["id"], stratum["id"], OFFLINE_SEQ_OFFSET + sequence, 0, number["arm"]),
        )
        allocation_id = cur.lastrowid
        cur = conn.execute(
            """INSERT INTO participants(trial_id,site_id,external_id,stratum_id,allocation_id,allocation_code,enrolled_by,created_at)
               VALUES(?,?,?,?,?,?,?,?)""",
            (trial["id"], seg["site_id"], external_id, stratum["id"], allocation_id, allocation_code, actor["id"], ts),
        )
        participant_id = cur.lastrowid
        conn.execute("UPDATE allocations SET used_by=?,used_at=? WHERE id=?", (participant_id, ts, allocation_id))
        conn.execute(
            "UPDATE segment_numbers SET state='used',participant_id=?,updated_at=? WHERE id=?",
            (participant_id, ts, number["id"]),
        )
        conn.execute(
            """INSERT INTO offline_upload_items(trial_id,segment_id,site_id,external_id,sequence,status,attempts,participant_id,created_at,updated_at,reconciled_at)
               VALUES(?,?,?,?,?,'reconciled',1,?,?,?,?)
               ON CONFLICT(trial_id,external_id) DO UPDATE SET
                 segment_id=excluded.segment_id, site_id=excluded.site_id, sequence=excluded.sequence,
                 status='reconciled', participant_id=excluded.participant_id, error=NULL,
                 attempts=attempts+1, updated_at=excluded.updated_at, reconciled_at=excluded.reconciled_at""",
            (trial["id"], seg["id"], seg["site_id"], external_id, sequence, participant_id, ts, ts, ts),
        )
        self._audit(conn, trial["id"], actor["id"], "offline.reconcile", {
            "segment_id": seg["id"], "participant_id": participant_id, "external_id": external_id,
            "sequence": sequence, "site_id": seg["site_id"],
        })
        return {"external_id": external_id, "sequence": sequence, "status": "reconciled",
                "participant_id": participant_id, "allocation_code": allocation_code, "attempts": 1}

    def retry_segment(self, user_id, segment_id):
        """未核销清单（pending/failed）逐条重试；已核销编号不会回收或重复处理。"""
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                actor = self._user(conn, user_id, {"site", "coordinator"})
                seg = conn.execute("SELECT * FROM hosted_segments WHERE id=?", (segment_id,)).fetchone()
                if not seg:
                    raise BusinessError("号段不存在", 404, "not_found")
                if actor["role"] == "site" and seg["site_id"] != actor["site_id"]:
                    raise BusinessError("只能重试本中心的号段", 403, "site_isolation")
                trial = self._trial(conn, seg["trial_id"])
                rows = conn.execute(
                    "SELECT * FROM offline_upload_items WHERE segment_id=? AND status IN ('pending','failed') ORDER BY id",
                    (segment_id,),
                ).fetchall()
                results = []
                for row in rows:
                    try:
                        conn.execute("SAVEPOINT item")
                        result = self._reconcile_item(conn, trial, seg, actor, row["external_id"], row["sequence"])
                        conn.execute("RELEASE SAVEPOINT item")
                        results.append(result)
                    except Exception as exc:
                        conn.execute("ROLLBACK TO SAVEPOINT item")
                        conn.execute("RELEASE SAVEPOINT item")
                        conn.execute(
                            "UPDATE offline_upload_items SET status='failed',error=?,attempts=attempts+1,updated_at=? WHERE id=?",
                            (getattr(exc, "message", str(exc)), now(), row["id"]),
                        )
                        results.append({"external_id": row["external_id"], "sequence": row["sequence"],
                                        "status": "failed", "error": getattr(exc, "message", str(exc)),
                                        "code": getattr(exc, "code", "reconcile_failed")})
                self._audit(conn, seg["trial_id"], user_id, "offline.retry", {
                    "segment_id": segment_id, "retried": len(rows),
                    "succeeded": sum(r["status"] == "reconciled" for r in results),
                    "failed": sum(r["status"] == "failed" for r in results),
                })
                conn.commit()
                return {"segment_id": segment_id, "results": results,
                        "summary": {"retried": len(rows),
                                    "succeeded": sum(r["status"] == "reconciled" for r in results),
                                    "failed": sum(r["status"] == "failed" for r in results)},
                        "segment": self._segment_detail(conn, segment_id)}
            except Exception:
                conn.rollback()
                raise

    def release_segment(self, user_id, segment_id):
        """确认释放号段中未使用的名额；已核销编号保留不回收。"""
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                actor = self._user(conn, user_id, {"coordinator"})
                seg = conn.execute("SELECT * FROM hosted_segments WHERE id=?", (segment_id,)).fetchone()
                if not seg:
                    raise BusinessError("号段不存在", 404, "not_found")
                if seg["status"] != "active":
                    raise BusinessError("号段已释放", 409, "segment_released")
                pending = conn.execute(
                    "SELECT COUNT(*) FROM offline_upload_items WHERE segment_id=? AND status IN ('pending','failed')",
                    (segment_id,),
                ).fetchone()[0]
                if pending:
                    raise BusinessError("存在未核销清单，请先重试或处理后再释放", 409, "pending_items")
                ts = now()
                cur = conn.execute(
                    "UPDATE segment_numbers SET state='released',updated_at=? WHERE segment_id=? AND state='reserved'",
                    (ts, segment_id),
                )
                released = cur.rowcount
                conn.execute("UPDATE hosted_segments SET status='released',released_at=? WHERE id=?", (ts, segment_id))
                self._audit(conn, seg["trial_id"], user_id, "segment.release", {
                    "segment_id": segment_id, "site_id": seg["site_id"], "released_numbers": released,
                    "seq_start": seg["seq_start"], "seq_end": seg["seq_end"],
                })
                conn.commit()
                return self._segment_view(conn, segment_id)
            except Exception:
                conn.rollback()
                raise

    def set_site_active(self, user_id, site_id, active):
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._user(conn, user_id, {"coordinator"})
                site = conn.execute("SELECT * FROM sites WHERE id=?", (site_id,)).fetchone()
                if not site:
                    raise BusinessError("中心不存在", 404, "not_found")
                conn.execute("UPDATE sites SET active=? WHERE id=?", (1 if active else 0, site_id))
                conn.execute("UPDATE users SET active=? WHERE site_id=? AND role='site'", (1 if active else 0, site_id))
                released_segments, kept = [], []
                if not active:
                    segments = conn.execute(
                        "SELECT * FROM hosted_segments WHERE site_id=? AND status='active'", (site_id,)
                    ).fetchall()
                    for seg in segments:
                        pending = conn.execute(
                            "SELECT COUNT(*) FROM offline_upload_items WHERE segment_id=? AND status IN ('pending','failed')",
                            (seg["id"],),
                        ).fetchone()[0]
                        if pending:
                            kept.append(seg["id"])
                            continue
                        ts = now()
                        cur = conn.execute(
                            "UPDATE segment_numbers SET state='released',updated_at=? WHERE segment_id=? AND state='reserved'",
                            (ts, seg["id"]),
                        )
                        conn.execute("UPDATE hosted_segments SET status='released',released_at=? WHERE id=?", (ts, seg["id"]))
                        released_segments.append({"segment_id": seg["id"], "released_numbers": cur.rowcount})
                self._audit(conn, None, user_id, "site.set_active", {
                    "site_id": site_id, "active": active,
                    "released_segments": released_segments, "segments_with_pending": kept,
                })
                conn.commit()
                return {"site_id": site_id, "active": active,
                        "released_segments": released_segments, "segments_with_pending": kept}
            except Exception:
                conn.rollback()
                raise


class Handler(BaseHTTPRequestHandler):
    server_version = "Randomization/1.0"
    def _store(self): return self.server.store  # type: ignore[attr-defined]
    def _body(self):
        length = int(self.headers.get("Content-Length", "0"))
        try: data = json.loads(self.rfile.read(length) or b"{}")
        except (json.JSONDecodeError, UnicodeDecodeError): raise BusinessError("请求体必须是合法 JSON", 400, "invalid_json")
        if not isinstance(data, dict): raise BusinessError("JSON 顶层必须是对象", 422, "invalid_json")
        return data
    def _send(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(status); self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
    def _dispatch(self, method):
        path = urlparse(self.path).path.rstrip("/") or "/"; parts = [p for p in path.split("/") if p]
        user = self.headers.get("X-User-Id", ""); store = self._store()
        if method == "GET" and path == "/":
            body = (BASE_DIR / "web" / "index.html").read_bytes(); self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(body)))
            self.end_headers(); self.wfile.write(body); return
        if method == "GET" and path == "/health": return self._send(200, {"ok": True})
        if parts == ["api", "trials"] and method == "POST":
            d=self._body(); return self._send(201, store.create_trial(user,d.get("name",""),d.get("protocol_version",""),d.get("arms"),d.get("strata_factors"),d.get("block_size"),d.get("seed","")))
        if len(parts) >= 3 and parts[:2] == ["api", "trials"]:
            trial_id=int(parts[2])
            if len(parts)==4 and parts[3]=="protocol" and method=="POST":
                d=self._body(); return self._send(200, store.update_protocol(user,trial_id,d.get("protocol_version",""),d.get("arms"),d.get("strata_factors"),d.get("block_size"),d.get("seed")))
            if len(parts)==4 and parts[3]=="start" and method=="POST": return self._send(200, store.start_trial(user,trial_id))
            if len(parts)==4 and parts[3]=="participants" and method=="GET": return self._send(200, {"items": store.list_participants(user,trial_id)})
            if len(parts)==4 and parts[3]=="enroll" and method=="POST":
                d=self._body(); return self._send(201, store.enroll(user,trial_id,d.get("external_id",""),d.get("factors",{})))
            if len(parts)==4 and parts[3]=="summary" and method=="GET": return self._send(200, store.trial_summary(user,trial_id))
            if len(parts)==4 and parts[3]=="segments" and method=="POST":
                d=self._body(); return self._send(201, store.reserve_segment(user,trial_id,d.get("site_id",""),d.get("factors",{}),d.get("count")))
            if len(parts)==4 and parts[3]=="segments" and method=="GET": return self._send(200, {"items": store.list_segments(user,trial_id)})
            if len(parts)==6 and parts[3]=="segments" and parts[5]=="offline-items" and method=="POST":
                d=self._body(); return self._send(200, store.upload_offline(user,trial_id,int(parts[4]),d.get("items",[])))
        if parts == ["api","sites"] and method=="GET": return self._send(200, {"items": store.list_sites(user)})
        if len(parts)==3 and parts[:2]==["api","sites"] and method=="POST":
            d=self._body(); return self._send(200, store.set_site_active(user,parts[2],bool(d.get("active"))))
        if len(parts)==3 and parts[:2]==["api","segments"] and method=="GET": return self._send(200, store.get_segment(user,int(parts[2])))
        if len(parts)==4 and parts[:2]==["api","segments"] and parts[3]=="release" and method=="POST":
            return self._send(200, store.release_segment(user,int(parts[2])))
        if len(parts)==4 and parts[:2]==["api","segments"] and parts[3]=="retry" and method=="POST":
            return self._send(200, store.retry_segment(user,int(parts[2])))
        if len(parts)==3 and parts[:2]==["api","participants"] and method=="GET": return self._send(200, store.get_participant(user,int(parts[2])))
        if len(parts)==4 and parts[:2]==["api","participants"] and parts[3]=="unblinding-requests" and method=="POST":
            d=self._body(); return self._send(201, store.request_unblinding(user,int(parts[2]),d.get("reason","")))
        if len(parts)==4 and parts[:2]==["api","unblinding-requests"] and parts[3]=="approve" and method=="POST":
            return self._send(200, store.approve_unblinding(user,int(parts[2])))
        raise BusinessError("接口不存在",404,"not_found")
    def _handle(self, method):
        try: self._dispatch(method)
        except BusinessError as exc: self._send(exc.status,{"error":{"code":exc.code,"message":exc.message}})
        except (ValueError,TypeError): self._send(400,{"error":{"code":"invalid_path","message":"路径参数格式错误"}})
        except Exception as exc: self._send(500,{"error":{"code":"internal_error","message":str(exc)}})
    def do_GET(self): self._handle("GET")
    def do_POST(self): self._handle("POST")
    def log_message(self, fmt, *args): print(f"{self.address_string()} - {fmt % args}")


class RandomizationServer(ThreadingHTTPServer):
    daemon_threads = True
    def __init__(self, address, store): self.store=store; super().__init__(address,Handler)


def main():
    parser=argparse.ArgumentParser(description="临床试验随机分配与盲法服务")
    parser.add_argument("--db",default=str(DEFAULT_DB)); parser.add_argument("--port",type=int,default=8104)
    parser.add_argument("--init",action="store_true"); parser.add_argument("--seed",action="store_true")
    args=parser.parse_args(); store=RandomizationStore(args.db); store.init_schema()
    if args.seed: store.seed()
    if args.init or args.seed: print(f"数据库已初始化: {args.db}"); return
    server=RandomizationServer(("127.0.0.1",args.port),store); print(f"随机化服务运行于 http://127.0.0.1:{args.port}")
    try: server.serve_forever()
    except KeyboardInterrupt: pass
    finally: server.server_close()

if __name__=="__main__": main()
