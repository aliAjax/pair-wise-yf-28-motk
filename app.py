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
                CREATE TABLE IF NOT EXISTS sites(
                    id TEXT PRIMARY KEY, name TEXT NOT NULL,
                    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
                    created_at TEXT NOT NULL, deactivated_at TEXT
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
                CREATE TABLE IF NOT EXISTS number_segments(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trial_id INTEGER NOT NULL REFERENCES trials(id),
                    site_id TEXT NOT NULL REFERENCES sites(id),
                    stratum_key TEXT NOT NULL,
                    start_no INTEGER NOT NULL, end_no INTEGER NOT NULL,
                    total_count INTEGER NOT NULL,
                    written_off_count INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active','released')),
                    note TEXT,
                    created_by TEXT NOT NULL REFERENCES users(id), created_at TEXT NOT NULL,
                    released_at TEXT, released_by TEXT
                );
                CREATE TABLE IF NOT EXISTS segment_numbers(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trial_id INTEGER NOT NULL REFERENCES trials(id),
                    segment_id INTEGER REFERENCES number_segments(id),
                    seq_no INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'available' CHECK(status IN ('available','reserved','written_off')),
                    external_id TEXT, participant_id INTEGER REFERENCES participants(id),
                    written_off_by TEXT REFERENCES users(id), written_off_at TEXT,
                    UNIQUE(trial_id,seq_no)
                );
                CREATE TABLE IF NOT EXISTS upload_batches(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trial_id INTEGER NOT NULL REFERENCES trials(id),
                    site_id TEXT NOT NULL REFERENCES sites(id),
                    segment_id INTEGER REFERENCES number_segments(id),
                    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','processing','completed','partial','failed')),
                    total_count INTEGER NOT NULL, processed_count INTEGER NOT NULL DEFAULT 0,
                    note TEXT, created_by TEXT NOT NULL REFERENCES users(id),
                    created_at TEXT NOT NULL, updated_at TEXT
                );
                CREATE TABLE IF NOT EXISTS upload_items(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id INTEGER NOT NULL REFERENCES upload_batches(id),
                    external_id TEXT NOT NULL,
                    factors_json TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','written_off','failed')),
                    seq_no INTEGER, participant_id INTEGER REFERENCES participants(id),
                    error TEXT, created_at TEXT NOT NULL, updated_at TEXT
                );
                CREATE TABLE IF NOT EXISTS audit_log(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, trial_id INTEGER REFERENCES trials(id),
                    actor_id TEXT NOT NULL REFERENCES users(id), action TEXT NOT NULL,
                    detail TEXT NOT NULL, created_at TEXT NOT NULL
                );
                """
            )
            self._migrate(conn)

    def _migrate(self, conn):
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(participants)")}
        if "segment_id" not in cols:
            conn.execute("ALTER TABLE participants ADD COLUMN segment_id INTEGER REFERENCES number_segments(id)")
        if "segment_number_id" not in cols:
            conn.execute("ALTER TABLE participants ADD COLUMN segment_number_id INTEGER REFERENCES segment_numbers(id)")
        if "source" not in cols:
            conn.execute("ALTER TABLE participants ADD COLUMN source TEXT NOT NULL DEFAULT 'online' CHECK(source IN ('online','offline'))")

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
                "INSERT OR IGNORE INTO sites(id,name,active,created_at) VALUES(?,?,1,?)",
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
        if user["role"] == "site":
            site = conn.execute("SELECT active FROM sites WHERE id=?", (user["site_id"],)).fetchone()
            if site is not None and site["active"] == 0:
                raise BusinessError("中心已停用", 403, "site_deactivated")
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

    def _stratum(self, conn, trial, factors, site_id):
        expected = json.loads(trial["strata_factors_json"])
        if set(factors) != set(expected):
            raise BusinessError(f"必须提供分层因素: {', '.join(expected)}", 422, "invalid_factors")
        normalized = {k: str(factors[k]).strip() for k in sorted(expected)}
        if any(not v for v in normalized.values()):
            raise BusinessError("分层因素值不能为空", 422, "invalid_factors")
        key = f"{site_id}|" + json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
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
                "trial": {"id": trial["id"], "name": trial["name"], "protocol_version": trial["protocol_version"], "status": trial["status"],
                          "arms": json.loads(trial["arms_json"]), "strata_factors": json.loads(trial["strata_factors_json"])},
                "participants_visible": total, "by_site": [dict(x) for x in by_site],
                "audit": [dict(x) | {"detail": json.loads(x["detail"])} for x in audit],
            }

    # ---- 号段托管 ----

    def _stratum_key(self, conn, trial, factors):
        expected = json.loads(trial["strata_factors_json"])
        if not isinstance(factors, dict) or set(factors) != set(expected):
            raise BusinessError(f"必须提供分层因素: {', '.join(expected)}", 422, "invalid_factors")
        normalized = {k: str(factors[k]).strip() for k in sorted(expected)}
        if any(not v for v in normalized.values()):
            raise BusinessError("分层因素值不能为空", 422, "invalid_factors")
        return json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def _stratum_for_segment(self, conn, trial, stratum_key):
        row = conn.execute("SELECT * FROM strata WHERE trial_id=? AND stratum_key=?", (trial["id"], stratum_key)).fetchone()
        if row:
            return row
        factors = json.loads(stratum_key)
        cur = conn.execute(
            "INSERT INTO strata(trial_id,stratum_key,factors_json,created_at) VALUES(?,?,?,?)",
            (trial["id"], stratum_key, json.dumps(factors, ensure_ascii=False, sort_keys=True), now()),
        )
        return conn.execute("SELECT * FROM strata WHERE id=?", (cur.lastrowid,)).fetchone()

    @staticmethod
    def _find_free_run(blocked, count):
        start = 1
        while True:
            if all((start + i) not in blocked for i in range(count)):
                return start
            start += 1

    def create_segment(self, user_id, trial_id, site_id, factors, count, note=""):
        if isinstance(count, bool) or not isinstance(count, int) or count < 1 or count > 10000:
            raise BusinessError("号段数量必须为 1-10000 的整数", 422, "invalid_count")
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._user(conn, user_id, {"coordinator"})
                trial = self._trial(conn, trial_id)
                if trial["status"] != "running":
                    raise BusinessError("试验尚未开始或已停止", 409, "trial_not_running")
                site = conn.execute("SELECT * FROM sites WHERE id=?", (site_id,)).fetchone()
                if not site or not site["active"]:
                    raise BusinessError("中心不存在或已停用", 404, "site_not_found")
                stratum_key = self._stratum_key(conn, trial, factors)
                existing = conn.execute("SELECT seq_no, status FROM segment_numbers WHERE trial_id=?", (trial_id,)).fetchall()
                blocked = {r["seq_no"] for r in existing if r["status"] in ("reserved", "written_off")}
                start = self._find_free_run(blocked, count)
                end = start + count - 1
                cur = conn.execute(
                    """INSERT INTO number_segments(trial_id,site_id,stratum_key,start_no,end_no,total_count,status,note,created_by,created_at)
                       VALUES(?,?,?,?,?,?,'active',?,?,?)""",
                    (trial_id, site_id, stratum_key, start, end, count, note.strip(), user_id, now()),
                )
                segment_id = cur.lastrowid
                existing_seq = {r["seq_no"] for r in existing}
                for seq_no in range(start, end + 1):
                    if seq_no in existing_seq:
                        conn.execute(
                            "UPDATE segment_numbers SET segment_id=?, status='reserved' WHERE trial_id=? AND seq_no=? AND status='available'",
                            (segment_id, trial_id, seq_no),
                        )
                    else:
                        conn.execute(
                            "INSERT INTO segment_numbers(trial_id,segment_id,seq_no,status) VALUES(?,?,?,'reserved')",
                            (trial_id, segment_id, seq_no),
                        )
                self._audit(conn, trial_id, user_id, "segment.create", {
                    "segment_id": segment_id, "site_id": site_id, "stratum_key": stratum_key,
                    "start_no": start, "end_no": end, "count": count,
                })
                conn.commit()
                return self._segment_with_counts(conn, segment_id)
            except Exception:
                conn.rollback()
                raise

    def _segment_with_counts(self, conn, segment_id):
        seg = conn.execute("SELECT * FROM number_segments WHERE id=?", (segment_id,)).fetchone()
        if not seg:
            raise BusinessError("号段不存在", 404, "not_found")
        counts = conn.execute(
            "SELECT status, COUNT(*) c FROM segment_numbers WHERE segment_id=? GROUP BY status", (segment_id,)
        ).fetchall()
        by_status = {r["status"]: r["c"] for r in counts}
        d = dict(seg)
        d["written_off_count"] = by_status.get("written_off", 0)
        d["unused_count"] = by_status.get("reserved", 0)
        d["released_count"] = by_status.get("available", 0)
        return d

    def list_segments(self, user_id, trial_id):
        with self.connect() as conn:
            actor = self._user(conn, user_id, {"site", "coordinator", "monitor"})
            self._trial(conn, trial_id)
            if actor["role"] == "site":
                rows = conn.execute(
                    "SELECT * FROM number_segments WHERE trial_id=? AND site_id=? ORDER BY id", (trial_id, actor["site_id"])
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM number_segments WHERE trial_id=? ORDER BY id", (trial_id,)).fetchall()
            return [self._segment_with_counts(conn, r["id"]) for r in rows]

    def get_segment(self, user_id, segment_id):
        with self.connect() as conn:
            actor = self._user(conn, user_id, {"site", "coordinator", "monitor"})
            seg = conn.execute("SELECT * FROM number_segments WHERE id=?", (segment_id,)).fetchone()
            if not seg:
                raise BusinessError("号段不存在", 404, "not_found")
            if actor["role"] == "site" and seg["site_id"] != actor["site_id"]:
                raise BusinessError("只能查看本中心号段", 403, "site_isolation")
            numbers = conn.execute(
                "SELECT seq_no,status,external_id,participant_id,written_off_at FROM segment_numbers WHERE segment_id=? ORDER BY seq_no",
                (segment_id,),
            ).fetchall()
            d = self._segment_with_counts(conn, segment_id)
            d["numbers"] = [dict(n) for n in numbers]
            return d

    def _create_offline_participant(self, conn, trial, actor, external_id, factors, number):
        stratum = self._stratum_for_segment(conn, trial, self._stratum_key(conn, trial, factors))
        seq_no = number["seq_no"]
        arms = json.loads(trial["arms_json"])
        arm = arms[(seq_no - 1) % len(arms)]
        allocation_code = f"{seq_no:06d}"
        cur = conn.execute(
            "INSERT INTO allocations(trial_id,stratum_id,sequence,block_no,arm,used_by,used_at) VALUES(?,?,?,?,?,?,?)",
            (trial["id"], stratum["id"], seq_no, 0, arm, None, now()),
        )
        allocation_id = cur.lastrowid
        cur = conn.execute(
            """INSERT INTO participants(trial_id,site_id,external_id,stratum_id,allocation_id,allocation_code,enrolled_by,created_at,source,segment_id,segment_number_id)
               VALUES(?,?,?,?,?,?,?,?,'offline',?,?)""",
            (trial["id"], actor["site_id"], external_id, stratum["id"], allocation_id, allocation_code,
             actor["id"], now(), number["segment_id"], number["id"]),
        )
        participant_id = cur.lastrowid
        conn.execute("UPDATE allocations SET used_by=? WHERE id=?", (participant_id, allocation_id))
        return conn.execute("SELECT * FROM participants WHERE id=?", (participant_id,)).fetchone()

    def write_off_number(self, user_id, segment_id, external_id, factors):
        external_id = str(external_id).strip()
        if not external_id:
            raise BusinessError("外部受试者编号不能为空", 422, "invalid_external_id")
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                actor = self._user(conn, user_id, {"site"})
                seg = conn.execute("SELECT * FROM number_segments WHERE id=?", (segment_id,)).fetchone()
                if not seg:
                    raise BusinessError("号段不存在", 404, "not_found")
                if seg["site_id"] != actor["site_id"]:
                    raise BusinessError("只能核销本中心号段", 403, "site_isolation")
                if seg["status"] != "active":
                    raise BusinessError("号段已释放，不能核销", 409, "segment_released")
                trial = self._trial(conn, seg["trial_id"])
                if trial["status"] != "running":
                    raise BusinessError("试验尚未开始或已停止", 409, "trial_not_running")
                stratum_key = self._stratum_key(conn, trial, factors)
                if stratum_key != seg["stratum_key"]:
                    raise BusinessError("分层因素与号段不匹配", 422, "stratum_mismatch")
                existing = conn.execute(
                    "SELECT * FROM participants WHERE trial_id=? AND external_id=?", (trial["id"], external_id)
                ).fetchone()
                if existing:
                    if existing["site_id"] != actor["site_id"]:
                        raise BusinessError("不能核销其他中心的受试者", 403, "site_isolation")
                    conn.commit()
                    return self._blinded_participant(conn, existing, actor, allow_arm=False, idempotent=True)
                number = conn.execute(
                    "SELECT * FROM segment_numbers WHERE segment_id=? AND status='reserved' ORDER BY seq_no LIMIT 1",
                    (segment_id,),
                ).fetchone()
                if not number:
                    raise BusinessError("号段已无可用名额，请联系协调员补领", 409, "no_number_available")
                participant = self._create_offline_participant(conn, trial, actor, external_id, factors, number)
                conn.execute(
                    "UPDATE segment_numbers SET status='written_off', external_id=?, participant_id=?, written_off_by=?, written_off_at=? WHERE id=?",
                    (external_id, participant["id"], user_id, now(), number["id"]),
                )
                conn.execute("UPDATE number_segments SET written_off_count=written_off_count+1 WHERE id=?", (segment_id,))
                self._audit(conn, trial["id"], user_id, "segment.write_off", {
                    "segment_id": segment_id, "seq_no": number["seq_no"],
                    "external_id": external_id, "participant_id": participant["id"],
                })
                conn.commit()
                return self._blinded_participant(conn, participant, actor, allow_arm=False, idempotent=False)
            except Exception:
                conn.rollback()
                raise

    def release_segment(self, user_id, segment_id, confirm=False):
        if not confirm:
            raise BusinessError("请确认释放未用号段", 422, "confirm_required")
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._user(conn, user_id, {"coordinator"})
                seg = conn.execute("SELECT * FROM number_segments WHERE id=?", (segment_id,)).fetchone()
                if not seg:
                    raise BusinessError("号段不存在", 404, "not_found")
                if seg["status"] != "active":
                    raise BusinessError("号段已释放", 409, "already_released")
                released = conn.execute(
                    "UPDATE segment_numbers SET status='available' WHERE segment_id=? AND status='reserved'",
                    (segment_id,),
                ).rowcount
                conn.execute(
                    "UPDATE number_segments SET status='released', released_at=?, released_by=? WHERE id=?",
                    (now(), user_id, segment_id),
                )
                self._audit(conn, seg["trial_id"], user_id, "segment.release", {
                    "segment_id": segment_id, "released_count": released,
                })
                conn.commit()
                return self._segment_with_counts(conn, segment_id)
            except Exception:
                conn.rollback()
                raise

    def deactivate_site(self, user_id, site_id):
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._user(conn, user_id, {"coordinator"})
                site = conn.execute("SELECT * FROM sites WHERE id=?", (site_id,)).fetchone()
                if not site:
                    raise BusinessError("中心不存在", 404, "not_found")
                if not site["active"]:
                    raise BusinessError("中心已停用", 409, "already_deactivated")
                conn.execute("UPDATE sites SET active=0, deactivated_at=? WHERE id=?", (now(), site_id))
                segments = conn.execute(
                    "SELECT id FROM number_segments WHERE site_id=? AND status='active'", (site_id,)
                ).fetchall()
                released_total = 0
                for s in segments:
                    released_total += conn.execute(
                        "UPDATE segment_numbers SET status='available' WHERE segment_id=? AND status='reserved'",
                        (s["id"],),
                    ).rowcount
                    conn.execute(
                        "UPDATE number_segments SET status='released', released_at=?, released_by=? WHERE id=?",
                        (now(), user_id, s["id"]),
                    )
                self._audit(conn, None, user_id, "site.deactivate", {"site_id": site_id, "released_numbers": released_total})
                conn.commit()
                return {"site_id": site_id, "active": False, "released_numbers": released_total}
            except Exception:
                conn.rollback()
                raise

    def activate_site(self, user_id, site_id):
        with self.connect() as conn:
            self._user(conn, user_id, {"coordinator"})
            site = conn.execute("SELECT * FROM sites WHERE id=?", (site_id,)).fetchone()
            if not site:
                raise BusinessError("中心不存在", 404, "not_found")
            if site["active"]:
                raise BusinessError("中心已启用", 409, "already_active")
            conn.execute("UPDATE sites SET active=1, deactivated_at=NULL WHERE id=?", (site_id,))
            self._audit(conn, None, user_id, "site.activate", {"site_id": site_id})
            return {"site_id": site_id, "active": True}

    def list_sites(self, user_id):
        with self.connect() as conn:
            self._user(conn, user_id, {"coordinator", "monitor"})
            rows = conn.execute("SELECT * FROM sites ORDER BY id").fetchall()
            return [dict(r) for r in rows]

    # ---- 上传批次（离线核销清单）----

    def create_upload_batch(self, user_id, trial_id, items, segment_id=None, note=""):
        if not isinstance(items, list) or not items:
            raise BusinessError("请提供核销清单", 422, "invalid_items")
        cleaned, seen = [], set()
        for it in items:
            if not isinstance(it, dict):
                raise BusinessError("清单项格式错误", 422, "invalid_items")
            ext = str(it.get("external_id", "")).strip()
            if not ext:
                raise BusinessError("外部受试者编号不能为空", 422, "invalid_external_id")
            if ext in seen:
                raise BusinessError(f"清单内外部编号重复: {ext}", 422, "duplicate_external_id")
            seen.add(ext)
            factors = it.get("factors", {})
            if not isinstance(factors, dict):
                raise BusinessError("分层因素格式错误", 422, "invalid_factors")
            cleaned.append({"external_id": ext, "factors": factors})
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                actor = self._user(conn, user_id, {"site"})
                trial = self._trial(conn, trial_id)
                if trial["status"] != "running":
                    raise BusinessError("试验尚未开始或已停止", 409, "trial_not_running")
                if segment_id is not None:
                    seg = conn.execute("SELECT * FROM number_segments WHERE id=?", (segment_id,)).fetchone()
                    if not seg or seg["site_id"] != actor["site_id"] or seg["status"] != "active":
                        raise BusinessError("号段不存在、不属于本中心或已释放", 404, "segment_not_found")
                cur = conn.execute(
                    """INSERT INTO upload_batches(trial_id,site_id,segment_id,status,total_count,note,created_by,created_at,updated_at)
                       VALUES(?,?,?,'pending',?,?,?,?,?)""",
                    (trial_id, actor["site_id"], segment_id, len(cleaned), note.strip(), user_id, now(), now()),
                )
                batch_id = cur.lastrowid
                for it in cleaned:
                    conn.execute(
                        "INSERT INTO upload_items(batch_id,external_id,factors_json,status,created_at,updated_at) VALUES(?,?,?,'pending',?,?)",
                        (batch_id, it["external_id"], json.dumps(it["factors"], ensure_ascii=False, sort_keys=True), now(), now()),
                    )
                self._audit(conn, trial_id, user_id, "upload_batch.create", {
                    "batch_id": batch_id, "total": len(cleaned), "segment_id": segment_id,
                })
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return self.process_upload_batch(user_id, batch_id)

    def _find_segment_for_writeoff(self, conn, trial, site_id, stratum_key, segment_id):
        if segment_id is not None:
            seg = conn.execute("SELECT * FROM number_segments WHERE id=?", (segment_id,)).fetchone()
            if (seg and seg["site_id"] == site_id and seg["status"] == "active"
                    and seg["stratum_key"] == stratum_key):
                has = conn.execute(
                    "SELECT 1 FROM segment_numbers WHERE segment_id=? AND status='reserved' LIMIT 1",
                    (seg["id"],),
                ).fetchone()
                if has:
                    return seg
        seg = conn.execute(
            """SELECT s.* FROM number_segments s
               WHERE s.trial_id=? AND s.site_id=? AND s.stratum_key=? AND s.status='active'
                 AND EXISTS (SELECT 1 FROM segment_numbers n WHERE n.segment_id=s.id AND n.status='reserved')
               ORDER BY s.id LIMIT 1""",
            (trial["id"], site_id, stratum_key),
        ).fetchone()
        if not seg:
            raise BusinessError("无可用号段，请联系协调员预领", 409, "no_segment_available")
        return seg

    def _write_off_for_item(self, conn, trial, actor, batch, item):
        external_id = item["external_id"]
        factors = json.loads(item["factors_json"])
        existing = conn.execute(
            "SELECT * FROM participants WHERE trial_id=? AND external_id=?", (trial["id"], external_id)
        ).fetchone()
        if existing:
            if existing["site_id"] != actor["site_id"]:
                raise BusinessError("外部编号已被其他中心使用", 409, "external_id_conflict")
            return existing
        stratum_key = self._stratum_key(conn, trial, factors)
        seg = self._find_segment_for_writeoff(conn, trial, actor["site_id"], stratum_key, batch["segment_id"])
        number = conn.execute(
            "SELECT * FROM segment_numbers WHERE segment_id=? AND status='reserved' ORDER BY seq_no LIMIT 1",
            (seg["id"],),
        ).fetchone()
        if not number:
            raise BusinessError("号段已无可用名额", 409, "no_number_available")
        participant = self._create_offline_participant(conn, trial, actor, external_id, factors, number)
        conn.execute(
            "UPDATE segment_numbers SET status='written_off', external_id=?, participant_id=?, written_off_by=?, written_off_at=? WHERE id=?",
            (external_id, participant["id"], actor["id"], now(), number["id"]),
        )
        conn.execute("UPDATE number_segments SET written_off_count=written_off_count+1 WHERE id=?", (seg["id"],))
        return participant

    def process_upload_batch(self, user_id, batch_id):
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                actor = self._user(conn, user_id, {"site"})
                batch = conn.execute("SELECT * FROM upload_batches WHERE id=?", (batch_id,)).fetchone()
                if not batch:
                    raise BusinessError("上传批次不存在", 404, "not_found")
                if batch["site_id"] != actor["site_id"]:
                    raise BusinessError("只能处理本中心批次", 403, "site_isolation")
                if batch["status"] == "completed":
                    conn.commit()
                    return self._batch_with_items(conn, batch_id)
                conn.execute("UPDATE upload_batches SET status='processing', updated_at=? WHERE id=?", (now(), batch_id))
                trial = self._trial(conn, batch["trial_id"])
                items = conn.execute(
                    "SELECT * FROM upload_items WHERE batch_id=? AND status IN ('pending','failed') ORDER BY id", (batch_id,)
                ).fetchall()
                for item in items:
                    try:
                        participant = self._write_off_for_item(conn, trial, actor, batch, item)
                        conn.execute(
                            "UPDATE upload_items SET status='written_off', participant_id=?, updated_at=? WHERE id=?",
                            (participant["id"], now(), item["id"]),
                        )
                    except BusinessError as exc:
                        conn.execute(
                            "UPDATE upload_items SET status='failed', error=?, updated_at=? WHERE id=?",
                            (exc.message, now(), item["id"]),
                        )
                total_written = conn.execute(
                    "SELECT COUNT(*) FROM upload_items WHERE batch_id=? AND status='written_off'", (batch_id,)
                ).fetchone()[0]
                total_failed = conn.execute(
                    "SELECT COUNT(*) FROM upload_items WHERE batch_id=? AND status='failed'", (batch_id,)
                ).fetchone()[0]
                conn.execute("UPDATE upload_batches SET processed_count=?, updated_at=? WHERE id=?", (total_written, now(), batch_id))
                if total_failed == 0:
                    conn.execute("UPDATE upload_batches SET status='completed' WHERE id=?", (batch_id,))
                elif total_written == 0:
                    conn.execute("UPDATE upload_batches SET status='failed' WHERE id=?", (batch_id,))
                else:
                    conn.execute("UPDATE upload_batches SET status='partial' WHERE id=?", (batch_id,))
                self._audit(conn, batch["trial_id"], user_id, "upload_batch.process", {
                    "batch_id": batch_id, "written": total_written, "failed": total_failed,
                })
                conn.commit()
                return self._batch_with_items(conn, batch_id)
            except Exception:
                conn.rollback()
                raise

    def _batch_with_items(self, conn, batch_id):
        batch = conn.execute("SELECT * FROM upload_batches WHERE id=?", (batch_id,)).fetchone()
        if not batch:
            raise BusinessError("上传批次不存在", 404, "not_found")
        items = conn.execute(
            "SELECT id,external_id,status,participant_id,error,created_at,updated_at FROM upload_items WHERE batch_id=? ORDER BY id",
            (batch_id,),
        ).fetchall()
        d = dict(batch)
        d["items"] = [dict(it) for it in items]
        return d

    def list_upload_batches(self, user_id, trial_id):
        with self.connect() as conn:
            actor = self._user(conn, user_id, {"site", "coordinator", "monitor"})
            self._trial(conn, trial_id)
            if actor["role"] == "site":
                rows = conn.execute(
                    "SELECT * FROM upload_batches WHERE trial_id=? AND site_id=? ORDER BY id DESC", (trial_id, actor["site_id"])
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM upload_batches WHERE trial_id=? ORDER BY id DESC", (trial_id,)).fetchall()
            return [self._batch_with_items(conn, r["id"]) for r in rows]

    def get_upload_batch(self, user_id, batch_id):
        with self.connect() as conn:
            actor = self._user(conn, user_id, {"site", "coordinator", "monitor"})
            batch = conn.execute("SELECT * FROM upload_batches WHERE id=?", (batch_id,)).fetchone()
            if not batch:
                raise BusinessError("上传批次不存在", 404, "not_found")
            if actor["role"] == "site" and batch["site_id"] != actor["site_id"]:
                raise BusinessError("只能查看本中心批次", 403, "site_isolation")
            return self._batch_with_items(conn, batch_id)

    def resume_pending_batches(self):
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT id,created_by FROM upload_batches WHERE status IN ('pending','partial') ORDER BY id"
            ).fetchall()
        for r in rows:
            try:
                self.process_upload_batch(r["created_by"], r["id"])
            except Exception as exc:
                print(f"恢复批次 {r['id']} 失败: {exc}")

    def list_audit(self, user_id, trial_id):
        with self.connect() as conn:
            actor = self._user(conn, user_id, {"site", "coordinator", "monitor"})
            self._trial(conn, trial_id)
            if actor["role"] == "site":
                rows = conn.execute(
                    """SELECT a.* FROM audit_log a JOIN users u ON u.id=a.actor_id
                       WHERE a.trial_id=? AND u.site_id=? ORDER BY a.id""",
                    (trial_id, actor["site_id"]),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM audit_log WHERE trial_id=? OR trial_id IS NULL ORDER BY id", (trial_id,)
                ).fetchall()
            return [dict(r) | {"detail": json.loads(r["detail"])} for r in rows]


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
        if len(parts)==3 and parts[:2]==["api","participants"] and method=="GET": return self._send(200, store.get_participant(user,int(parts[2])))
        if len(parts)==4 and parts[:2]==["api","participants"] and parts[3]=="unblinding-requests" and method=="POST":
            d=self._body(); return self._send(201, store.request_unblinding(user,int(parts[2]),d.get("reason","")))
        if len(parts)==4 and parts[:2]==["api","unblinding-requests"] and parts[3]=="approve" and method=="POST":
            return self._send(200, store.approve_unblinding(user,int(parts[2])))
        # 号段托管
        if len(parts)==4 and parts[:2]==["api","trials"] and parts[3]=="segments" and method=="POST":
            d=self._body(); return self._send(201, store.create_segment(user,trial_id,d.get("site_id",""),d.get("factors",{}),d.get("count"),d.get("note","")))
        if len(parts)==4 and parts[:2]==["api","trials"] and parts[3]=="segments" and method=="GET":
            return self._send(200, {"items": store.list_segments(user,trial_id)})
        if len(parts)==3 and parts[:2]==["api","segments"] and method=="GET":
            return self._send(200, store.get_segment(user,int(parts[2])))
        if len(parts)==4 and parts[:2]==["api","segments"] and parts[3]=="write-off" and method=="POST":
            d=self._body(); return self._send(201, store.write_off_number(user,int(parts[2]),d.get("external_id",""),d.get("factors",{})))
        if len(parts)==4 and parts[:2]==["api","segments"] and parts[3]=="release" and method=="POST":
            d=self._body(); return self._send(200, store.release_segment(user,int(parts[2]),bool(d.get("confirm"))))
        if len(parts)==4 and parts[:2]==["api","trials"] and parts[3]=="upload-batches" and method=="POST":
            d=self._body(); return self._send(201, store.create_upload_batch(user,trial_id,d.get("items",[]),d.get("segment_id"),d.get("note","")))
        if len(parts)==4 and parts[:2]==["api","trials"] and parts[3]=="upload-batches" and method=="GET":
            return self._send(200, {"items": store.list_upload_batches(user,trial_id)})
        if len(parts)==3 and parts[:2]==["api","upload-batches"] and method=="GET":
            return self._send(200, store.get_upload_batch(user,int(parts[2])))
        if len(parts)==4 and parts[:2]==["api","upload-batches"] and parts[3]=="process" and method=="POST":
            return self._send(200, store.process_upload_batch(user,int(parts[2])))
        if len(parts)==4 and parts[:2]==["api","trials"] and parts[3]=="audit" and method=="GET":
            return self._send(200, {"items": store.list_audit(user,trial_id)})
        if len(parts)==2 and parts==["api","sites"] and method=="GET":
            return self._send(200, {"items": store.list_sites(user)})
        if len(parts)==4 and parts[:2]==["api","sites"] and parts[3]=="deactivate" and method=="POST":
            return self._send(200, store.deactivate_site(user,parts[2]))
        if len(parts)==4 and parts[:2]==["api","sites"] and parts[3]=="activate" and method=="POST":
            return self._send(200, store.activate_site(user,parts[2]))
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
    threading.Thread(target=store.resume_pending_batches, daemon=True).start()
    try: server.serve_forever()
    except KeyboardInterrupt: pass
    finally: server.server_close()

if __name__=="__main__": main()
