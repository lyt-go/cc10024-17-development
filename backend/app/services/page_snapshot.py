"""页面快照领域服务：页面DOM快照的采集与存储，消费login_mgr凭证和canvas_config配置。

新增「采集页批量校验入档」：采集端一次提交多页，逐页解码、Schema校验、正文落盘，
并为每一页生成持久化的快照接收回执（data/receipts/<批次>.jsonl）。
回执即补送依据：整批重发按稳定来源身份（capture_id）幂等去重，
损坏页修正后可单独补送，成功与失败计数只取有对应回执的页面。
"""
from __future__ import annotations

import base64
import hashlib
from typing import Any

from app.store import store
from app.services.base import BaseModuleService, register_hook
from app import persistence as persist

# 入档页 Schema 校验目标（规则挂在 schema_check 模块下，由 Schema 校验定位字段）
INTAKE_SCHEMA_TARGET = "page_snapshot_intake"

# 入档页字段类型约束：能读取但字段类型不合规时，由这些规则定位到具体字段
_INTAKE_RULES = [
    {"规则编号": "SNAP-INTAKE-001", "字段名": "capture_id", "类型约束": "str", "空值策略": "禁止"},
    {"规则编号": "SNAP-INTAKE-002", "字段名": "source", "类型约束": "str", "空值策略": "禁止"},
    {"规则编号": "SNAP-INTAKE-003", "字段名": "page_version", "类型约束": "str", "空值策略": "允许"},
    {"规则编号": "SNAP-INTAKE-004", "字段名": "encoding", "类型约束": "str", "空值策略": "允许"},
    {"规则编号": "SNAP-INTAKE-005", "字段名": "body_b64", "类型约束": "str", "空值策略": "禁止"},
]


class IntakeInterrupted(RuntimeError):
    """故障注入：模拟批次结果返回前的中断（已落盘的回执仍然有效）。"""


def _receipt_file(batch_id: str) -> str:
    return f"{batch_id}.jsonl"


def load_receipt_ledger(batch_id: str) -> list[dict[str, Any]]:
    """读取批次回执账本（追加式JSONL，重启后仍在）。"""
    return persist.read_jsonl("receipts", _receipt_file(batch_id))


def fold_receipts(ledger: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """把回执账本按来源身份折叠：同一capture_id只保留最新一份回执。

    重发同样的页会在账本里追加新回执，但折叠后每个身份仍只有一条当前状态，
    因此成功/失败计数不会因为重发而增加。
    """
    folded: dict[str, dict[str, Any]] = {}
    for record in ledger:
        folded[str(record.get("capture_id"))] = record
    return folded


def load_receipts(batch_id: str) -> dict[str, dict[str, Any]]:
    """读取并折叠批次既有回执：{capture_id: 当前回执}。重启后消费用。"""
    return fold_receipts(load_receipt_ledger(batch_id))


def receipt_counts(batch_id: str) -> dict[str, int]:
    """成功/失败计数：只取有对应回执的页面（折叠后的身份数）。"""
    folded = load_receipts(batch_id)
    return {
        "stored": sum(1 for r in folded.values() if r.get("status") == "stored"),
        "failed": sum(1 for r in folded.values() if r.get("status") == "failed"),
        "known": len(folded),
    }


def _body_digest(body_text: str) -> str:
    return hashlib.sha256(body_text.encode("utf-8")).hexdigest()


def archive_filename(capture_id: str) -> str:
    return f"archive_{capture_id}.json"


def verify_archived_body(capture_id: str, expected_digest: str | None = None) -> bool:
    """核对归档正文与摘要：正文重新哈希后必须和归档摘要（及回执摘要）对上。"""
    archived = persist.read_json("snapshots", archive_filename(capture_id))
    if not archived:
        return False
    digest = _body_digest(str(archived.get("body", "")))
    if archived.get("body_digest") != digest:
        return False
    if expected_digest and digest != expected_digest:
        return False
    return True


class PageSnapshotService(BaseModuleService):
    MODULE = "page_snapshot"
    ENTITY = "页面快照"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["快照编号", "采集来源", "页面版本", "DOM摘要", "快照时间", "快照类型", "数据量", "回收时间"]
    REQUIRED_FIELDS = ["快照编号", "采集来源", "页面版本"]
    STATUS_ORDER = ["草稿", "已存档", "已清洗", "已回收"]
    ACTION_RULES = {"标记存档": "已存档", "确认清洗": "已清洗", "启动回收": "已回收"}
    NEGATIVE_ACTIONS = []

    def capture_snapshot(self, page_url: str, dom_hash: str, raw_dom: str) -> dict[str, Any]:
        """采集页面快照并存入snapshots/目录。"""
        snapshot = {
            "page_url": page_url,
            "dom_hash": dom_hash,
            "raw_dom_length": len(raw_dom),
            "timestamp": persist.now_iso(),
            "dom_preview": raw_dom[:500],
        }
        persist.write_json("snapshots", f"page_{dom_hash}.json", snapshot)
        return snapshot

    # ------------------------------------------------------------------
    # 采集页批量校验入档 + 快照接收回执
    # ------------------------------------------------------------------

    def _ensure_intake_rules(self) -> None:
        """确保入档页的Schema校验规则已生效（规则由schema_check模块持有）。"""
        from app.services.schema_check import SchemaCheckService

        existing = {r.get("规则编号") for r in store.rows("schema_check")}
        svc = SchemaCheckService()
        for spec in _INTAKE_RULES:
            if spec["规则编号"] in existing:
                continue
            entry, _ = svc.create_entry({
                "规则编号": spec["规则编号"],
                "目标数据": INTAKE_SCHEMA_TARGET,
                "字段名": spec["字段名"],
                "类型约束": spec["类型约束"],
                "格式约束": "",
                "空值策略": spec["空值策略"],
                "异常归类": "入档校验",
                "规则版本": "v1",
            })
            if entry:
                svc.run_action(entry["id"], "提交校验")
                svc.run_action(entry["id"], "标记生效")

    def ingest_batch(
        self,
        batch_id: str,
        pages: list[dict[str, Any]],
        crash_after: int | None = None,
    ) -> dict[str, Any]:
        """批量校验入档：一次提交多页，逐页处理并逐页落回执。

        每页流程：解码正文 → Schema校验字段类型 → 正文落盘（按capture_id幂等）→ 追加回执。
        回执逐页持久化，批次结果返回前中断也不会丢失已落盘回执；
        采集端可凭稳定来源身份整批重发或单独补送，计数不重复。

        crash_after：测试用故障注入，在第N页回执落盘后抛出 IntakeInterrupted。
        """
        self._ensure_intake_rules()
        ledger = load_receipt_ledger(batch_id)
        folded = fold_receipts(ledger)
        next_seq = max((int(r.get("seq", 0)) for r in ledger), default=0)

        receipts: list[dict[str, Any]] = []
        for page in pages:
            receipt = self._ingest_one(batch_id, page, ledger, folded, next_seq)
            next_seq = max(next_seq, int(receipt.get("seq", 0)))
            persist.append_jsonl("receipts", _receipt_file(batch_id), receipt)
            ledger.append(receipt)
            folded[str(receipt.get("capture_id"))] = receipt
            receipts.append(receipt)
            if crash_after is not None and len(receipts) >= crash_after:
                raise IntakeInterrupted(
                    f"批次{batch_id}在第{len(receipts)}页落盘后中断，"
                    f"已落盘{len(ledger)}份回执，批次结果未返回"
                )

        counts = receipt_counts(batch_id)
        return {
            "batch_id": batch_id,
            "submitted": len(pages),
            "receipts": receipts,
            "stored_count": counts["stored"],
            "failed_count": counts["failed"],
            "known_pages": counts["known"],
        }

    def _ingest_one(
        self,
        batch_id: str,
        page: dict[str, Any],
        ledger: list[dict[str, Any]],
        folded: dict[str, dict[str, Any]],
        next_seq: int,
    ) -> dict[str, Any]:
        """处理单页：返回待落盘的回执。失败回执同样带原来源身份。"""
        raw_id = page.get("capture_id")
        capture_id = str(raw_id) if raw_id is not None else f"{batch_id}#no-id"
        prior = folded.get(capture_id)
        attempt = sum(1 for r in ledger if str(r.get("capture_id")) == capture_id) + 1
        # 批次内稳定页序：重发沿用首次投递的页序，新页往后排
        seq = int(prior.get("seq", 0)) if prior is not None else next_seq + 1

        receipt: dict[str, Any] = {
            "receipt_id": f"RCV-{batch_id}-{len(ledger) + 1:04d}",
            "batch_id": batch_id,
            "capture_id": capture_id,
            "seq": seq,
            "attempt": attempt,
            "status": "failed",
            "error_class": None,
            "diagnostic": "",
            "fields": [],
            "body_digest": None,
            "body_file": None,
            "note": "",
            "received_at": persist.now_iso(),
        }

        # 1) 解码正文：整页无法解码时记录正文的解码问题
        body_text, decode_error = self._decode_body(page)
        if decode_error:
            receipt["error_class"] = "body_decode"
            receipt["diagnostic"] = decode_error
            return receipt

        # 2) Schema校验：能读取但字段类型不合规时，保留Schema校验定位的字段
        from app.services.schema_check import SchemaCheckService

        violations = SchemaCheckService().validate_fields(page, INTAKE_SCHEMA_TARGET)
        if violations:
            receipt["error_class"] = "schema_violation"
            receipt["diagnostic"] = "；".join(v["message"] for v in violations)
            receipt["fields"] = [
                {"field": v["field"], "expected": v["expected"],
                 "actual": v["actual"], "rule": v["rule"]}
                for v in violations
            ]
            return receipt

        # 3) 正文落盘：同一捕获身份只保留一份正式正文，重发不重复写
        assert body_text is not None
        digest = _body_digest(body_text)
        filename = archive_filename(capture_id)
        archived = persist.read_json("snapshots", filename)
        if archived and archived.get("body_digest") == digest:
            receipt["note"] = "duplicate_resend"  # 重发同样的页：正文一致，不重复落盘
        elif archived:
            # 同身份不同正文：保留首份正式正文，回执摘要对齐已归档正文
            receipt["note"] = "identity_conflict_kept_first"
            digest = str(archived.get("body_digest"))
        else:
            persist.write_json("snapshots", filename, {
                "capture_id": capture_id,
                "batch_id": batch_id,
                "source": page.get("source"),
                "page_version": page.get("page_version"),
                "encoding": page.get("encoding") or "utf-8",
                "body": body_text,
                "body_digest": digest,
                "archived_at": persist.now_iso(),
            })
            receipt["note"] = "stored"

        receipt["status"] = "stored"
        receipt["body_digest"] = digest
        receipt["body_file"] = filename
        return receipt

    @staticmethod
    def _decode_body(page: dict[str, Any]) -> tuple[str | None, str | None]:
        """按声明编码解码正文；无法解码时返回正文的解码问题描述。"""
        body_b64 = page.get("body_b64")
        encoding = page.get("encoding") or "utf-8"
        if not isinstance(body_b64, str) or not body_b64:
            return None, (
                f"正文解码失败：body_b64载荷缺失或类型不合规"
                f"（期望str，实际为{type(body_b64).__name__}）"
            )
        try:
            raw = base64.b64decode(body_b64, validate=True)
            return raw.decode(str(encoding), errors="strict"), None
        except (ValueError, LookupError) as exc:
            return None, f"正文解码失败：无法按{encoding}解码（{exc}）"

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        snap_files = persist.list_files("snapshots", ".json")
        base["snapshot_files"] = len([f for f in snap_files if f.startswith("page_")])
        # 批量入档产物：正式正文与回执账本
        base["archived_bodies"] = len([f for f in snap_files if f.startswith("archive_")])
        receipt_files = persist.list_files("receipts", ".jsonl")
        base["receipt_batches"] = len(receipt_files)
        stored = failed = 0
        for name in receipt_files:
            counts = receipt_counts(name[:-len(".jsonl")])
            stored += counts["stored"]
            failed += counts["failed"]
        base["stored_receipts"] = stored
        base["failed_receipts"] = failed
        # 关联的登录凭证
        credentials = store.rows("login_mgr")
        snaps = store.rows(self.MODULE)
        sites = {r.get("采集来源") for r in snaps}
        base["sites_with_credentials"] = sum(1 for c in credentials if c.get("目标站点") in sites)
        return base
