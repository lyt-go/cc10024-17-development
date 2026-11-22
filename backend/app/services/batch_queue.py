"""批次队列领域服务：采集任务批次管理，消费script_schedule调度命令，写入run_history运行记录。

新增「快照入档确认」：把页面快照的接收回执关联到本次确认，
基于磁盘上的回执账本（重启后仍在）生成可回放范围和补采清单，
并把待补数量回写到批次队列条目；补送成功时只更新相应来源页的后续处理结果。
"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.services.base import BaseModuleService, register_hook
from app import persistence as persist
from app.services.page_snapshot import load_receipts, verify_archived_body


def _complete_batch_to_history(entry: dict[str, Any]) -> None:
    """批次确认完成时写入run_history。"""
    run_rows = store.rows("run_history")
    new_id = max((int(r.get("id", 0)) for r in run_rows), default=0) + 1
    record = {
        "id": new_id,
        "记录编号": f"RUN-BATCH-{new_id:04d}",
        "关联计划": entry.get("关联计划", ""),
        "开始时间": persist.now_iso(),
        "结束时间": persist.now_iso(),
        "采集行数": str(entry.get("任务数量", 0)),
        "失败行数": str(entry.get("失败数", 0)),
        "执行日志": f"批次{entry.get('批次编号')}执行完成",
        "异常摘要": "",
        "status": "已完成",
        "pending": False,
        "abnormal": False,
        "version": "",
        "updated_at": persist.now_iso(),
    }
    run_rows.append(record)
    persist.append_jsonl("runs", "run_history.jsonl", {
        "module": "run_history", "entity": "运行记录", "entry_id": new_id,
        "status": "已完成", "version": "", "action": "auto_create_from_batch",
        "timestamp": persist.now_iso(), "abnormal": False,
        "source_module": "batch_queue", "source_id": entry.get("id"),
    })

register_hook("batch_queue", "确认完成", _complete_batch_to_history)


class BatchQueueService(BaseModuleService):
    MODULE = "batch_queue"
    ENTITY = "采集批次"
    OPERATION_TERM = "调度"
    LIST_FIELDS = ["批次编号", "关联计划", "任务数量", "完成数", "失败数", "批次状态", "截止时间", "优先级"]
    REQUIRED_FIELDS = ["批次编号", "关联计划", "任务数量"]
    STATUS_ORDER = ["排队中", "执行中", "已完成", "已超时"]
    ACTION_RULES = {"发起执行": "执行中", "确认完成": "已完成", "标记超时": "已超时"}
    NEGATIVE_ACTIONS = []

    def confirm_intake(
        self,
        batch_id: str,
        expected_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        """快照入档确认：消费该批既有回执，生成可回放范围和补采清单。

        回执账本在磁盘上（data/receipts/<批次>.jsonl），重启后照样消费；
        每次确认把当时折叠后的回执关联至本次确认记录。
        补送成功后再次确认时，只有相应来源页的后续处理结果会被更新，
        其余页保留上一确认的处理结果。
        """
        folded = load_receipts(batch_id)

        # 逐页核对：stored 且归档正文与摘要对得上 → 可回放；否则进补采清单
        replayable: list[dict[str, Any]] = []
        recollect: list[dict[str, Any]] = []
        for capture_id, receipt in sorted(
            folded.items(), key=lambda kv: (int(kv[1].get("seq", 0)), kv[0])
        ):
            digest = receipt.get("body_digest")
            if receipt.get("status") == "stored" and verify_archived_body(capture_id, digest):
                replayable.append({
                    "capture_id": capture_id,
                    "seq": receipt.get("seq"),
                    "body_digest": digest,
                })
            else:
                recollect.append({
                    "capture_id": capture_id,
                    "error_class": receipt.get("error_class") or "body_verify_failed",
                    "diagnostic": receipt.get("diagnostic") or "归档正文缺失或摘要对不上",
                    "fields": receipt.get("fields", []),
                })
        # 批次应收但未收到回执的页 → 同样列入补采清单
        for capture_id in (expected_ids or []):
            if capture_id not in folded:
                recollect.append({
                    "capture_id": capture_id,
                    "error_class": "missing",
                    "diagnostic": "批次未收到该页回执",
                    "fields": [],
                })

        stored_count = sum(1 for r in folded.values() if r.get("status") == "stored")
        failed_count = sum(1 for r in folded.values() if r.get("status") == "failed")

        # 合并上一确认：来源身份的回执状态未变的页保留原处理结果，
        # 只有状态发生变化的页（如补送成功的损坏页）才更新后续处理结果。
        previous = persist.read_json("receipts", f"{batch_id}.confirmation.json") or {}
        previous_results: dict[str, Any] = previous.get("page_results", {})
        page_results: dict[str, Any] = {}
        replayable_ids = {p["capture_id"] for p in replayable}
        for capture_id, receipt in folded.items():
            fingerprint = [
                receipt.get("status"), receipt.get("body_digest"), receipt.get("error_class"),
            ]
            old = previous_results.get(capture_id)
            if old and old.get("fingerprint") == fingerprint:
                page_results[capture_id] = old
                continue
            page_results[capture_id] = {
                "capture_id": capture_id,
                "result": "replayable" if capture_id in replayable_ids else "pending_recollect",
                "digest_verified": capture_id in replayable_ids,
                "diagnostic": "" if capture_id in replayable_ids else receipt.get("diagnostic", ""),
                "receipt_id": receipt.get("receipt_id"),
                "fingerprint": fingerprint,
                "updated_at": persist.now_iso(),
            }

        confirmation = {
            "batch_id": batch_id,
            "confirmed_at": persist.now_iso(),
            "confirmations": int(previous.get("confirmations", 0)) + 1,
            # 快照接收回执关联至本次确认
            "receipt_ids": {cid: r.get("receipt_id") for cid, r in folded.items()},
            "replayable_range": replayable,
            "recollect_list": recollect,
            "page_results": page_results,
            "stored_count": stored_count,
            "failed_count": failed_count,
            "pending_count": len(recollect),
        }
        persist.write_json("receipts", f"{batch_id}.confirmation.json", confirmation)

        self._sync_batch_entry(batch_id, confirmation, expected_ids)
        return confirmation

    def _sync_batch_entry(
        self,
        batch_id: str,
        confirmation: dict[str, Any],
        expected_ids: list[str] | None,
    ) -> None:
        """把确认结果回写到批次队列条目：完成/失败/待补数量与批次状态。"""
        entry = next(
            (r for r in store.rows(self.MODULE) if r.get("批次编号") == batch_id), None,
        )
        if entry is None:
            entry, _ = self.create_entry({
                "批次编号": batch_id,
                "关联计划": "快照入档回执",
                "任务数量": len(expected_ids or confirmation["receipt_ids"]),
            })
        assert entry is not None
        entry["完成数"] = confirmation["stored_count"]
        entry["失败数"] = confirmation["failed_count"]
        entry["待补数"] = confirmation["pending_count"]
        entry["updated_at"] = persist.now_iso()
        # 状态流转：有待补 → 执行中；全部收妥 → 确认完成（触发run_history记录）
        if confirmation["pending_count"] > 0:
            if entry.get("status") != "执行中":
                self.run_action(int(entry["id"]), "发起执行")
        elif entry.get("status") != "已完成":
            self.run_action(int(entry["id"]), "确认完成")

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        batches = store.rows(self.MODULE)
        total_tasks = sum(int(r.get("任务数量", 0)) for r in batches)
        base["total_tasks"] = total_tasks
        # 队列里的待补数量（快照入档确认回写）
        base["pending_recollect"] = sum(
            int(r.get("待补数", 0) or 0) for r in batches
            if str(r.get("待补数", "")).isdigit()
        )
        # 关联的run_history
        runs = persist.read_jsonl("runs", "run_history.jsonl")
        batch_runs = [r for r in runs if "batch" in str(r.get("action", ""))]
        base["batch_runs"] = len(batch_runs)
        # 关联的error_recover
        errors = store.rows("error_recover")
        batch_plans = {r.get("关联计划") for r in batches}
        base["recovery_linked"] = sum(1 for e in errors if e.get("关联计划") in batch_plans)
        return base