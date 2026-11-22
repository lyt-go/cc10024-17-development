"""快照接收回执场景测试：批量校验入档、中断恢复、幂等重发、单独补送、批次确认。

场景（对应需求）：
- 同一批有两份可解析快照和一份编码损坏快照，操作员一次提交多条；
- 第一份已落盘而批次尚未收到结果时发生中断；
- 重启后消费该批既有回执；采集端整批重发（幂等，不增计数）；
- 单独补送修正后的损坏页，只更新相应来源页的后续处理结果；
- 整组确认后逐条可回放、队列里的待补数量；
- 另送一份可解码但来源字段类型错误的页，对照正文解码与字段违规的诊断；
- 每个捕获身份只有一份正式正文，归档正文与摘要对得上。
"""
from __future__ import annotations

import base64
import os
import sys

backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from app import persistence as persist
from app.store import store

BATCH = "TEST-RCV-0001"
CAP1, CAP2, CAP3, CAP4 = "TEST-CAP-01", "TEST-CAP-02", "TEST-CAP-03", "TEST-CAP-04"


def _cleanup() -> None:
    """清理本测试批次的产物（不影响其他模块数据）。"""
    for sub, names in [
        ("receipts", [f"{BATCH}.jsonl", f"{BATCH}.confirmation.json"]),
        ("snapshots", [f"archive_{c}.json" for c in (CAP1, CAP2, CAP3, CAP4)]),
    ]:
        for name in names:
            path = persist.data_path(sub, name)
            if os.path.exists(path):
                os.remove(path)


_cleanup()


def _ok_page(capture_id: str, text: str, source="collector-a") -> dict:
    return {
        "capture_id": capture_id,
        "source": source,
        "page_version": "v1",
        "encoding": "utf-8",
        "body_b64": base64.b64encode(text.encode("utf-8")).decode("ascii"),
    }


def _corrupted_page(capture_id: str) -> dict:
    # 含非法UTF-8字节序列的正文：整页无法解码
    raw = b"<html>\xff\xfebroken</html>"
    return {
        "capture_id": capture_id,
        "source": "collector-a",
        "page_version": "v1",
        "encoding": "utf-8",
        "body_b64": base64.b64encode(raw).decode("ascii"),
    }


PAGE1 = _ok_page(CAP1, "<html>page one</html>")
PAGE2 = _ok_page(CAP2, "<html>page two</html>")
PAGE3_BROKEN = _corrupted_page(CAP3)
PAGE3_FIXED = _ok_page(CAP3, "<html>page three fixed</html>")
# 可解码但来源字段为错误类型（source 期望 str，实际 int）
PAGE4_BAD_SOURCE = _ok_page(CAP4, "<html>page four</html>", source=123)


def _batch_entry() -> dict:
    entry = next(r for r in store.rows("batch_queue") if r.get("批次编号") == BATCH)
    return entry


def test_intake_rules_registered():
    """入档页的Schema校验规则由schema_check模块持有并生效。"""
    from app.services.page_snapshot import PageSnapshotService, INTAKE_SCHEMA_TARGET

    PageSnapshotService()._ensure_intake_rules()
    rules = [
        r for r in store.rows("schema_check")
        if r.get("目标数据") == INTAKE_SCHEMA_TARGET and r.get("status") == "已生效"
    ]
    fields = {r.get("字段名") for r in rules}
    assert {"capture_id", "source", "body_b64"} <= fields, f"入档规则未生效: {fields}"


def test_crash_after_first_page_and_restart_consumes_receipts():
    """一次提交三页，第一份落盘后中断；重启后确认消费既有回执。"""
    from app.services.page_snapshot import (
        IntakeInterrupted, PageSnapshotService, load_receipts,
    )
    from app.services.batch_queue import BatchQueueService

    svc = PageSnapshotService()
    interrupted = False
    try:
        svc.ingest_batch(BATCH, [PAGE1, PAGE2, PAGE3_BROKEN], crash_after=1)
    except IntakeInterrupted:
        interrupted = True
    assert interrupted, "故障注入未生效：应在第一页落盘后中断"

    # 中断时只有第一份回执落盘
    folded = load_receipts(BATCH)
    assert list(folded) == [CAP1], f"中断后应只有第一份回执: {list(folded)}"
    assert folded[CAP1]["status"] == "stored"

    # 模拟重启：全新服务实例，确认只能消费磁盘上的既有回执
    fresh_queue = BatchQueueService()
    conf = fresh_queue.confirm_intake(BATCH, expected_ids=[CAP1, CAP2, CAP3])
    assert [p["capture_id"] for p in conf["replayable_range"]] == [CAP1]
    # 未收到回执的两页进入补采清单
    assert {r["capture_id"] for r in conf["recollect_list"]} == {CAP2, CAP3}
    assert conf["pending_count"] == 2
    # 回执已关联至本次确认
    assert conf["receipt_ids"].get(CAP1, "").startswith(f"RCV-{BATCH}-")
    # 队列里的待补数量
    assert int(_batch_entry()["待补数"]) == 2
    assert _batch_entry()["status"] == "执行中"


def test_resend_whole_batch_is_idempotent():
    """整批重发原三页：稳定来源身份去重，成功/失败计数不增加。"""
    from app.services.page_snapshot import (
        PageSnapshotService, load_receipt_ledger, receipt_counts,
    )
    from app.services.batch_queue import BatchQueueService

    svc = PageSnapshotService()
    summary = svc.ingest_batch(BATCH, [PAGE1, PAGE2, PAGE3_BROKEN])
    assert summary["submitted"] == 3

    receipts = {r["capture_id"]: r for r in summary["receipts"]}
    # 重发同样的页：正文一致，不重复落盘，attempt 递增但计数不增
    assert receipts[CAP1]["note"] == "duplicate_resend"
    assert receipts[CAP1]["attempt"] == 2
    assert receipts[CAP1]["status"] == "stored"
    assert receipts[CAP2]["status"] == "stored"
    # 编码损坏页：记录正文的解码问题，带原来源身份
    assert receipts[CAP3]["status"] == "failed"
    assert receipts[CAP3]["error_class"] == "body_decode"
    assert "解码" in receipts[CAP3]["diagnostic"]
    assert receipts[CAP3]["capture_id"] == CAP3

    # 成功和失败计数只取有对应回执的页面（按身份折叠，重发不增）
    counts = receipt_counts(BATCH)
    assert counts == {"stored": 2, "failed": 1, "known": 3}, counts
    assert summary["stored_count"] == 2 and summary["failed_count"] == 1
    # 账本保留每次投递审计轨迹，但折叠后每身份一条
    assert len(load_receipt_ledger(BATCH)) == 4  # 中断1 + 重发3

    # 整组确认：逐条可回放的页 + 队列待补数量
    conf = BatchQueueService().confirm_intake(BATCH, expected_ids=[CAP1, CAP2, CAP3])
    assert [p["capture_id"] for p in conf["replayable_range"]] == [CAP1, CAP2]
    recollect = {r["capture_id"]: r for r in conf["recollect_list"]}
    assert list(recollect) == [CAP3]
    assert recollect[CAP3]["error_class"] == "body_decode"
    assert conf["pending_count"] == 1
    assert int(_batch_entry()["待补数"]) == 1
    return conf


def test_redeliver_fixed_page_updates_only_that_page():
    """单独补送修正后的损坏页：只更新相应来源页的后续处理结果。"""
    from app.services.page_snapshot import PageSnapshotService, receipt_counts
    from app.services.batch_queue import BatchQueueService

    # 补送前再确认一次，拿到各来源页后续处理结果的基线
    queue = BatchQueueService()
    before = queue.confirm_intake(BATCH, expected_ids=[CAP1, CAP2, CAP3])
    assert [r["capture_id"] for r in before["recollect_list"]] == [CAP3]
    assert before["page_results"][CAP3]["result"] == "pending_recollect"

    # 单独补送修正后的损坏页
    summary = PageSnapshotService().ingest_batch(BATCH, [PAGE3_FIXED])
    assert summary["receipts"][0]["status"] == "stored"
    assert summary["receipts"][0]["capture_id"] == CAP3
    counts = receipt_counts(BATCH)
    assert counts == {"stored": 3, "failed": 0, "known": 3}, counts

    after = queue.confirm_intake(BATCH, expected_ids=[CAP1, CAP2, CAP3])
    assert [p["capture_id"] for p in after["replayable_range"]] == [CAP1, CAP2, CAP3]
    assert after["recollect_list"] == []
    assert after["pending_count"] == 0
    assert int(_batch_entry()["待补数"]) == 0
    assert _batch_entry()["status"] == "已完成"

    # 补送成功只更新相应来源页的后续处理结果，其余页保留上一确认的结果
    for cid in (CAP1, CAP2):
        assert after["page_results"][cid]["updated_at"] == before["page_results"][cid]["updated_at"]
        assert after["page_results"][cid]["receipt_id"] == before["page_results"][cid]["receipt_id"]
    assert after["page_results"][CAP3]["result"] == "replayable"
    assert after["page_results"][CAP3]["fingerprint"] != before["page_results"][CAP3]["fingerprint"]


def test_schema_violation_vs_decode_failure_diagnostics():
    """另送可解码但来源字段类型错误的页：对照正文解码与字段违规的诊断。"""
    from app.services.page_snapshot import PageSnapshotService, receipt_counts
    from app.services.batch_queue import BatchQueueService

    summary = PageSnapshotService().ingest_batch(BATCH, [PAGE4_BAD_SOURCE])
    receipt = summary["receipts"][0]
    assert receipt["status"] == "failed"
    assert receipt["error_class"] == "schema_violation"
    assert receipt["capture_id"] == CAP4
    # 保留Schema校验定位的字段
    located = {f["field"]: f for f in receipt["fields"]}
    assert "source" in located, f"字段违规应定位到source: {receipt['fields']}"
    assert located["source"]["expected"] == "str"
    assert located["source"]["actual"] == "int"

    # 对照两类失败的诊断：CAP3当初是正文解码问题，CAP4是字段违规
    from app.services.page_snapshot import load_receipt_ledger
    cap3_decode = [r for r in load_receipt_ledger(BATCH)
                   if r["capture_id"] == CAP3 and r["error_class"] == "body_decode"]
    assert cap3_decode and "解码" in cap3_decode[0]["diagnostic"]
    assert "source" in receipt["diagnostic"]

    # 计数仍只取有对应回执的页面：3成功 + 1失败
    counts = receipt_counts(BATCH)
    assert counts == {"stored": 3, "failed": 1, "known": 4}, counts

    conf = BatchQueueService().confirm_intake(
        BATCH, expected_ids=[CAP1, CAP2, CAP3, CAP4])
    recollect = {r["capture_id"]: r for r in conf["recollect_list"]}
    assert list(recollect) == [CAP4]
    assert recollect[CAP4]["error_class"] == "schema_violation"
    assert "source" in str(recollect[CAP4]["fields"])
    assert conf["stored_count"] == 3 and conf["failed_count"] == 1
    assert conf["pending_count"] == 1
    assert int(_batch_entry()["待补数"]) == 1
    # 有待补 → 批次回到执行中
    assert _batch_entry()["status"] == "执行中"

    # Schema违规同时写入schema_check模块的违规记录
    violations = persist.read_jsonl("reports", "schema_violations.jsonl")
    intake_violations = [v for v in violations if v.get("target_module") == "page_snapshot_intake"]
    assert intake_violations, "字段违规应写入schema_violations.jsonl"
    assert "source" in intake_violations[-1].get("fields", [])


def test_single_official_body_per_capture():
    """每个捕获身份只有一份正式正文，归档正文与摘要对得上。"""
    from app.services.page_snapshot import (
        PageSnapshotService, archive_filename, load_receipts, verify_archived_body,
    )

    # 再重发一次第一页：正文文件数不变
    PageSnapshotService().ingest_batch(BATCH, [PAGE1])
    files = [f for f in persist.list_files("snapshots", ".json") if f.startswith("archive_TEST-CAP")]
    assert sorted(files) == sorted(archive_filename(c) for c in (CAP1, CAP2, CAP3)), files

    folded = load_receipts(BATCH)
    for cid in (CAP1, CAP2, CAP3):
        # 已归档正文必须能和摘要对上（归档摘要与回执摘要一致）
        assert verify_archived_body(cid, folded[cid]["body_digest"]), cid
    # 损坏页补送后归档的是修正版正文
    archived = persist.read_json("snapshots", archive_filename(CAP3))
    assert archived["body"] == "<html>page three fixed</html>"
    # 字段违规页没有正式正文
    assert persist.read_json("snapshots", archive_filename(CAP4)) is None


def test_intake_api_routes_registered():
    """批量入档与批次确认接口已挂载到应用。"""
    from app.main import app

    paths = set(app.openapi()["paths"])
    assert "/api/page_snapshot/intake" in paths
    assert "/api/batch_queue/confirm-intake" in paths


if __name__ == "__main__":
    test_intake_rules_registered()
    print("PASS: test_intake_rules_registered")
    test_crash_after_first_page_and_restart_consumes_receipts()
    print("PASS: test_crash_after_first_page_and_restart_consumes_receipts")
    test_resend_whole_batch_is_idempotent()
    print("PASS: test_resend_whole_batch_is_idempotent")
    test_redeliver_fixed_page_updates_only_that_page()
    print("PASS: test_redeliver_fixed_page_updates_only_that_page")
    test_schema_violation_vs_decode_failure_diagnostics()
    print("PASS: test_schema_violation_vs_decode_failure_diagnostics")
    test_single_official_body_per_capture()
    print("PASS: test_single_official_body_per_capture")
    test_intake_api_routes_registered()
    print("PASS: test_intake_api_routes_registered")
    print("\nALL 7 RECEIPT TESTS PASSED")
