"""快照入档专用接口：采集页批量校验入档、快照接收回执、批次入档确认。

与通用资源路由分开：批量入档一次提交多页并逐页返回接收回执，
批次确认消费该批既有回执，生成可回放范围和补采清单。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services.batch_queue import BatchQueueService
from app.services.page_snapshot import IntakeInterrupted, PageSnapshotService

router = APIRouter(prefix="/api", tags=["快照入档回执"])


class IntakePayload(BaseModel):
    """一次提交多条采集页：每页带稳定来源身份capture_id和base64正文。"""

    batch_id: str
    pages: list[dict[str, Any]] = Field(default_factory=list)


class ConfirmPayload(BaseModel):
    batch_id: str
    expected_ids: list[str] | None = None


@router.post("/page_snapshot/intake", response_model=dict)
def intake_batch(payload: IntakePayload) -> dict[str, Any]:
    """批量校验入档：逐页解码、Schema校验、落盘并生成快照接收回执。"""
    service = PageSnapshotService()
    try:
        summary = service.ingest_batch(payload.batch_id, payload.pages)
    except IntakeInterrupted as exc:  # 故障注入仅测试触发；正常请求不会走到
        return {"ok": False, "message": str(exc), "summary": None}
    return {
        "ok": True,
        "message": (
            f"批次{payload.batch_id}入档完成：存妥{summary['stored_count']}页，"
            f"失败{summary['failed_count']}页"
        ),
        "summary": summary,
    }


@router.post("/batch_queue/confirm-intake", response_model=dict)
def confirm_intake(payload: ConfirmPayload) -> dict[str, Any]:
    """批次入档确认：消费既有回执，生成可回放范围和补采清单。"""
    service = BatchQueueService()
    confirmation = service.confirm_intake(payload.batch_id, payload.expected_ids)
    return {
        "ok": True,
        "message": (
            f"批次{payload.batch_id}第{confirmation['confirmations']}次确认："
            f"可回放{len(confirmation['replayable_range'])}页，"
            f"待补{confirmation['pending_count']}页"
        ),
        "confirmation": confirmation,
    }
