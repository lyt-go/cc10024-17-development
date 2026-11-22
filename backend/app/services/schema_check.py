"""Schema校验领域服务：数据结构、字段类型、空值策略校验，在run_history确认完成和导出前触发。"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.services.base import BaseModuleService
from app import persistence as persist


class SchemaCheckService(BaseModuleService):
    MODULE = "schema_check"
    ENTITY = "Schema规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则编号", "目标数据", "字段名", "类型约束", "格式约束", "空值策略", "异常归类", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "目标数据", "字段名"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    def validate_fields(self, record: dict[str, Any], target_module: str) -> list[dict[str, Any]]:
        """对单条记录执行所有已生效的Schema校验规则，返回结构化违规列表（定位到字段）。"""
        rules = [r for r in store.rows(self.MODULE) if r.get("目标数据") == target_module and r.get("status") == "已生效"]
        violations: list[dict[str, Any]] = []
        for rule in rules:
            field = rule.get("字段名", "")
            expected_type = rule.get("类型约束", "")
            format_constraint = rule.get("格式约束", "")
            null_policy = rule.get("空值策略", "")
            rule_no = rule.get("规则编号", "")
            value = record.get(field)

            def _violate(message: str, expected: str, actual: str) -> None:
                violations.append({
                    "field": field,
                    "expected": expected,
                    "actual": actual,
                    "rule": rule_no,
                    "message": message,
                })

            if value is None or (isinstance(value, str) and not value.strip()):
                if null_policy == "禁止":
                    _violate(f"字段{field}不允许为空（规则{rule_no}）", "非空", "空")
                continue
            if expected_type == "int":
                try:
                    int(value)
                except (ValueError, TypeError):
                    _violate(f"字段{field}期望int类型，实际为{type(value).__name__}",
                             "int", type(value).__name__)
            elif expected_type == "float":
                try:
                    float(value)
                except (ValueError, TypeError):
                    _violate(f"字段{field}期望float类型，实际为{type(value).__name__}",
                             "float", type(value).__name__)
            elif expected_type == "str":
                if not isinstance(value, str):
                    _violate(f"字段{field}期望str类型，实际为{type(value).__name__}",
                             "str", type(value).__name__)
            elif expected_type == "date" and format_constraint:
                if not isinstance(value, str) or len(value) < 8:
                    _violate(f"字段{field}日期格式不符合{format_constraint}",
                             format_constraint, type(value).__name__)
        if violations:
            persist.append_jsonl("reports", "schema_violations.jsonl", {
                "target_module": target_module,
                "record_summary": str(record)[:100],
                "violations": [v["message"] for v in violations],
                "fields": [v["field"] for v in violations],
                "timestamp": persist.now_iso(),
            })
        return violations

    def validate_record(self, record: dict[str, Any], target_module: str) -> list[str]:
        """对单条记录执行所有已生效的Schema校验规则，返回违规描述列表。"""
        return [v["message"] for v in self.validate_fields(record, target_module)]

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        # Schema违规统计
        violations = persist.read_jsonl("reports", "schema_violations.jsonl")
        base["total_violations"] = len(violations)
        # 关联的质量防护规则
        guards = store.rows("quality_guard")
        base["linked_quality_rules"] = sum(1 for g in guards if g.get("status") == "已生效")
        # 关联的导出通道
        channels = store.rows("export_channel")
        base["linked_export_channels"] = len(channels)
        return base