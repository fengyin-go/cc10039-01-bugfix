"""湖内质量监控领域服务：配置数据湖内数据的完整性、时效性和准确性监控规则。

上游接收入湖管道完成事件，对入湖数据执行质量检查。
下游联动血缘图谱记录质量问题，再汇总到治理报告中。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class QualityMonitorService(BaseModuleService):
    MODULE = "quality_monitor"
    ENTITY = "监控规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则名称", "监控指标", "告警阈值", "目标湖表", "检查频率"]
    REQUIRED_FIELDS = ["规则名称", "监控指标", "告警阈值"]
    STATUS_ORDER = ["待启用", "已启用", "已停用"]
    ACTION_RULES = {"启用监控": "已启用", "停用监控": "已停用"}
    NEGATIVE_ACTIONS = ["停用监控"]

    # 质量检查结果存储
    _check_results: list[dict[str, Any]] = []

    def enable_monitor(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """启用监控规则：验证目标湖表存在。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "监控规则不存在"
        if entry.get("status") != "待启用":
            return None, f"监控规则状态为{entry.get('status')}，不可启用"

        table_name = entry.get("目标湖表", "")

        # 验证目标湖表已注册
        lake_tables = store.find_by_field("lake_table", "表名", table_name)
        if not lake_tables:
            return None, f"目标湖表 {table_name} 不存在"

        entry["status"] = "已启用"
        entry["pending"] = False
        entry["abnormal"] = False

        store.log_event("quality_rule_enabled", "quality_monitor", "lake_table", {
            "rule_id": entry_id,
            "table_name": table_name,
            "metric": entry.get("监控指标", ""),
        })

        return entry, f"监控规则 {entry.get('规则名称')} 已启用，指标={entry.get('监控指标')}"

    def check_quality(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """执行质量检查：对目标湖表数据执行指标检查，命中阈值则触发告警。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "监控规则不存在"
        if entry.get("status") != "已启用":
            return None, f"监控规则状态为{entry.get('status')}，不可执行检查"

        table_name = entry.get("目标湖表", "")
        metric = entry.get("监控指标", "")
        threshold = entry.get("告警阈值", "")

        # 模拟检查：统计目标湖表的数据行数
        lake_table_rows = store.find_by_field("lake_table", "表名", table_name)
        row_count = len(lake_table_rows)

        # 阈值比较
        try:
            threshold_value = int(threshold) if threshold.isdigit() else 0
        except (ValueError, AttributeError):
            threshold_value = 0

        passed = row_count >= threshold_value
        result = {
            "rule_id": entry_id,
            "rule_name": entry.get("规则名称"),
            "table_name": table_name,
            "metric": metric,
            "threshold": threshold,
            "actual_value": row_count,
            "passed": passed,
            "checked_at": __import__("datetime").datetime.now().isoformat(),
        }
        self._check_results.append(result)

        # 质量不通过时联动血缘和治理报告
        if not passed:
            store.log_event("quality_failed_to_lineage", "quality_monitor", "lineage_graph", {
                "rule_id": entry_id,
                "table_name": table_name,
                "metric": metric,
                "threshold": threshold,
                "actual": row_count,
            })

            # 更新血缘链路为待验证
            lineages = store.find_by_field("lineage_graph", "中间转换", table_name)
            if not lineages:
                lineages = store.find_by_field("lineage_graph", "下游消费", table_name)
            for lin in lineages:
                lin["status"] = "待验证"
                lin["abnormal"] = True
        else:
            store.log_event("quality_passed_to_govern", "quality_monitor", "govern_report", {
                "rule_id": entry_id,
                "table_name": table_name,
                "metric": metric,
                "actual": row_count,
            })

        return entry, (
            f"质量检查完成：{metric}={row_count}，阈值={threshold}，"
            + ("通过" if passed else "不通过")
        )

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "启用监控":
            return self.enable_monitor(entry_id)
        return super().run_action(entry_id, action)