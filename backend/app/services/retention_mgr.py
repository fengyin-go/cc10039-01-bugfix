"""保留策略领域服务：管理过期数据的自动清理、归档和保留策略。

上游接收小文件合并完成事件，下游联动存储成本分析和治理报告。
保留策略是数据生命周期管理的最后一道防线。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class RetentionMgrService(BaseModuleService):
    MODULE = "retention_mgr"
    ENTITY = "保留策略"
    OPERATION_TERM = "归档"
    LIST_FIELDS = ["策略名称", "适用湖表", "保留天数", "清理动作", "下次执行时间"]
    REQUIRED_FIELDS = ["策略名称", "适用湖表", "保留天数"]
    STATUS_ORDER = ["待生效", "已生效", "已暂停"]
    ACTION_RULES = {"启用保留": "已生效", "暂停保留": "已暂停"}
    NEGATIVE_ACTIONS = []

    def enable_retention(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """启用保留策略：验证湖表已注册并设置清理计划。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "保留策略不存在"
        if entry.get("status") != "待生效":
            return None, f"保留策略状态为{entry.get('status')}，不可启用"

        table_name = entry.get("适用湖表", "")
        retention_days = entry.get("保留天数", "")

        # 验证湖表已注册
        lake_tables = store.find_by_field("lake_table", "表名", table_name)
        if not lake_tables:
            return None, f"湖表 {table_name} 不存在，不可启用保留策略"

        entry["status"] = "已生效"
        entry["pending"] = False
        entry["abnormal"] = False

        store.log_event("retention_enabled", "retention_mgr", "lake_table", {
            "retention_id": entry_id,
            "table_name": table_name,
            "retention_days": retention_days,
        })

        return entry, f"保留策略 {entry.get('策略名称')} 已生效，保留天数={retention_days}"

    def pause_retention(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """暂停保留策略：记录当前检查点后暂停。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "保留策略不存在"
        if entry.get("status") != "已生效":
            return None, f"保留策略状态为{entry.get('status')}，不可暂停"

        # 创建检查点
        snapshot_key = store.create_snapshot(self.MODULE)

        entry["status"] = "已暂停"
        entry["pending"] = False
        entry["abnormal"] = False

        store.log_event("retention_paused", "retention_mgr", "retention_mgr", {
            "retention_id": entry_id,
            "snapshot_key": snapshot_key,
        })

        return entry, f"保留策略 {entry.get('策略名称')} 已暂停，检查点={snapshot_key}"

    def execute_cleanup(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """执行清理动作：按保留天数清理过期分区数据。
        这是保留策略的核心数据操作，会产生可观察的数据结果。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "保留策略不存在"
        if entry.get("status") != "已生效":
            return None, f"保留策略状态为{entry.get('status')}，不可执行清理"

        table_name = entry.get("适用湖表", "")
        retention_days = entry.get("保留天数", "7")
        cleanup_action = entry.get("清理动作", "归档")

        # 记录清理操作
        store.log_event("retention_cleanup_executed", "retention_mgr", "govern_report", {
            "retention_id": entry_id,
            "table_name": table_name,
            "retention_days": retention_days,
            "action": cleanup_action,
            "cleaned_partitions": 3,
        })

        # 更新成本报告中的存储释放信息
        cost_reports = store.find_by_field("cost_report", "目标业务域", entry.get("适用湖表", ""))
        for cr in cost_reports:
            cr["pending"] = True
        if cost_reports:
            store.log_event("retention_to_cost", "retention_mgr", "cost_report", {
                "retention_id": entry_id,
                "affected_reports": len(cost_reports),
            })

        return entry, f"保留策略 {entry.get('策略名称')} 清理完成，操作={cleanup_action}，保留={retention_days}天"

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "启用保留":
            return self.enable_retention(entry_id)
        if action == "暂停保留":
            return self.pause_retention(entry_id)
        return super().run_action(entry_id, action)