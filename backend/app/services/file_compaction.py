"""小文件合并领域服务：调度和执行小文件合并任务以减少元数据开销。

小文件合并具备幂等追踪能力：同一湖表的同一批次合并任务不会重复执行。
上游接收冷热分层迁移事件，下游联动保留策略的清理动作。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class FileCompactionService(BaseModuleService):
    MODULE = "file_compaction"
    ENTITY = "合并任务"
    OPERATION_TERM = "调度"
    LIST_FIELDS = ["任务编号", "目标湖表", "文件阈值", "合并后文件数", "执行状态"]
    REQUIRED_FIELDS = ["任务编号", "目标湖表", "文件阈值"]
    STATUS_ORDER = ["待调度", "执行中", "已完成", "执行失败"]
    ACTION_RULES = {"触发合并": "执行中", "取消任务": "执行失败"}
    NEGATIVE_ACTIONS = []

    # 幂等追踪：已完成的 task_id 集合
    _completed_tasks: set[str] = set()

    def trigger_compaction(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """触发小文件合并任务，支持幂等追踪。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "合并任务不存在"
        if entry.get("status") != "待调度":
            return None, f"合并任务状态为{entry.get('status')}，不可触发"

        task_no = entry.get("任务编号", "")
        table_name = entry.get("目标湖表", "")

        # 幂等检查：同一任务编号已完成的不再触发
        if task_no in self._completed_tasks:
            return None, f"任务 {task_no} 已经完成，不可重复触发"

        # 检查同湖表是否有其他执行中的合并
        active_compactions = [
            c for c in store.rows(self.MODULE)
            if c.get("目标湖表") == table_name
            and c.get("status") == "执行中"
            and c.get("id") != entry_id
        ]
        if active_compactions:
            return None, f"湖表 {table_name} 已有合并任务在执行中（任务编号：{active_compactions[0].get('任务编号')}），不可并发触发"

        # 自身状态流转
        entry["status"] = "执行中"
        entry["pending"] = True
        entry["abnormal"] = False

        store.log_event("compaction_triggered", "file_compaction", "file_compaction", {
            "task_no": task_no,
            "table_name": table_name,
            "file_threshold": entry.get("文件阈值", ""),
        })

        return entry, f"合并任务 {task_no} 已触发"

    def complete_compaction(self, entry_id: int, result_file_count: str = "") -> tuple[dict[str, Any] | None, str]:
        """完成合并任务，记录幂等标识，通知保留策略清理旧文件。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "合并任务不存在"
        if entry.get("status") != "执行中":
            return None, f"合并任务状态为{entry.get('status')}，不可完成"

        task_no = entry.get("任务编号", "")
        table_name = entry.get("目标湖表", "")

        entry["status"] = "已完成"
        entry["pending"] = False
        entry["abnormal"] = False
        if result_file_count:
            entry["合并后文件数"] = result_file_count

        # 幂等追踪
        self._completed_tasks.add(task_no)

        store.log_event("compaction_completed", "file_compaction", "retention_mgr", {
            "task_no": task_no,
            "table_name": table_name,
            "result_file_count": result_file_count,
        })

        # 通知保留策略：合并完成后的旧文件可清理
        retentions = store.find_by_field("retention_mgr", "适用湖表", table_name)
        for r in retentions:
            if r.get("status") == "已生效":
                r["pending"] = True

        return entry, f"合并任务 {task_no} 已完成，结果文件数={result_file_count}，已通知保留策略清理旧文件"

    def is_task_completed(self, task_no: str) -> bool:
        """幂等检查：查询指定任务编号是否已完成。"""
        return task_no in self._completed_tasks

    def compaction_status_for_table(self, table_name: str) -> dict[str, Any]:
        """查询指定湖表的合并任务状态摘要。"""
        compactions = store.find_by_field(self.MODULE, "目标湖表", table_name)
        return {
            "table_name": table_name,
            "total_tasks": len(compactions),
            "pending": sum(1 for c in compactions if c.get("status") == "待调度"),
            "running": sum(1 for c in compactions if c.get("status") == "执行中"),
            "completed": sum(1 for c in compactions if c.get("status") == "已完成"),
            "failed": sum(1 for c in compactions if c.get("status") == "执行失败"),
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "触发合并":
            return self.trigger_compaction(entry_id)
        return super().run_action(entry_id, action)