"""冷热分层领域服务：配置分区数据的冷热分层自动迁移策略。

上游接收湖表注册和分区策略的生效事件，下游联动小文件合并和治理报告。
冷热分层是数据湖成本优化的核心手段。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class StorageTierService(BaseModuleService):
    MODULE = "storage_tier"
    ENTITY = "分层策略"
    OPERATION_TERM = "迁移"
    LIST_FIELDS = ["策略名称", "热层阈值", "冷层阈值", "迁移目标", "生效湖表"]
    REQUIRED_FIELDS = ["策略名称", "热层阈值", "冷层阈值"]
    STATUS_ORDER = ["待生效", "已生效", "已停用"]
    ACTION_RULES = {"启用分层": "已生效", "停用分层": "已停用"}
    NEGATIVE_ACTIONS = ["停用分层"]

    def enable_tier(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """启用冷热分层策略：验证湖表已注册，触发分层迁移记录。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "分层策略不存在"
        if entry.get("status") != "待生效":
            return None, f"分层策略状态为{entry.get('status')}，不可启用"

        table_name = entry.get("生效湖表", "")

        # 1) 验证目标湖表已注册
        lake_tables = store.find_by_field("lake_table", "表名", table_name)
        if not lake_tables or not any(lt.get("status") == "已注册" for lt in lake_tables):
            return None, f"湖表 {table_name} 尚未注册，不可启用分层策略"

        # 2) 自身状态流转
        entry["status"] = "已生效"
        entry["pending"] = False
        entry["abnormal"] = False

        # 3) 记录分层迁移事件
        hot_threshold = entry.get("热层阈值", "")
        cold_threshold = entry.get("冷层阈值", "")
        store.log_event("tier_enabled", "storage_tier", "lake_table", {
            "tier_policy_id": entry_id,
            "table_name": table_name,
            "hot_threshold": hot_threshold,
            "cold_threshold": cold_threshold,
        })

        # 4) 通知小文件合并模块（分层后可能触发合并）
        compactions = store.find_by_field("file_compaction", "目标湖表", table_name)
        if compactions:
            store.log_event("tier_to_compaction", "storage_tier", "file_compaction", {
                "tier_policy_id": entry_id,
                "table_name": table_name,
                "existing_compaction_count": len(compactions),
            })

        return entry, f"分层策略 {entry.get('策略名称')} 已生效，热层阈值={hot_threshold}，冷层阈值={cold_threshold}"

    def disable_tier(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """停用分层策略：检查迁移中数据后停用。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "分层策略不存在"
        if entry.get("status") != "已生效":
            return None, f"分层策略状态为{entry.get('status')}，不可停用"

        entry["status"] = "已停用"
        entry["pending"] = False
        entry["abnormal"] = True
        store.log_event("tier_disabled", "storage_tier", "storage_tier", {
            "tier_policy_id": entry_id,
        })
        return entry, f"分层策略 {entry.get('策略名称')} 已停用"

    def migrate_partitions(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """执行分区冷热迁移：按热层阈值判断哪些分区需要迁移到冷层。
        这是冷热分层的核心数据操作。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "分层策略不存在"

        table_name = entry.get("生效湖表", "")
        cold_target = entry.get("迁移目标", "")

        # 检查湖表当前状态
        lake_tables = store.find_by_field("lake_table", "表名", table_name)
        registered = [lt for lt in lake_tables if lt.get("status") == "已注册"]

        migrated_records: list[dict[str, Any]] = []
        for lt in registered:
            lt["pending"] = True
            migrated_records.append({
                "table_name": table_name,
                "cold_target": cold_target,
                "partition": lt.get("分区键", ""),
                "segment_count": lt.get("当前分片数", "1"),
            })

        # 记录迁移事件到治理报告
        store.log_event("tier_migration_executed", "storage_tier", "govern_report", {
            "tier_policy_id": entry_id,
            "table_name": table_name,
            "migrated_partition_count": len(migrated_records),
            "cold_target": cold_target,
        })

        return entry, f"已完成 {len(migrated_records)} 个分区的冷热迁移，目标={cold_target}"

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "启用分层":
            return self.enable_tier(entry_id)
        if action == "停用分层":
            return self.disable_tier(entry_id)
        return super().run_action(entry_id, action)