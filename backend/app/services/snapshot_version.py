"""快照版本领域服务：管理湖表数据的快照版本和基于时间点的数据恢复。

上游接收 schema 演进和回填任务的事件，下游为治理报告提供版本追溯数据。
每个快照带版本号和时间戳，支持废弃管理和版本对比。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class SnapshotVersionService(BaseModuleService):
    MODULE = "snapshot_version"
    ENTITY = "快照版本"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["湖表名", "快照时间戳", "版本号", "记录数", "快照类型"]
    REQUIRED_FIELDS = ["湖表名", "快照时间戳", "版本号"]
    STATUS_ORDER = ["生成中", "可用", "已废弃"]
    ACTION_RULES = {"生成快照": "可用", "废弃快照": "已废弃"}
    NEGATIVE_ACTIONS = []

    def generate_snapshot(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """生成快照版本：创建模块数据深拷贝并记录版本。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "快照版本不存在"
        if entry.get("status") != "生成中":
            return None, f"快照版本状态为{entry.get('status')}，不可生成"

        table_name = entry.get("湖表名", "")
        version_no = entry.get("版本号", "")

        # 检查同湖表是否有同名版本快照
        existing = [
            sv for sv in store.rows(self.MODULE)
            if sv.get("湖表名") == table_name
            and sv.get("版本号") == version_no
            and sv.get("id") != entry_id
        ]
        if existing:
            return None, f"湖表 {table_name} 已存在版本 {version_no} 的快照，不可重复生成"

        # 创建底层数据快照
        snapshot_key = store.create_snapshot(f"{table_name}_v{version_no}")

        entry["status"] = "可用"
        entry["pending"] = False
        entry["abnormal"] = False
        entry["快照类型"] = "全量快照"

        store.log_event("snapshot_created", "snapshot_version", "snapshot_version", {
            "snapshot_id": entry_id,
            "table_name": table_name,
            "version_no": version_no,
            "snapshot_key": snapshot_key,
        })

        return entry, f"湖表 {table_name} 版本 {version_no} 快照已生成，key={snapshot_key}"

    def discard_snapshot(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """废弃快照版本：检查是否有后续快照依赖。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "快照版本不存在"
        if entry.get("status") != "可用":
            return None, f"快照版本状态为{entry.get('status')}，不可废弃"

        table_name = entry.get("湖表名", "")
        version_no = entry.get("版本号", "")

        # 检查 schema 演进是否引用了此版本
        schema_changes = store.find_by_field("schema_evolution", "变更前快照", version_no)
        pending_changes = [sc for sc in schema_changes if sc.get("status") in ("草稿", "待审批")]
        if pending_changes:
            return None, f"版本 {version_no} 仍被 {len(pending_changes)} 个待处理 Schema 变更引用，不可废弃"

        entry["status"] = "已废弃"
        entry["pending"] = False
        entry["abnormal"] = False

        store.log_event("snapshot_discarded", "snapshot_version", "schema_evolution", {
            "snapshot_id": entry_id,
            "table_name": table_name,
            "version_no": version_no,
        })

        return entry, f"快照版本 {version_no} 已废弃"

    def available_snapshots_for_table(self, table_name: str) -> list[dict[str, Any]]:
        """查询指定湖表的可用快照列表。"""
        return [
            sv for sv in store.rows(self.MODULE)
            if sv.get("湖表名") == table_name and sv.get("status") == "可用"
        ]

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "生成快照":
            return self.generate_snapshot(entry_id)
        if action == "废弃快照":
            return self.discard_snapshot(entry_id)
        return super().run_action(entry_id, action)