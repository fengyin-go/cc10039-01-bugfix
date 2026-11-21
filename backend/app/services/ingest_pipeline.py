"""入湖管道领域服务：管理数据从来源系统加载到数据湖的管道配置与执行。

上游与湖表注册联动；管道完工时生成批号，把批号和实际写入量作为
批次回执上报给质量监控，供其按批次锁定口径生成入湖批次质量判定。
支持管道状态追踪和失败恢复。
"""
from __future__ import annotations

import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.services.quality_monitor import QualityMonitorService
from app.store import store


class IngestPipelineService(BaseModuleService):
    MODULE = "ingest_pipeline"
    ENTITY = "入湖任务"
    OPERATION_TERM = "入湖"
    LIST_FIELDS = ["任务名称", "来源系统", "目标湖表", "数据格式", "执行状态"]
    REQUIRED_FIELDS = ["任务名称", "来源系统", "目标湖表"]
    STATUS_ORDER = ["待启动", "运行中", "已完成", "运行失败"]
    ACTION_RULES = {"启动任务": "运行中", "取消任务": "运行失败"}
    NEGATIVE_ACTIONS = []

    # 管道批次追踪：pipeline_id -> 已上报批次数，用于生成批号
    _batch_records: dict[int, int] = {}

    def start_pipeline(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """启动入湖管道：验证目标湖表已注册后启动。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "入湖任务不存在"
        if entry.get("status") != "待启动":
            return None, f"入湖任务状态为{entry.get('status')}，不可启动"

        table_name = entry.get("目标湖表", "")
        source = entry.get("来源系统", "")
        data_format = entry.get("数据格式", "")

        # 验证目标湖表已注册
        lake_tables = store.find_by_field("lake_table", "表名", table_name)
        if not lake_tables or not any(lt.get("status") == "已注册" for lt in lake_tables):
            return None, f"目标湖表 {table_name} 尚未注册或已下线，不可启动管道"

        # 自身状态流转
        entry["status"] = "运行中"
        entry["pending"] = True
        entry["abnormal"] = False

        store.log_event("pipeline_started", "ingest_pipeline", "lake_table", {
            "pipeline_id": entry_id,
            "table_name": table_name,
            "source": source,
            "data_format": data_format,
        })

        return entry, f"入湖管道 {entry.get('任务名称')} 已启动，来源={source}，目标={table_name}"

    def _next_batch_no(self, entry_id: int) -> str:
        """为管道生成下一个入湖批号。"""
        seq = self._batch_records.get(entry_id, 0) + 1
        self._batch_records[entry_id] = seq
        return f"BATCH-{entry_id:04d}-{seq:03d}"

    def complete_pipeline(
        self, entry_id: int, record_count: int, batch_no: str | None = None
    ) -> tuple[dict[str, Any] | None, str]:
        """完成入湖管道：生成批号，把批号和实际写入量上报质量监控。

        同批完成回执重复提交（状态已完成且批号一致）时幂等处理：
        质量核验只记一次，已出具的判定和告警保留原批次来源。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "入湖任务不存在"

        # 同批完成回执重复提交：转发质量监控幂等登记，核验结果保持原记录
        if entry.get("status") == "已完成" and batch_no and batch_no == entry.get("最近批号"):
            receipt, created, note = QualityMonitorService().receive_batch_receipt({
                "batch_no": batch_no,
                "pipeline_id": entry_id,
                "pipeline_name": entry.get("任务名称", ""),
                "table_name": entry.get("目标湖表", ""),
                "record_count": record_count,
                "completed_at": datetime.datetime.now().isoformat(),
            })
            return entry, f"批次 {batch_no} 完成回执重复提交：{note}"

        if entry.get("status") != "运行中":
            return None, f"入湖任务状态为{entry.get('status')}，不可完成"

        table_name = entry.get("目标湖表", "")
        batch_no = batch_no or self._next_batch_no(entry_id)
        completed_at = datetime.datetime.now().isoformat()

        entry["status"] = "已完成"
        entry["pending"] = False
        entry["abnormal"] = False
        entry["最近批号"] = batch_no
        entry["最近批次行数"] = record_count

        # 管道完工：把批号和实际写入量作为批次回执上报质量监控
        receipt, created, note = QualityMonitorService().receive_batch_receipt({
            "batch_no": batch_no,
            "pipeline_id": entry_id,
            "pipeline_name": entry.get("任务名称", ""),
            "table_name": table_name,
            "record_count": record_count,
            "completed_at": completed_at,
        })

        # 提醒该湖表已启用的监控规则：有新批次待核验
        quality_rules = store.find_by_field("quality_monitor", "目标湖表", table_name)
        enabled_rules = [qr for qr in quality_rules if qr.get("status") == "已启用"]
        if created and enabled_rules:
            for qr in enabled_rules:
                qr["pending"] = True

        # 更新湖表状态
        lake_tables = store.find_by_field("lake_table", "表名", table_name)
        for lt in lake_tables:
            lt["pending"] = True

        return entry, (
            f"入湖管道 {entry.get('任务名称')} 已完成，批次 {batch_no}，"
            f"入湖 {record_count} 条记录，已上报质量监控（{note}）"
        )

    def fail_pipeline(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """管道失败：保留状态用于重试恢复。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "入湖任务不存在"
        if entry.get("status") != "运行中":
            return None, f"入湖任务状态为{entry.get('status')}，不可标记失败"

        entry["status"] = "运行失败"
        entry["pending"] = False
        entry["abnormal"] = True

        store.log_event("pipeline_failed", "ingest_pipeline", "task_scheduler", {
            "pipeline_id": entry_id,
            "table_name": entry.get("目标湖表", ""),
        })

        return entry, f"入湖管道 {entry.get('任务名称')} 运行失败"

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "启动任务":
            return self.start_pipeline(entry_id)
        return super().run_action(entry_id, action)
