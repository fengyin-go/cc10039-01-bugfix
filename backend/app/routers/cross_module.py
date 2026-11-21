"""跨模块编排端点：暴露跨模块数据流链路、事件日志和快照信息。

提供 /api/cross/events, /api/cross/snapshots, /api/cross/pipeline-status 等端点，
让前端和治理报告可以消费跨模块链路数据。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services import service_for, SERVICES
from app.store import store
from app.services.partition_policy import PartitionPolicyService
from app.services.lake_table import LakeTableService
from app.services.storage_tier import StorageTierService
from app.services.file_compaction import FileCompactionService
from app.services.retention_mgr import RetentionMgrService
from app.services.backfill_job import BackfillJobService
from app.services.govern_report import GovernReportService
from app.services.quality_monitor import QualityMonitorService

router = APIRouter(prefix="/api/cross", tags=["跨模块编排"])


class PipelineCompletionPayload(BaseModel):
    """管道完工上报：实际写入量必填，批号缺省时由管道生成。"""

    pipeline_id: int
    record_count: int = Field(ge=0)
    batch_no: str | None = None


class QualityCheckPayload(BaseModel):
    """质量检查编排：按监控规则启动或完成检查。"""

    rule_id: int


@router.get("/events")
def list_cross_events(
    source: str | None = Query(default=None),
    target: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
) -> dict[str, Any]:
    """查询跨模块事件日志，支持按 source/target 筛选。"""
    if source and target:
        events = store.events_between(source, target)
    elif source or target:
        events = store.events_for_module(source or target or "")
    else:
        events = list(store._events)
    return {
        "total": len(events),
        "items": events[-limit:],
    }


@router.get("/snapshots")
def list_snapshots() -> dict[str, Any]:
    """查询所有模块快照记录。"""
    return {
        "total": len(store._snapshots),
        "keys": store.snapshot_keys(),
    }


@router.get("/overview")
def cross_module_overview() -> dict[str, Any]:
    """跨模块统计概览。"""
    return store.cross_module_stats()


@router.get("/ingest-batches")
def list_ingest_batches(
    table_name: str | None = Query(default=None),
    pipeline_id: int | None = Query(default=None),
    batch_no: str | None = Query(default=None),
) -> dict[str, Any]:
    """入湖批次回执清单：管道完工上报的批号与实际写入量。

    供管道工作台复核批次与行数；同批回执重复提交只登记一次。
    """
    batches = QualityMonitorService().batch_receipts()
    if table_name:
        batches = [b for b in batches if str(b.get("table_name", "")).casefold() == table_name.casefold()]
    if pipeline_id is not None:
        batches = [b for b in batches if b.get("pipeline_id") == pipeline_id]
    if batch_no:
        batches = [b for b in batches if b.get("batch_no") == batch_no]
    return {"total": len(batches), "items": batches}


@router.get("/quality-checks")
def list_quality_checks(
    rule_id: int | None = Query(default=None),
    table_name: str | None = Query(default=None),
    batch_no: str | None = Query(default=None),
) -> dict[str, Any]:
    """质量检查详情：每条检查记录锁定一个已完成批次，行数以该批次回执为准。"""
    records = QualityMonitorService().check_records()
    if rule_id is not None:
        records = [r for r in records if r.get("rule_id") == rule_id]
    if table_name:
        records = [r for r in records if str(r.get("table_name", "")).casefold() == table_name.casefold()]
    if batch_no:
        records = [r for r in records if r.get("batch_no") == batch_no]
    return {"total": len(records), "items": records}


@router.get("/quality-alerts")
def list_quality_alerts(
    table_name: str | None = Query(default=None),
    batch_no: str | None = Query(default=None),
) -> dict[str, Any]:
    """质量告警清单：每条告警保留其原批次来源。"""
    alerts = QualityMonitorService().alerts()
    if table_name:
        alerts = [a for a in alerts if str(a.get("table_name", "")).casefold() == table_name.casefold()]
    if batch_no:
        alerts = [a for a in alerts if a.get("batch_no") == batch_no]
    return {"total": len(alerts), "items": alerts}


@router.post("/pipeline-completions")
def report_pipeline_completion(payload: PipelineCompletionPayload) -> dict[str, Any]:
    """管道完工上报：生成批号，把批号和实际写入量报送质量监控。

    同批完成回执重复提交时幂等处理，质量核验只记一次。
    """
    service = service_for("ingest_pipeline")
    if service is None:
        raise HTTPException(status_code=404, detail="入湖管道模块未注册")
    entry, message = service.complete_pipeline(
        payload.pipeline_id, payload.record_count, batch_no=payload.batch_no)
    return {"ok": entry is not None, "message": message, "entry": entry}


@router.post("/quality-checks/start")
def start_quality_check(payload: QualityCheckPayload) -> dict[str, Any]:
    """启动质量检查：锁定目标湖表最新一个已完成且未核验的入湖批次。"""
    record, message = QualityMonitorService().start_check(payload.rule_id)
    return {"ok": record is not None, "message": message, "record": record}


@router.post("/quality-checks/finish")
def finish_quality_check(payload: QualityCheckPayload) -> dict[str, Any]:
    """完成质量检查：按锁定批次回执出具判定，回写原管道并联动脉络。"""
    record, message = QualityMonitorService().finish_check(payload.rule_id)
    return {"ok": record is not None, "message": message, "record": record}


@router.get("/pipeline-status")
def pipeline_status() -> dict[str, Any]:
    """汇总数据湖治理全链路状态。
    从分区策略 → 湖表注册 → 冷热分层 → 小文件合并 → 保留策略 → 治理报告。
    """
    # 分区策略概览
    policies = store.rows("partition_policy")
    active_policies = [p for p in policies if p.get("status") == "已生效"]

    # 湖表注册概览
    lake_tables = store.rows("lake_table")
    registered_tables = [lt for lt in lake_tables if lt.get("status") == "已注册"]

    # 冷热分层概览
    tiers = store.rows("storage_tier")
    active_tiers = [t for t in tiers if t.get("status") == "已生效"]

    # 小文件合并概览
    compactions = store.rows("file_compaction")
    pending_compactions = [c for c in compactions if c.get("status") == "待调度"]
    running_compactions = [c for c in compactions if c.get("status") == "执行中"]
    completed_compactions = [c for c in compactions if c.get("status") == "已完成"]

    # 保留策略概览
    retentions = store.rows("retention_mgr")
    active_retentions = [r for r in retentions if r.get("status") == "已生效"]

    # 回填任务概览
    backfills = store.rows("backfill_job")
    active_backfills = [b for b in backfills if b.get("status") in ("待调度", "回填中")]

    # 血缘链路概览
    lineages = store.rows("lineage_graph")
    broken_lineages = [l for l in lineages if l.get("status") == "链路断裂"]

    # 治理报告概览
    reports = store.rows("govern_report")

    # 跨模块事件
    cross_stats = store.cross_module_stats()

    return {
        "pipeline": {
            "partition_policies": {"total": len(policies), "active": len(active_policies)},
            "lake_tables": {"total": len(lake_tables), "registered": len(registered_tables)},
            "storage_tiers": {"total": len(tiers), "active": len(active_tiers)},
            "file_compactions": {
                "total": len(compactions),
                "pending": len(pending_compactions),
                "running": len(running_compactions),
                "completed": len(completed_compactions),
            },
            "retention_mgrs": {"total": len(retentions), "active": len(active_retentions)},
            "backfill_jobs": {"total": len(backfills), "active": len(active_backfills)},
            "lineage_graphs": {"total": len(lineages), "broken": len(broken_lineages)},
            "govern_reports": {"total": len(reports)},
        },
        "cross_module": cross_stats,
        "health": {
            "has_broken_lineages": len(broken_lineages) > 0,
            "has_active_backfills": len(active_backfills) > 0,
            "all_lake_tables_registered": len(registered_tables) == len(lake_tables),
        },
    }