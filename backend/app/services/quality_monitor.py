"""湖内质量监控领域服务：配置数据湖内数据的完整性、时效性和准确性监控规则。

上游接收入湖管道完工时上报的批次回执（批号 + 实际写入量），
检查时锁定一个已完成批次，按管道上报的行数和规则阈值生成入湖批次质量判定；
判定结果作为回执写回原管道，不通过时联动血缘图谱标注待复查链路，
并出具保留原批次来源的告警。同批回执重复提交只核验一次。
"""
from __future__ import annotations

import datetime
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

    # 入湖批次回执登记簿：batch_no -> 回执。按批号幂等，同批只登记、只核验一次。
    _batch_receipts: dict[str, dict[str, Any]] = {}
    # 质量检查记录（检查详情）：每条记录锁定一个已完成批次，行数以该批次回执为准。
    _check_records: list[dict[str, Any]] = []
    # 质量告警：出具后保留原批次来源，重复提交不覆盖、不重复。
    _alerts: list[dict[str, Any]] = []

    # ---- 批次回执接收（入湖管道 → 质量监控） ----

    def receive_batch_receipt(
        self, receipt: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, bool, str]:
        """登记入湖管道完工上报的批次回执（批号 + 实际写入量）。

        按批号幂等：同批完成回执重复提交时返回首次登记的原记录，
        不重复登记、不重置已有判定，已出具的告警保留原批次来源。
        返回 (回执记录, 是否新登记, 消息)。
        """
        batch_no = str(receipt.get("batch_no") or "").strip()
        if not batch_no:
            return None, False, "缺少批号，无法登记入湖批次回执"

        existing = self._batch_receipts.get(batch_no)
        if existing is not None:
            return existing, False, f"批次 {batch_no} 回执已登记，质量核验保持原记录"

        try:
            record_count = int(receipt.get("record_count") or 0)
        except (TypeError, ValueError):
            record_count = 0

        record = {
            "id": len(self._batch_receipts) + 1,
            "batch_no": batch_no,
            "pipeline_id": receipt.get("pipeline_id"),
            "pipeline_name": receipt.get("pipeline_name", ""),
            "table_name": receipt.get("table_name", ""),
            "record_count": record_count,
            "status": "已上报",
            "verdict": None,
            "rule_id": None,
            "completed_at": receipt.get("completed_at"),
            "reported_at": datetime.datetime.now().isoformat(),
            "checked_at": None,
        }
        self._batch_receipts[batch_no] = record

        store.log_event("pipeline_to_quality", "ingest_pipeline", "quality_monitor", {
            "pipeline_id": record["pipeline_id"],
            "table_name": record["table_name"],
            "batch_no": batch_no,
            "record_count": record_count,
        })
        return record, True, f"批次 {batch_no} 回执已登记，入湖 {record_count} 行"

    def batch_receipt(self, batch_no: str) -> dict[str, Any] | None:
        """按批号查询批次回执。"""
        return self._batch_receipts.get(batch_no)

    def batch_receipts(self) -> list[dict[str, Any]]:
        """全部批次回执（按登记顺序）。"""
        return list(self._batch_receipts.values())

    def check_records(self) -> list[dict[str, Any]]:
        """全部质量检查记录（检查详情）。"""
        return list(self._check_records)

    def alerts(self) -> list[dict[str, Any]]:
        """全部质量告警，每条保留其原批次来源。"""
        return list(self._alerts)

    # ---- 监控规则启停 ----

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

    # ---- 质量检查：启动（锁定批次） → 完成（出具判定） ----

    @staticmethod
    def _threshold_value(raw: Any) -> int:
        try:
            return int(str(raw).strip())
        except (TypeError, ValueError):
            return 0

    def _inflight_check(self, rule_id: int) -> dict[str, Any] | None:
        for record in self._check_records:
            if record["rule_id"] == rule_id and record["status"] == "检查中":
                return record
        return None

    def start_check(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """启动质量检查：锁定目标湖表最新一个已完成且未核验的入湖批次。

        检查行数在启动时即按该批次回执锁定，检查进行期间其它写入
        （同表后续批次或其它表批次）不得混入本次检查口径。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "监控规则不存在"
        if entry.get("status") != "已启用":
            return None, f"监控规则状态为{entry.get('status')}，不可执行检查"

        inflight = self._inflight_check(entry_id)
        if inflight is not None:
            return inflight, f"检查已在进行中，锁定批次 {inflight['batch_no']}"

        table_name = entry.get("目标湖表", "")
        candidates = [
            receipt for receipt in self._batch_receipts.values()
            if receipt["status"] == "已上报"
            and str(receipt["table_name"]).casefold() == str(table_name).casefold()
        ]
        if not candidates:
            return None, f"目标湖表 {table_name} 暂无待核验的入湖批次，无法启动检查"

        # 锁定最新一个已完成批次（检查开始时刻的口径）
        batch = max(candidates, key=lambda item: item["id"])
        threshold = self._threshold_value(entry.get("告警阈值", ""))

        record = {
            "id": len(self._check_records) + 1,
            "rule_id": entry_id,
            "rule_name": entry.get("规则名称"),
            "table_name": table_name,
            "metric": entry.get("监控指标", ""),
            "threshold": threshold,
            "batch_no": batch["batch_no"],
            "pipeline_id": batch["pipeline_id"],
            "actual_value": batch["record_count"],
            "passed": None,
            "status": "检查中",
            "started_at": datetime.datetime.now().isoformat(),
            "checked_at": None,
        }
        self._check_records.append(record)
        entry["pending"] = True

        return record, (
            f"质量检查已启动：锁定批次 {batch['batch_no']}，"
            f"入湖行数 {batch['record_count']}，阈值 {threshold}"
        )

    def finish_check(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """完成质量检查：按锁定批次的回执行数出具入湖批次质量判定。

        判定回写原管道（质量回执），批次回执标记为已核验；
        不通过时出具告警（保留批次来源）并将该湖表血缘链路标注为待验证。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "监控规则不存在"

        record = self._inflight_check(entry_id)
        if record is None:
            return None, "没有进行中的质量检查，请先启动检查"

        batch = self._batch_receipts.get(record["batch_no"])
        if batch is None:
            return None, f"锁定批次 {record['batch_no']} 的回执不存在，无法完成检查"

        # 行数以锁定批次的完成回执为准，不混入检查期间的其它写入
        actual = int(batch["record_count"])
        threshold = int(record["threshold"])
        passed = actual >= threshold
        checked_at = datetime.datetime.now().isoformat()
        verdict = "通过" if passed else "不通过"

        record["actual_value"] = actual
        record["passed"] = passed
        record["status"] = "已完成"
        record["checked_at"] = checked_at

        # 批次回执登记核验结论（同批只核验一次）
        batch["status"] = "已核验"
        batch["verdict"] = verdict
        batch["rule_id"] = entry_id
        batch["checked_at"] = checked_at

        # 判定回到原管道：管道上可凭回执确认要复跑的批次
        pipeline = store.find("ingest_pipeline", int(batch.get("pipeline_id") or 0))
        if pipeline is not None:
            pipeline["质量回执"] = {
                "批号": batch["batch_no"],
                "实际行数": actual,
                "判定": verdict,
                "阈值": threshold,
                "规则名称": record["rule_name"],
                "核验时间": checked_at,
            }
            pipeline["最近质检批号"] = batch["batch_no"]
            pipeline["最近质检结论"] = verdict

        store.log_event("quality_verdict_to_pipeline", "quality_monitor", "ingest_pipeline", {
            "rule_id": entry_id,
            "pipeline_id": batch["pipeline_id"],
            "table_name": record["table_name"],
            "batch_no": batch["batch_no"],
            "actual_value": actual,
            "threshold": threshold,
            "passed": passed,
        })

        if not passed:
            # 出具告警：保留原批次来源
            self._alerts.append({
                "id": len(self._alerts) + 1,
                "rule_id": entry_id,
                "rule_name": record["rule_name"],
                "table_name": record["table_name"],
                "batch_no": batch["batch_no"],
                "pipeline_id": batch["pipeline_id"],
                "metric": record["metric"],
                "threshold": threshold,
                "actual_value": actual,
                "status": "已出具",
                "created_at": checked_at,
            })
            store.log_event("quality_failed_to_lineage", "quality_monitor", "lineage_graph", {
                "rule_id": entry_id,
                "table_name": record["table_name"],
                "batch_no": batch["batch_no"],
                "metric": record["metric"],
                "threshold": threshold,
                "actual": actual,
            })

            # 血缘侧按判定标注待复查的路径（仅本批真实未达标的湖表链路）
            table_name = record["table_name"]
            lineages = store.find_by_field("lineage_graph", "中间转换", table_name)
            if not lineages:
                lineages = store.find_by_field("lineage_graph", "下游消费", table_name)
            for lin in lineages:
                lin["status"] = "待验证"
                lin["abnormal"] = True
                lin["复查批号"] = batch["batch_no"]
        else:
            store.log_event("quality_passed_to_govern", "quality_monitor", "govern_report", {
                "rule_id": entry_id,
                "table_name": record["table_name"],
                "batch_no": batch["batch_no"],
                "metric": record["metric"],
                "actual": actual,
            })

        entry["pending"] = False

        return record, (
            f"质量检查完成：批次 {batch['batch_no']}，{record['metric']}={actual}，"
            f"阈值={threshold}，{verdict}"
        )

    def check_quality(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """执行质量检查（启动并立即完成）：按锁定批次行数与阈值生成判定。"""
        record, msg = self.start_check(entry_id)
        if record is None:
            return None, msg
        return self.finish_check(entry_id)

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "启用监控":
            return self.enable_monitor(entry_id)
        return super().run_action(entry_id, action)
