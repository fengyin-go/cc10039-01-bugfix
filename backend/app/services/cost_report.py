"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class CostReportService(BaseModuleService):
    MODULE = "cost_report"
    ENTITY = "成本报告"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["报告周期", "目标业务域", "存储总量", "计算资源消耗", "成本趋势"]
    REQUIRED_FIELDS = ["报告周期", "目标业务域", "存储总量"]
    STATUS_ORDER = ["计算中", "可用", "已归档"]
    ACTION_RULES = {"生成报告": "可用", "归档报告": "已归档"}
    NEGATIVE_ACTIONS = []
