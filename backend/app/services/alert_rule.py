"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class AlertRuleService(BaseModuleService):
    MODULE = "alert_rule"
    ENTITY = "告警规则"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["规则名称", "告警指标", "告警级别", "通知渠道", "生效范围"]
    REQUIRED_FIELDS = ["规则名称", "告警指标", "告警级别"]
    STATUS_ORDER = ["待启用", "已启用", "已关闭"]
    ACTION_RULES = {"启用规则": "已启用", "关闭规则": "已关闭"}
    NEGATIVE_ACTIONS = []
