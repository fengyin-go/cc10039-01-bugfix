"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class AccessAuditService(BaseModuleService):
    MODULE = "access_audit"
    ENTITY = "访问记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["访问者", "目标湖表", "操作类型", "访问时间", "返回记录数"]
    REQUIRED_FIELDS = ["访问者", "目标湖表", "操作类型"]
    STATUS_ORDER = ["已记录", "异常访问"]
    ACTION_RULES = {"标记异常": "异常访问", "忽略告警": "已记录"}
    NEGATIVE_ACTIONS = ["忽略告警"]
