"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class LoadBalanceService(BaseModuleService):
    MODULE = "load_balance"
    ENTITY = "分片配置"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["湖表名", "当前分片数", "分片策略", "均衡阈值", "最后均衡时间"]
    REQUIRED_FIELDS = ["湖表名", "当前分片数", "分片策略"]
    STATUS_ORDER = ["均衡", "倾斜", "均衡中"]
    ACTION_RULES = {"触发均衡": "均衡中", "调整策略": "均衡"}
    NEGATIVE_ACTIONS = []
