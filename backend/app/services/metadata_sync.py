"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class MetadataSyncService(BaseModuleService):
    MODULE = "metadata_sync"
    ENTITY = "同步任务"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["任务名称", "源端类型", "目标端类型", "同步频率", "最后同步时间"]
    REQUIRED_FIELDS = ["任务名称", "源端类型", "目标端类型"]
    STATUS_ORDER = ["待同步", "同步中", "同步成功", "同步失败"]
    ACTION_RULES = {"手动同步": "同步中", "跳过同步": "同步失败"}
    NEGATIVE_ACTIONS = []
