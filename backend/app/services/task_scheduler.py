"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class TaskSchedulerService(BaseModuleService):
    MODULE = "task_scheduler"
    ENTITY = "调度任务"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["任务名称", "调度表达式", "关联管道", "最大并行度", "任务状态"]
    REQUIRED_FIELDS = ["任务名称", "调度表达式", "关联管道"]
    STATUS_ORDER = ["待激活", "已激活", "已暂停", "调度失败"]
    ACTION_RULES = {"激活任务": "已激活", "暂停任务": "已暂停"}
    NEGATIVE_ACTIONS = []
