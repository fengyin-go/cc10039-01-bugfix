"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class DataCatalogService(BaseModuleService):
    MODULE = "data_catalog"
    ENTITY = "目录条目"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["条目名称", "数据层级", "所属湖表", "字段定义", "安全等级"]
    REQUIRED_FIELDS = ["条目名称", "数据层级", "所属湖表"]
    STATUS_ORDER = ["待发布", "已发布", "已过期"]
    ACTION_RULES = {"发布条目": "已发布", "标记过期": "已过期"}
    NEGATIVE_ACTIONS = []
