"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class DataDictionaryService(BaseModuleService):
    MODULE = "data_dictionary"
    ENTITY = "字典条目"
    OPERATION_TERM = "标准化"
    LIST_FIELDS = ["字典名称", "适用范围", "值域列表", "版本号", "更新说明"]
    REQUIRED_FIELDS = ["字典名称", "适用范围", "值域列表"]
    STATUS_ORDER = ["草稿", "已发布", "已停用"]
    ACTION_RULES = {"发布字典": "已发布", "停用字典": "已停用"}
    NEGATIVE_ACTIONS = ["停用字典"]
