"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, cross_module

# cross_module 的固定前缀 /api/cross 必须先注册，否则其二段路径会被
# resource 的 /api/{module}/{entry_id} 动态路由抢先匹配（entry_id 校验报 422）。
ROUTERS = [cross_module, resource]