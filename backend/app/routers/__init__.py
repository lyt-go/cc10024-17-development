"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, snapshot_intake

# snapshot_intake 的固定路径（/api/page_snapshot/intake 等）先于通用
# /api/{module}/{entry_id} 注册，避免被动态路径抢先匹配。
ROUTERS = [snapshot_intake, resource]
