"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, pipeline_router

# 流水线固定路径必须排在 /api/{module}/{entry_id} 之前注册，
# 否则 /api/pipeline/snapshot 会被当作模块+条目 ID 抢先匹配。
ROUTERS = [pipeline_router, resource]
