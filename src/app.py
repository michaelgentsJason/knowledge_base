"""整个服务的入口文件"""
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# 导入路由
from src.router.hotspot.router import router as hotspot_router
# from src.router.filekb.router import router as filekb_router  # 知识库路由（待实现）

# 导入Redis客户端
from src.dbs.redis_stack.init import redis_client

@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用程序的生命周期管理器"""
    # 启动时的初始化
    print("🚀 应用启动中...")

    # 可以在这里初始化数据库连接、缓存等
    # await init dbs
    try:
        # 异步ping测试Redis连接
        await redis_client.get_client().ping()
        print("✅ Redis连接测试成功")
    except Exception as e:
        print(f"❌ Redis连接测试失败: {e}")
        # 可以选择是否在连接失败时退出应用
        # raise e

    # await mongo_client.init_client()  # MongoDB暂时不用管

    yield

    # 关闭时的清理工作
    print("🛑 应用关闭中...")
    try:
        await redis_client.get_client().close()
        print("✅ Redis连接已关闭")
    except Exception as e:
        print(f"⚠️ Redis连接关闭时出现问题: {e}")


app = FastAPI(
    title="企业AI服务平台",
    description="提供热点问题管理和知识库服务",
    version="1.0.0",
    lifespan=lifespan
)

# 添加CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境中应该指定具体的源
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app.include_router(hotspot_router, tags=['销售平台接口'])
# app.include_router(filekb_router, tags=['培训平台接口'])  # 知识库路由（待实现）

# 根路径接口
@app.get("/", summary="服务根路径")
async def root():
    """
    服务根路径，返回基本信息
    """
    return {
        "service": "企业AI服务平台",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "hotspot": "/hotspot",
            "docs": "/docs",
            "health": "/hotspot/health"
        }
    }

# 全局健康检查
@app.get("/health", summary="全局健康检查")
async def global_health():
    """
    全局健康检查接口
    """
    return {
        "status": "healthy",
        "services": {
            "hotspot": "active",
            "filekb": "pending"  # 知识库服务待实现
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8060,
        log_level="info"
    )