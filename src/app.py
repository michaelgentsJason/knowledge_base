"""整个服务的入口文件"""
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用程序的生命周期管理器"""
    # await init dbs
    # await mongo_client.init_client()
    yield


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 可根据需要指定允许的源
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(customer_router,tags=['销售平台接口'])
app.include_router(training_router,tags=['培训平台接口'])


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8060) # 容器内端口，容器外端口需要映射