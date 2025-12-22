import os
from typing import List

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response

from api.routers.system import router as system_router
from api.routers.stock import router as stock_router

app = FastAPI(title="stockProject API")

# 允许跨域来源：默认允许 Vite 本地开发地址；也可用环境变量 ALLOWED_ORIGINS 配置，逗号分隔
allowed_origins_env = os.getenv("ALLOWED_ORIGINS", "")
if allowed_origins_env:
    allowed_origins: List[str] = [
        o.strip() for o in allowed_origins_env.split(",") if o.strip()
    ]
else:
    allowed_origins = ["http://localhost:5173", "http://127.0.0.1:5173"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# 挂载路由
app.include_router(system_router)
app.include_router(stock_router, prefix="/api")


if __name__ == "__main__":
    import uvicorn

    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("main:app", host=host, port=port, reload=True)
