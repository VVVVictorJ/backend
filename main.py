import os
from typing import List

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from services.stock_service import get_stock_info

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


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/")
def root() -> dict:
    return {"message": "stockProject FastAPI is running"}


@app.get("")
async def api_get_stock(
    code: str = Query(..., description="股票代码，如 600519 或 002415"),
    source: str = Query("em", description="数据来源：em=东方财富，ak=AkShare"),
    raw_only: bool = Query(False, description="是否仅返回原始数据/行"),
) -> JSONResponse:
    """
    查询单只股票信息。
    - EM：返回映射后的可读字段，并附带量比/委比的百分比字段
    - AK：返回挑选后的常用列
    """
    data = await get_stock_info(code=code, source=source, raw_only=raw_only)
    return JSONResponse(content=data)


if __name__ == "__main__":
    import uvicorn

    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("main:app", host=host, port=port, reload=True)
