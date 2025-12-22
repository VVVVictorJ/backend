from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from services.stock_service import get_stock_info

router = APIRouter(tags=["stock"])


@router.get("/stock")
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


