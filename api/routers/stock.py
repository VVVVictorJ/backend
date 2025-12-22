from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from services.stock_service import get_stock_info, get_filtered_stock_rows

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


@router.get("/stocks/filtered")
async def api_get_filtered_stocks(
    concurrency: int = Query(8, ge=1, le=64, description="并发请求数（1-64）"),
    limit: int = Query(0, ge=0, description="最多返回前N条，0表示全部"),
    pz: int = Query(1000, ge=100, le=5000, description="每页条数（拉取全市场列表时使用）"),
) -> JSONResponse:
    """
    获取“已过滤”的股票详情列表（单股接口），规则与 filtered_stock_details.py 对齐：
    - 从东方财富列表接口拉全市场
    - 归一化百分比列后筛选：量比>5%、换手率>1%、涨幅(2%,5%)
    - 对筛选结果逐只调用单股接口，返回精简字段：代码、名称、最新价、涨跌幅、委比、主力净流入
    """
    rows = await get_filtered_stock_rows(concurrency=concurrency, limit=limit, pz=pz)
    return JSONResponse(content={"count": len(rows), "items": rows})

