from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from services.stock_service import get_stock_info, get_filtered_stock_rows
from services.stock_service_para import get_filtered_stock_rows_by_params

router = APIRouter(tags=["stock"])


@router.get("/stock")
async def api_get_stock(
    code: str = Query(..., description="股票代码,如 600519 或 002415"),
    source: str = Query("em", description="数据来源:em=东方财富,ak=AkShare"),
    raw_only: bool = Query(False, description="是否仅返回原始数据/行"),
) -> JSONResponse:
    """
    查询单只股票信息。
    - EM:返回原始字段(f57、f58、f43、f170、f50、f162、f167、f191、f137 等)
    - AK:返回挑选后的常用列
    """
    data = await get_stock_info(code=code, source=source, raw_only=raw_only)
    return JSONResponse(content=data)


@router.get("/stock/filtered")
async def api_get_filtered_stocks(
    concurrency: int = Query(8, ge=1, le=64, description="并发请求数(1-64)"),
    limit: int = Query(0, ge=0, description="最多返回前N条,0表示全部"),
    pz: int = Query(
        1000, ge=100, le=5000, description="每页条数(拉取全市场列表时使用)"
    ),
) -> JSONResponse:
    """
    获取“已过滤”的股票详情列表(单股接口),规则与 filtered_stock_details.py 对齐:
    - 从东方财富列表接口拉全市场
    - 归一化百分比列后筛选:量比>5%、换手率>1%、涨幅(2%,5%)
    - 对筛选结果逐只调用单股接口,返回精简字段:代码、名称、最新价、涨跌幅、委比、主力净流入
    """
    rows = await get_filtered_stock_rows(concurrency=concurrency, limit=limit, pz=pz)
    return JSONResponse(content={"count": len(rows), "items": rows})


@router.get("/stock/filtered/param")
async def api_get_filtered_stocks_param(
    pct_min: float = Query(2.0, description="涨跌幅最小值(%),默认2"),
    pct_max: float = Query(5.0, description="涨跌幅最大值(%),默认5"),
    lb_min: float = Query(5.0, description="量比最小值(%),默认5"),
    hs_min: float = Query(1.0, description="换手率最小值(%),默认1"),
    wb_min: float = Query(20.0, description="委比最小值(%),默认20"),
    concurrency: int = Query(8, ge=1, le=64, description="并发请求数(1-64)"),
    limit: int = Query(0, ge=0, description="最多返回前N条,0表示全部"),
    pz: int = Query(
        1000, ge=100, le=5000, description="每页条数(拉取全市场列表时使用)"
    ),
) -> JSONResponse:
    """
    自定义参数版本的过滤接口:
    - 列表阶段应用:涨跌幅范围(pct_min~pct_max)、量比最小 lb_min、换手率最小 hs_min
    - 单股阶段应用:委比最小 wb_min
    返回单股接口精简原始 f 字段:f57,f58,f43,f170,f50,f168,f191,f137
    """
    rows = await get_filtered_stock_rows_by_params(
        pct_min=pct_min,
        pct_max=pct_max,
        lb_min=lb_min,
        hs_min=hs_min,
        wb_min=wb_min,
        concurrency=concurrency,
        limit=limit,
        pz=pz,
    )
    return JSONResponse(content={"count": len(rows), "items": rows})
