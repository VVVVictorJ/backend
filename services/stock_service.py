import asyncio
from typing import Any, Dict, List, Optional

import httpx
import pandas as pd
from starlette.concurrency import run_in_threadpool

# 东方财富列表接口（用于获取候选代码）
EM_LIST_URL = "https://push2.eastmoney.com/api/qt/clist/get"
EM_LIST_FS = "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23"  # 上A+科创, 深A+创业板
EM_LIST_FIELDS = "f12,f14,f15,f3,f10,f8"

# 东方财富单只行情接口与字段映射（参考 debug_single_stock.py）
EM_SINGLE_URL = "https://push2.eastmoney.com/api/qt/stock/get"
EM_SINGLE_FIELDS = "f57,f58,f43,f170,f50,f168,f191,f137"
EM_MAP: Dict[str, str] = {
    "f57": "代码",
    "f58": "名称",
    "f43": "最新价",
    "f170": "涨跌幅",
    "f50": "量比",
    "f168": "换手率",
    "f191": "委比",
    "f137": "主力净流入",
}


"""
@input:
  - code: 股票代码
@output:
  - str: 股票代码
@description: 将股票代码转换为东方财富secid
@logic: 
  1. 如果股票代码以6开头,则返回f"1.{code}"
  2. 否则返回f"0.{code}"
"""


def code_to_secid(code: str) -> str:
    code = code.strip()
    if code.startswith("6"):
        return f"1.{code}"
    return f"0.{code}"


def _normalize_percent_like(value: Any) -> float:
    if value is None:
        return float("nan")
    s = str(value)
    if "%" in s:
        return pd.to_numeric(s.replace("%", ""), errors="coerce")
    return pd.to_numeric(s, errors="coerce")


def _compute_em_metrics(data: Dict[str, Any]) -> Dict[str, Any]:
    parsed = {EM_MAP[k]: data.get(k) for k in EM_MAP.keys()}

    lb_num = _normalize_percent_like(parsed.get("量比"))
    if pd.notna(lb_num) and abs(lb_num) > 100:
        liangbi_pct = lb_num / 100.0
    else:
        liangbi_pct = lb_num

    wb_num = _normalize_percent_like(parsed.get("委比"))
    if pd.notna(wb_num) and abs(wb_num) > 100:
        wb_num = wb_num / 100.0

    parsed["量比(%)"] = liangbi_pct
    parsed["委比(%)"] = wb_num
    return parsed


async def fetch_em_single(code: str, raw_only: bool = False) -> Dict[str, Any]:
    secid = code_to_secid(code)
    params = {
        "secid": secid,
        "fields": EM_SINGLE_FIELDS,
        "fltt": 2,
        "invt": 2,
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
    }
    async with httpx.AsyncClient(
        headers={"Accept-Encoding": "gzip"}, timeout=10
    ) as client:
        resp = await client.get(EM_SINGLE_URL, params=params)
        resp.raise_for_status()
        j = resp.json()
    data = (j or {}).get("data") or {}
    # 按需求返回原始 f 字段（如 f57、f58 等）
    # raw_only=true 时同样返回 data 段的原始结构,保持结构一致
    return {"source": "em", "code": code, "data": data}


def _fetch_ak_single_row_sync(code: str) -> pd.DataFrame:
    # 延迟导入,避免在未使用 AK 时引入开销
    import akshare as ak  # type: ignore

    df = ak.stock_zh_a_spot_em()
    # 标准化涨跌幅为数值
    if "涨跌幅" in df.columns:
        df["涨跌幅"] = pd.to_numeric(
            df["涨跌幅"].astype(str).str.replace("%", "", regex=False),
            errors="coerce",
        )
    if "涨跌额" in df.columns:
        df["涨跌额"] = pd.to_numeric(df["涨跌额"], errors="coerce")
    return df[df["代码"].astype(str) == code]


async def fetch_ak_single(code: str, raw_only: bool = False) -> Dict[str, Any]:
    df = await run_in_threadpool(_fetch_ak_single_row_sync, code)
    if raw_only:
        if df.empty:
            row = {}
        else:
            row = df.iloc[0].to_dict()
        return {"source": "ak", "code": code, "data": row}

    if df.empty:
        return {
            "source": "ak",
            "code": code,
            "data": {},
            "message": "AkShare 无该代码数据",
        }

    show_cols: List[str] = [
        c
        for c in [
            "代码",
            "名称",
            "最新价",
            "涨跌幅",
            "涨跌额",
            "成交额",
            "成交量",
            "换手率",
            "量比",
            "市盈率-动态",
            "市净率",
            "总市值",
            "流通市值",
            "今开",
            "昨收",
            "最高",
            "最低",
        ]
        if c in df.columns
    ]
    row = df.iloc[0]
    picked = {c: (None if pd.isna(row[c]) else row[c]) for c in show_cols}
    return {"source": "ak", "code": code, "data": picked}


async def get_stock_info(
    code: str, source: str = "em", raw_only: bool = False
) -> Dict[str, Any]:
    source = (source or "em").lower()
    if source == "ak":
        return await fetch_ak_single(code, raw_only=raw_only)
    # 默认 em
    return await fetch_em_single(code, raw_only=raw_only)


# ========== 批量筛选（参考 getInfoModule/filtered_stock_details.py 与 em_all_stocks.py） ==========
def _normalize_percent_like_series(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.replace("%", "", regex=False)
    return pd.to_numeric(s, errors="coerce")


def _em_percent_rule_series(series: pd.Series) -> pd.Series:
    num = _normalize_percent_like_series(series)
    return num.where(num.abs() <= 100, num / 100.0)


def _normalize_list_display(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if "量比" in df.columns:
        df["量比"] = _em_percent_rule_series(df["量比"])
    if "涨跌幅" in df.columns:
        df["涨跌幅"] = _em_percent_rule_series(df["涨跌幅"])
    if "换手率" in df.columns:
        df["换手率"] = _em_percent_rule_series(df["换手率"])
    return df


"""
@input:
  - df: 股票数据DataFrame
@output:
  - pd.DataFrame: 股票数据DataFrame
@description: 筛选符合量比>5%、换手率>1%、涨幅(2%,5%)条件的股票
@logic: 
  1. 如果df为空,则返回空DataFrame
  2. 如果df的列中不包含"量比","换手率","涨跌幅",则返回空DataFrame
  3. 返回符合量比>5%、换手率>1%、涨幅(2%,5%)条件的股票数据DataFrame
"""


def _filter_candidates(df: pd.DataFrame) -> pd.DataFrame:
    """
    条件:量比>5%、换手率>1%、涨幅在(2%,5%)
    注意:上述三列需已被 _normalize_list_display 归一化为数值百分比:2 表示 2%
    """
    if df.empty:
        return df
    for col in ["量比", "换手率", "涨跌幅"]:
        if col not in df.columns:
            return df.iloc[0:0]
    cond = (
        (df["量比"] > 5.0)
        & (df["换手率"] > 1.0)
        & (df["涨跌幅"] > 2.0)
        & (df["涨跌幅"] < 5.0)
    )
    return df.loc[cond].copy()


async def _em_list_fetch_page(
    client: httpx.AsyncClient, pn: int, pz: int
) -> Dict[str, Any]:
    params = {
        "pn": pn,
        "pz": pz,
        "po": 1,
        "np": 1,
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": 2,
        "invt": 2,
        "fid": "f3",
        "fs": EM_LIST_FS,
        "fields": EM_LIST_FIELDS,
    }
    r = await client.get(EM_LIST_URL, params=params, timeout=10)
    r.raise_for_status()
    return r.json()


def _em_list_payload_to_df(payload: Dict[str, Any]) -> pd.DataFrame:
    data = (payload or {}).get("data") or {}
    diff = data.get("diff") or []
    rows = []
    for item in diff:
        row = {
            "代码": item.get("f12"),
            "名称": item.get("f14"),
            "最新价": item.get("f15"),
            "涨跌幅": item.get("f3"),
            "量比": item.get("f10"),
            "换手率": item.get("f8"),
        }
        rows.append(row)
    return pd.DataFrame(rows)


"""
@input:
  - concurrency: 并发请求数
  - pz: 每页条数
@output:
  - pd.DataFrame: 股票数据DataFrame
@description: 获取全市场股票数据
@logic: 
  1. 获取第一页股票数据
  2. 获取总页数
  3. 如果总页数<=1,则返回第一页股票数据
  4. 如果总页数>1,则并发请求剩余页数股票数据
  5. 返回股票数据DataFrame
@return:pd.DataFrame
"""


async def _load_list_all_async(concurrency: int = 6, pz: int = 1000) -> pd.DataFrame:
    limits = httpx.Limits(
        max_connections=concurrency, max_keepalive_connections=concurrency
    )
    async with httpx.AsyncClient(
        http2=False, limits=limits, headers={"Accept-Encoding": "gzip"}
    ) as client:
        first = await _em_list_fetch_page(client, pn=1, pz=pz)
        df = _em_list_payload_to_df(first)
        total = ((first.get("data") or {}).get("total")) or len(df)
        pages = (total + pz - 1) // pz
        if pages <= 1:
            return df
        # 并发请求剩余页数股票数据
        sem = asyncio.Semaphore(concurrency)

        async def task(pn: int):
            async with sem:
                return _em_list_payload_to_df(
                    await _em_list_fetch_page(client, pn=pn, pz=pz)
                )

        tasks = [task(pn) for pn in range(2, pages + 1)]
        if tasks:
            others = await asyncio.gather(*tasks, return_exceptions=False)
            if others:
                df = pd.concat([df] + others, ignore_index=True)
        return df


"""
@input:
  - concurrency: 并发请求数
  - pz: 每页条数
@output:
  - List[str]: 股票代码列表
@description: 获取符合量比>5%、换手率>1%、涨幅(2%,5%)条件的股票代码
@logic: 
  1. 获取全市场股票数据
  2. 归一化百分比字段
  3. 筛选符合量比>5%、换手率>1%、涨幅(2%,5%)条件的股票
  4. 返回股票代码列表
"""


async def get_filtered_codes_async(concurrency: int = 6, pz: int = 1000) -> List[str]:
    df = await _load_list_all_async(concurrency=max(1, concurrency), pz=max(100, pz))
    df = _normalize_list_display(df)
    df = _filter_candidates(df)
    if "代码" not in df.columns:
        return []
    codes = df["代码"].dropna().astype(str).tolist()
    return codes


"""
"""


def _compute_display_row_from_em_data(data: Dict[str, Any]) -> Dict[str, Any]:
    # 基于 EM_MAP 取出可读键,并归一化百分比字段
    # row = {EM_MAP[k]: data.get(k) for k in EM_MAP.keys() if k in EM_MAP}
    row = {
        k: data.get(k)
        for k in ["f57", "f58", "f43", "f170", "f50", "f168", "f191", "f137"]
    }
    # 百分比字段:量比/委比/涨跌幅 统一规则:去%后,绝对值>100则/100
    for pct_key in ("f10", "f3"):
        # for pct_key in ("量比", "委比", "涨跌幅"):
        val = _normalize_percent_like(row.get(pct_key))
        if pd.notna(val) and abs(val) > 100:
            val = val / 100.0
        row[pct_key] = val
    # # 数值列转 float
    # for k in ("最新价", "市盈率-动态", "市净率", "主力净流入"):
    #     if k in row and row[k] is not None:
    #         try:
    #             row[k] = float(row[k])
    #         except Exception:
    #             pass
    # 仅保留核心列,贴近 filtered_stock_details.py 的展示
    # keep_keys = ["代码", "名称", "最新价", "涨跌幅", "委比", "主力净流入"]
    keep_keys = ["f57", "f58", "f43", "f170", "f50", "f168", "f191", "f137"]
    return {k: row.get(k) for k in keep_keys if k in row}


"""
@input:
  - concurrency: 并发请求数
  - limit: 最多返回前N条,0表示全部
  - pz: 每页条数
@output:
  - List[Dict[str, Any]]: 股票详情列表
@description: 筛选符合量比>5%、换手率>1%、涨幅(2%,5%)条件的股票
@logic: 
  1. 获取符合量比>5%、换手率>1%、涨幅(2%,5%)条件的股票代码
  2. 并发请求单只接口,返回股票详情
  3. 返回股票详情列表
@return:List[Dict[str, Any]]
"""


async def get_filtered_stock_rows(
    concurrency: int = 8, limit: int = 0, pz: int = 1000
) -> List[Dict[str, Any]]:
    """
    返回筛选后的股票详情列表（单股接口）,字段贴近 filtered_stock_details.py 的展示:
    代码、名称、最新价、涨跌幅、委比、主力净流入
    """
    codes = await get_filtered_codes_async(
        concurrency=max(1, concurrency), pz=max(100, pz)
    )
    if not codes:
        return []
    if limit and limit > 0:
        codes = codes[:limit]

    limits = httpx.Limits(
        max_connections=concurrency, max_keepalive_connections=concurrency
    )
    async with httpx.AsyncClient(
        http2=False, limits=limits, headers={"Accept-Encoding": "gzip"}
    ) as client:
        sem = asyncio.Semaphore(concurrency)

        async def task(code: str):
            async with sem:
                try:
                    secid = code_to_secid(code)
                    params = {
                        "secid": secid,
                        "fields": EM_SINGLE_FIELDS,
                        "fltt": 2,
                        "invt": 2,
                        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                    }
                    r = await client.get(EM_SINGLE_URL, params=params, timeout=10)
                    r.raise_for_status()
                    j = r.json()
                    data = (j or {}).get("data") or {}
                    # return data
                    return _compute_display_row_from_em_data(data)
                except Exception:
                    return {}

        rows = await asyncio.gather(*(task(c) for c in codes))
        # 过滤空行
        return [r for r in rows if r]
