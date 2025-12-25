from __future__ import annotations

import asyncio
from typing import Any, Dict, List

import httpx
import pandas as pd

# 东方财富列表接口(与 getInfoModule/em_all_stocks.py 一致)
EM_LIST_URL = "https://push2.eastmoney.com/api/qt/clist/get"
EM_LIST_FS = "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23"  # 上A+科创, 深A+创业板
EM_LIST_FIELDS = "f12,f14,f15,f3,f10,f8"  # 代码/名称/最新价/涨跌幅/量比/换手率

# 单股接口(与现有 services 保持一致：返回 f 字段)
EM_SINGLE_URL = "https://push2.eastmoney.com/api/qt/stock/get"
EM_SINGLE_FIELDS = "f57,f58,f43,f170,f50,f168,f191,f137"  # 代码/名称/最新价/涨跌幅/量比/换手率/委比/主力净流入


def _normalize_percent_like_series(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.replace("%", "", regex=False)
    return pd.to_numeric(s, errors="coerce")


def _em_percent_rule_series(series: pd.Series) -> pd.Series:
    """
    归一化百分比:去%后转数字；若绝对值>100则/100(例如 -624 => -6.24)
    """
    num = _normalize_percent_like_series(series)
    return num.where(num.abs() <= 100, num / 100.0)


def _normalize_list_display(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    # 量比
    if "f10" in df.columns:
        df["f10"] = _em_percent_rule_series(df["f10"])
    # 涨跌幅
    if "f3" in df.columns:
        df["f3"] = _em_percent_rule_series(df["f3"])
    # 换手率
    if "f8" in df.columns:
        df["f8"] = _em_percent_rule_series(df["f8"])
    return df


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
        rows.append(
            {
                "f12": item.get("f12"),  # 代码
                "f14": item.get("f14"),  # 名称
                "f15": item.get("f15"),  # 最新价
                "f3": item.get("f3"),  # 涨跌幅
                "f10": item.get("f10"),  # 量比
                "f8": item.get("f8"),  # 换手率
            }
        )
    return pd.DataFrame(rows)


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


def _code_to_secid(code: str) -> str:
    code = (code or "").strip()
    if code.startswith("6"):
        return f"1.{code}"
    return f"0.{code}"


def _normalize_percent_like_scalar(value: Any) -> float:
    if value is None:
        return float("nan")
    s = str(value)
    if "%" in s:
        num = pd.to_numeric(s.replace("%", ""), errors="coerce")
    else:
        num = pd.to_numeric(s, errors="coerce")
    try:
        val = float(num)
    except Exception:
        try:
            val = float(num.item())  # numpy 标量
        except Exception:
            return float("nan")
    if pd.notna(val) and abs(val) > 100:
        return val / 100.0
    return val


def _build_row_from_single_em(data: Dict[str, Any]) -> Dict[str, Any]:
    # 返回精简原始 f 字段(与现有接口保持一致)
    keep_keys = ["f57", "f58", "f43", "f170", "f50", "f168", "f191", "f137"]
    row = {k: data.get(k) for k in keep_keys}
    return row


async def get_filtered_stock_rows_by_params(
    *,
    pct_min: float = 2.0,
    pct_max: float = 5.0,
    lb_min: float = 5.0,
    hs_min: float = 1.0,
    wb_min: float = 20.0,
    concurrency: int = 8,
    limit: int = 0,
    pz: int = 1000,
) -> List[Dict[str, Any]]:
    """
    分两阶段筛选：
    1) 列表接口按 涨跌幅范围/量比最小/换手率最小 过滤出候选代码
    2) 对候选代码拉单股接口，再按 委比最小 过滤
    返回单股接口的精简原始 f 字段
    """
    df = await _load_list_all_async(concurrency=max(1, concurrency), pz=max(100, pz))
    df = _normalize_list_display(df)
    if df.empty:
        return []
    # 过滤出需要的列
    for need in ("f12", "f3", "f10", "f8"):
        if need not in df.columns:
            return []
    # 过滤出符合条件的行
    cond = (
        (df["f3"] > float(pct_min))
        & (df["f3"] < float(pct_max))
        & (df["f10"] > float(lb_min))
        & (df["f8"] > float(hs_min))
    )
    df = df.loc[cond]
    # 提取代码列
    if df.empty or "f12" not in df.columns:
        return []
    codes = df["f12"].dropna().astype(str).tolist()
    if limit and limit > 0:
        codes = codes[:limit]
    if not codes:
        return []

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
                    secid = _code_to_secid(code)
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
                    # 过滤：委比最小值
                    wb_val = _normalize_percent_like_scalar(data.get("f191"))
                    if pd.isna(wb_val) or wb_val < float(wb_min):
                        return {}
                    return _build_row_from_single_em(data)
                except Exception:
                    return {}

        rows = await asyncio.gather(*(task(c) for c in codes))
        return [r for r in rows if r]
