import asyncio
from typing import Any, Dict, List, Optional

import httpx
import pandas as pd
from starlette.concurrency import run_in_threadpool

# 东方财富单只行情接口与字段映射（参考 debug_single_stock.py）
EM_SINGLE_URL = "https://push2.eastmoney.com/api/qt/stock/get"
EM_SINGLE_FIELDS = "f57,f58,f43,f170,f50,f162,f167,f191,f137"
EM_MAP: Dict[str, str] = {
    "f57": "代码",
    "f58": "名称",
    "f43": "最新价",
    "f170": "涨跌幅",
    "f50": "量比",
    "f162": "市盈率-动态",
    "f167": "市净率",
    "f191": "委比",
    "f137": "主力净流入",
}


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
    async with httpx.AsyncClient(headers={"Accept-Encoding": "gzip"}, timeout=10) as client:
        resp = await client.get(EM_SINGLE_URL, params=params)
        resp.raise_for_status()
        j = resp.json()
    data = (j or {}).get("data") or {}
    if raw_only:
        return {"source": "em", "code": code, "data": j}
    return {"source": "em", "code": code, "data": _compute_em_metrics(data)}


def _fetch_ak_single_row_sync(code: str) -> pd.DataFrame:
    # 延迟导入，避免在未使用 AK 时引入开销
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
        return {"source": "ak", "code": code, "data": {}, "message": "AkShare 无该代码数据"}

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


async def get_stock_info(code: str, source: str = "em", raw_only: bool = False) -> Dict[str, Any]:
    source = (source or "em").lower()
    if source == "ak":
        return await fetch_ak_single(code, raw_only=raw_only)
    # 默认 em
    return await fetch_em_single(code, raw_only=raw_only)


