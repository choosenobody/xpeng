#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
xpeng_alert_bot.py  (v2.2.1)
增强版：增加对读取 Excel 的安全检查，避免在 CI 环境中遇到损坏或非 xlsx 文件时崩溃。

主要改动：
1. 引入 read_excel_safe()：读取 Excel 前检查文件头是否为 ZIP/Microsoft Office 格式（以 PK 开头）。
2. 在 main() 中使用 read_excel_safe 读取 KPI_Monitor 工作表；若检查失败则发送提示并优雅退出。
3. 保持其他逻辑不变。

原始脚本功能：
1) 可选抓取实时股价(Yahoo Finance) → 回写 Excel 的 Assumptions.Current Price
2) 读取 Summary/Base IV 与 KPI_Monitor → 生成交易信号
3) 记录状态：
   - 文本：status_log.csv（易审计，推荐）
   - Excel：附加工作表 Status_Log（便于汇总）
4) 发送 Telegram 通知
"""

import os, sys, re, csv, time, math, datetime
from typing import Optional, Tuple
import pandas as pd
import numpy as np

from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

# ---------- 工具函数：安全读取 Excel ----------
def _read_head(path: str, n: int = 256) -> bytes:
    """读取文件头前 n 字节。"""
    with open(path, "rb") as f:
        return f.read(n)

def _diagnose_not_xlsx(head: bytes) -> str:
    """
    根据文件头诊断当前文件为何不是合法的 xlsx。
    返回用户友好提示（中文）。
    """
    text = head.decode("utf-8", "ignore").strip()
    # Git LFS 指针文件通常含有此行
    if "git-lfs.github.com/spec/v1" in text:
        return (
            "检测到文件是 Git LFS 指针，而非实际的 .xlsx 数据。"
            "请在 checkout 时启用 LFS（例如 actions/checkout@v4 中设置 lfs: true）。"
        )
    # HTML 文件头
    if text.lower().startswith("<!doctype html") or text.lower().startswith("<html"):
        return (
            "检测到文件内容像是 HTML（可能下载的是网页或错误页）。"
            "请检查下载路径是否正确，并确保已跟随重定向。"
        )
    # 默认提示
    return (
        "无法识别的 Excel 文件格式，可能已损坏或被其它内容覆盖。"
        "请检查 CI 下载/上传流程或重新生成该文件。"
    )

def read_excel_safe(xlsx_path: str, sheet: str) -> pd.DataFrame:
    """
    安全读取 Excel 指定工作表。若文件头不是合法的 zip/xlsx，则抛出更易理解的异常。

    :param xlsx_path: Excel 文件路径
    :param sheet: 工作表名称
    :return: pandas.DataFrame
    """
    # 检查是否存在
    if not os.path.exists(xlsx_path):
        raise FileNotFoundError(f"Excel 文件不存在：{xlsx_path}")
    # 读取前两个字节，判断是否为 zip (PK 头)
    head2 = _read_head(xlsx_path, 2)
    if head2 != b"PK":
        head = _read_head(xlsx_path, 256)
        tip = _diagnose_not_xlsx(head)
        raise ValueError(f"文件 '{xlsx_path}' 不是有效的 .xlsx：{tip}")
    # 安全读取
    return pd.read_excel(xlsx_path, sheet_name=sheet, engine="openpyxl")

def fetch_live_price(symbol: str, price_field: str = "Close") -> Optional[float]:
    try:
        import yfinance as yf
        t = yf.Ticker(symbol)
        df = t.history(period="1d")
        if df.shape[0] == 0:
            return None
        field = price_field if price_field in df.columns else "Close"
        px = float(df[field].iloc[-1])
        if np.isnan(px):
            return None
        return px
    except Exception:
        return None

def update_assumptions_price(xlsx_path: str, new_price: float) -> None:
    wb = load_workbook(xlsx_path)
    ws = wb["Assumptions"]
    # 找到 Item == "Current Price" 的行，更新 Value 列
    headers = [ws.cell(1, c).value for c in range(1, ws.max_column+1)]
    item_idx = headers.index("Item")+1
    val_idx  = headers.index("Value")+1
    found = False
    for r in range(2, ws.max_row+1):
        if str(ws.cell(r, item_idx).value).strip() == "Current Price":
            ws.cell(r, val_idx, float(new_price))
            found = True
            break
    if not found:
        # 若没找到则追加一行
        ws.append(["Current Price", float(new_price), "HKD", "auto-updated"])
    # 确保有 Status_Log 工作表
    if "Status_Log" not in wb.sheetnames:
        wb.create_sheet("Status_Log")
        wsl = wb["Status_Log"]
        wsl.append([
            "timestamp_utc","price_hkd","base_iv_hkd","discount_pct",
            "ok_vehicle_gm","ok_fcf","ok_techsvc","ok_robotics",
            "kpi_pass","signal","rating_upgrade"
        ])
    wb.save(xlsx_path)

def compute_wacc(rf, erp, beta, tax, debt_ratio, pre_tax_cost_debt):
    ke = rf + beta * erp
    kd_after = pre_tax_cost_debt * (1 - tax)
    return ke * (1 - debt_ratio) + kd_after * debt_ratio

def project_revenue_series(start_rev, cagr, n_years=10):
    return [start_rev * ((1 + cagr) ** i) for i in range(1, n_years+1)]

def dcf_base_iv(xlsx_path: str) -> Optional[float]:
    try:
        xls = pd.ExcelFile(xlsx_path)
        A = pd.read_excel(xls, "Assumptions")
        R = pd.read_excel(xls, "Start_Rev_2025")
        S = pd.read_excel(xls, "Scenarios")
        amap = dict(zip(A["Item"], A["Value"]))
        rf=float(amap.get("Risk-Free Rate (Rf)",0.0181))
        erp=float(amap.get("Equity Risk Premium (ERP)",0.059))
        beta=float(amap.get("Beta",1.04))
        tax=float(amap.get("Tax Rate",0.25))
        d_ratio=float(amap.get("Target Debt Ratio (D/(D+E))",0.10))
        kd_pre=float(amap.get("Pre-tax Cost of Debt",0.045))
        g=float(amap.get("Terminal Growth (g)",0.02))
        s2c=float(amap.get("Sales-to-Capital",2.5))
        shares=float(amap.get("Share Count (bn)",1.909771413))
        net_cash=float(amap.get("Net Cash (bn)",39.9))
        start_rev=float(R["Value"].iloc[0])
        wacc = compute_wacc(rf, erp, beta, tax, d_ratio, kd_pre)
        base_df = S[S["Scenario"]=="Base"].copy()
        rev_cagr = float(base_df["Rev_CAGR"].iloc[0])
        ebit_path = base_df["EBIT_margin"].values.astype(float)
        rev = np.array(project_revenue_series(start_rev, rev_cagr, n_years=len(ebit_path)))
        ebit = rev * ebit_path
        nopat = ebit * (1 - tax)
        reinv = (rev * rev_cagr) / max(1e-6, s2c)
        fcff = nopat - reinv
        years = np.arange(1, len(fcff)+1)
        disc = (1 + wacc) ** years
        pv_fcff = float(np.sum(fcff / disc))
        tv = float((fcff[-1] * (1 + g)) / (wacc - g))
        pv_tv = float(tv / ((1+wacc)**len(fcff)))
        ev = pv_fcff + pv_tv
        equity = ev + net_cash
        per_share = (equity * 1e9) / (shares * 1e9)
        return float(per_share)
    except Exception:
        return None

def kpi_flags(K: pd.DataFrame) -> Tuple[bool,bool,bool,bool,bool,int]:
    def take(metric):
        row = K[K["Metric"]==metric]
        return None if row.empty else row.iloc[0]
    r_gm = take("Vehicle GM (%)")
    r_fcf = take("FCF (TTM, bn HKD)")
    r_ts  = take("Tech/Service Rev Share (%)")
    r_rb  = take("Robotics Rev Share (%)")
    def pass_ge(row, default):
        if row is None: return False
        latest = float(row["Latest"])
        tgt = str(row["Target/Threshold"])
        m = re.search(r"(-?\d+(\.\d+)?)", tgt)
        target_num = float(m.group(1)) if m else default
        return latest >= target_num
    ok_gm  = pass_ge(r_gm, 15)
    ok_fcf = pass_ge(r_fcf, 0)
    ok_ts  = pass_ge(r_ts, 10)
    ok_rb  = pass_ge(r_rb, 5) if r_rb is not None else False
    ok_rt  = ok_rb or ok_ts
    kpi_pass = sum([ok_gm, ok_fcf, ok_rt])
    return ok_gm, ok_fcf, ok_ts, ok_rb, ok_rt, kpi_pass

def append_logs(xlsx_path, price, base_iv, ok_gm, ok_fcf, ok_ts, ok_rb, ok_rt, kpi_pass, signal, rating_up):
    ts_utc = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    discount = (price/base_iv - 1.0)*100 if (base_iv and base_iv==base_iv and base_iv>0) else np.nan
    row = {
        "timestamp_utc": ts_utc,
        "price_hkd": round(price, 4) if price==price else "",
        "base_iv_hkd": round(base_iv, 4) if base_iv==base_iv else "",
        "discount_pct": round(discount, 3) if discount==discount else "",
        "ok_vehicle_gm": int(bool(ok_gm)),
        "ok_fcf": int(bool(ok_fcf)),
        "ok_techsvc": int(bool(ok_ts)),
        "ok_robotics": int(bool(ok_rb)),
        "kpi_pass": int(kpi_pass),
        "signal": signal,
        "rating_upgrade": int(bool(rating_up))
    }
    csv_path = "status_log.csv"
    write_header = not os.path.exists(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if write_header:
            w.writeheader()
        w.writerow(row)
    try:
        wb = load_workbook(xlsx_path)
        ws = wb["Status_Log"] if "Status_Log" in wb.sheetnames else wb.create_sheet("Status_Log")
        if ws.max_row == 1 and ws.cell(1,1).value != "timestamp_utc":
            ws.append(["timestamp_utc","price_hkd","base_iv_hkd","discount_pct",
                       "ok_vehicle_gm","ok_fcf","ok_techsvc","ok_robotics",
                       "kpi_pass","signal","rating_upgrade"])
        ws.append([ts_utc, price, base_iv, discount, int(bool(ok_gm)), int(bool(ok_fcf)),
                   int(bool(ok_ts)), int(bool(ok_rb)), int(kpi_pass), signal, int(bool(rating_up))])
        wb.save(xlsx_path)
    except Exception:
        pass

def send_telegram(text: str):
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID 未配置；仅打印：\n"+text)
        return
    import urllib.request, urllib.parse
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown"
    }).encode("utf-8")
    with urllib.request.urlopen(url, data=data, timeout=20) as r:
        r.read()

def main(xlsx_path: str):
    # 1) 读取/抓价并写回 Excel
    live = os.environ.get("LIVE_PRICE","1") == "1"
    symbol = os.environ.get("YF_SYMBOL","9868.HK")
    price_field = os.environ.get("PRICE_FIELD","Close")
    price_live = fetch_live_price(symbol, price_field) if live else None
    # 读取 Assumptions
    try:
        xls = pd.ExcelFile(xlsx_path)
        A = pd.read_excel(xls, "Assumptions")
    except Exception as e:
        # Excel 损坏也会在此抛出
        msg = f"📉 无法读取 Excel 文件 `{xlsx_path}`：{e}"
        send_telegram(msg)
        return 0
    amap = dict(zip(A["Item"], A["Value"]))
    price = price_live if (price_live is not None) else float(amap.get("Current Price", 0))
    if price_live is not None:
        update_assumptions_price(xlsx_path, price)
    # 2) Base IV：优先读 Summary；缺失时用 DCF 兜底
    try:
        S = pd.read_excel(xls, "Summary")
        base_row = S[S["Scenario"]=="Base"]
        base_iv = float(base_row["IV_HKD_per_share"].values[0]) if not base_row.empty else None
    except Exception:
        base_iv = None
    if (base_iv is None) or (base_iv != base_iv):
        base_iv = dcf_base_iv(xlsx_path)
    # 3) KPI & “机器人/技术服务”达标（使用安全读取）
    try:
        K = read_excel_safe(xlsx_path, "KPI_Monitor")
        ok_gm, ok_fcf, ok_ts, ok_rb, ok_rt, kpi_pass = kpi_flags(K)
    except Exception as e:
        # 读取 KPI 工作表失败：通常是 xlsx 有问题
        msg = f"⚠️ 无法读取 KPI_Monitor：{e}"
        send_telegram(msg)
        return 0
    # 4) 交易信号 & 评级建议
    signal = "观察"
    if base_iv and base_iv==base_iv:
        if price <= 0.80 * base_iv:
            signal = "加仓"
        elif price <= 0.90 * base_iv:
            signal = "建仓"
    rating_up = (kpi_pass >= 2) and ok_rt
    # 5) 记录 & 推送
    append_logs(xlsx_path, price, base_iv, ok_gm, ok_fcf, ok_ts, ok_rb, ok_rt, kpi_pass, signal, rating_up)
    lines = []
    lines.append("*XPENG Monitor*")
    lines.append(f"Symbol: `{symbol}` | Price: HK${price:.2f} | Base IV: {('N/A' if not base_iv else f'HK${base_iv:.2f}')}")
    lines.append(f"KPI — VehicleGM: {'PASS' if ok_gm else 'FAIL'}, FCF: {'PASS' if ok_fcf else 'FAIL'}, Tech/Service: {'PASS' if ok_ts else 'FAIL'}, Robotics: {'PASS' if ok_rb else 'NA'}")
    lines.append(f"Signal: *{signal}*  | KPI≥2 且 机器人/技术服务达标 → {'*评级自动上调建议*' if rating_up else '暂不升级'}")
    send_telegram("\n".join(lines))
    return 0

if __name__=="__main__":
    if len(sys.argv)<2:
        print("Usage: python xpeng_alert_bot.py /path/to/XPeng_Valuation_Monitor_v2.xlsx")
        sys.exit(1)
    sys.exit(main(sys.argv[1]) or 0)
