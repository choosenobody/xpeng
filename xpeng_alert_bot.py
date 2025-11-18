#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
xpeng_alert_bot.py  (v2.2)
功能：
1) 可选抓取实时股价(Yahoo Finance) → 回写 Excel 的 Assumptions.Current Price
2) 读取 Summary/Base IV 与 KPI_Monitor → 生成交易信号
3) 记录状态：
   - 文本：status_log.csv（易审计，推荐）
   - Excel：附加工作表 Status_Log（便于汇总）
4) 发送 Telegram 通知

环境变量：
- TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID   # 必填(若要推送)
- LIVE_PRICE=1                            # 开启实时股价（默认1）
- YF_SYMBOL=9868.HK                       # 雅虎符号；美股可设 XPEV
- PRICE_FIELD=Close                       # 'Close' 或 'Adj Close'（默认 Close）
- TZ=Asia/Hong_Kong                       # 仅写入日志用

依赖：pandas, openpyxl, yfinance (GitHub Actions 会安装)
"""

import os, sys, re, csv, time, math, datetime
from typing import Optional
import pandas as pd
import numpy as np

# ---------- Yahoo 价格 ----------
def fetch_live_price(symbol: str, price_field: str = "Close") -> Optional[float]:
    try:
        import yfinance as yf
        t = yf.Ticker(symbol)
        # 用 period=1d 可跨市场；如需日内可考虑 interval="1m"
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

# ---------- Excel 读写 ----------
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

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

# ---------- DCF 兜底（若 Summary 缺失） ----------
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

# ---------- KPI 读取 ----------
def kpi_flags(K: pd.DataFrame):
    def take(metric):
        row = K[K["Metric"]==metric]
        return None if row.empty else row.iloc[0]
    r_gm = take("Vehicle GM (%)")
    r_fcf = take("FCF (TTM, bn HKD)")
    r_ts  = take("Tech/Service Rev Share (%)")
    r_rb  = take("Robotics Rev Share (%)")  # 若你在 Excel 中新增此 KPI，将优先使用

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
    ok_rb  = pass_ge(r_rb, 5) if r_rb is not None else False  # 若给了机器人占比，默认阈值5%
    ok_robot_or_tech = ok_rb or ok_ts
    kpi_pass = sum([ok_gm, ok_fcf, ok_robot_or_tech])
    return ok_gm, ok_fcf, ok_ts, ok_rb, ok_robot_or_tech, kpi_pass

# ---------- 状态记录 ----------
def append_logs(xlsx_path, price, base_iv, ok_gm, ok_fcf, ok_ts, ok_rb, ok_rt, kpi_pass, signal, rating_up):
    ts_utc = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    discount = (price/base_iv - 1.0)*100 if (base_iv and base_iv==base_iv and base_iv>0) else np.nan
    # CSV（文本持久化）
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

    # Excel（可视化）
    try:
        from openpyxl import load_workbook
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

# ---------- Telegram ----------
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

# ---------- 主流程 ----------
def main(xlsx_path: str):
    # 1) 读取/抓价并写回 Excel
    live = os.environ.get("LIVE_PRICE","1") == "1"
    symbol = os.environ.get("YF_SYMBOL","9868.HK")
    price_field = os.environ.get("PRICE_FIELD","Close")
    price_live = fetch_live_price(symbol, price_field) if live else None

    xls = pd.ExcelFile(xlsx_path)
    A = pd.read_excel(xls, "Assumptions")
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

    # 3) KPI & “机器人/技术服务”达标
    K = pd.read_excel(xls, "KPI_Monitor")
    ok_gm, ok_fcf, ok_ts, ok_rb, ok_rt, kpi_pass = kpi_flags(K)

    # 4) 交易信号 & 评级建议
    signal = "观察"
    if base_iv and base_iv==base_iv:
        if price <= 0.80 * base_iv:
            signal = "加仓"
        elif price <= 0.90 * base_iv:
            signal = "建仓"
    rating_up = (kpi_pass >= 2) and ok_rt  # 任两项KPI达标且“机器人/技术服务”达标

    # 5) 记录 & 推送
    append_logs(xlsx_path, price, base_iv, ok_gm, ok_fcf, ok_ts, ok_rb, ok_rt, kpi_pass, signal, rating_up)

    lines = []
    lines.append("*XPENG Monitor*")
    lines.append(f"Symbol: `{symbol}` | Price: HK${price:.2f} | Base IV: {('N/A' if not base_iv else f'HK${base_iv:.2f}')}")
    lines.append(f"KPI — VehicleGM: {'PASS' if ok_gm else 'FAIL'}, FCF: {'PASS' if ok_fcf else 'FAIL'}, Tech/Service: {'PASS' if ok_ts else 'FAIL'}, Robotics: {'PASS' if ok_rb else 'NA'}")
    lines.append(f"Signal: *{signal}*  | KPI≥2 且 机器人/技术服务达标 → {'*评级自动上调建议*' if rating_up else '暂不升级'}")
    send_telegram("\n".join(lines))

if __name__=="__main__":
    if len(sys.argv)<2:
        print("Usage: python xpeng_alert_bot.py /path/to/XPeng_Valuation_Monitor_v2.xlsx"); sys.exit(1)
    main(sys.argv[1])
