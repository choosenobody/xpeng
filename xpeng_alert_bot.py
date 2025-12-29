#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
xpeng_alert_bot.py  (v2.2.2)
修复点（本次重点）：
1) Telegram 推送中对 KPI 的 PASS/FAIL/NA 增加“数值细节”（Latest、阈值/目标、缺失原因），避免只看到 FAIL/NA 却不知道差在哪里。
2) 保留 v2.2 的功能：可选抓取实时股价(yfinance) → 回写 Assumptions.Current Price；读取 Summary/Base IV；读取 KPI_Monitor；记录日志；推送 Telegram。
3) 增强 Excel 读取健壮性：在读取前检查文件是否为合法 .xlsx（ZIP 头 'PK'），并识别 Git LFS 指针/HTML 错误页等常见问题。

环境变量：
- TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID   # 必填(若要推送)
- LIVE_PRICE=1                            # 开启实时股价（默认1）
- YF_SYMBOL=9868.HK                       # 雅虎符号；美股可设 XPEV
- PRICE_FIELD=Close                       # 'Close' 或 'Adj Close'（默认 Close）
- TZ=Asia/Hong_Kong                       # 仅写入日志用

依赖：pandas, openpyxl, yfinance (GitHub Actions 会安装)
"""

import os, sys, re, csv, datetime
from pathlib import Path
from typing import Optional, Tuple, Dict, Any

import pandas as pd
import numpy as np

# ------------------------- Excel 安全检查 & 读取 -------------------------

def _read_head(path: Path, n: int = 256) -> bytes:
    return path.read_bytes()[:n]

def _diagnose_not_xlsx(head: bytes) -> str:
    text = head.decode("utf-8", "ignore").strip()
    if "git-lfs.github.com/spec/v1" in text:
        return (
            "检测到这是 Git LFS 指针文件，而不是实际的 .xlsx 二进制。\n"
            "修复：GitHub Actions 的 actions/checkout 增加 `lfs: true`，并运行 `git lfs pull`。"
        )
    low = text.lower()
    if low.startswith("<!doctype html") or low.startswith("<html"):
        return (
            "检测到文件内容像 HTML（很可能下载到了 404/鉴权/重定向页面），并非 .xlsx。\n"
            "修复：下载时使用 curl -fL，并检查 URL / 权限 / 重定向。"
        )
    return (
        "文件不是有效的 .xlsx（缺少 ZIP 头 'PK'），可能已损坏或被错误内容覆盖。\n"
        "建议：重新生成/上传该 xlsx，或检查 CI 中的下载与缓存流程。"
    )

def ensure_xlsx_ok(xlsx_path: str) -> None:
    p = Path(xlsx_path)
    if not p.exists():
        raise FileNotFoundError(f"Excel 文件不存在：{xlsx_path}")
    head2 = _read_head(p, 2)
    if head2 != b"PK":
        head = _read_head(p, 256)
        raise ValueError(
            f"Excel 文件不是有效 .xlsx：{xlsx_path}\n{_diagnose_not_xlsx(head)}\n"
            f"文件大小：{p.stat().st_size} bytes"
        )

def read_sheet_safe(xlsx_path: str, sheet: str) -> pd.DataFrame:
    """
    单次读取某工作表：在读取前验证文件头，避免 BadZipFile 之类“信息不充分”的错误。
    """
    ensure_xlsx_ok(xlsx_path)
    return pd.read_excel(xlsx_path, sheet_name=sheet, engine="openpyxl")

# ------------------------- Yahoo 价格 -------------------------

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

# ------------------------- Excel 读写 -------------------------

from openpyxl import load_workbook

def update_assumptions_price(xlsx_path: str, new_price: float) -> None:
    wb = load_workbook(xlsx_path)
    ws = wb["Assumptions"]
    headers = [ws.cell(1, c).value for c in range(1, ws.max_column + 1)]
    item_idx = headers.index("Item") + 1
    val_idx  = headers.index("Value") + 1
    found = False
    for r in range(2, ws.max_row + 1):
        if str(ws.cell(r, item_idx).value).strip() == "Current Price":
            ws.cell(r, val_idx, float(new_price))
            found = True
            break
    if not found:
        ws.append(["Current Price", float(new_price), "HKD", "auto-updated"])

    if "Status_Log" not in wb.sheetnames:
        wb.create_sheet("Status_Log")
        wsl = wb["Status_Log"]
        wsl.append([
            "timestamp_utc","price_hkd","base_iv_hkd","discount_pct",
            "ok_vehicle_gm","ok_fcf","ok_techsvc","ok_robotics",
            "kpi_pass","signal","rating_upgrade"
        ])
    wb.save(xlsx_path)

# ------------------------- DCF 兜底（若 Summary 缺失） -------------------------

def compute_wacc(rf, erp, beta, tax, debt_ratio, pre_tax_cost_debt):
    ke = rf + beta * erp
    kd_after = pre_tax_cost_debt * (1 - tax)
    return ke * (1 - debt_ratio) + kd_after * debt_ratio

def project_revenue_series(start_rev, cagr, n_years=10):
    return [start_rev * ((1 + cagr) ** i) for i in range(1, n_years+1)]

def dcf_base_iv(xlsx_path: str) -> Optional[float]:
    try:
        A = read_sheet_safe(xlsx_path, "Assumptions")
        R = read_sheet_safe(xlsx_path, "Start_Rev_2025")
        S = read_sheet_safe(xlsx_path, "Scenarios")
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

# ------------------------- KPI 解析（增强：输出 Latest/Target/原因） -------------------------

def _to_float(x) -> Optional[float]:
    try:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return None
        s = str(x).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None

def _parse_target(x, default: Optional[float] = None) -> Optional[float]:
    if x is None:
        return default
    s = str(x)
    m = re.search(r"(-?\d+(\.\d+)?)", s)
    if not m:
        return default
    try:
        return float(m.group(1))
    except Exception:
        return default

def _get_metric_row(K: pd.DataFrame, names: list, contains_keywords: list = None):
    # 1) 精确匹配
    mcol = K["Metric"].astype(str).str.strip()
    for name in names:
        row = K[mcol == str(name).strip()]
        if not row.empty:
            return row.iloc[0]
    # 2) 关键字包含匹配（用于“Robotics”被写成“机器人/智驾机器人”等）
    if contains_keywords:
        for kw in contains_keywords:
            row = K[mcol.str.contains(str(kw), case=False, na=False)]
            if not row.empty:
                return row.iloc[0]
    return None

def _eval_kpi_ge(row, default_target: float) -> Tuple[Optional[bool], Optional[float], Optional[float], str]:
    """
    返回 (ok, latest, target, reason)
    ok: True/False/None(NA)
    """
    if row is None:
        return None, None, None, "KPI_Monitor 未提供该指标行"
    latest = _to_float(row.get("Latest"))
    target = _parse_target(row.get("Target/Threshold"), default_target)

    if latest is None:
        return None, None, target, "Latest 为空/不可解析"
    if target is None:
        return None, latest, None, "Target/Threshold 为空/不可解析"

    ok = bool(latest >= target)
    return ok, float(latest), float(target), ""

def kpi_details(K: pd.DataFrame) -> Dict[str, Any]:
    # 指标名称兼容：按你的 Excel 实际命名可继续加 alias
    gm_row = _get_metric_row(K, ["Vehicle GM (%)", "Vehicle GM", "Vehicle GM%"], ["Vehicle GM", "GM"])
    fcf_row = _get_metric_row(K, ["FCF (TTM, bn HKD)", "FCF (TTM)", "FCF"], ["FCF"])
    ts_row  = _get_metric_row(K, ["Tech/Service Rev Share (%)", "Tech/Service Share (%)", "Tech/Service"], ["Tech", "Service"])
    rb_row  = _get_metric_row(K, ["Robotics Rev Share (%)", "Robotics Share (%)", "Robotics"], ["robot", "机器人", "robotics"])

    ok_gm,  gm_latest, gm_target, gm_reason = _eval_kpi_ge(gm_row, 15)
    ok_fcf, fcf_latest, fcf_target, fcf_reason = _eval_kpi_ge(fcf_row, 0)
    ok_ts,  ts_latest, ts_target, ts_reason = _eval_kpi_ge(ts_row, 10)

    # Robotics 默认阈值 5%；若无该行则 NA
    if rb_row is None:
        ok_rb, rb_latest, rb_target, rb_reason = None, None, 5.0, "KPI_Monitor 未提供 Robotics 指标行"
    else:
        ok_rb, rb_latest, rb_target, rb_reason = _eval_kpi_ge(rb_row, 5)

    # “机器人/技术服务”达标：任一为 True；若两者都 NA，则 NA
    if ok_ts is None and ok_rb is None:
        ok_rt = None
    else:
        ok_rt = bool(ok_ts is True or ok_rb is True)

    # kpi_pass：GM、FCF、(TS or RB) 三项里通过多少项
    tri_ok = []
    if ok_gm is True: tri_ok.append(True)
    if ok_fcf is True: tri_ok.append(True)
    if ok_rt is True: tri_ok.append(True)
    kpi_pass = len(tri_ok)

    return dict(
        ok_gm=ok_gm, gm_latest=gm_latest, gm_target=gm_target, gm_reason=gm_reason,
        ok_fcf=ok_fcf, fcf_latest=fcf_latest, fcf_target=fcf_target, fcf_reason=fcf_reason,
        ok_ts=ok_ts, ts_latest=ts_latest, ts_target=ts_target, ts_reason=ts_reason,
        ok_rb=ok_rb, rb_latest=rb_latest, rb_target=rb_target, rb_reason=rb_reason,
        ok_rt=ok_rt,
        kpi_pass=kpi_pass
    )

def _fmt(x: Optional[float], nd: int = 2) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "NA"
    try:
        return f"{float(x):.{nd}f}"
    except Exception:
        return "NA"

def _pf(ok: Optional[bool]) -> str:
    if ok is True: return "PASS"
    if ok is False: return "FAIL"
    return "NA"

# ------------------------- 状态记录 -------------------------

def append_logs(xlsx_path, price, base_iv, ok_gm, ok_fcf, ok_ts, ok_rb, ok_rt, kpi_pass, signal, rating_up):
    ts_utc = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    discount = (price/base_iv - 1.0)*100 if (base_iv and base_iv==base_iv and base_iv>0) else np.nan

    row = {
        "timestamp_utc": ts_utc,
        "price_hkd": round(price, 4) if price==price else "",
        "base_iv_hkd": round(base_iv, 4) if base_iv==base_iv else "",
        "discount_pct": round(discount, 3) if discount==discount else "",
        "ok_vehicle_gm": int(ok_gm is True),
        "ok_fcf": int(ok_fcf is True),
        "ok_techsvc": int(ok_ts is True),
        "ok_robotics": int(ok_rb is True),
        "kpi_pass": int(kpi_pass),
        "signal": signal,
        "rating_upgrade": int(rating_up is True)
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
        ws.append([ts_utc, price, base_iv, discount,
                   int(ok_gm is True), int(ok_fcf is True),
                   int(ok_ts is True), int(ok_rb is True),
                   int(kpi_pass), signal, int(rating_up is True)])
        wb.save(xlsx_path)
    except Exception:
        pass

# ------------------------- Telegram -------------------------

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

# ------------------------- 主流程 -------------------------

def main(xlsx_path: str):
    try:
        ensure_xlsx_ok(xlsx_path)
    except Exception as e:
        send_telegram(f"📉 XPENG Monitor：Excel 文件不可用\n\n{e}")
        return 0

    # 1) 读取 Assumptions 获取 Current Price（可被实时价格覆盖）
    try:
        A = read_sheet_safe(xlsx_path, "Assumptions")
        amap = dict(zip(A["Item"], A["Value"]))
    except Exception as e:
        send_telegram(f"📉 XPENG Monitor：读取 Assumptions 失败\n\n{e}")
        return 0

    # 2) 可选抓取实时股价并写回
    live = os.environ.get("LIVE_PRICE","1") == "1"
    symbol = os.environ.get("YF_SYMBOL","9868.HK")
    price_field = os.environ.get("PRICE_FIELD","Close")
    price_live = fetch_live_price(symbol, price_field) if live else None
    price = float(price_live) if (price_live is not None) else float(amap.get("Current Price", 0))

    if price_live is not None:
        try:
            update_assumptions_price(xlsx_path, price)
        except Exception as e:
            # 写回失败不致命：继续跑，但给出提醒
            send_telegram(f"⚠️ XPENG Monitor：实时价格写回 Assumptions 失败（不影响本次信号计算）\n\n{e}")

    # 3) Base IV：优先读 Summary；缺失时用 DCF 兜底
    base_iv = None
    try:
        S = read_sheet_safe(xlsx_path, "Summary")
        base_row = S[S["Scenario"]=="Base"]
        base_iv = float(base_row["IV_HKD_per_share"].values[0]) if not base_row.empty else None
    except Exception:
        base_iv = None
    if (base_iv is None) or (base_iv != base_iv):
        base_iv = dcf_base_iv(xlsx_path)

    # 4) KPI 读取 + 解析细节
    try:
        K = read_sheet_safe(xlsx_path, "KPI_Monitor")
    except Exception as e:
        send_telegram(f"📉 XPENG Monitor：读取 KPI_Monitor 失败\n\n{e}")
        return 0

    kd = kpi_details(K)
    ok_gm, ok_fcf, ok_ts, ok_rb, ok_rt, kpi_pass = (
        kd["ok_gm"], kd["ok_fcf"], kd["ok_ts"], kd["ok_rb"], kd["ok_rt"], kd["kpi_pass"]
    )

    # 5) 交易信号 & 评级建议
    signal = "观察"
    if base_iv and base_iv==base_iv and base_iv > 0:
        if price <= 0.80 * base_iv:
            signal = "加仓"
        elif price <= 0.90 * base_iv:
            signal = "建仓"

    rating_up = (kpi_pass >= 2) and (ok_rt is True)

    # 6) 记录
    append_logs(xlsx_path, price, base_iv, ok_gm, ok_fcf, ok_ts, ok_rb, ok_rt, kpi_pass, signal, rating_up)

    # 7) 生成更“可解释”的 Telegram 内容
    ts_utc = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    if base_iv and base_iv==base_iv and base_iv > 0:
        premium_pct = (price / base_iv - 1.0) * 100
        iv_line = f"Base IV: HK${base_iv:.2f} | 溢价: {premium_pct:+.1f}%"
    else:
        iv_line = "Base IV: N/A"

    lines = []
    lines.append("*XPENG Monitor*")
    lines.append(f"Time: {ts_utc}")
    lines.append(f"Symbol: `{symbol}` | Price: HK${price:.2f}")
    lines.append(iv_line)
    lines.append(f"Signal: *{signal}* | KPI通过数: {kpi_pass}/3 | 评级建议: {'*上调*' if rating_up else '暂不升级'}")
    lines.append("")
    lines.append("*KPI 细节（Latest vs 阈值 → 结论）*")

    # Vehicle GM
    if kd["ok_gm"] is None:
        lines.append(f"- Vehicle GM (%): NA（{kd['gm_reason']}）")
    else:
        lines.append(f"- Vehicle GM (%): {_fmt(kd['gm_latest'])} vs ≥{_fmt(kd['gm_target'])} → {_pf(kd['ok_gm'])}")

    # FCF
    if kd["ok_fcf"] is None:
        lines.append(f"- FCF (TTM, bn HKD): NA（{kd['fcf_reason']}）")
    else:
        lines.append(f"- FCF (TTM, bn HKD): {_fmt(kd['fcf_latest'])} vs ≥{_fmt(kd['fcf_target'])} → {_pf(kd['ok_fcf'])}")

    # Tech/Service
    if kd["ok_ts"] is None:
        lines.append(f"- Tech/Service Rev Share (%): NA（{kd['ts_reason']}）")
    else:
        lines.append(f"- Tech/Service Rev Share (%): {_fmt(kd['ts_latest'])} vs ≥{_fmt(kd['ts_target'])} → {_pf(kd['ok_ts'])}")

    # Robotics
    if kd["ok_rb"] is None:
        # 这里把“NA 的原因”讲清楚，并给出“怎么补齐数据”的指引
        lines.append(f"- Robotics Rev Share (%): NA（{kd['rb_reason']}；建议在 KPI_Monitor 新增 Metric='Robotics Rev Share (%)' 行）")
    else:
        lines.append(f"- Robotics Rev Share (%): {_fmt(kd['rb_latest'])} vs ≥{_fmt(kd['rb_target'])} → {_pf(kd['ok_rb'])}")

    # 机器人/技术服务综合
    if ok_rt is None:
        rt_line = "NA（Tech/Service 与 Robotics 均缺失）"
    else:
        rt_line = _pf(ok_rt)
    lines.append(f"- 机器人/技术服务综合（任一PASS即PASS）：{rt_line}")

    send_telegram("\n".join(lines))
    return 0

if __name__=="__main__":
    if len(sys.argv)<2:
        print("Usage: python xpeng_alert_bot.py /path/to/XPeng_Valuation_Monitor_v2.xlsx")
        sys.exit(1)
    sys.exit(main(sys.argv[1]))
