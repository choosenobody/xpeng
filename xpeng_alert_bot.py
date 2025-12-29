#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
xpeng_alert_bot.py  (v2.2.4)

修复点：
1) Telegram 400 Bad Request 不再导致 CI 失败：
   - 默认用 parse_mode=HTML（更稳），动态内容 HTML 转义
   - 发送失败自动降级为纯文本重试
   - 即使两次都失败，也只打印错误并正常退出(0)

2) 保留 v2.2.3：
   - Robotics NA 补齐：优先 KPI_Monitor；缺失则读 ROBOTICS_LATEST/ROBOTICS_TARGET；可选 AUTO_PATCH_KPI=1 写回 KPI_Monitor
   - 标题/字段中文化
   - .xlsx ZIP 头检查，避免 BadZipFile / LFS 指针 / HTML 错页

环境变量：
- TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID   # 必填(若要推送)
- TELEGRAM_PARSE_MODE=HTML              # 默认 HTML；可设为空禁用 parse_mode
- LIVE_PRICE=1                          # 默认1
- YF_SYMBOL=9868.HK
- PRICE_FIELD=Close
- ROBOTICS_LATEST=6.5                   # 机器人收入占比（%），用于补齐缺失
- ROBOTICS_TARGET=5                     # 阈值（%），可选，默认5
- AUTO_PATCH_KPI=1                      # 可选：自动把 Robotics 行写回 KPI_Monitor
"""

import os, sys, re, csv, datetime
from pathlib import Path
from typing import Optional, Tuple, Dict, Any

import pandas as pd
import numpy as np
from openpyxl import load_workbook

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

def maybe_patch_kpi_robotics_row(xlsx_path: str, latest: float, target: float) -> bool:
    if os.environ.get("AUTO_PATCH_KPI", "0") != "1":
        return False
    try:
        wb = load_workbook(xlsx_path)
        if "KPI_Monitor" not in wb.sheetnames:
            return False
        ws = wb["KPI_Monitor"]
        headers = [str(ws.cell(1, c).value).strip() if ws.cell(1, c).value is not None else "" for c in range(1, ws.max_column+1)]
        if "Metric" not in headers or "Latest" not in headers or "Target/Threshold" not in headers:
            return False
        mcol = headers.index("Metric") + 1

        for r in range(2, ws.max_row+1):
            v = ws.cell(r, mcol).value
            if v is None:
                continue
            s = str(v)
            if ("robot" in s.lower()) or ("机器人" in s) or ("Robotics" in s):
                return False

        ws.append(["Robotics Rev Share (%)", float(latest), float(target)])
        wb.save(xlsx_path)
        return True
    except Exception:
        return False

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

# ------------------------- KPI 解析 -------------------------

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
    mcol = K["Metric"].astype(str).str.strip()
    for name in names:
        row = K[mcol == str(name).strip()]
        if not row.empty:
            return row.iloc[0]
    if contains_keywords:
        for kw in contains_keywords:
            row = K[mcol.str.contains(str(kw), case=False, na=False)]
            if not row.empty:
                return row.iloc[0]
    return None

def _eval_kpi_ge(row, default_target: float) -> Tuple[Optional[bool], Optional[float], Optional[float], str]:
    if row is None:
        return None, None, None, "KPI_Monitor 未提供该指标行"
    latest = _to_float(row.get("Latest"))
    target = _parse_target(row.get("Target/Threshold"), default_target)
    if latest is None:
        return None, None, target, "Latest 为空/不可解析"
    if target is None:
        return None, latest, None, "Target/Threshold 为空/不可解析"
    return bool(latest >= target), float(latest), float(target), ""

def kpi_details(K: pd.DataFrame, xlsx_path: str) -> Dict[str, Any]:
    gm_row = _get_metric_row(K, ["Vehicle GM (%)", "Vehicle GM", "Vehicle GM%"], ["Vehicle GM", "GM", "整车毛利", "毛利率"])
    fcf_row = _get_metric_row(K, ["FCF (TTM, bn HKD)", "FCF (TTM)", "FCF"], ["FCF", "自由现金流"])
    ts_row  = _get_metric_row(K, ["Tech/Service Rev Share (%)", "Tech/Service Share (%)", "Tech/Service"], ["Tech", "Service", "服务", "科技"])
    rb_row  = _get_metric_row(K, ["Robotics Rev Share (%)", "Robotics Share (%)", "Robotics"], ["robot", "机器人", "robotics"])

    ok_gm,  gm_latest, gm_target, gm_reason = _eval_kpi_ge(gm_row, 15)
    ok_fcf, fcf_latest, fcf_target, fcf_reason = _eval_kpi_ge(fcf_row, 0)
    ok_ts,  ts_latest, ts_target, ts_reason = _eval_kpi_ge(ts_row, 10)

    rb_source = "excel"
    if rb_row is None:
        env_latest = _to_float(os.environ.get("ROBOTICS_LATEST"))
        if env_latest is not None:
            env_target = _to_float(os.environ.get("ROBOTICS_TARGET"))
            if env_target is None:
                env_target = 5.0
            ok_rb = bool(env_latest >= env_target)
            rb_latest, rb_target = float(env_latest), float(env_target)
            rb_reason = "来自环境变量 ROBOTICS_LATEST"
            rb_source = "env"
            maybe_patch_kpi_robotics_row(xlsx_path, rb_latest, rb_target)
        else:
            ok_rb, rb_latest, rb_target = None, None, 5.0
            rb_reason = "KPI_Monitor 未提供 Robotics 指标行，且未设置 ROBOTICS_LATEST"
    else:
        ok_rb, rb_latest, rb_target, rb_reason = _eval_kpi_ge(rb_row, 5)

    ok_rt = None if (ok_ts is None and ok_rb is None) else bool(ok_ts is True or ok_rb is True)

    kpi_pass = 0
    if ok_gm is True: kpi_pass += 1
    if ok_fcf is True: kpi_pass += 1
    if ok_rt is True: kpi_pass += 1

    return dict(
        ok_gm=ok_gm, gm_latest=gm_latest, gm_target=gm_target, gm_reason=gm_reason,
        ok_fcf=ok_fcf, fcf_latest=fcf_latest, fcf_target=fcf_target, fcf_reason=fcf_reason,
        ok_ts=ok_ts, ts_latest=ts_latest, ts_target=ts_target, ts_reason=ts_reason,
        ok_rb=ok_rb, rb_latest=rb_latest, rb_target=rb_target, rb_reason=rb_reason, rb_source=rb_source,
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

    # 也写回 Excel 的 Status_Log（失败不致命）
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

# ------------------------- Telegram（稳健：HTML + 失败降级） -------------------------

def _post_telegram(token: str, payload: dict) -> Tuple[bool, str]:
    import urllib.request, urllib.parse
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode(payload).encode("utf-8")
    try:
        with urllib.request.urlopen(url, data=data, timeout=20) as r:
            body = r.read().decode("utf-8", "ignore")
        return True, body
    except Exception as e:
        # Telegram 通常会在 body 里返回具体原因（比如 can't parse entities）
        try:
            body = e.read().decode("utf-8", "ignore")  # type: ignore
        except Exception:
            body = str(e)
        return False, body

def strip_telegram_markup(s: str) -> str:
    return re.sub(r"</?[^>]+>", "", s)

def send_telegram(text: str, parse_mode: Optional[str] = None) -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID 未配置；仅打印：\n"+text)
        return

    payload = {"chat_id": chat_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode

    ok, body = _post_telegram(token, payload)
    if ok:
        return

    # 失败：降级为纯文本再发一次；仍失败也绝不抛异常
    print("⚠️ Telegram 发送失败，准备降级重试。返回：", body)
    payload2 = {"chat_id": chat_id, "text": strip_telegram_markup(text)}
    ok2, body2 = _post_telegram(token, payload2)
    if not ok2:
        print("❌ Telegram 降级发送仍失败：", body2)

# ------------------------- 主流程 -------------------------

def main(xlsx_path: str) -> int:
    try:
        ensure_xlsx_ok(xlsx_path)
    except Exception as e:
        send_telegram(f"📉 小鹏估值监控：Excel 文件不可用\n\n{e}", parse_mode=None)
        return 0

    try:
        A = read_sheet_safe(xlsx_path, "Assumptions")
        amap = dict(zip(A["Item"], A["Value"]))
    except Exception as e:
        send_telegram(f"📉 小鹏估值监控：读取 Assumptions 失败\n\n{e}", parse_mode=None)
        return 0

    live = os.environ.get("LIVE_PRICE","1") == "1"
    symbol = os.environ.get("YF_SYMBOL","9868.HK")
    price_field = os.environ.get("PRICE_FIELD","Close")
    price_live = fetch_live_price(symbol, price_field) if live else None
    price = float(price_live) if (price_live is not None) else float(amap.get("Current Price", 0))

    if price_live is not None:
        try:
            update_assumptions_price(xlsx_path, price)
        except Exception as e:
            send_telegram(f"⚠️ 小鹏估值监控：实时价格写回失败（不影响本次信号计算）\n\n{e}", parse_mode=None)

    base_iv = None
    try:
        S = read_sheet_safe(xlsx_path, "Summary")
        base_row = S[S["Scenario"]=="Base"]
        base_iv = float(base_row["IV_HKD_per_share"].values[0]) if not base_row.empty else None
    except Exception:
        base_iv = None
    if (base_iv is None) or (base_iv != base_iv):
        base_iv = dcf_base_iv(xlsx_path)

    try:
        K = read_sheet_safe(xlsx_path, "KPI_Monitor")
    except Exception as e:
        send_telegram(f"📉 小鹏估值监控：读取 KPI_Monitor 失败\n\n{e}", parse_mode=None)
        return 0

    kd = kpi_details(K, xlsx_path)
    ok_gm, ok_fcf, ok_rt, kpi_pass = kd["ok_gm"], kd["ok_fcf"], kd["ok_rt"], kd["kpi_pass"]

    signal = "观察"
    if base_iv and base_iv==base_iv and base_iv > 0:
        if price <= 0.80 * base_iv:
            signal = "加仓"
        elif price <= 0.90 * base_iv:
            signal = "建仓"

    rating_up = (kpi_pass >= 2) and (ok_rt is True)

    append_logs(
        xlsx_path, price, base_iv,
        kd["ok_gm"], kd["ok_fcf"], kd["ok_ts"], kd["ok_rb"], kd["ok_rt"],
        kpi_pass, signal, rating_up
    )

    # --- HTML 消息（稳） ---
    import html
    ts_utc = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    sym_esc = html.escape(symbol)

    if base_iv and base_iv==base_iv and base_iv > 0:
        premium_pct = (price / base_iv - 1.0) * 100
        iv_line = f"基准内在价值: HK${base_iv:.2f} | 溢价: {premium_pct:+.1f}%"
    else:
        iv_line = "基准内在价值: N/A"

    def esc(s: str) -> str:
        return html.escape(s)

    lines = []
    lines.append("<b>小鹏估值监控</b>")
    lines.append(esc(f"时间: {ts_utc}"))
    lines.append(f"代码: <code>{sym_esc}</code> | 现价: {esc(f'HK${price:.2f}')}")
    lines.append(esc(iv_line))
    lines.append(esc(f"信号: {signal} | KPI通过数: {kpi_pass}/3 | 评级建议: {'上调' if rating_up else '暂不升级'}"))
    lines.append("")
    lines.append("<b>KPI 明细（最新值 vs 阈值 → 结论）</b>")

    if kd["ok_gm"] is None:
        lines.append(esc(f"- 整车毛利率(%): NA（{kd['gm_reason']}）"))
    else:
        lines.append(esc(f"- 整车毛利率(%): {_fmt(kd['gm_latest'])} vs ≥{_fmt(kd['gm_target'])} → {_pf(kd['ok_gm'])}"))

    if kd["ok_fcf"] is None:
        lines.append(esc(f"- 自由现金流TTM(十亿港币): NA（{kd['fcf_reason']}）"))
    else:
        lines.append(esc(f"- 自由现金流TTM(十亿港币): {_fmt(kd['fcf_latest'])} vs ≥{_fmt(kd['fcf_target'])} → {_pf(kd['ok_fcf'])}"))

    if kd["ok_ts"] is None:
        lines.append(esc(f"- 科技/服务收入占比(%): NA（{kd['ts_reason']}）"))
    else:
        lines.append(esc(f"- 科技/服务收入占比(%): {_fmt(kd['ts_latest'])} vs ≥{_fmt(kd['ts_target'])} → {_pf(kd['ok_ts'])}"))

    if kd["ok_rb"] is None:
        lines.append(esc(f"- 机器人收入占比(%): NA（{kd['rb_reason']}；可设置 ROBOTICS_LATEST=xx 补齐）"))
    else:
        src = "（来自环境变量）" if kd.get("rb_source") == "env" else ""
        lines.append(esc(f"- 机器人收入占比(%): {_fmt(kd['rb_latest'])} vs ≥{_fmt(kd['rb_target'])} → {_pf(kd['ok_rb'])} {src}"))

    rt_line = "NA（科技/服务 与 机器人 均缺失）" if kd["ok_rt"] is None else _pf(kd["ok_rt"])
    lines.append(esc(f"- 机器人/科技服务综合（任一PASS即PASS）：{rt_line}"))

    mode = os.environ.get("TELEGRAM_PARSE_MODE", "HTML").strip() or None
    send_telegram("\n".join(lines), parse_mode=mode)
    return 0

if __name__=="__main__":
    if len(sys.argv)<2:
        print("Usage: python xpeng_alert_bot.py XPeng_Valuation_Monitor_v2.xlsx")
        sys.exit(1)
    # 任何情况下都不要让 CI 因推送失败 exit 1
    try:
        sys.exit(main(sys.argv[1]) or 0)
    except Exception as e:
        print("❌ 脚本出现未捕获异常（已吞掉，避免 CI 失败）：", e)
        sys.exit(0)
