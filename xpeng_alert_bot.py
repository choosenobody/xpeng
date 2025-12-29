#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
xpeng_alert_bot.py (v3.0.0)

目标：
- Robotics Rev Share (%) 不再卡在 NA：当未来 Excel 中出现机器人相关披露数据时，脚本会自动尝试“全表扫描识别”，并写回 KPI_Monitor。
- 若仍未披露：按 0%（保守）并明确标注原因。
- Telegram 发送失败不再导致 CI 失败（吞掉异常 + 输出返回体以便排查）。

依赖：
  pip install pandas openpyxl yfinance

Excel 约定：
- 主要从 KPI_Monitor 读取 KPI（Metric/Latest/Target/Threshold）
- v3 增加：Source/Period/Ref/AutoKeywords/LastUpdatedUTC（可选列；没有也不影响）

环境变量：
- TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
- LIVE_PRICE=1（默认1）  YF_SYMBOL=9868.HK  PRICE_FIELD=Close
- ROBOTICS_LATEST=xx / ROBOTICS_TARGET=5（可选兜底；不建议长期用）
"""

import os, sys, re, csv, datetime
from pathlib import Path
from typing import Optional, Tuple, Dict, Any

import pandas as pd
import numpy as np
from openpyxl import load_workbook

# ------------------------- Excel 安全检查 -------------------------

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
            "检测到文件内容像 HTML（可能下载到了 404/鉴权/重定向页面），并非 .xlsx。\n"
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

# ------------------------- KPI 工具 -------------------------

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

# ------------------------- v3 自动识别：全表扫描机器人披露 -------------------------

_SHARE_PATTERNS = [
    r"Robotics\s*Rev\s*Share",
    r"Robotics.*Share",
    r"Robotaxi.*Share",
    r"Humanoid.*Share",
    r"robot.*share",
    r"机器人.*占比",
    r"机器人.*收入.*占比",
    r"人形.*占比",
]

_ROB_REV_PATTERNS = [
    r"Robotics\s*Revenue",
    r"Robotaxi\s*Revenue",
    r"Humanoid.*Revenue",
    r"robot.*revenue",
    r"机器人.*(收入|营收)",
    r"人形.*(收入|营收)",
]

_TOTAL_REV_PATTERNS = [
    r"Total\s*Revenues?",
    r"Total\s*Revenue",
    r"总(收入|营收)",
    r"营业收入",
]

def _looks_like_pct(x: float) -> bool:
    return (x >= 0.0) and (x <= 100.0)

def _find_neighbor_number(ws, r: int, c: int, max_right: int = 6, max_down: int = 3) -> Optional[float]:
    # 先向右找
    for dc in range(1, max_right+1):
        v = ws.cell(r, c+dc).value
        f = _to_float(v)
        if f is not None:
            return f
    # 再向下找
    for dr in range(1, max_down+1):
        v = ws.cell(r+dr, c).value
        f = _to_float(v)
        if f is not None:
            return f
    return None

def detect_robotics_share_from_workbook(xlsx_path: str) -> Tuple[Optional[float], str]:
    """
    返回: (share_pct, evidence)
    evidence 用于消息/写回 Ref，例如: "SheetX!B12 (share)" 或 "SheetY!C8/C9 (rev/total)"
    """
    try:
        wb = load_workbook(xlsx_path, data_only=True, read_only=True)
    except Exception:
        wb = load_workbook(xlsx_path, data_only=True)

    share_re = re.compile("|".join(_SHARE_PATTERNS), re.IGNORECASE)
    robrev_re = re.compile("|".join(_ROB_REV_PATTERNS), re.IGNORECASE)
    totalrev_re = re.compile("|".join(_TOTAL_REV_PATTERNS), re.IGNORECASE)

    # 1) 先找“占比”直接披露
    for sname in wb.sheetnames:
        ws = wb[sname]
        max_r = min(ws.max_row or 0, 200)
        max_c = min(ws.max_column or 0, 30)
        for r in range(1, max_r+1):
            for c in range(1, max_c+1):
                v = ws.cell(r, c).value
                if isinstance(v, str) and share_re.search(v):
                    num = _find_neighbor_number(ws, r, c)
                    if num is not None and _looks_like_pct(float(num)):
                        coord = ws.cell(r, c).coordinate
                        return float(num), f"{sname}!{coord}（占比字段邻近数值）"

    # 2) 找“机器人收入”和“总营收”并计算占比
    for sname in wb.sheetnames:
        ws = wb[sname]
        max_r = min(ws.max_row or 0, 200)
        max_c = min(ws.max_column or 0, 30)

        rob_candidates = []
        total_candidates = []

        for r in range(1, max_r+1):
            for c in range(1, max_c+1):
                v = ws.cell(r, c).value
                if isinstance(v, str):
                    if robrev_re.search(v):
                        num = _find_neighbor_number(ws, r, c)
                        if num is not None:
                            rob_candidates.append((float(num), r, c))
                    elif totalrev_re.search(v):
                        num = _find_neighbor_number(ws, r, c)
                        if num is not None:
                            total_candidates.append((float(num), r, c))

        # 配对：优先同一张表里“最合理”的一对（robot < total）
        best = None
        for rob, rr, rc in rob_candidates:
            for tot, tr, tc in total_candidates:
                if rob <= 0 or tot <= 0:
                    continue
                if rob >= tot:
                    continue
                share = rob / tot * 100.0
                if share < 0.0 or share > 50.0:
                    # 过大多半误配，保守过滤
                    continue
                best = (share, rr, rc, tr, tc)
                break
            if best:
                break

        if best:
            share, rr, rc, tr, tc = best
            rob_coord = ws.cell(rr, rc).coordinate
            tot_coord = ws.cell(tr, tc).coordinate
            return float(share), f"{sname}!{rob_coord}/{tot_coord}（机器人收入/总营收推导）"

    return None, ""

def upsert_kpi_robotics(xlsx_path: str, share_pct: float, ref: str, source: str = "自动识别(财报/表格)") -> bool:
    """
    把 Robotics Rev Share (%) 写回 KPI_Monitor（Latest/Source/Ref/LastUpdatedUTC/Status）
    """
    try:
        wb = load_workbook(xlsx_path)
        if "KPI_Monitor" not in wb.sheetnames:
            return False
        ws = wb["KPI_Monitor"]
        headers = [ws.cell(1, c).value for c in range(1, ws.max_column+1)]
        h2c = {str(h).strip(): i for i, h in enumerate(headers, start=1) if h is not None}

        def col(name: str) -> Optional[int]:
            return h2c.get(name)

        # 找行
        target_row = None
        mcol = col("Metric")
        if not mcol:
            return False
        for r in range(2, ws.max_row+1):
            v = ws.cell(r, mcol).value
            if v is None:
                continue
            if str(v).strip() == "Robotics Rev Share (%)":
                target_row = r
                break
        if target_row is None:
            # 若缺行就追加
            target_row = ws.max_row + 1
            ws.cell(target_row, mcol).value = "Robotics Rev Share (%)"

        # 写入
        if col("Latest"):
            ws.cell(target_row, col("Latest")).value = float(share_pct)
        if col("Source"):
            ws.cell(target_row, col("Source")).value = source
        if col("Ref"):
            ws.cell(target_row, col("Ref")).value = ref
        if col("LastUpdatedUTC"):
            ws.cell(target_row, col("LastUpdatedUTC")).value = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # 计算 Status
        t = None
        if col("TargetNum"):
            t = _to_float(ws.cell(target_row, col("TargetNum")).value)
        if t is None and col("Target/Threshold"):
            t = _parse_target(ws.cell(target_row, col("Target/Threshold")).value, 5.0)
        if t is None:
            t = 5.0
        status = "PASS" if float(share_pct) >= float(t) else "FAIL"
        if col("Status"):
            ws.cell(target_row, col("Status")).value = status
        if col("StatusRule") and (ws.cell(target_row, col("StatusRule")).value in (None, "")):
            ws.cell(target_row, col("StatusRule")).value = "PASS if >=Target"

        wb.save(xlsx_path)
        return True
    except Exception:
        return False

# ------------------------- 状态记录 -------------------------

def append_logs(price, base_iv, ok_gm, ok_fcf, ok_ts, ok_rb, ok_rt, kpi_pass, signal, rating_up):
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

# ------------------------- Telegram（失败不致命） -------------------------

def send_telegram(text: str) -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID 未配置；仅打印：\n"+text)
        return

    import urllib.request, urllib.parse, urllib.error
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode("utf-8")
    try:
        with urllib.request.urlopen(url, data=data, timeout=20) as r:
            r.read()
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "ignore")
        print(f"[Telegram] HTTP {e.code} 返回：\n{body}\n---\n原始消息：\n{text}")
    except Exception as e:
        print(f"[Telegram] 发送失败：{e}\n---\n原始消息：\n{text}")

# ------------------------- 主流程 -------------------------

def main(xlsx_path: str) -> int:
    try:
        ensure_xlsx_ok(xlsx_path)
    except Exception as e:
        send_telegram(f"小鹏估值监控：Excel 文件不可用\n\n{e}")
        return 0

    # 1) Assumptions
    try:
        A = read_sheet_safe(xlsx_path, "Assumptions")
        amap = dict(zip(A["Item"], A["Value"]))
    except Exception as e:
        send_telegram(f"小鹏估值监控：读取 Assumptions 失败\n\n{e}")
        return 0

    # 2) 现价
    live = os.environ.get("LIVE_PRICE","1") == "1"
    symbol = os.environ.get("YF_SYMBOL","9868.HK")
    price_field = os.environ.get("PRICE_FIELD","Close")
    price_live = fetch_live_price(symbol, price_field) if live else None
    price = float(price_live) if (price_live is not None) else float(amap.get("Current Price", 0))

    # 3) Base IV（优先 Summary）
    base_iv = None
    try:
        S = read_sheet_safe(xlsx_path, "Summary")
        base_row = S[S["Scenario"]=="Base"]
        if not base_row.empty and "IV_HKD_per_share" in base_row.columns:
            base_iv = float(base_row["IV_HKD_per_share"].values[0])
    except Exception:
        base_iv = None

    # 4) KPI
    try:
        K = read_sheet_safe(xlsx_path, "KPI_Monitor")
    except Exception as e:
        send_telegram(f"小鹏估值监控：读取 KPI_Monitor 失败\n\n{e}")
        return 0

    # --- v3：先做机器人自动识别（只在当前 Latest=0 且被标记未披露/空时触发） ---
    rob_row = _get_metric_row(K, ["Robotics Rev Share (%)"], ["robot", "机器人", "robotics"])
    rob_latest = _to_float(rob_row.get("Latest")) if rob_row is not None else None
    rob_source = str(rob_row.get("Source")).strip() if (rob_row is not None and "Source" in rob_row.index) else ""
    need_probe = (rob_latest is None) or (rob_latest == 0.0 and ("未披露" in rob_source or rob_source == ""))

    probe_note = ""
    if need_probe:
        share, evidence = detect_robotics_share_from_workbook(xlsx_path)
        if share is not None:
            ok_write = upsert_kpi_robotics(xlsx_path, share, evidence, source="自动识别(财报/表格)")
            if ok_write:
                # 重新读取 KPI_Monitor 以获得最新值
                K = read_sheet_safe(xlsx_path, "KPI_Monitor")
                probe_note = f"⚠️ 检测到机器人披露数据，已自动写回：{share:.2f}%（{evidence}）"
        else:
            # 仍未披露：按0%在逻辑层处理（KPI_Monitor v3 默认已是0）
            probe_note = "机器人收入占比：未检测到披露字段（按0%保守）"

    gm_row = _get_metric_row(K, ["Vehicle GM (%)", "Vehicle GM", "Vehicle GM%"], ["Vehicle GM", "GM", "整车毛利", "毛利率"])
    fcf_row = _get_metric_row(K, ["FCF (TTM, bn HKD)", "FCF (TTM)", "FCF"], ["FCF", "自由现金流"])
    ts_row  = _get_metric_row(K, ["Tech/Service Rev Share (%)", "Tech/Service Share (%)", "Tech/Service"], ["Tech", "Service", "服务", "科技"])
    rb_row  = _get_metric_row(K, ["Robotics Rev Share (%)"], ["robot", "机器人", "robotics"])

    ok_gm,  gm_latest, gm_target, gm_reason = _eval_kpi_ge(gm_row, 15)
    ok_fcf, fcf_latest, fcf_target, fcf_reason = _eval_kpi_ge(fcf_row, 0)
    ok_ts,  ts_latest, ts_target, ts_reason = _eval_kpi_ge(ts_row, 10)

    # Robotics：表优先；env 兜底；再否则 0%
    rb_source2 = "excel"
    if rb_row is None:
        env_latest = _to_float(os.environ.get("ROBOTICS_LATEST"))
        env_target = _to_float(os.environ.get("ROBOTICS_TARGET")) or 5.0
        if env_latest is not None:
            ok_rb, rb_latest, rb_target = bool(env_latest >= env_target), float(env_latest), float(env_target)
            rb_reason = "来自环境变量 ROBOTICS_LATEST（不建议长期使用）"
            rb_source2 = "env"
        else:
            ok_rb, rb_latest, rb_target = False, 0.0, 5.0
            rb_reason = "财报未单列披露机器人收入/占比（按0%保守）"
            rb_source2 = "default0"
    else:
        ok_rb, rb_latest, rb_target, rb_reason = _eval_kpi_ge(rb_row, 5)
        if ok_rb is None:
            # Latest 缺失也按0%
            ok_rb, rb_latest, rb_target = False, 0.0, rb_target if rb_target is not None else 5.0
            rb_reason = "Robotics 行 Latest 缺失（按0%保守）"
            rb_source2 = "default0"

    ok_rt = None if (ok_ts is None and ok_rb is None) else bool(ok_ts is True or ok_rb is True)

    kpi_pass = 0
    if ok_gm is True: kpi_pass += 1
    if ok_fcf is True: kpi_pass += 1
    if ok_rt is True: kpi_pass += 1

    # 5) 信号（示例逻辑）
    signal = "观察"
    if base_iv and base_iv==base_iv and base_iv > 0:
        if price <= 0.80 * base_iv:
            signal = "加仓"
        elif price <= 0.90 * base_iv:
            signal = "建仓"
    rating_up = (kpi_pass >= 2) and (ok_rt is True)

    append_logs(price, base_iv if base_iv else np.nan, ok_gm, ok_fcf, ok_ts, ok_rb, ok_rt, kpi_pass, signal, rating_up)

    # 6) 生成消息（纯文本）
    ts_utc = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    if base_iv and base_iv==base_iv and base_iv > 0:
        premium_pct = (price / base_iv - 1.0) * 100
        iv_line = f"基准内在价值: HK${base_iv:.2f} | 溢价: {premium_pct:+.1f}%"
    else:
        iv_line = "基准内在价值: N/A"

    def pf(ok: Optional[bool]) -> str:
        return "PASS" if ok is True else ("FAIL" if ok is False else "NA")

    def fmt(x: Optional[float], nd: int = 2) -> str:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return "NA"
        return f"{float(x):.{nd}f}"

    lines = []
    lines.append("小鹏估值监控")
    lines.append(f"时间: {ts_utc}")
    lines.append(f"代码: {symbol} | 现价: HK${price:.2f}")
    lines.append(iv_line)
    lines.append(f"信号: {signal} | KPI通过数: {kpi_pass}/3 | 评级建议: {'上调' if rating_up else '暂不升级'}")
    if probe_note:
        lines.append("")
        lines.append(probe_note)

    lines.append("")
    lines.append("KPI 明细（最新值 vs 阈值 → 结论）")
    lines.append(f"- 整车毛利率(%): {fmt(gm_latest)} vs ≥{fmt(gm_target)} → {pf(ok_gm)}" + (f"（{gm_reason}）" if ok_gm is None else ""))
    lines.append(f"- 自由现金流TTM(十亿港币): {fmt(fcf_latest)} vs ≥{fmt(fcf_target)} → {pf(ok_fcf)}" + (f"（{fcf_reason}）" if ok_fcf is None else ""))
    lines.append(f"- 科技/服务收入占比(%): {fmt(ts_latest)} vs ≥{fmt(ts_target)} → {pf(ok_ts)}" + (f"（{ts_reason}）" if ok_ts is None else ""))
    lines.append(f"- 机器人收入占比(%): {fmt(rb_latest)} vs ≥{fmt(rb_target)} → {pf(ok_rb)}（{rb_reason}）")
    lines.append(f"- 机器人/科技服务综合（任一PASS即PASS）：{pf(ok_rt) if ok_rt is not None else 'NA（均缺失）'}")

    send_telegram("\n".join(lines))
    return 0

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python xpeng_alert_bot.py XPeng_Valuation_Monitor_v3.xlsx")
        sys.exit(1)
    try:
        sys.exit(main(sys.argv[1]) or 0)
    except Exception as e:
        # 任何异常都不允许让 CI 失败
        print("❌ 未捕获异常（已吞掉，避免 CI 失败）：", e)
        sys.exit(0)
