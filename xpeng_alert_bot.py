#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# xpeng_alert_bot.py
import os, sys, re, urllib.request, urllib.parse
import pandas as pd
import numpy as np

MARGINS = {"enter":0.90, "add":0.80}

def send_telegram(token, chat_id, text):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown"
    }).encode("utf-8")
    with urllib.request.urlopen(url, data=data, timeout=20) as r:
        return r.read()

def main(xlsx_path):
    xls = pd.ExcelFile(xlsx_path)
    A = pd.read_excel(xls, "Assumptions")
    S = pd.read_excel(xls, "Summary")
    K = pd.read_excel(xls, "KPI_Monitor")
    amap = dict(zip(A["Item"], A["Value"]))
    price = float(amap.get("Current Price", 0))
    base_row = S[S["Scenario"]=="Base"]
    base_iv = float(base_row["IV_HKD_per_share"].values[0]) if not base_row.empty else float("nan")

    def take(metric, default_target):
        row = K[K["Metric"]==metric]
        if row.empty: 
            return False
        latest = float(row["Latest"].values[0])
        tgt = row["Target/Threshold"].values[0]
        # parse numeric
        m = re.search(r"(-?\d+(\.\d+)?)", str(tgt))
        target_num = float(m.group(1)) if m else default_target
        return latest >= target_num

    ok1 = take("Vehicle GM (%)", 15)
    ok2 = take("FCF (TTM, bn HKD)", 0)
    ok3 = take("Tech/Service Rev Share (%)", 10)
    kpi_pass = sum([ok1,ok2,ok3])

    lines = []
    lines.append("*XPeng Monitor*")
    lines.append(f"Price: HK${price:.2f} | Base IV: {('N/A' if base_iv!=base_iv else f'HK${base_iv:.2f}')}")
    lines.append(f"KPI — VehicleGM: {'PASS' if ok1 else 'FAIL'}, FCF: {'PASS' if ok2 else 'FAIL'}, Tech/Service: {'PASS' if ok3 else 'FAIL'}")
    if base_iv==base_iv:
        if price <= MARGINS['add'] * base_iv:
            lines.append(f"Signal: *加仓* (≤{MARGINS['add']*100:.0f}% of Base IV)")
        elif price <= MARGINS['enter'] * base_iv:
            lines.append(f"Signal: *建仓* (≤{MARGINS['enter']*100:.0f}% of Base IV)")
        else:
            lines.append("Signal: 观察")
    if kpi_pass >= 2:
        lines.append("评级条件：*满足任两项* → 可上调为“买入”")

    text = "\n".join(lines)
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID 未设置。仅打印：\n"+text)
        return
    try:
        send_telegram(token, chat_id, text)
        print("Telegram 推送完成。")
    except Exception as e:
        print("Telegram 推送失败：", e, "\n消息：\n", text)

if __name__=="__main__":
    if len(sys.argv)<2:
        print("Usage: python xpeng_alert_bot.py /path/to/XPeng_Valuation_Monitor_v2.xlsx"); sys.exit(1)
    main(sys.argv[1])
