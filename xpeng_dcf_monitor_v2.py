#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# xpeng_dcf_monitor_v2.py
import sys, math
import pandas as pd
import numpy as np

def compute_wacc(rf, erp, beta, tax, debt_ratio, pre_tax_cost_debt):
    ke = rf + beta * erp
    kd_after = pre_tax_cost_debt * (1 - tax)
    wacc = ke * (1 - debt_ratio) + kd_after * debt_ratio
    return ke, kd_after, wacc

def project_revenue_series(start_rev, cagr, n_years=10):
    return [start_rev * ((1 + cagr) ** i) for i in range(1, n_years+1)]

def dcf_valuation(start_rev_bn, rev_cagr, ebit_margins, wacc, tax_rate, sales_to_capital, net_cash_bn, shares_bn, terminal_growth):
    rev_series = np.array(project_revenue_series(start_rev_bn, rev_cagr, n_years=len(ebit_margins)))
    ebit_series = rev_series * np.array(ebit_margins)
    nopat_series = ebit_series * (1 - tax_rate)
    growth_rate = np.full_like(rev_series, rev_cagr, dtype=float)
    reinvest_series = (rev_series * growth_rate) / max(1e-6, sales_to_capital)
    fcff_series = nopat_series - reinvest_series
    years = np.arange(1, len(fcff_series)+1)
    disc = np.array([(1+wacc)**t for t in years])
    pv_fcff = float(np.sum(fcff_series / disc))
    tv = float((fcff_series[-1] * (1 + terminal_growth)) / (wacc - terminal_growth))
    pv_tv = float(tv / ((1+wacc)**len(fcff_series)))
    ev = pv_fcff + pv_tv
    equity_value = ev + net_cash_bn
    per_share = (equity_value * 1e9) / (shares_bn * 1e9)
    return per_share, ev, equity_value

def main(xlsx_path):
    xls = pd.ExcelFile(xlsx_path)
    A = pd.read_excel(xls, "Assumptions")
    S = pd.read_excel(xls, "Scenarios")
    R = pd.read_excel(xls, "Start_Rev_2025")
    amap = dict(zip(A["Item"], A["Value"]))
    rf=float(amap.get("Risk-Free Rate (Rf)",0.0181)); erp=float(amap.get("Equity Risk Premium (ERP)",0.059)); beta=float(amap.get("Beta",1.04))
    tax=float(amap.get("Tax Rate",0.25)); d_ratio=float(amap.get("Target Debt Ratio (D/(D+E))",0.10)); kd_pre=float(amap.get("Pre-tax Cost of Debt",0.045))
    g=float(amap.get("Terminal Growth (g)",0.02)); s2c=float(amap.get("Sales-to-Capital",2.5))
    shares=float(amap.get("Share Count (bn)",1.909771413)); net_cash=float(amap.get("Net Cash (bn)",39.9)); price=float(amap.get("Current Price",0))
    start_rev=float(pd.read_excel(xls,"Start_Rev_2025")["Value"].iloc[0])
    ke,kd_after,wacc=compute_wacc(rf,erp,beta,tax,d_ratio,kd_pre)
    print(f"WACC={wacc:.4f}  (Ke={ke:.4f}, Kd_after={kd_after:.4f})  LTg={g:.3f}")
    res={}
    for scn in S["Scenario"].unique():
        df=S[S["Scenario"]==scn].copy()
        iv,ev,eq=dcf_valuation(start_rev,float(df['Rev_CAGR'].iloc[0]),df['EBIT_margin'].values,wacc,tax,s2c,net_cash,shares,g)
        res[scn]=iv
        print(f"{scn}: IV={iv:.2f} HKD/share")
    base_iv=res.get("Base",float("nan"))
    print(f"Price={price:.2f}, Base_IV={base_iv:.2f}, Discount={(price/base_iv-1)*100 if base_iv==base_iv else float('nan'):.1f}%")
if __name__=="__main__":
    if len(sys.argv)<2:
        print("Usage: python xpeng_dcf_monitor_v2.py /path/to/XPeng_Valuation_Monitor_v2.xlsx"); sys.exit(1)
    main(sys.argv[1])
