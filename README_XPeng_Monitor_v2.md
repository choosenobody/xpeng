
# XPeng Valuation & KPI Monitor (v2)

## Files
- `XPeng_Valuation_Monitor_v2.xlsx` — 假设、场景、KPI、敏感性热力图（WACC×g）与摘要。
- `xpeng_dcf_monitor_v2.py` — 本地三情景复算。
- `xpeng_alert_bot.py` — 读取 Excel，判断 KPI 与安全边际，推送 Telegram。
- `.github/workflows/xpeng_kpi_alert.yml` — GitHub Actions 定时任务（每日 20:00 北京时间）。

## 使用
1. 将四个文件放入你的 GitHub 仓库根目录（或 `data/`，并在 workflow 中改路径）。
2. 在仓库 Settings → Secrets and variables → Actions 新增：
   - `TELEGRAM_BOT_TOKEN`
   - `TELEGRAM_CHAT_ID`
3. 每月更新 Excel：
   - `Assumptions.Current Price`、`Start_Rev_2025.Value`、`KPI_Monitor` 的 `Latest` 列。
   - 需要更精细路径时，编辑 `Scenarios` 的 `Rev_CAGR` 与 `EBIT_margin`。
4. 手动运行：`python xpeng_dcf_monitor_v2.py XPeng_Valuation_Monitor_v2.xlsx`。
5. 自动推送：Actions 会按 cron 读取 Excel 并发送提醒。

## 阈值与纪律
- KPI（满足任两项 → 上调为“买入”）：
  - 车辆毛利 ≥ 15%
  - FCF(TTM) ≥ 0
  - 技术/服务收入占比 ≥ 10%
- 安全边际：
  - 价格 ≤ 90% × Base IV → 建仓
  - 价格 ≤ 80% × Base IV → 加仓
- 触发红旗（任一）→ 降仓 50%–100%。
