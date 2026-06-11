"""
EXAMPLE OUTPUT — Live Dashboard Display

Dijalankan setiap 30 detik dari dashboard._print_display_loop()
"""

# ═══════════════════════════════════════════════════════════════════════════════════════════════════
# 01:45:32 | [DASHBOARD] LIVE MONITORING — Real-time Market Context
# ═══════════════════════════════════════════════════════════════════════════════════════════════════

Symbol        Price         Δ%       OI (USD)        Vol Ratio  CVD        Status
------  --------  -------  --------  ------  ---------  
BTCUSDT     43520.50   +0.02%   123,456,789       2.15x    +1234.56   SIGNAL
ETHUSDT      2345.10   -0.05%    89,123,456       1.87x     -567.82   G1
BNBUSDT       312.45   +0.08%    45,678,901       2.43x     +234.11   G2
XRPUSDT         0.52   -0.03%    12,345,678       1.32x      -89.34   G1
ADAUSDT         0.78   +0.01%     8,901,234       1.54x      +12.45   WAITING
DOGEUSDT        0.11   -0.02%     6,234,567       0.98x      -45.23   WAITING
SOLusdt        65.34   +0.04%     4,567,890       2.01x     +123.45   G3
MATICUSDT       0.95   +0.00%     3,456,789       1.23x      -34.56   G1
LINKUSDT       13.45   -0.01%     2,345,678       1.67x      +89.12   WAITING
UNIUSDT         7.89   +0.03%     1,234,567       1.98x     +156.78   G1

─────────────────────────────────────────────────────────────────────────────────────────────────

[LIQUIDATIONS] Last 10 events:
  💥 BTCUSDT SHORT LIQ | Qty=0.0150 | Price=43520.00 | ~$652.80 | 5.2s ago
  💥 ETHUSDT LONG LIQ  | Qty=0.1250 | Price=2345.10  | ~$293.14 | 12.3s ago
  💥 BNBUSDT SHORT LIQ | Qty=2.3400 | Price=312.50   | ~$731.25 | 18.5s ago
  💥 XRPUSDT LONG LIQ  | Qty=5000.0000 | Price=0.52 | ~$2600.00 | 25.1s ago
  💥 ADAUSDT SHORT LIQ | Qty=3500.0000 | Price=0.78 | ~$2730.00 | 31.7s ago
  💥 DOGEUSDT LONG LIQ | Qty=25000.0000 | Price=0.11 | ~$2750.00 | 38.9s ago
  💥 SOLUSDT SHORT LIQ | Qty=1.5000 | Price=65.34   | ~$98.01 | 45.3s ago
  💥 MATICUSDT LONG LIQ | Qty=12500.0000 | Price=0.95 | ~$11875.00 | 52.1s ago
  💥 LINKUSDT SHORT LIQ | Qty=0.8900 | Price=13.45   | ~$11.97 | 59.4s ago
  💥 UNIUSDT LONG LIQ  | Qty=2.3400 | Price=7.89    | ~$18.45 | 66.2s ago

─────────────────────────────────────────────────────────────────────────────────────────────────
═══════════════════════════════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════════════════════════════
# 01:47:32 | [Market Context] Condition: SIDEWAYS
# ═══════════════════════════════════════════════════════════════════════════════════════════════════

[Market Context] Condition: SIDEWAYS | 
Avg Price Move: 0.341% (threshold: 1.50%) | 
Avg Vol Ratio: 1.42x (threshold: 4.00x) | 
Avg Wick Ratio: 24.3% (threshold: 40.0%)

→ Interpretation: Market is flat/sideways with low volatility
→ This explains high G1 rejection rate (153/156)
→ Bot is functioning correctly, just waiting for bigger moves

# ═══════════════════════════════════════════════════════════════════════════════════════════════════
# 01:49:32 | [Gates] PERIODIC STATS (every 2 minutes)
# ═══════════════════════════════════════════════════════════════════════════════════════════════════

[Gates] Total=1023 | Doji=45 G1=153 G2_empty=0 G2_vol=567 G3_wick=89 G3_side=12 G4=98 G5=34 Debounce=18 → Signals=7

Breakdown:
├─ Total candles evaluated: 1023
├─ Doji (no range): 45
├─ G1 (price move < 1.5%): 153
│   └─ Average rejected price move: 0.34% ← Shows market is flat
├─ G2 empty: 0 (buffer has enough data)
├─ G2 (volume ratio < 4x): 567
│   └─ Average rejected volume ratio: 1.42x ← Low activity
├─ G3 wick: 89
├─ G3 side mismatch: 12
├─ G4 zscore: 98
├─ G5 OBI: 34
├─ Debounce: 18 (same symbol, 3m cooldown)
└─ SIGNALS EMITTED: 7 ✓ (0.68% pass rate — selective, as designed)

# ═══════════════════════════════════════════════════════════════════════════════════════════════════
# REAL-TIME EXAMPLE: Bot Detects Signal
# ═══════════════════════════════════════════════════════════════════════════════════════════════════

01:50:15 | 🎯 SINYAL [BTCUSDT] SHORT | Entry=43501.55 TP=43338.04 SL=43640.60 | 
         | Move=1.87% VolRatio=4.23x Wick=42.1% OBI=0.38 Z=1.23

→ Dashboard instantly updates BTCUSDT status from "G1" to "SIGNAL"

01:50:16 | [Trade] ENTRY BTCUSDT SHORT @ $43,501.55 | Size=$1000 | SL=$43,640.60 TP=$43,338.04

01:50:45 | [Trade] EXIT BTCUSDT SHORT via TP @ $43,338.04 | PnL=$163.51 (+0.49%) | Duration=29s

→ Dashboard records trade completion, resets status to "WAITING"

# ═══════════════════════════════════════════════════════════════════════════════════════════════════
# REAL-TIME EXAMPLE: Large Liquidation Event
# ═══════════════════════════════════════════════════════════════════════════════════════════════════

01:52:30 | 💥 LIQUIDATION [BTCUSDT] LONG LIQ | Qty=0.050 | Price=43480.00 | ~$2174.00
01:52:31 | 💥 LIQUIDATION [BTCUSDT] LONG LIQ | Qty=0.075 | Price=43475.00 | ~$3260.63
01:52:32 | 💥 LIQUIDATION [BTCUSDT] SHORT LIQ | Qty=0.100 | Price=43490.00 | ~$4349.00

→ Dashboard Liquidation feed shows 3 events in 2 seconds for same symbol
→ Could indicate cascade (potentially profitable entry condition)

# ═══════════════════════════════════════════════════════════════════════════════════════════════════
# CVD TRACKING EXAMPLE
# ═══════════════════════════════════════════════════════════════════════════════════════════════════

In dashboard display, CVD column shows:
- BTCUSDT CVD: +5678.90 (BULLISH: buyers dominant)
- ETHUSDT CVD: -2341.23 (BEARISH: sellers dominant)
- BNBUSDT CVD: +0.00 (NEUTRAL: balanced)

This comes from aggTrade stream tracking:
- Each BUY-initiated aggTrade: CVD += quantity
- Each SELL-initiated aggTrade: CVD -= quantity
- Reset to 0.0 when candle closes, fresh calculation next minute

Example CVD divergence alert (future enhancement):
  Price: ↓ (trending down)
  CVD: + (strong buyer accumulation)
  → Potential reversal coming (buyers taking control despite downtrend)

# ═══════════════════════════════════════════════════════════════════════════════════════════════════
"""
