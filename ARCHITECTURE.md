# MLF Bot — Blueprint Arsitektur & Dokumentasi Teknis
**"Micro-Liquidation Fade & Order Book Imbalance"**

---

## Struktur File

```
mlf_bot/
├── main.py                  # Orchestrator + entry point (asyncio.run)
├── config.py                # Semua parameter (EntryConfig, RiskConfig, dll)
├── requirements.txt
├── core/
│   ├── models.py            # Dataclasses: Kline, Position, AnomalySignal, dll
│   ├── data_fetcher.py      # Async REST + WebSocket (Binance Futures)
│   ├── trading_logic.py     # "Crazy Logic" engine — deteksi anomali
│   └── paper_trader.py      # Eksekusi, sizing, TP/SL/TimeStop, statistik
└── utils/
    └── logger.py            # CSV trades + JSONL equity + session summary
```

---

## Alur Data Runtime

```
Binance WS (kline_1m)
    │
    ▼
DataFetcher._handle_kline_msg()
    ├── kline.is_closed=False → kline_buf.set_live()
    └── kline.is_closed=True  → kline_buf.push_closed()
                                 → _on_kline_closed_mux(kline) [asyncio.create_task]
                                        │
                         ┌─────────────┴──────────────┐
                         ▼                            ▼
              TradingLogic                     PaperTrader
              .on_kline_closed()               .on_kline_update()
                         │                            │
              [4 kondisi entry]            [cek TP/SL/TimeStop
              [OBI fetch REST]              per posisi aktif]
                         │                            │
                  AnomalySignal                  ClosedTrade
                         │                            │
                         ▼                            ▼
              PaperTrader.on_signal()         TradeLogger.log_trade()
              [buka posisi baru]              [CSV + JSONL + console]
```

---

## "Crazy Logic" — 4+1 Kondisi Entry

### [1] Micro-Volatility Snap
```
price_move = max(high-open, open-low) / open * 100
VALID jika price_move >= 1.5%
```
Mengukur seberapa jauh harga "terlempar" dalam 1 candle.
Pump ke atas → kandidat SHORT. Dump ke bawah → kandidat LONG.

### [2] Volume Exhaustion Climax
```
volume_ratio = candle.quote_volume / avg_quote_volume(30m)
VALID jika volume_ratio >= 4.0x
```
Volume meledak = kepanikan/likuidasi. Bukan awal tren, tapi akhir tren mikro.

### [3] Wick Rejection
```
wick_ratio = max(upper_wick, lower_wick) / total_range
VALID jika wick_ratio >= 0.40
wick_side harus BERLAWANAN dengan snap_side
```
Wick = bandar memasang limit order raksasa menahan harga.
Upper wick setelah pump → SHORT. Lower wick setelah dump → LONG.

### [4] Z-Score Filter
```
z = (close - mean_30) / std_30
VALID jika |z| <= 3.5
```
Tolak entry jika anomali sudah "stale" — kita ketinggalan kereta.

### [+1] Order Book Imbalance (konfirmasi)
```
OBI = bid_vol_10levels / (bid_vol + ask_vol)
SHORT valid: OBI < 0.40 (seller dominan)
LONG  valid: OBI > 0.60 (buyer dominan)
```
Layer final: pastikan tekanan pasar mendukung reversal.

---

## Exit & Risk Management

| Parameter        | Nilai Default    | Logika                               |
|------------------|------------------|--------------------------------------|
| Take Profit      | +0.50%           | Ambil pantulan pertama, jangan serakah |
| Stop Loss        | Ujung wick       | Di-clamp ke max -1.0%               |
| Time-Stop        | 5 candle (5 min) | Anomali HFT tidak bertahan lama     |
| Margin per Trade | 5% balance       | Isolasi risiko ketat                 |
| Leverage         | 10x              | Sizing efektif, bukan gambling       |
| Max Posisi       | 5 simultan       | Diversifikasi, bukan overtrading     |
| Taker Fee        | 0.04% per sisi   | Realistis, tidak menutupi biaya nyata |

---

## Output Logs

| File                        | Format    | Isi                                         |
|-----------------------------|-----------|---------------------------------------------|
| `logs/trades.csv`           | CSV       | Setiap trade: entry, exit, PnL, signal_meta |
| `logs/equity.jsonl`         | JSON Lines| Balance snapshot setiap trade close         |
| `logs/session_summary.json` | JSON      | Statistik sesi lengkap (ditulis saat stop)  |
| `logs/bot.log`              | Text      | Semua log level DEBUG ke atas               |

---

## Cara Menjalankan

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Jalankan bot
python main.py

# 3. Monitor live
tail -f logs/bot.log

# 4. Analisis hasil (Python/pandas)
import pandas as pd
trades = pd.read_csv("logs/trades.csv")
print(trades.groupby("close_reason")["pnl_usd"].describe())
```

---

## Analisis Pasca-Sesi (Rekomendasi)

```python
import pandas as pd, json

# Load trades
df = pd.read_csv("logs/trades.csv")

# Win rate per sinyal parameter
df["win"] = df["pnl_usd"] > 0

# Korelasi: apakah volume_ratio lebih tinggi = win rate lebih tinggi?
print(df.groupby(pd.cut(df["volume_ratio"], bins=5))[["win","pnl_usd"]].mean())

# Optimal entry window (jam berapa sinyal paling profitable?)
df["entry_hour"] = pd.to_datetime(df["entry_time"], unit="ms").dt.hour
print(df.groupby("entry_hour")[["win","pnl_usd"]].mean())

# Equity curve
equity = pd.read_json("logs/equity.jsonl", lines=True)
equity.plot(x="ts", y="balance", title="Equity Curve")
```

---

## Tuning Parameter (Panduan)

| Kondisi Observasi             | Aksi Tuning                                      |
|-------------------------------|--------------------------------------------------|
| Win rate < 55%                | Naikkan MIN_PRICE_MOVE ke 2.0%, perketat OBI     |
| Terlalu sedikit sinyal (<20/d)| Turunkan MIN_WICK_RATIO ke 0.30                  |
| Banyak Time-Stop              | Kurangi TIME_STOP_CANDLES ke 3                   |
| SL sering kena                | Perluas TP_PCT ke 0.7%, perlonggar toleransi     |
| Volatilitas rendah            | Turunkan MIN_PRICE_MOVE ke 1.0% untuk sideway    |

---

## Peringatan & Limitasi

1. **Paper trading ≠ live trading**: Slippage nyata bisa 2-5x lebih buruk
   dari asumsi fill di harga TP/SL persis.
2. **Latency WebSocket**: Binance Combined Stream bisa delay 200-500ms.
   Untuk HFT nyata, pindah ke co-located server di AWS Tokyo.
3. **Rate Limit**: Binance REST limit 1200 req/menit. Fetch OB per sinyal
   bisa menjadi bottleneck jika simbol terlalu banyak bereaksi bersamaan.
4. **Wash trading**: Volume spike di altcoin kecil bisa palsu.
   Pertimbangkan menambahkan `min_quote_volume` filter per simbol.
5. **Backtesting**: Blueprint ini untuk forward testing. Untuk backtest
   historis, ganti DataFetcher dengan replay engine dari kline archive.
