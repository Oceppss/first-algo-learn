"""
config.py — Centralized configuration untuk MLF Bot
"Micro-Liquidation Fade & Order Book Imbalance"
"""

from dataclasses import dataclass, field
from typing import List

# ──────────────────────────────────────────────
# BINANCE API
# ──────────────────────────────────────────────
BINANCE_REST_BASE    = "https://fapi.binance.com"
BINANCE_WS_BASE      = "wss://fstream.binance.com/market"
BINANCE_WS_COMBINED  = "wss://fstream.binance.com/market/stream?streams="

# ──────────────────────────────────────────────
# UNIVERSE — Daftar koin yang dipantau
# Pilih high-liquidity futures agar depth valid
# ──────────────────────────────────────────────
WATCHLIST: List[str] = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "LTCUSDT", "DOTUSDT",
    "ADAUSDT", "MATICUSDT", "NEARUSDT", "APTUSDT", "ARBUSDT",
    "OPUSDT", "SUIUSDT", "SEIUSDT", "TIAUSDT", "INJUSDT",
    "WLDUSDT", "FETUSDT", "RENDERUSDT", "JUPUSDT", "TONUSDT",
    "TRXUSDT", "ATOMUSDT", "FILUSDT", "HBARUSDT", "ALGOUSDT",
    "SANDUSDT", "MANAUSDT", "AAVEUSDT", "UNIUSDT", "CRVUSDT",
    "GMXUSDT", "PENDLEUSDT", "STRKUSDT", "EIGENUSDT", "ENAUSDT",
]

# ──────────────────────────────────────────────
# ENTRY CONDITIONS — "Crazy Logic" Parameters
# ──────────────────────────────────────────────
@dataclass
class EntryConfig:
    # Micro-Volatility Snap: minimum % move dalam 1-2 candle
    MIN_PRICE_MOVE_PCT: float = 1.5          # 1.5% dalam 1-2 menit
    LOOKBACK_CANDLES: int     = 2            # jumlah candle window observasi

    # Volume Exhaustion (Climax): volume spike multiplier
    VOLUME_SPIKE_MULTIPLIER: float = 4.0     # min 4x vs avg 30m
    VOLUME_AVG_WINDOW: int         = 30      # menit untuk baseline volume

    # Wick Detection: minimum wick ratio vs total candle range
    MIN_WICK_RATIO: float = 0.40             # wick >= 40% total range

    # Order Book Imbalance (opsional layer filter tambahan)
    OBI_THRESHOLD: float  = 0.60            # bid/(bid+ask) volume ratio
    OBI_DEPTH_LEVELS: int = 10              # level depth book yang dibaca

    # Z-Score filter: tolak entry jika price sudah terlalu jauh
    # (artinya anomali sudah "stale", ketinggalan kereta)
    ZSCORE_MAX_ENTRY: float = 3.5           # max z-score untuk entry

# ──────────────────────────────────────────────
# EXIT & RISK MANAGEMENT — HFT Style
# ──────────────────────────────────────────────
@dataclass
class RiskConfig:
    # Take Profit — ambil pantulan pertama saja
    TP_PCT: float = 0.50                    # 0.5% ROI per trade (sebelum fee)
    # Alternatif: bisa ganti ke Z-Score based TP (lihat trading_logic.py)

    # Stop Loss — hard stop di ujung wick anomali
    SL_PCT: float = 1.0                     # max cut loss -1.0%

    # Time-Stop — anomali HFT tidak boleh hidup lama
    TIME_STOP_CANDLES: int = 5              # tutup paksa setelah 5 candle 1m

    # Position sizing — isolasi margin ketat
    MARGIN_PER_TRADE_PCT: float = 0.05      # 5% dari paper balance per trade
    LEVERAGE: int               = 10        # leverage (hanya untuk simulasi sizing)
    MAX_OPEN_POSITIONS: int     = 5         # maks posisi simultan

    # Fee estimation (Binance Futures maker/taker)
    TAKER_FEE: float = 0.0004              # 0.04% per sisi

# ──────────────────────────────────────────────
# PAPER TRADING
# ──────────────────────────────────────────────
@dataclass
class PaperConfig:
    INITIAL_BALANCE: float = 10_000.0       # USD
    LOG_DIR: str           = "logs/"
    TRADE_LOG_FILE: str    = "logs/trades.csv"
    EQUITY_LOG_FILE: str   = "logs/equity.jsonl"
    SUMMARY_LOG_FILE: str  = "logs/session_summary.json"

# ──────────────────────────────────────────────
# WEBSOCKET STREAM CONFIG
# ──────────────────────────────────────────────
@dataclass
class StreamConfig:
    KLINE_INTERVAL: str        = "1m"
    RECONNECT_DELAY_SEC: float = 3.0
    PING_INTERVAL_SEC: float   = 20.0
    # Maks simbol per koneksi WS (Binance limit: 200 stream per conn)
    MAX_STREAMS_PER_CONN: int  = 100

# ──────────────────────────────────────────────
# INSTANTIATE GLOBAL CONFIGS
# ──────────────────────────────────────────────
ENTRY_CFG  = EntryConfig()
RISK_CFG   = RiskConfig()
PAPER_CFG  = PaperConfig()
STREAM_CFG = StreamConfig()
