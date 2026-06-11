"""
models.py — Semua dataclass/model data untuk MLF Bot
"""

from dataclasses import dataclass, field
from typing import Optional, List, Literal
from enum import Enum
import time


class Side(str, Enum):
    LONG  = "LONG"
    SHORT = "SHORT"


class CloseReason(str, Enum):
    TAKE_PROFIT = "TAKE_PROFIT"
    STOP_LOSS   = "STOP_LOSS"
    TIME_STOP   = "TIME_STOP"
    MANUAL      = "MANUAL"


@dataclass
class Kline:
    """Representasi satu candle 1m dari Binance."""
    symbol:       str
    open_time:    int           # unix ms
    open:         float
    high:         float
    low:          float
    close:        float
    volume:       float         # base asset volume (e.g., BTC)
    quote_volume: float         # USDT volume — lebih relevan untuk deteksi anomali
    trades:       int           # jumlah trade dalam candle
    is_closed:    bool          # True = candle sudah selesai

    @property
    def body_size(self) -> float:
        return abs(self.close - self.open)

    @property
    def total_range(self) -> float:
        return self.high - self.low if self.high != self.low else 1e-9

    @property
    def upper_wick(self) -> float:
        return self.high - max(self.open, self.close)

    @property
    def lower_wick(self) -> float:
        return min(self.open, self.close) - self.low

    @property
    def is_bearish(self) -> bool:
        return self.close < self.open

    @property
    def is_bullish(self) -> bool:
        return self.close >= self.open


@dataclass
class OrderBookSnapshot:
    """Snapshot top-N level order book."""
    symbol:    str
    timestamp: int
    bids:      List[List[float]]   # [[price, qty], ...]
    asks:      List[List[float]]   # [[price, qty], ...]

    @property
    def best_bid(self) -> float:
        return float(self.bids[0][0]) if self.bids else 0.0

    @property
    def best_ask(self) -> float:
        return float(self.asks[0][0]) if self.asks else 0.0

    @property
    def spread_pct(self) -> float:
        if self.best_ask == 0:
            return 0.0
        return (self.best_ask - self.best_bid) / self.best_ask * 100

    def bid_volume(self, levels: int = 10) -> float:
        return sum(float(b[1]) for b in self.bids[:levels])

    def ask_volume(self, levels: int = 10) -> float:
        return sum(float(a[1]) for a in self.asks[:levels])

    def order_book_imbalance(self, levels: int = 10) -> float:
        """
        OBI = bid_vol / (bid_vol + ask_vol)
        > 0.5: tekanan beli lebih besar (bullish bias)
        < 0.5: tekanan jual lebih besar (bearish bias)
        """
        bv = self.bid_volume(levels)
        av = self.ask_volume(levels)
        total = bv + av
        return bv / total if total > 0 else 0.5


@dataclass
class AnomalySignal:
    """
    Output dari TradingLogic setelah semua kondisi terpenuhi.
    Siap dikirim ke PaperTrader untuk eksekusi.
    """
    symbol:         str
    side:           Side
    entry_price:    float
    tp_price:       float
    sl_price:       float
    signal_time:    int            # unix ms

    # Metadata diagnostik — penting untuk analisis pasca-trade
    price_move_pct: float          # % move trigger
    volume_ratio:   float          # vol spike vs avg
    wick_ratio:     float          # wick / total range
    obi:            float          # order book imbalance
    z_score:        float          # z-score harga saat entry

    def to_dict(self) -> dict:
        return {
            "symbol":         self.symbol,
            "side":           self.side.value,
            "entry_price":    self.entry_price,
            "tp_price":       self.tp_price,
            "sl_price":       self.sl_price,
            "signal_time":    self.signal_time,
            "price_move_pct": round(self.price_move_pct, 4),
            "volume_ratio":   round(self.volume_ratio, 2),
            "wick_ratio":     round(self.wick_ratio, 4),
            "obi":            round(self.obi, 4),
            "z_score":        round(self.z_score, 4),
        }


@dataclass
class Position:
    """Posisi aktif di paper trader."""
    id:             str
    symbol:         str
    side:           Side
    entry_price:    float
    tp_price:       float
    sl_price:       float
    size_usd:       float          # notional size dalam USD
    quantity:       float          # qty koin
    entry_time:     int            # unix ms
    candle_count:   int = 0        # counter untuk time-stop
    signal:         Optional[AnomalySignal] = None

    @property
    def unrealized_pnl(self, current_price: float = 0.0) -> float:
        """Hitung PnL tanpa fee. Perlu pass current_price."""
        return 0.0  # computed di PaperTrader.get_unrealized_pnl()

    def to_dict(self) -> dict:
        return {
            "id":          self.id,
            "symbol":      self.symbol,
            "side":        self.side.value,
            "entry_price": self.entry_price,
            "tp_price":    self.tp_price,
            "sl_price":    self.sl_price,
            "size_usd":    round(self.size_usd, 2),
            "quantity":    self.quantity,
            "entry_time":  self.entry_time,
            "candle_count": self.candle_count,
        }


@dataclass
class ClosedTrade:
    """Record trade yang sudah ditutup."""
    id:             str
    symbol:         str
    side:           Side
    entry_price:    float
    exit_price:     float
    size_usd:       float
    quantity:       float
    entry_time:     int
    exit_time:      int
    close_reason:   CloseReason
    pnl_usd:        float          # net PnL setelah fee
    pnl_pct:        float          # % ROI trade ini
    duration_sec:   float
    signal_meta:    Optional[dict] = None

    @property
    def is_winner(self) -> bool:
        return self.pnl_usd > 0

    def to_csv_row(self) -> dict:
        return {
            "id":           self.id,
            "symbol":       self.symbol,
            "side":         self.side.value,
            "entry_price":  self.entry_price,
            "exit_price":   self.exit_price,
            "size_usd":     round(self.size_usd, 2),
            "pnl_usd":      round(self.pnl_usd, 4),
            "pnl_pct":      round(self.pnl_pct, 4),
            "duration_sec": round(self.duration_sec, 1),
            "close_reason": self.close_reason.value,
            "entry_time":   self.entry_time,
            "exit_time":    self.exit_time,
            **(self.signal_meta or {}),
        }


# ═══════════════════════════════════════════════════════════════
# QUANTITATIVE TRADING MODELS (untuk Multi-Variable Strategy)
# ═══════════════════════════════════════════════════════════════

@dataclass
class OISnapshot:
    """Open Interest snapshot dengan timestamp untuk historical tracking."""
    symbol:    str
    oi_value:  float        # Open Interest value
    timestamp: int          # unix ms
    
    @property
    def age_minutes(self) -> float:
        """Berapa menit yang lalu snapshot ini diambil."""
        return (int(time.time() * 1000) - self.timestamp) / 60000


@dataclass
class QuantitativePosition(Position):
    """Extended Position dengan quantitative-specific state."""
    
    # Quantitative state tracking
    entry_oi:           float = 0.0        # OI saat entry
    initial_atr:        float = 0.0        # ATR saat entry (untuk SL/TP)
    current_oi:         float = 0.0        # Current OI (updated setiap minute)
    oi_60m_base:        float = 0.0        # OI dari 60 min lalu
    
    # Trail stop tracking
    trailing_sl:        float = 0.0        # Current trailing stop loss level
    
    # Position status
    entry_rule_a:       bool = False       # OI accumulation 3-5% rule met
    entry_rule_b:       bool = False       # Price increasing rule met
    entry_rule_c:       bool = False       # CVD above threshold rule met
    
    # Case tracking (for debugging/logging)
    last_case:          Optional[Literal["HOLD_TRAIL", "FORCED_TRAP", "FORCED_EXHAUST"]] = None


@dataclass
class QuantitativeSignal:
    """Signal yang di-generate dari quantitative rules (berbeda dari AnomalySignal)."""
    symbol:         str
    signal_type:    Literal["ENTRY_LONG", "EXIT_FORCED", "EXIT_TRAILING"]
    entry_price:    float
    sl_price:       float
    tp_price:       float
    atr:            float           # ATR saat signal
    oi_change_pct:  float           # OI change % dari 60m lalu
    cvd_current:    float           # Current CVD value
    timestamp:      int             # unix ms
    reason:         str             # Deskripsi singkat alasan signal
    
    def to_dict(self) -> dict:
        return {
            "symbol":         self.symbol,
            "signal_type":    self.signal_type,
            "entry_price":    round(self.entry_price, 8),
            "sl_price":       round(self.sl_price, 8),
            "tp_price":       round(self.tp_price, 8),
            "atr":            round(self.atr, 8),
            "oi_change_pct":  round(self.oi_change_pct, 2),
            "cvd_current":    round(self.cvd_current, 2),
            "timestamp":      self.timestamp,
            "reason":         self.reason,
        }
