"""
trading_logic.py — "Crazy Logic" Engine untuk MLF Bot

Semua 4 kondisi entry harus terpenuhi BERSAMAAN:
  [1] Micro-Volatility Snap   : price spike >= 1.5% dalam 1-2 menit
  [2] Volume Exhaustion Climax: volume spike >= 4x rata-rata 30m terakhir
  [3] Wick Rejection           : wick >= 40% dari total candle range
  [4] Z-Score Filter           : z-score tidak melampaui batas (antistal entry)

Layer tambahan (opsional, meningkatkan presisi):
  [5] Order Book Imbalance (OBI): konfirmasi dari tekanan bid/ask
"""

import logging
import math
import time
from typing import Optional

from config import ENTRY_CFG, RISK_CFG
from core.data_fetcher import DataFetcher
from core.models import AnomalySignal, Kline, Side

logger = logging.getLogger("MLF.TradingLogic")


class TradingLogic:
    """
    Engine deteksi sinyal anomali.

    Flow per kline_closed event:
        on_kline_closed(kline)
          ├── [GATE] cek candle valid (cukup range)
          ├── [1] check_micro_volatility_snap()
          ├── [2] check_volume_exhaustion()
          ├── [3] check_wick_rejection()
          ├── [4] check_zscore_filter()
          ├── [5] check_obi() ← async, fetch OB on-demand
          └── → emit AnomalySignal ke PaperTrader
    """

    def __init__(self, fetcher: DataFetcher, on_signal_callback=None):
        self.fetcher     = fetcher
        self._on_signal  = on_signal_callback
        self._signal_log: dict = {}   # symbol → last signal time (debounce)

        # Gate counters — incremented on each early return for diagnostics
        self._gate_counts = {
            "total":    0,   # all closed candles evaluated
            "doji":     0,   # gate: range too small
            "g1_move":  0,   # gate 1: price move too small
            "g2_empty": 0,   # gate 2: buffer empty
            "g2_vol":   0,   # gate 2: volume ratio too low
            "g3_wick":  0,   # gate 3: wick ratio too small
            "g3_side":  0,   # gate 3: wick/snap side mismatch
            "g4_zscore":0,   # gate 4: z-score too extreme
            "g5_obi":   0,   # gate 5: OBI filter
            "debounce": 0,   # debounce
            "signals":  0,   # passed all gates
        }
        
        # Market context tracking — untuk mengerti kondisi pasar
        self._market_context = {
            "price_moves": [],       # nilai price_move yang ditolak di G1
            "volume_ratios": [],     # nilai volume_ratio yang ditolak di G2
            "wick_ratios": [],       # nilai wick_ratio yang ditolak di G3
            "last_stats_time": 0,    # timestamp periodic stats terakhir
        }

    # ═══════════════════════════════════════════════════════
    # ENTRY POINT — dipanggil oleh DataFetcher setiap candle closed
    # ═══════════════════════════════════════════════════════
    async def on_kline_closed(self, kline: Kline):
        """
        Main entry point. Evaluasi semua kondisi entry untuk 1 candle.
        Fully async, non-blocking.
        """
        symbol = kline.symbol
        self._gate_counts["total"] += 1

        # ── GATE: candle harus punya range yang bermakna ──────────────
        if kline.total_range < kline.close * 0.0001:
            self._gate_counts["doji"] += 1
            logger.debug(f"[{symbol}] SKIP: range too small ({kline.total_range:.6f} < {kline.close * 0.0001:.6f})")
            return  # candle doji/flat, skip

        buf = self.fetcher.kline_buf

        # ── [1] MICRO-VOLATILITY SNAP ─────────────────────────────────
        price_move_pct, snap_side = self._check_micro_volatility_snap(kline)
        if price_move_pct < ENTRY_CFG.MIN_PRICE_MOVE_PCT:
            self._gate_counts["g1_move"] += 1
            self._market_context["price_moves"].append(price_move_pct)
            logger.debug(f"[{symbol}] SKIP [1] Micro-Volatility Snap: move={price_move_pct:.2f}% < threshold {ENTRY_CFG.MIN_PRICE_MOVE_PCT:.2f}%")
            return
        # snap_side: "UP" atau "DOWN"

        # ── [2] VOLUME EXHAUSTION CLIMAX ─────────────────────────────
        avg_vol = buf.avg_volume(symbol, window=ENTRY_CFG.VOLUME_AVG_WINDOW)
        if avg_vol == 0:
            self._gate_counts["g2_empty"] += 1
            logger.debug(f"[{symbol}] SKIP [2] Volume Exhaustion: avg_vol=0 (buffer insufficient)")
            return  # belum cukup data
        volume_ratio = kline.quote_volume / avg_vol
        if volume_ratio < ENTRY_CFG.VOLUME_SPIKE_MULTIPLIER:
            self._gate_counts["g2_vol"] += 1
            self._market_context["volume_ratios"].append(volume_ratio)
            logger.debug(f"[{symbol}] SKIP [2] Volume Exhaustion: ratio={volume_ratio:.2f}x < threshold {ENTRY_CFG.VOLUME_SPIKE_MULTIPLIER:.2f}x (avg_vol={avg_vol:.1f})")
            return

        # ── [3] WICK REJECTION ───────────────────────────────────────
        wick_ratio, wick_side = self._check_wick_rejection(kline)
        if wick_ratio < ENTRY_CFG.MIN_WICK_RATIO:
            self._gate_counts["g3_wick"] += 1
            self._market_context["wick_ratios"].append(wick_ratio)
            logger.debug(f"[{symbol}] SKIP [3] Wick Rejection: wick_ratio={wick_ratio:.2%} < threshold {ENTRY_CFG.MIN_WICK_RATIO:.2%}")
            return
        # wick_side harus BERLAWANAN dengan snap_side untuk fade
        # (pump → upper wick → SHORT; dump → lower wick → LONG)
        if snap_side == "UP"   and wick_side != "UPPER":
            self._gate_counts["g3_side"] += 1
            logger.debug(f"[{symbol}] SKIP [3] Wick Rejection: side mismatch (snap={snap_side}, wick={wick_side})")
            return
        if snap_side == "DOWN" and wick_side != "LOWER":
            self._gate_counts["g3_side"] += 1
            logger.debug(f"[{symbol}] SKIP [3] Wick Rejection: side mismatch (snap={snap_side}, wick={wick_side})")
            return

        # ── [4] Z-SCORE FILTER ───────────────────────────────────────
        z_score = self._compute_zscore(kline, symbol)
        if abs(z_score) > ENTRY_CFG.ZSCORE_MAX_ENTRY:
            self._gate_counts["g4_zscore"] += 1
            logger.debug(f"[{symbol}] SKIP [4] Z-Score: zscore={z_score:.2f} > max {ENTRY_CFG.ZSCORE_MAX_ENTRY:.2f}")
            return

        # ── [5] ORDER BOOK IMBALANCE ─────────────────────────────────
        # Fetch order book live (async, ~3ms latency)
        ob = await self.fetcher.fetch_order_book(symbol, limit=ENTRY_CFG.OBI_DEPTH_LEVELS)
        obi = ob.order_book_imbalance(ENTRY_CFG.OBI_DEPTH_LEVELS) if ob else 0.5

        # Validasi OBI:
        #   SHORT (fade pump): OBI rendah (<0.4) = seller dominan, konfirmasi reversal
        #   LONG  (fade dump): OBI tinggi (>0.6) = buyer dominan, konfirmasi reversal
        entry_side = Side.SHORT if snap_side == "UP" else Side.LONG
        obi_valid  = True
        if entry_side == Side.SHORT and obi > (1 - ENTRY_CFG.OBI_THRESHOLD):
            obi_valid = False
            self._gate_counts["g5_obi"] += 1
            logger.debug(f"[{symbol}] SKIP [5] OBI: SHORT but OBI={obi:.2f} still bullish (threshold < {1 - ENTRY_CFG.OBI_THRESHOLD:.2f})")
        elif entry_side == Side.LONG and obi < ENTRY_CFG.OBI_THRESHOLD:
            obi_valid = False
            self._gate_counts["g5_obi"] += 1
            logger.debug(f"[{symbol}] SKIP [5] OBI: LONG but OBI={obi:.2f} still bearish (threshold > {ENTRY_CFG.OBI_THRESHOLD:.2f})")

        if not obi_valid:
            return  # OBI tidak konfirmasi

        # ── DEBOUNCE: Jangan signal simbol yang sama dalam 3 menit ───
        now_ms  = int(time.time() * 1000)
        last_ts = self._signal_log.get(symbol, 0)
        if now_ms - last_ts < 3 * 60 * 1000:
            self._gate_counts["debounce"] += 1
            logger.debug(f"[{symbol}] SKIP: Debounce active (elapsed {now_ms - last_ts}ms < 180000ms)")
            return
        self._signal_log[symbol] = now_ms

        # ── HITUNG TP & SL ───────────────────────────────────────────
        entry_price, tp_price, sl_price = self._compute_prices(
            kline, entry_side
        )

        # ── EMIT SINYAL ──────────────────────────────────────────────
        signal = AnomalySignal(
            symbol         = symbol,
            side           = entry_side,
            entry_price    = entry_price,
            tp_price       = tp_price,
            sl_price       = sl_price,
            signal_time    = now_ms,
            price_move_pct = price_move_pct,
            volume_ratio   = volume_ratio,
            wick_ratio     = wick_ratio,
            obi            = obi,
            z_score        = z_score,
        )

        logger.info(
            f"🎯 SINYAL [{symbol}] {entry_side.value} | "
            f"Entry={entry_price:.4f} TP={tp_price:.4f} SL={sl_price:.4f} | "
            f"Move={price_move_pct:.2f}% VolRatio={volume_ratio:.1f}x "
            f"Wick={wick_ratio:.2%} OBI={obi:.2f} Z={z_score:.2f}"
        )

        self._gate_counts["signals"] += 1
        if self._on_signal:
            await self._on_signal(signal)

    def log_filter_stats(self):
        """Log gate-breakdown summary at INFO level (called periodically from main.py)."""
        g = self._gate_counts
        total = g["total"] or 1  # avoid div/0
        
        # Market context analysis
        market_cond = self._classify_market_condition()
        
        logger.info(
            f"[Gates] Total={g['total']} | "
            f"Doji={g['doji']} "
            f"G1={g['g1_move']} "
            f"G2_empty={g['g2_empty']} G2_vol={g['g2_vol']} "
            f"G3_wick={g['g3_wick']} G3_side={g['g3_side']} "
            f"G4={g['g4_zscore']} "
            f"G5={g['g5_obi']} "
            f"Debounce={g['debounce']} "
            f"→ Signals={g['signals']}"
        )
        
        # Log market condition context
        logger.info(
            f"[Market Context] Condition: {market_cond['condition']} | "
            f"Avg Price Move: {market_cond['avg_price_move']:.3f}% "
            f"(threshold: {ENTRY_CFG.MIN_PRICE_MOVE_PCT:.2f}%) | "
            f"Avg Vol Ratio: {market_cond['avg_volume_ratio']:.2f}x "
            f"(threshold: {ENTRY_CFG.VOLUME_SPIKE_MULTIPLIER:.2f}x) | "
            f"Avg Wick Ratio: {market_cond['avg_wick_ratio']:.2%} "
            f"(threshold: {ENTRY_CFG.MIN_WICK_RATIO:.2%})"
        )
    
    def _classify_market_condition(self) -> dict:
        """
        Klasifikasi kondisi pasar berdasarkan nilai yang ditolak di gate.
        Membantu memahami apakah threshold terlalu ketat atau market memang sideways.
        
        Returns dict dengan:
            - condition: "EXTREME" (signals bertebaran) | "NORMAL" (sinyal biasa) | 
                        "SIDEWAYS" (price move terlalu kecil) | "LOW_VOL" (volume terlalu rendah)
            - avg_price_move: rata-rata price_move dari rejected candles
            - avg_volume_ratio: rata-rata volume_ratio dari rejected candles
            - avg_wick_ratio: rata-rata wick_ratio dari rejected candles
        """
        moves = self._market_context["price_moves"]
        vols = self._market_context["volume_ratios"]
        wicks = self._market_context["wick_ratios"]
        
        avg_move = sum(moves) / len(moves) if moves else 0.0
        avg_vol = sum(vols) / len(vols) if vols else 0.0
        avg_wick = sum(wicks) / len(wicks) if wicks else 0.0
        
        signals = self._gate_counts["signals"]
        total = self._gate_counts["total"]
        signal_rate = signals / max(total, 1)
        
        # Determine condition
        if signal_rate > 0.05:  # >5% signals passing all gates
            condition = "EXTREME"
        elif avg_move < ENTRY_CFG.MIN_PRICE_MOVE_PCT * 0.3:  # very small moves
            condition = "SIDEWAYS"
        elif avg_vol < ENTRY_CFG.VOLUME_SPIKE_MULTIPLIER * 0.3:  # very low volume
            condition = "LOW_VOL"
        else:
            condition = "NORMAL"
        
        return {
            "condition": condition,
            "avg_price_move": avg_move,
            "avg_volume_ratio": avg_vol,
            "avg_wick_ratio": avg_wick,
            "signal_rate": signal_rate,
        }

    # ═══════════════════════════════════════════════════════
    # CONDITION CHECKS
    # ═══════════════════════════════════════════════════════

    def _check_micro_volatility_snap(self, kline: Kline) -> tuple[float, str]:
        """
        Kondisi [1]: Harga bergerak >= X% dalam 1 candle ini.
        Wick termasuk dalam kalkulasi karena kita mengukur high-low range.

        Returns: (price_move_pct, "UP"/"DOWN")
        """
        # Ukur dari open ke high (bullish snap) atau open ke low (bearish snap)
        up_move   = (kline.high  - kline.open) / kline.open * 100
        down_move = (kline.open  - kline.low)  / kline.open * 100

        if up_move >= down_move:
            return up_move, "UP"
        else:
            return down_move, "DOWN"

    def _check_wick_rejection(self, kline: Kline) -> tuple[float, str]:
        """
        Kondisi [3]: Wick harus >= 40% dari total candle range.
        Upper wick = rejection dari atas (bullish spike ditolak)
        Lower wick = rejection dari bawah (bearish spike ditolak)

        Returns: (wick_ratio, "UPPER"/"LOWER")
        """
        total_range = kline.total_range
        upper_wick_ratio = kline.upper_wick / total_range
        lower_wick_ratio = kline.lower_wick / total_range

        if upper_wick_ratio >= lower_wick_ratio:
            return upper_wick_ratio, "UPPER"
        else:
            return lower_wick_ratio, "LOWER"

    def _compute_zscore(self, kline: Kline, symbol: str) -> float:
        """
        Kondisi [4]: Z-Score dari close price vs distribusi 30 candle terakhir.
        Tujuan: tolak entry jika harga sudah terlalu jauh dari distribusi normal
        (anomali sudah berumur, kita ketinggalan momen).
        """
        mean   = self.fetcher.kline_buf.price_mean(symbol, window=30)
        stddev = self.fetcher.kline_buf.price_stddev(symbol, window=30)
        if stddev == 0:
            return 0.0
        return (kline.close - mean) / stddev

    def _compute_prices(
        self,
        kline: Kline,
        side: Side,
    ) -> tuple[float, float, float]:
        """
        Hitung entry, TP, dan SL berdasarkan logika HFT scalping.

        Entry  : close price candle anomali (eksekusi market order saat candle tutup)
        TP     : entry ± TP_PCT% (ambil pantulan pertama)
        SL     : ujung wick terjauh candle anomali

        Untuk SHORT (fade pump):
            SL = kline.high (ujung atas wick, jika wick di atas)
            TP = entry - TP_PCT%
        Untuk LONG (fade dump):
            SL = kline.low  (ujung bawah wick, jika wick di bawah)
            TP = entry + TP_PCT%
        """
        entry = kline.close

        if side == Side.SHORT:
            tp = entry * (1 - RISK_CFG.TP_PCT / 100)
            sl = kline.high  # hard stop di puncak wick
        else:  # LONG
            tp = entry * (1 + RISK_CFG.TP_PCT / 100)
            sl = kline.low   # hard stop di dasar wick

        # Validasi: pastikan SL distance tidak melampaui SL_PCT hard limit
        sl_distance_pct = abs(entry - sl) / entry * 100
        if sl_distance_pct > RISK_CFG.SL_PCT:
            # Jika wick terlalu panjang (>1%), clamp SL ke max hard stop
            if side == Side.SHORT:
                sl = entry * (1 + RISK_CFG.SL_PCT / 100)
            else:
                sl = entry * (1 - RISK_CFG.SL_PCT / 100)

        return round(entry, 6), round(tp, 6), round(sl, 6)

    # ═══════════════════════════════════════════════════════
    # POSITION EXIT CHECK — dipanggil per kline update live
    # ═══════════════════════════════════════════════════════
    def check_exit(
        self,
        current_high:  float,
        current_low:   float,
        current_close: float,
        tp:            float,
        sl:            float,
        side:          Side,
    ) -> Optional[str]:
        """
        Cek apakah posisi harus ditutup.
        Returns: "TP", "SL", atau None
        Dipanggil setiap live kline update untuk kecepatan maksimum.
        """
        if side == Side.LONG:
            if current_high >= tp:
                return "TP"
            if current_low  <= sl:
                return "SL"
        elif side == Side.SHORT:
            if current_low  <= tp:
                return "TP"
            if current_high >= sl:
                return "SL"
        return None
