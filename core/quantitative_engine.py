"""
quantitative_engine.py — Multi-Variable Quantitative Trading Engine

Logic Implemented:
  1. Entry Rules (LONG only):
     - Rule A: OI increase 3-5% over 60 min window (steady accumulation)
     - Rule B: Price increasing in current minute (Price_Change > 0)
     - Rule C: CVD clearly above threshold (buyer pressure)
     → If ALL 3 met: Execute LONG with SL=Entry-(1.5*ATR), TP=Entry+(2.0*ATR)

  2. Exit Management (Active LONG Position):
     - Case 1: Price↑ + OI↓ → Hold & trail SL up (lock profit)
     - Case 2: Price↓ + OI↑ → Force close (trapped by shorters)
     - Case 3: Price↓ + OI↓ → Force close (buyer exhaustion)
"""

import logging
import statistics
import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, Optional, Tuple

from config import RISK_CFG
from core.models import QuantitativeSignal

logger = logging.getLogger("MLF.QuantitativeEngine")


@dataclass
class ATRData:
    """ATR calculation helper."""
    true_ranges: Deque[float]
    atr_value: float = 0.0
    period: int = 14  # Standard ATR period
    
    def calculate(self, high: float, low: float, prev_close: float) -> float:
        """
        Calculate True Range and update ATR.
        True Range = max(high-low, |high-prevClose|, |low-prevClose|)
        """
        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close)
        )
        self.true_ranges.append(tr)
        
        if len(self.true_ranges) >= self.period:
            self.atr_value = statistics.mean(list(self.true_ranges)[-self.period:])
        else:
            self.atr_value = statistics.mean(self.true_ranges) if self.true_ranges else 0.0
        
        return self.atr_value


class QuantitativeEngine:
    """
    Multi-Variable Quantitative Trading Engine.
    
    Tracks entry conditions, exit cases, and position management based on:
    - ATR (volatility)
    - OI accumulation (60-min window)
    - CVD threshold (buyer/seller pressure)
    - Liquidation events
    - Price action
    """
    
    def __init__(self, symbols: list):
        self.symbols = [s.upper() for s in symbols]
        
        # ATR tracking per symbol
        self._atr_data: Dict[str, ATRData] = {
            sym: ATRData(true_ranges=deque(maxlen=50)) for sym in self.symbols
        }
        
        # OI history per symbol (keep 60+ minutes of history)
        self._oi_history: Dict[str, Deque[Tuple[int, float]]] = {
            sym: deque(maxlen=400) for sym in self.symbols  # FIX: BUG2 - 60min × 6 polls/min + buffer
        }
        
        # CVD thresholds (dynamic, can be tuned per symbol)
        self._cvd_thresholds: Dict[str, float] = {sym: 5000.0 for sym in self.symbols}
        
        # FIX: BUG3 - Track last price for USD conversion + USD thresholds
        self._last_price: Dict[str, float] = {sym: 0.0 for sym in self.symbols}
        self._cvd_thresholds_usd: Dict[str, float] = {sym: 50_000.0 for sym in self.symbols}  # $50K default
        
        # Entry state tracking
        self._last_entry_attempt: Dict[str, int] = {}  # symbol → timestamp
        self._entry_cooldown_ms = 180000  # 3-minute debounce
        
        # Liquidation monitoring
        self._recent_liquidations: Dict[str, Deque[Tuple[int, str]]] = {
            sym: deque(maxlen=10) for sym in self.symbols
        }
    
    # ═══════════════════════════════════════════════════════════════
    # ATR MANAGEMENT
    # ═══════════════════════════════════════════════════════════════
    
    def update_atr(self, symbol: str, high: float, low: float, prev_close: float) -> float:
        """
        Update ATR untuk simbol.
        Returns: Current ATR value
        """
        if symbol not in self._atr_data:
            return 0.0
        
        self._last_price[symbol] = prev_close  # FIX: BUG3 - Track price for CVD USD conversion
        atr = self._atr_data[symbol].calculate(high, low, prev_close)
        return atr
    
    def get_atr(self, symbol: str) -> float:
        """Get current ATR untuk simbol."""
        if symbol not in self._atr_data:
            return 0.0
        return self._atr_data[symbol].atr_value
    
    # ═══════════════════════════════════════════════════════════════
    # OI HISTORY & 60-MINUTE ACCUMULATION TRACKING
    # ═══════════════════════════════════════════════════════════════
    
    def record_oi(self, symbol: str, oi_value: float, timestamp_ms: int):
        """
        Record OI snapshot untuk simbol.
        Called setiap minute dari data source.
        """
        if symbol not in self._oi_history:
            return
        
        self._oi_history[symbol].append((timestamp_ms, oi_value))
    
    def get_oi_change_percent_60m(self, symbol: str) -> Optional[float]:
        """
        Calculate OI change % dari 60 menit lalu vs current.
        
        Returns:
          - float: Percentage change (e.g., 3.5 for +3.5%)
          - None: Jika tidak cukup data (< 60 minutes)
        """
        if symbol not in self._oi_history:
            return None
        
        history = self._oi_history[symbol]
        if len(history) < 2:
            return None
        
        # Get current OI (most recent)
        current_time, current_oi = history[-1]
        
        # Get OI dari ~60 minutes ago
        target_time = current_time - 60 * 60 * 1000  # 60 minutes in ms
        
        # Find closest entry >= 60 min ago
        oi_60m_ago = None
        for ts, oi in history:
            if ts <= target_time:
                oi_60m_ago = oi
            else:
                break
        
        if oi_60m_ago is None or oi_60m_ago == 0:
            return None
        
        # Calculate change %
        change_pct = (current_oi - oi_60m_ago) / oi_60m_ago * 100
        return change_pct
    
    # ═══════════════════════════════════════════════════════════════
    # ENTRY RULES EVALUATION
    # ═══════════════════════════════════════════════════════════════
    
    def evaluate_entry_rules(
        self,
        symbol: str,
        current_price: float,
        prev_close: float,
        current_oi: float,
        cvd_current: float,
    ) -> Optional[QuantitativeSignal]:
        """
        Evaluate all 3 entry rules untuk LONG position.
        
        Returns:
          - QuantitativeSignal: Jika semua 3 rules terpenuhi
          - None: Jika ada rule yang gagal
        """
        current_time_ms = int(time.time() * 1000)
        
        # ── DEBOUNCE: Tidak entry simbol yang sama dalam 3 menit ────
        last_attempt = self._last_entry_attempt.get(symbol, 0)
        if current_time_ms - last_attempt < self._entry_cooldown_ms:
            logger.debug(
                f"[{symbol}] Entry debounce active. "
                f"Elapsed: {current_time_ms - last_attempt}ms < {self._entry_cooldown_ms}ms"
            )
            return None
        
        # ── RULE A: OI Accumulation 3-5% ─────────────────────────────
        oi_change_pct = self.get_oi_change_percent_60m(symbol)
        rule_a_met = False
        reason_a = ""
        
        if oi_change_pct is None:
            reason_a = "OI history insufficient (< 60min data)"
        elif not (3.0 <= oi_change_pct <= 5.0):
            reason_a = (
                f"OI change {oi_change_pct:.2f}% outside 3-5% window "
                f"(too low or too high)"
            )
        else:
            rule_a_met = True
            reason_a = f"✓ OI steady accumulation: {oi_change_pct:.2f}%"
        
        if not rule_a_met:
            logger.debug(f"[{symbol}] Rule A FAIL: {reason_a}")
            return None
        
        # ── RULE B: Price Increasing ───────────────────────────────
        price_change = current_price - prev_close
        rule_b_met = price_change > 0
        reason_b = ""
        
        if not rule_b_met:
            reason_b = f"Price declining: {price_change:.8f}"
            logger.debug(f"[{symbol}] Rule B FAIL: {reason_b}")
            return None
        else:
            reason_b = f"✓ Price increasing: {price_change:.8f}"
        
        # ── RULE C: CVD Above Threshold ────────────────────────────
        # FIX: BUG3 - Convert CVD (base asset qty) to USD before comparing
        last_price = self._last_price.get(symbol, 0.0)
        cvd_usd = cvd_current * last_price if last_price > 0 else cvd_current
        threshold_usd = self._cvd_thresholds_usd.get(symbol, 50_000.0)
        rule_c_met = cvd_usd > threshold_usd
        reason_c = ""
        
        if not rule_c_met:
            reason_c = f"CVD ${cvd_usd:,.0f} < threshold ${threshold_usd:,.0f}"
            logger.debug(f"[{symbol}] Rule C FAIL: {reason_c}")
            return None
        else:
            reason_c = f"✓ Aggressive buyer execution: CVD ${cvd_usd:,.0f} > ${threshold_usd:,.0f}"
        
        # ── ALL RULES MET: Generate Entry Signal ────────────────────
        logger.info(
            f"🟢 BUY AGRESYF [{symbol}] | "
            f"{reason_a} | {reason_b} | {reason_c}"
        )
        
        # Calculate entry prices
        atr = self.get_atr(symbol)
        entry_price = current_price
        sl_price = entry_price - (1.5 * atr)
        tp_price = entry_price + (2.0 * atr)
        
        # Record entry attempt for debounce
        self._last_entry_attempt[symbol] = current_time_ms
        
        signal = QuantitativeSignal(
            symbol=symbol,
            signal_type="ENTRY_LONG",
            entry_price=entry_price,
            sl_price=sl_price,
            tp_price=tp_price,
            atr=atr,
            oi_change_pct=oi_change_pct,
            cvd_current=cvd_current,
            timestamp=current_time_ms,
            reason=f"All rules met: OI+{oi_change_pct:.2f}%, Price↑, CVD>{threshold:.0f}",
        )
        
        return signal
    
    # ═══════════════════════════════════════════════════════════════
    # EXIT CASES EVALUATION (Active LONG Position)
    # ═══════════════════════════════════════════════════════════════
    
    def evaluate_exit_cases(
        self,
        symbol: str,
        current_price: float,
        prev_close: float,
        current_oi: float,
        entry_price: float,
        entry_oi: float,
        current_atr: float,
        current_sl: float,
    ) -> Optional[Tuple[str, QuantitativeSignal]]:
        """
        Evaluate 3 exit cases untuk active LONG position.
        
        Returns:
          - (case_name, signal): Jika exit dipicu
          - None: Jika hold condition
        
        Cases:
          1. Price↑ + OI↓: Hold, trail stop (lock profit)
          2. Price↓ + OI↑: Force close (trapped)
          3. Price↓ + OI↓: Force close (exhausted)
        """
        current_time_ms = int(time.time() * 1000)
        price_change = current_price - prev_close
        oi_change = current_oi - entry_oi
        
        case_name = None
        close_signal = None
        
        # ── CASE 1: Price UP + OI DOWN ─────────────────────────────
        if price_change > 0 and oi_change < 0:
            case_name = "HOLD_TRAIL"
            # Calculate new trailing stop (tighten it)
            new_sl = current_price - (1.5 * current_atr)
            new_sl = max(new_sl, current_sl)  # Only move up, never down
            
            logger.debug(
                f"[{symbol}] CASE 1 (HOLD_TRAIL): "
                f"Price↑({price_change:.8f}) OI↓({oi_change:.0f}) | "
                f"Trailing SL from {current_sl:.8f} to {new_sl:.8f}"
            )
            
            # Return signal to tighten trail, not force close
            close_signal = QuantitativeSignal(
                symbol=symbol,
                signal_type="EXIT_TRAILING",
                entry_price=entry_price,
                sl_price=new_sl,
                tp_price=current_price + (2.0 * current_atr),
                atr=current_atr,
                oi_change_pct=(oi_change / entry_oi * 100) if entry_oi > 0 else 0,
                cvd_current=0.0,  # Not used for exit
                timestamp=current_time_ms,
                reason=f"Shorters covering (OI↓), Trail SL: {current_sl:.8f} → {new_sl:.8f}",
            )
            return (case_name, close_signal)
        
        # ── CASE 2: Price DOWN + OI UP ────────────────────────────
        elif price_change < 0 and oi_change > 0:
            case_name = "FORCED_TRAP"
            logger.warning(
                f"[{symbol}] CASE 2 (FORCED_TRAP - INVALIDATION): "
                f"Price↓({price_change:.8f}) OI↑({oi_change:.0f}) | "
                f"FORCE CLOSE: Aggressive shorters dumping market!"
            )
            
            close_signal = QuantitativeSignal(
                symbol=symbol,
                signal_type="EXIT_FORCED",
                entry_price=entry_price,
                sl_price=current_price,  # Force close at current price
                tp_price=current_price,
                atr=current_atr,
                oi_change_pct=(oi_change / entry_oi * 100) if entry_oi > 0 else 0,
                cvd_current=0.0,  # Not used for exit
                timestamp=current_time_ms,
                reason=f"Trapped: Shorters attacking (OI↑{oi_change:.0f}). Force close.",
            )
            return (case_name, close_signal)
        
        # ── CASE 3: Price DOWN + OI DOWN ──────────────────────────
        elif price_change < 0 and oi_change < 0:
            case_name = "FORCED_EXHAUST"
            logger.warning(
                f"[{symbol}] CASE 3 (FORCED_EXHAUST - EXHAUSTION): "
                f"Price↓({price_change:.8f}) OI↓({oi_change:.0f}) | "
                f"FORCE CLOSE: Buyer momentum lost, volume drying up!"
            )
            
            close_signal = QuantitativeSignal(
                symbol=symbol,
                signal_type="EXIT_FORCED",
                entry_price=entry_price,
                sl_price=current_price,  # Force close at current price
                tp_price=current_price,
                atr=current_atr,
                oi_change_pct=(oi_change / entry_oi * 100) if entry_oi > 0 else 0,
                cvd_current=0.0,  # Not used for exit
                timestamp=current_time_ms,
                reason=f"Exhaustion: Buyers lost momentum (OI↓{oi_change:.0f}). Force close.",
            )
            return (case_name, close_signal)
        
        # ── DEFAULT: HOLD ──────────────────────────────────────────
        # Price same or other combinations → HOLD
        return None
    
    # ═══════════════════════════════════════════════════════════════
    # LIQUIDATION MONITORING
    # ═══════════════════════════════════════════════════════════════
    
    def record_liquidation(self, symbol: str, liq_type: str, timestamp_ms: int):
        """
        Record liquidation event.
        liq_type: "LONG_LIQ" atau "SHORT_LIQ"
        """
        if symbol not in self._recent_liquidations:
            return
        
        self._recent_liquidations[symbol].append((timestamp_ms, liq_type))
        
        # Check for cascade (3+ liquidations dalam 60 detik)
        cascade_count = sum(
            1 for ts, _ in self._recent_liquidations[symbol]
            if timestamp_ms - ts <= 60000
        )
        
        if cascade_count >= 3:
            logger.warning(
                f"[{symbol}] LIQUIDATION CASCADE DETECTED: "
                f"{cascade_count} events dalam 60 detik!"
            )
    
    def has_liquidation_cascade(self, symbol: str) -> bool:
        """Check jika ada liquidation cascade (3+ dalam 60 detik)."""
        if symbol not in self._recent_liquidations:
            return False
        
        current_time = int(time.time() * 1000)
        recent = [
            ts for ts, _ in self._recent_liquidations[symbol]
            if current_time - ts <= 60000
        ]
        
        return len(recent) >= 3
