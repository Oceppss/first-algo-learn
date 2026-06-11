"""
paper_trader.py — PaperTrader: Simulasi eksekusi posisi untuk MLF Bot

Tanggung jawab:
  - Menerima AnomalySignal → buka posisi paper (legacy mode)
  - ATAU: Gunakan QuantitativeEngine untuk multi-variable strategy (NEW)
  - Monitor posisi aktif per kline update
  - Eksekusi TP / SL / Time-Stop
  - Tracking equity dan statistik real-time
  - Thread-safe via asyncio.Lock
"""

import asyncio
import logging
import time
import uuid
from typing import Dict, List, Optional, Tuple

from config import PAPER_CFG, RISK_CFG
from core.models import (
    AnomalySignal, ClosedTrade, CloseReason, Kline, Position, Side,
    QuantitativePosition, QuantitativeSignal
)
from utils.logger import TradeLogger

logger = logging.getLogger("MLF.PaperTrader")


class PaperTrader:
    """
    Paper trading engine.

    Cara pakai:
        trader = PaperTrader()
        # Register sebagai callback di TradingLogic:
        logic = TradingLogic(fetcher, on_signal_callback=trader.on_signal)
        # Register sebagai callback di DataFetcher (monitor live kline):
        fetcher = DataFetcher(symbols, on_kline_closed=trader.on_kline_update)
    """

    def __init__(self):
        self.balance:   float = PAPER_CFG.INITIAL_BALANCE
        self.equity:    float = PAPER_CFG.INITIAL_BALANCE
        self._positions: Dict[str, Position]    = {}   # id → Position
        self._by_symbol: Dict[str, List[str]]   = {}   # symbol → [pos_ids]
        self._closed:    List[ClosedTrade]       = []
        self._lock:      asyncio.Lock            = asyncio.Lock()
        self._logger:    TradeLogger             = TradeLogger()

        # Stats
        self._total_signals  = 0
        self._rejected_signals = 0
        
        # Quantitative engine integration
        self._quantengine = None
        self._fetcher = None
        self._use_quant_mode = False
    
    def set_quantitative_engine(self, quantengine, fetcher):
        """
        Register quantitative engine untuk multi-variable strategy.
        Setelah ini, paper trader akan menggunakan quantitative rules.
        """
        self._quantengine = quantengine
        self._fetcher = fetcher
        self._use_quant_mode = True
        logger.info(
            "[PaperTrader] Quantitative Engine Mode ACTIVATED | "
            "Using multi-variable entry/exit logic (OI, ATR, CVD)"
        )

    # ═══════════════════════════════════════════════════════
    # ON SIGNAL — buka posisi baru
    # ═══════════════════════════════════════════════════════
    async def on_signal(self, signal: AnomalySignal):
        """
        Callback dari TradingLogic.
        Cek kapasitas → sizing → buka posisi.
        """
        async with self._lock:
            self._total_signals += 1
            symbol = signal.symbol

            # ── GATE 1: Max open positions ────────────────────────────
            if len(self._positions) >= RISK_CFG.MAX_OPEN_POSITIONS:
                logger.debug(
                    f"[{symbol}] Ditolak: max {RISK_CFG.MAX_OPEN_POSITIONS} posisi sudah terbuka."
                )
                self._rejected_signals += 1
                return

            # ── GATE 2: Tidak ada posisi aktif untuk simbol yang sama ─
            if symbol in self._by_symbol and self._by_symbol[symbol]:
                logger.debug(f"[{symbol}] Ditolak: sudah ada posisi aktif.")
                self._rejected_signals += 1
                return

            # ── GATE 3: Cukup modal ───────────────────────────────────
            margin_required = self.balance * RISK_CFG.MARGIN_PER_TRADE_PCT
            if margin_required > self.balance * 0.95:
                logger.warning(f"[{symbol}] Ditolak: margin tidak cukup.")
                self._rejected_signals += 1
                return

            # ── SIZING ────────────────────────────────────────────────
            # Notional size = margin * leverage
            # Quantity = notional / entry_price
            notional_usd = margin_required * RISK_CFG.LEVERAGE
            quantity     = notional_usd / signal.entry_price

            # ── OPEN POSITION ─────────────────────────────────────────
            pos_id = str(uuid.uuid4())[:8]
            pos = Position(
                id          = pos_id,
                symbol      = symbol,
                side        = signal.side,
                entry_price = signal.entry_price,
                tp_price    = signal.tp_price,
                sl_price    = signal.sl_price,
                size_usd    = notional_usd,
                quantity    = quantity,
                entry_time  = signal.signal_time,
                signal      = signal,
            )

            self._positions[pos_id] = pos
            if symbol not in self._by_symbol:
                self._by_symbol[symbol] = []
            self._by_symbol[symbol].append(pos_id)

            logger.info(
                f"📂 BUKA [{symbol}] {signal.side.value} | "
                f"ID={pos_id} | Entry={signal.entry_price:.4f} | "
                f"Notional={notional_usd:.1f}USD | Qty={quantity:.4f} | "
                f"TP={signal.tp_price:.4f} | SL={signal.sl_price:.4f}"
            )
            await self._logger.log_position_open(pos)

    # ═══════════════════════════════════════════════════════
    # ON KLINE UPDATE — monitor posisi setiap candle
    # ═══════════════════════════════════════════════════════
    async def on_kline_update(self, kline: Kline):
        """
        Dipanggil setiap kline closed.
        
        Modes:
          - QUANT MODE: Evaluasi entry rules + exit cases (multi-variable)
          - LEGACY MODE: Evaluasi TP/SL/Time-Stop (signal-based)
        """
        symbol = kline.symbol
        
        # ── QUANTITATIVE MODE: Entry Rules + Exit Cases ─────────────
        if self._use_quant_mode and self._quantengine and self._fetcher:
            await self._on_kline_update_quantitative(kline)
        
        # ── LEGACY MODE: Standard TP/SL ────────────────────────────
        else:
            await self._on_kline_update_legacy(kline)
    
    async def _on_kline_update_quantitative(self, kline: Kline):
        """
        Quantitative mode: Evaluate entry rules and exit cases.
        """
        symbol = kline.symbol
        
        # Get required data from sources
        current_price = kline.close
        prev_close = kline.open
        current_oi = self._fetcher.get_current_oi(symbol)
        current_cvd = self._fetcher.kline_buf.get_last_closed_cvd(symbol)  # FIX: BUG1 - Use snapshot from previous candle
        
        async with self._lock:
            # Update ATR untuk simbol
            self._quantengine.update_atr(symbol, kline.high, kline.low, prev_close)
            current_atr = self._quantengine.get_atr(symbol)
            
            # ── EVALUATE ENTRY RULES ──────────────────────────────
            # Only evaluate entry jika tidak ada posisi aktif
            if symbol not in self._by_symbol or not self._by_symbol[symbol]:
                entry_signal = self._quantengine.evaluate_entry_rules(
                    symbol=symbol,
                    current_price=current_price,
                    prev_close=prev_close,
                    current_oi=current_oi,
                    cvd_current=current_cvd,
                )
                
                if entry_signal:
                    await self._execute_quantitative_entry(entry_signal, current_oi, current_atr)
            
            # ── EVALUATE EXIT CASES (Jika ada active position) ────
            pos_ids = list(self._by_symbol.get(symbol, []))
            for pos_id in pos_ids:
                pos = self._positions.get(pos_id)
                if pos is None or not isinstance(pos, QuantitativePosition):
                    continue
                
                # Update current OI dan CVD di position
                pos.current_oi = current_oi
                
                # Evaluate 3 exit cases
                exit_result = self._quantengine.evaluate_exit_cases(
                    symbol=symbol,
                    current_price=current_price,
                    prev_close=prev_close,
                    current_oi=current_oi,
                    entry_price=pos.entry_price,
                    entry_oi=pos.entry_oi,
                    current_atr=current_atr,
                    current_sl=pos.sl_price,
                )
                
                if exit_result:
                    case_name, exit_signal = exit_result
                    pos.last_case = case_name
                    
                    # Handle exit based on case
                    if case_name == "HOLD_TRAIL":
                        # Update trailing stop loss
                        pos.trailing_sl = exit_signal.sl_price
                        pos.sl_price = exit_signal.sl_price
                        logger.debug(
                            f"[{symbol}] Trail SL Updated: {pos.trailing_sl:.8f}"
                        )
                    
                    elif case_name in ["FORCED_TRAP", "FORCED_EXHAUST"]:
                        # Force close immediately
                        exit_price = current_price
                        reason = CloseReason.STOP_LOSS  # Treat as forced exit
                        await self._close_quantitative_position(
                            pos, exit_price, reason, kline.open_time, exit_signal
                        )
    
    async def _on_kline_update_legacy(self, kline: Kline):
        """
        Legacy mode: Standard signal-based exit evaluation.
        """
        symbol = kline.symbol
        if symbol not in self._by_symbol:
            return

        pos_ids_to_check = list(self._by_symbol.get(symbol, []))
        if not pos_ids_to_check:
            return

        async with self._lock:
            for pos_id in pos_ids_to_check:
                pos = self._positions.get(pos_id)
                if pos is None:
                    continue

                # Increment time-stop counter (per closed candle)
                if kline.is_closed:
                    pos.candle_count += 1

                # ── CEK TP / SL ───────────────────────────────────────
                close_reason = self._evaluate_exit(pos, kline)

                if close_reason:
                    exit_price = self._get_exit_price(pos, close_reason, kline)
                    await self._close_position(pos, exit_price, close_reason, kline.open_time)
    
    async def _execute_quantitative_entry(
        self,
        signal: QuantitativeSignal,
        entry_oi: float,
        entry_atr: float,
    ):
        """
        Execute LONG entry dari quantitative signal.
        Create QuantitativePosition dengan OI tracking.
        """
        symbol = signal.symbol
        
        # ── GATE: Max open positions ────────────────────────────────
        if len(self._positions) >= RISK_CFG.MAX_OPEN_POSITIONS:
            logger.debug(f"[{symbol}] Entry rejected: max {RISK_CFG.MAX_OPEN_POSITIONS} positions already open")
            return
        
        # ── GATE: No existing position for this symbol ──────────────
        if symbol in self._by_symbol and self._by_symbol[symbol]:
            logger.debug(f"[{symbol}] Entry rejected: position already exists for this symbol")
            return
        
        # ── SIZING ────────────────────────────────────────────────
        margin_required = self.balance * RISK_CFG.MARGIN_PER_TRADE_PCT
        notional_usd = margin_required * RISK_CFG.LEVERAGE
        quantity = notional_usd / signal.entry_price
        
        # ── CREATE QUANTITATIVE POSITION ────────────────────────────
        pos_id = str(uuid.uuid4())[:8]
        pos = QuantitativePosition(
            id          = pos_id,
            symbol      = symbol,
            side        = Side.LONG,  # Quantitative only does LONG
            entry_price = signal.entry_price,
            tp_price    = signal.tp_price,
            sl_price    = signal.sl_price,
            size_usd    = notional_usd,
            quantity    = quantity,
            entry_time  = signal.timestamp,
            signal      = None,  # No legacy signal
            
            # Quantitative-specific
            entry_oi    = entry_oi,
            current_oi  = entry_oi,
            oi_60m_base = 0.0,  # Will be updated
            initial_atr = entry_atr,
            trailing_sl = signal.sl_price,
            entry_rule_a = True,  # Passed all rules to get here
            entry_rule_b = True,
            entry_rule_c = True,
        )
        
        self._positions[pos_id] = pos
        if symbol not in self._by_symbol:
            self._by_symbol[symbol] = []
        self._by_symbol[symbol].append(pos_id)
        
        logger.info(
            f"🟢 BUY AGRESYF [{symbol}] QUANT ENTRY | "
            f"ID={pos_id} | Entry={signal.entry_price:.8f} | "
            f"Notional=${notional_usd:.0f} | Qty={quantity:.4f} | "
            f"TP={signal.tp_price:.8f} | SL={signal.sl_price:.8f} | "
            f"ATR={entry_atr:.8f} | OI_Change={signal.oi_change_pct:.2f}% | "
            f"CVD={signal.cvd_current:.0f}"
        )
        
        await self._logger.log_position_open(pos)
    
    async def _close_quantitative_position(
        self,
        pos: QuantitativePosition,
        exit_price: float,
        reason: CloseReason,
        exit_time: int,
        exit_signal: Optional[QuantitativeSignal] = None,
    ):
        """
        Close quantitative position dan compute PnL.
        """
        # Calculate PnL
        raw_pnl = (exit_price - pos.entry_price) / pos.entry_price * pos.size_usd
        fee_both_sides = pos.size_usd * RISK_CFG.TAKER_FEE * 2
        net_pnl = raw_pnl - fee_both_sides
        pnl_pct = net_pnl / (pos.size_usd / RISK_CFG.LEVERAGE) * 100
        
        # Update balance
        self.balance += net_pnl
        
        # Duration
        duration_sec = (exit_time - pos.entry_time) / 1000
        
        trade = ClosedTrade(
            id           = pos.id,
            symbol       = pos.symbol,
            side         = pos.side,
            entry_price  = pos.entry_price,
            exit_price   = exit_price,
            size_usd     = pos.size_usd,
            quantity     = pos.quantity,
            entry_time   = pos.entry_time,
            exit_time    = exit_time,
            close_reason = reason,
            pnl_usd      = round(net_pnl, 4),
            pnl_pct      = round(pnl_pct, 4),
            duration_sec = round(duration_sec, 1),
            signal_meta  = exit_signal.to_dict() if exit_signal else None,
        )
        
        self._closed.append(trade)
        
        # Clean up
        del self._positions[pos.id]
        if pos.symbol in self._by_symbol:
            self._by_symbol[pos.symbol] = [
                pid for pid in self._by_symbol[pos.symbol] if pid != pos.id
            ]
        
        emoji = "✅" if trade.is_winner else "❌"
        exit_case = pos.last_case if hasattr(pos, 'last_case') else "TP/SL"
        logger.info(
            f"{emoji} CLOSE [{pos.symbol}] {pos.side.value} | "
            f"ID={pos.id} | Exit=${exit_price:.8f} [{exit_case}] | "
            f"PnL=${net_pnl:+.2f} ({pnl_pct:+.2f}%) | "
            f"Dur={duration_sec:.0f}s | Balance=${self.balance:.2f}"
        )
        
        await self._logger.log_trade_closed(trade)
        await self._logger.log_equity(self.balance, int(time.time() * 1000))

    def _evaluate_exit(self, pos: Position, kline: Kline) -> Optional[CloseReason]:
        """
        Evaluasi kondisi exit berdasarkan high/low kline saat ini.
        """
        h = kline.high
        l = kline.low

        if pos.side == Side.LONG:
            if h >= pos.tp_price:
                return CloseReason.TAKE_PROFIT
            if l <= pos.sl_price:
                return CloseReason.STOP_LOSS

        elif pos.side == Side.SHORT:
            if l <= pos.tp_price:
                return CloseReason.TAKE_PROFIT
            if h >= pos.sl_price:
                return CloseReason.STOP_LOSS

        # TIME-STOP: jika sudah N candle dan belum TP/SL
        if kline.is_closed and pos.candle_count >= RISK_CFG.TIME_STOP_CANDLES:
            return CloseReason.TIME_STOP

        return None

    def _get_exit_price(
        self,
        pos: Position,
        reason: CloseReason,
        kline: Kline
    ) -> float:
        """
        Tentukan harga exit:
        - TP/SL: gunakan harga target persis (asumsi fill di level itu)
        - Time-Stop: gunakan close candle (market order)
        """
        if reason == CloseReason.TAKE_PROFIT:
            return pos.tp_price
        elif reason == CloseReason.STOP_LOSS:
            return pos.sl_price
        else:  # TIME_STOP
            return kline.close

    async def _close_position(
        self,
        pos: Position,
        exit_price: float,
        reason: CloseReason,
        exit_time: int,
    ):
        """Tutup posisi, hitung PnL, update balance."""
        # ── HITUNG PnL ────────────────────────────────────────────────
        if pos.side == Side.LONG:
            raw_pnl = (exit_price - pos.entry_price) / pos.entry_price * pos.size_usd
        else:  # SHORT
            raw_pnl = (pos.entry_price - exit_price) / pos.entry_price * pos.size_usd

        # Kurangi taker fee dua sisi (entry + exit)
        fee_both_sides = pos.size_usd * RISK_CFG.TAKER_FEE * 2
        net_pnl        = raw_pnl - fee_both_sides
        pnl_pct        = net_pnl / (pos.size_usd / RISK_CFG.LEVERAGE) * 100

        # Update balance
        self.balance += net_pnl

        # Duration
        duration_sec = (exit_time - pos.entry_time) / 1000

        trade = ClosedTrade(
            id           = pos.id,
            symbol       = pos.symbol,
            side         = pos.side,
            entry_price  = pos.entry_price,
            exit_price   = exit_price,
            size_usd     = pos.size_usd,
            quantity     = pos.quantity,
            entry_time   = pos.entry_time,
            exit_time    = exit_time,
            close_reason = reason,
            pnl_usd      = round(net_pnl, 4),
            pnl_pct      = round(pnl_pct, 4),
            duration_sec = round(duration_sec, 1),
            signal_meta  = pos.signal.to_dict() if pos.signal else None,
        )

        self._closed.append(trade)

        # Bersihkan tracking
        del self._positions[pos.id]
        if pos.symbol in self._by_symbol:
            self._by_symbol[pos.symbol] = [
                pid for pid in self._by_symbol[pos.symbol] if pid != pos.id
            ]

        emoji = "✅" if trade.is_winner else "❌"
        logger.info(
            f"{emoji} TUTUP [{pos.symbol}] {pos.side.value} | "
            f"ID={pos.id} | Exit={exit_price:.4f} [{reason.value}] | "
            f"PnL={net_pnl:+.2f}USD ({pnl_pct:+.2f}%) | "
            f"Dur={duration_sec:.0f}s | Balance={self.balance:.2f}"
        )

        await self._logger.log_trade_closed(trade)
        await self._logger.log_equity(self.balance, int(time.time() * 1000))

    # ═══════════════════════════════════════════════════════
    # STATS & REPORTING
    # ═══════════════════════════════════════════════════════
    def get_stats(self) -> dict:
        """Hitung statistik real-time."""
        if not self._closed:
            return {
                "total_trades":    0,
                "total_signals":   self._total_signals,
                "rejected":        self._rejected_signals,
                "win_rate":        0.0,
                "total_pnl_usd":   0.0,
                "avg_win_usd":     0.0,
                "avg_loss_usd":    0.0,
                "profit_factor":   0.0,
                "avg_duration_sec": 0.0,
                "tp_count":        0,
                "sl_count":        0,
                "timestop_count":  0,
                "balance":         round(self.balance, 2),
                "pnl_pct":         round((self.balance - PAPER_CFG.INITIAL_BALANCE) / PAPER_CFG.INITIAL_BALANCE * 100, 2),
                "open_positions":  len(self._positions),
            }

        winners = [t for t in self._closed if t.is_winner]
        total   = len(self._closed)
        win_rate = len(winners) / total * 100

        tp_trades   = [t for t in self._closed if t.close_reason == CloseReason.TAKE_PROFIT]
        sl_trades   = [t for t in self._closed if t.close_reason == CloseReason.STOP_LOSS]
        time_trades = [t for t in self._closed if t.close_reason == CloseReason.TIME_STOP]

        total_pnl   = sum(t.pnl_usd for t in self._closed)
        avg_win     = sum(t.pnl_usd for t in winners) / len(winners) if winners else 0
        losers      = [t for t in self._closed if not t.is_winner]
        avg_loss    = sum(t.pnl_usd for t in losers) / len(losers) if losers else 0
        profit_factor = abs(avg_win * len(winners) / (avg_loss * len(losers))) if losers and avg_loss != 0 else float('inf')

        avg_dur = sum(t.duration_sec for t in self._closed) / total

        return {
            "total_trades":    total,
            "total_signals":   self._total_signals,
            "rejected":        self._rejected_signals,
            "win_rate":        round(win_rate, 2),
            "total_pnl_usd":   round(total_pnl, 2),
            "avg_win_usd":     round(avg_win, 2),
            "avg_loss_usd":    round(avg_loss, 2),
            "profit_factor":   round(profit_factor, 2),
            "avg_duration_sec": round(avg_dur, 1),
            "tp_count":        len(tp_trades),
            "sl_count":        len(sl_trades),
            "timestop_count":  len(time_trades),
            "balance":         round(self.balance, 2),
            "pnl_pct":         round((self.balance - PAPER_CFG.INITIAL_BALANCE) / PAPER_CFG.INITIAL_BALANCE * 100, 2),
            "open_positions":  len(self._positions),
        }

    def print_stats(self):
        stats = self.get_stats()
        divider = "─" * 52
        print(f"\n{divider}")
        print(f"  📊  MLF BOT — PAPER TRADING STATISTICS")
        print(f"{divider}")
        print(f"  Trades Total   : {stats['total_trades']} (Signals: {stats['total_signals']})")
        print(f"  Win Rate       : {stats['win_rate']}%")
        print(f"  Profit Factor  : {stats['profit_factor']}")
        print(f"  TP / SL / Time : {stats['tp_count']} / {stats['sl_count']} / {stats['timestop_count']}")
        print(f"  Avg Win        : ${stats['avg_win_usd']:+.2f} | Avg Loss: ${stats['avg_loss_usd']:+.2f}")
        print(f"  Total PnL      : ${stats['total_pnl_usd']:+.2f}")
        print(f"  Balance        : ${stats['balance']:.2f} ({stats['pnl_pct']:+.2f}%)")
        print(f"  Open Positions : {stats['open_positions']}")
        print(f"{divider}\n")
