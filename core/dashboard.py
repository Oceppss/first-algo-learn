"""
dashboard.py — Live Monitoring Dashboard untuk MLF Bot

Menampilkan real-time data per simbol dalam format tabel:
  - Price + % change dari candle sebelumnya
  - Open Interest (polling REST endpoint)
  - Volume ratio (current vs 30m average)
  - CVD (Cumulative Volume Delta) — perlu aggTrade stream
  - Gate status (berapa gate yang sudah dilolos)
  - Liquidation feed dengan penerjemahan yang benar

Tujuan: membantu operator memahami market context dan mengapa sinyal jarang/sering muncul.
"""

import asyncio
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Deque

import aiohttp
from config import BINANCE_REST_BASE, WATCHLIST, ENTRY_CFG

logger = logging.getLogger("MLF.Dashboard")


@dataclass
class SymbolSnapshot:
    """Real-time snapshot untuk satu simbol."""
    symbol: str
    price: float = 0.0
    prev_close: float = 0.0
    price_change_pct: float = 0.0
    
    open_interest: float = 0.0  # USD value
    volume_current: float = 0.0
    volume_avg_30m: float = 0.0
    volume_ratio: float = 1.0
    
    cvd: float = 0.0  # Cumulative Volume Delta
    cvd_signal: str = ""  # "BULLISH" | "BEARISH" | "NEUTRAL"
    
    gate_status: str = "WAITING"  # "GATE1", "GATE2", etc atau "SIGNAL EMITTED"
    last_update_ts: float = field(default_factory=time.time)
    
    def is_stale(self, max_age_sec: float = 65.0) -> bool:
        """Check if data is older than max_age_sec."""
        return time.time() - self.last_update_ts > max_age_sec


@dataclass
class LiquidationEvent:
    """Liquidation event untuk display."""
    symbol: str
    position_type: str  # "SHORT LIQ" atau "LONG LIQ"
    qty: float
    price: float
    usd_value: float
    timestamp: float
    
    def age_sec(self) -> float:
        """Berapa lama yang lalu event ini terjadi (dalam detik)."""
        return time.time() - self.timestamp


@dataclass
class FifteenMinSnapshot:
    # NEW: 15m summary tracking per 15-minute window
    """Aggregated 15-minute snapshot per symbol."""
    symbol: str
    window_start: int           # unix ms — start of this 15-min window
    price_open: float = 0.0
    price_close: float = 0.0
    price_high: float = 0.0
    price_low: float = 0.0
    oi_open: float = 0.0
    oi_close: float = 0.0
    oi_change_pct: float = 0.0
    total_volume_usd: float = 0.0
    cvd_net: float = 0.0
    liq_long_count: int = 0
    liq_short_count: int = 0
    liq_long_usd: float = 0.0
    liq_short_usd: float = 0.0
    liq_net_usd: float = 0.0


class Dashboard:
    """
    Live monitoring dashboard. Kumpulin data dari berbagai source dan display.
    
    Cara pakai:
        dashboard = Dashboard(symbols=WATCHLIST, fetcher=fetcher)
        asyncio.create_task(dashboard.run())
        # setelah itu, dashboard.get_snapshot(symbol) bisa dipakai kapan saja
    """
    
    def __init__(self, symbols: List[str], fetcher=None):
        self.symbols = [s.upper() for s in symbols]
        self.fetcher = fetcher  # Reference untuk feed OI data
        self._snapshots: Dict[str, SymbolSnapshot] = {
            sym: SymbolSnapshot(symbol=sym) for sym in self.symbols
        }
        
        # Liquidation event history (untuk display feed)
        self._liquidation_queue: Deque[LiquidationEvent] = deque(maxlen=50)
        
        # REST session untuk polling OI
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        
        # Gate status tracker per simbol
        self._gate_status_map: Dict[str, str] = defaultdict(lambda: "INIT")
        
        # NEW: 15m summary accumulators
        self._15m_data: Dict[str, FifteenMinSnapshot] = {}
        self._15m_window_start: int = 0
        self._15m_task: Optional[asyncio.Task] = None
    
    async def start(self):
        """Start dashboard background tasks."""
        self._running = True
        connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
        self._session = aiohttp.ClientSession(connector=connector)
        
        # NEW: 15m summary initialization
        self._reset_15m_window()
        self._15m_task = asyncio.create_task(self._15m_summary_loop())
        
        # Run parallel tasks
        await asyncio.gather(
            self._poll_open_interest(),
            self._print_display_loop(),
            return_exceptions=True,
        )
    
    def stop(self):
        """Stop dashboard."""
        self._running = False
        # NEW: Cancel 15m summary task
        if self._15m_task:
            self._15m_task.cancel()
    
    async def _poll_open_interest(self, interval_sec: float = 10.0):
        """
        Poll Open Interest setiap N detik per simbol.
        Binance endpoint: GET /fapi/v1/openInterest
        Perlu request terpisah untuk tiap simbol (tidak bisa batch).
        """
        while self._running:
            if not self._session:
                await asyncio.sleep(interval_sec)
                continue
            
            tasks = [
                self._fetch_oi_for_symbol(sym)
                for sym in self.symbols
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(interval_sec)
    
    async def _fetch_oi_for_symbol(self, symbol: str):
        """Fetch dan update OI untuk satu simbol."""
        if not self._session:
            return
        
        try:
            url = f"{BINANCE_REST_BASE}/fapi/v1/openInterest"
            params = {"symbol": symbol}
            async with self._session.get(url, params=params, timeout=5) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    # data = {"symbol": "BTCUSDT", "openInterest": "123456.789", "time": 1234567890}
                    if "openInterest" in data:
                        oi = float(data["openInterest"])
                        if symbol in self._snapshots:
                            self._snapshots[symbol].open_interest = oi
                            self._snapshots[symbol].last_update_ts = time.time()
                            
                            # Feed OI to fetcher untuk quantitative engine
                            if self.fetcher:
                                await self.fetcher.update_oi(symbol, oi)
        except Exception as e:
            logger.debug(f"[OI] Fetch failed for {symbol}: {e}")
    
    async def _print_display_loop(self, interval_sec: float = 30.0):
        """Periodically print dashboard display."""
        while self._running:
            await asyncio.sleep(interval_sec)
            self._print_dashboard()
    
    def _print_dashboard(self):
        """Print current dashboard state ke console."""
        logger.info("=" * 120)
        logger.info("[DASHBOARD] LIVE MONITORING — Real-time Market Context")
        logger.info("=" * 120)
        
        # Per-symbol table
        logger.info(
            f"{'Symbol':<12} {'Price':<12} {'Δ%':<8} {'OI (USD)':<15} "
            f"{'Vol Ratio':<12} {'CVD':<12} {'Status':<20}"
        )
        logger.info("-" * 120)
        
        for symbol in self.symbols:
            snap = self._snapshots.get(symbol)
            if not snap:
                continue
            
            stale_marker = "⚠ STALE" if snap.is_stale() else ""
            logger.info(
                f"{symbol:<12} {snap.price:>11.4f} {snap.price_change_pct:>7.2f}% "
                f"{snap.open_interest:>14,.0f} {snap.volume_ratio:>11.2f}x "
                f"{snap.cvd:>11.2f} {self._gate_status_map.get(symbol, 'INIT'):<20} {stale_marker}"
            )
        
        logger.info("-" * 120)
        
        # Liquidation feed (last 10)
        liq_list = list(self._liquidation_queue)[-10:] if self._liquidation_queue else []
        if liq_list:
            logger.info("[LIQUIDATIONS] Last 10 events:")
            for liq in liq_list:
                age_str = f"{liq.age_sec():.1f}s ago"
                logger.info(
                    f"  💥 {liq.symbol} {liq.position_type:10s} | "
                    f"Qty={liq.qty:>8.4f} | Price={liq.price:>12.4f} | "
                    f"~${liq.usd_value:>12,.2f} | {age_str}"
                )
        
        logger.info("=" * 120)
    
    # ═══════════════════════════════════════════════════════
    # PUBLIC API — dipanggil dari main bot logic
    # ═══════════════════════════════════════════════════════
    
    def update_kline(
        self,
        symbol: str,
        price: float,
        prev_close: float,
        volume: float,
        avg_volume_30m: float,
    ):
        """Update price dan volume info dari kline update."""
        if symbol not in self._snapshots:
            return
        
        snap = self._snapshots[symbol]
        snap.price = price
        snap.prev_close = prev_close
        snap.price_change_pct = (price - prev_close) / prev_close * 100 if prev_close > 0 else 0.0
        snap.volume_current = volume
        snap.volume_avg_30m = avg_volume_30m
        snap.volume_ratio = volume / avg_volume_30m if avg_volume_30m > 0 else 1.0
        snap.last_update_ts = time.time()
        
        # NEW: Accumulate for 15m summary
        if symbol in self._15m_data:
            d = self._15m_data[symbol]
            if d.price_open == 0.0:
                d.price_open = price
            d.price_close = price
            d.price_high = max(d.price_high, price)
            d.price_low = min(d.price_low, price) if d.price_low > 0 else price
            d.total_volume_usd += volume
    
    def update_cvd(self, symbol: str, cvd_delta: float):
        """Update CVD value (accumulate delta for current candle)."""
        if symbol not in self._snapshots:
            return
        
        snap = self._snapshots[symbol]
        snap.cvd += cvd_delta
        
        # Simple CVD signal: positive = buyer dominance, negative = seller dominance
        if snap.cvd > 0:
            snap.cvd_signal = "BULLISH"
        elif snap.cvd < 0:
            snap.cvd_signal = "BEARISH"
        else:
            snap.cvd_signal = "NEUTRAL"
        
        # NEW: Accumulate for 15m summary
        if symbol in self._15m_data:
            self._15m_data[symbol].cvd_net += cvd_delta
    
    def update_gate_status(self, symbol: str, gate_level: str):
        """
        Update gate filtering status untuk simbol.
        gate_level: "G1", "G2", "G3", "G4", "G5", "SIGNAL"
        """
        self._gate_status_map[symbol] = gate_level
    
    def record_liquidation(
        self,
        symbol: str,
        side_raw: str,  # "BUY" atau "SELL" dari Binance
        qty: float,
        price: float,
    ):
        """
        Record liquidation event. Translate BUY/SELL ke position type.
        """
        position_type = "SHORT LIQ" if side_raw == "BUY" else "LONG LIQ" if side_raw == "SELL" else "UNK LIQ"
        usd_value = qty * price
        
        event = LiquidationEvent(
            symbol=symbol,
            position_type=position_type,
            qty=qty,
            price=price,
            usd_value=usd_value,
            timestamp=time.time(),
        )
        self._liquidation_queue.append(event)
        logger.debug(f"[Liq] Recorded: {position_type} {qty} @ {price} = ${usd_value:,.2f}")
        
        # NEW: Accumulate for 15m summary
        if symbol in self._15m_data:
            d = self._15m_data[symbol]
            if side_raw == "BUY":
                d.liq_short_count += 1
                d.liq_short_usd += usd_value
            elif side_raw == "SELL":
                d.liq_long_count += 1
                d.liq_long_usd += usd_value
            d.liq_net_usd = d.liq_short_usd - d.liq_long_usd
    
    def update_oi(self, symbol: str, oi_value: float):
        # NEW: Track OI for 15m summary
        """Update OI value untuk 15m accumulation."""
        if symbol not in self._15m_data:
            return
        
        d = self._15m_data[symbol]
        if d.oi_open == 0.0:
            d.oi_open = oi_value
        d.oi_close = oi_value
        if d.oi_open > 0:
            d.oi_change_pct = (d.oi_close - d.oi_open) / d.oi_open * 100
    
    def _reset_15m_window(self):
        # NEW: Reset 15m summary accumulators
        """Reset semua accumulator dan set window_start baru."""
        now = int(time.time() * 1000)
        for sym in self.symbols:
            current_price = self._snapshots[sym].price
            current_oi = self._snapshots[sym].open_interest
            self._15m_data[sym] = FifteenMinSnapshot(
                symbol=sym,
                window_start=now,
                price_open=current_price if current_price > 0 else 0.0,
                price_close=current_price if current_price > 0 else 0.0,
                price_high=current_price if current_price > 0 else 0.0,
                price_low=current_price if current_price > 0 else 0.0,
                oi_open=current_oi if current_oi > 0 else 0.0,
                oi_close=current_oi if current_oi > 0 else 0.0,
                oi_change_pct=0.0,
                total_volume_usd=0.0,
                cvd_net=0.0,
                liq_long_count=0,
                liq_short_count=0,
                liq_long_usd=0.0,
                liq_short_usd=0.0,
                liq_net_usd=0.0,
            )
        self._15m_window_start = now
    
    def _print_15m_summary(self):
        # NEW: Print 15m summary every 15 minutes
        """Print 15-minute aggregated summary untuk semua simbol."""
        now_str = datetime.utcnow().strftime("%H:%M UTC")
        
        logger.info("=" * 140)
        logger.info(f"  [15-MIN SUMMARY] Window ending {now_str}")
        logger.info("=" * 140)
        logger.info(
            f"{'Symbol':<12} {'Δ Price':>10} {'OI Δ%':>8} {'Vol USD':>16} "
            f"{'CVD Net':>14} {'LiqL':>6} {'LiqS':>6} {'Liq$L':>12} {'Liq$S':>12} {'Net Pressure':>16}"
        )
        logger.info("-" * 140)
        
        # Sort by abs(liq_net_usd) desc — most action first
        sorted_syms = sorted(
            self.symbols,
            key=lambda s: abs(self._15m_data[s].liq_net_usd),
            reverse=True
        )
        
        for sym in sorted_syms:
            d = self._15m_data[sym]
            if d.price_open == 0:
                continue
            
            price_chg_pct = (d.price_close - d.price_open) / d.price_open * 100
            price_arrow = "↑" if price_chg_pct > 0 else "↓" if price_chg_pct < 0 else "→"
            oi_arrow = "↑" if d.oi_change_pct > 0 else "↓" if d.oi_change_pct < 0 else "→"
            net_label = "SHORTS REKT" if d.liq_net_usd > 5000 else "LONGS REKT" if d.liq_net_usd < -5000 else "NEUTRAL"
            
            logger.info(
                f"{sym:<12} "
                f"{price_arrow}{price_chg_pct:+.2f}%  "
                f"{oi_arrow}{d.oi_change_pct:+.2f}%  "
                f"${d.total_volume_usd:>14,.0f}  "
                f"{d.cvd_net:>+13.0f}  "
                f"{d.liq_long_count:>6}  "
                f"{d.liq_short_count:>6}  "
                f"${d.liq_long_usd:>11,.0f}  "
                f"${d.liq_short_usd:>11,.0f}  "
                f"{net_label:>16}"
            )
        
        # Aggregate totals row
        total_vol = sum(d.total_volume_usd for d in self._15m_data.values())
        total_liq_long = sum(d.liq_long_usd for d in self._15m_data.values())
        total_liq_short = sum(d.liq_short_usd for d in self._15m_data.values())
        total_liq_count = sum(d.liq_long_count + d.liq_short_count for d in self._15m_data.values())
        net_total = total_liq_short - total_liq_long
        
        logger.info("-" * 140)
        logger.info(
            f"{'TOTAL':<12} {'':>10} {'':>8} "
            f"${total_vol:>14,.0f}  {'':>14}  "
            f"{'':>6}  {total_liq_count:>6}  "
            f"${total_liq_long:>11,.0f}  "
            f"${total_liq_short:>11,.0f}  "
            f"{'SHORTS REKT' if net_total > 0 else 'LONGS REKT':>16}"
        )
        logger.info("=" * 140 + "\n")
    
    async def _15m_summary_loop(self):
        # NEW: Background task for 15m summary
        """Print summary setiap 15 menit, lalu reset accumulator."""
        # Wait until next clean 15-min boundary (e.g., :00, :15, :30, :45)
        now = time.time()
        seconds_until_next = 900 - (now % 900)
        await asyncio.sleep(seconds_until_next)
        
        while self._running:
            self._print_15m_summary()
            self._reset_15m_window()
            await asyncio.sleep(900)
    
    def get_snapshot(self, symbol: str) -> Optional[SymbolSnapshot]:
        """Get current snapshot untuk simbol (read-only)."""
        return self._snapshots.get(symbol)
    
    def get_all_snapshots(self) -> Dict[str, SymbolSnapshot]:
        """Get all snapshots."""
        return dict(self._snapshots)
