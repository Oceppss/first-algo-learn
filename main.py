"""
main.py — Entry point utama MLF Bot
"Micro-Liquidation Fade & Order Book Imbalance"

Arsitektur runtime:
                          ┌─────────────────┐
                          │   main.py        │
                          │  (asyncio.run)   │
                          └────────┬────────┘
                                   │ orchestrate
             ┌─────────────────────┼──────────────────────┐
             │                     │                      │
    ┌────────▼──────┐    ┌─────────▼───────┐    ┌────────▼───────┐
    │  DataFetcher  │    │  TradingLogic   │    │  PaperTrader   │
    │  (WebSocket)  │───▶│  (Signal Gen)   │───▶│  (Execution)   │
    └───────────────┘    └─────────────────┘    └────────────────┘
         │  kline_closed                              │
         │  liquidation_event                         │ trade_closed
         │                                            ▼
         │                                    ┌───────────────┐
         └──────────────────────────────────▶ │  TradeLogger  │
                                              │  (CSV/JSONL)  │
                                              └───────────────┘
"""

import asyncio
import logging
import signal
import sys
import time
from pathlib import Path

# Ensure root directory is in sys.path for absolute imports
root_dir = str(Path(__file__).parent.absolute())
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from utils.dns_patch import apply_patch
apply_patch()

from config import PAPER_CFG, WATCHLIST
from core.data_fetcher import DataFetcher
from core.paper_trader import PaperTrader
from core.trading_logic import TradingLogic
from core.dashboard import Dashboard
from core.quantitative_engine import QuantitativeEngine
from utils.logger import setup_logging

logger = logging.getLogger("MLF.Main")


class MLFBot:
    """
    Orchestrator utama. Menginisialisasi dan menghubungkan semua komponen.

    Cara pakai:
        bot = MLFBot()
        asyncio.run(bot.run())
    """

    def __init__(self, symbols=None):
        self.symbols = symbols or WATCHLIST
        self._running = False

        # ── Inisialisasi komponen ──────────────────────────────────────
        self.trader    = PaperTrader()
        self.fetcher   = DataFetcher(
            symbols        = self.symbols,
            on_liquidation = self._on_liquidation,
            on_cvd_update  = self._on_cvd_update,
            on_oi_update   = self._on_oi_update,
        )
        self.dashboard = Dashboard(symbols=self.symbols, fetcher=self.fetcher)
        self.quantengine = QuantitativeEngine(symbols=self.symbols)
        
        self.logic = TradingLogic(
            fetcher            = self.fetcher,
            on_signal_callback = self.trader.on_signal,
        )
        
        # Pass quantitative engine ke paper trader
        self.trader.set_quantitative_engine(self.quantengine, self.fetcher)

        # Sambungkan kline_closed ke BOTH logic dan trader
        # Logic: deteksi sinyal entry
        # Trader: monitor time-stop dan exit posisi aktif
        self.fetcher._on_kline_closed = self._on_kline_closed_mux

        # Monitoring task handle
        self._stat_task: asyncio.Task = None
        self._dashboard_task: asyncio.Task = None

    async def _on_kline_closed_mux(self, kline):
        """
        Multiplexer: broadcast kline_closed ke Trading Logic DAN Paper Trader.
        Jalankan secara parallel agar tidak saling menunggu.
        """
        # Update dashboard dengan kline data
        avg_vol = self.fetcher.kline_buf.avg_volume(kline.symbol, window=30)
        self.dashboard.update_kline(
            symbol=kline.symbol,
            price=kline.close,
            prev_close=kline.open,
            volume=kline.quote_volume,
            avg_volume_30m=avg_vol,
        )
        
        await asyncio.gather(
            self.logic.on_kline_closed(kline),
            self.trader.on_kline_update(kline),
            return_exceptions=True,
        )

    async def _on_liquidation(self, liq_event: dict):
        """
        Handler event likuidasi.
        Translate BUY/SELL (order execution side) ke position type being liquidated.
        - BUY order executed → SHORT position was liquidated (engine bought to close)
        - SELL order executed → LONG position was liquidated (engine sold to close)
        """
        side_raw = liq_event['side']
        # Translate engine order execution side to position type being liquidated
        position_type = "SHORT LIQ" if side_raw == "BUY" else "LONG LIQ" if side_raw == "SELL" else "UNK LIQ"
        
        # Calculate estimated USD value
        usd_value = liq_event['qty'] * liq_event['price']
        
        logger.info(
            f"💥 LIQUIDATION [{liq_event['symbol']}] "
            f"{position_type} | Qty={liq_event['qty']} | "
            f"Price={liq_event['price']:.2f} | ~${usd_value:,.2f}"
        )
        
        # Update dashboard
        self.dashboard.record_liquidation(
            symbol=liq_event['symbol'],
            side_raw=side_raw,
            qty=liq_event['qty'],
            price=liq_event['price'],
        )
    
    async def _on_cvd_update(self, symbol: str, cvd: float):
        """
        Handler CVD update dari aggTrade stream.
        Update dashboard dengan CVD value.
        """
        self.dashboard.update_cvd(symbol, cvd)
    
    async def _on_oi_update(self, symbol: str, oi_value: float, timestamp_ms: int):
        """
        Handler OI update dari dashboard polling.
        Record OI untuk quantitative engine.
        """
        self.quantengine.record_oi(symbol, oi_value, timestamp_ms)

    async def _periodic_stats(self, interval_sec: int = 300):
        """Print statistik setiap N detik (default 5 menit)."""
        while self._running:
            await asyncio.sleep(interval_sec)
            self.logic.log_filter_stats()
            self.trader.print_stats()

    async def run(self):
        """Start bot. Blocking sampai Ctrl+C atau error fatal."""
        self._running = True

        logger.info("=" * 60)
        logger.info("  MLF BOT — Micro-Liquidation Fade & OBI Scalper")
        logger.info(f"  Memantau {len(self.symbols)} simbol")
        logger.info(f"  Paper Balance: ${PAPER_CFG.INITIAL_BALANCE:,.0f}")
        logger.info("=" * 60)

        # ── Bootstrap: ambil histori kline via REST ────────────────────
        await self.fetcher.bootstrap()

        # ── Start periodic stats printer ──────────────────────────────
        self._stat_task = asyncio.create_task(self._periodic_stats(120))
        
        # ── Start dashboard ────────────────────────────────────────────
        self._dashboard_task = asyncio.create_task(self.dashboard.start())

        # ── Start WebSocket streams (blocking hingga stop) ────────────
        ws_task = asyncio.create_task(self.fetcher.run())

        logger.info("🚀 Bot berjalan. Tekan Ctrl+C untuk berhenti.\n")

        try:
            await ws_task
        except asyncio.CancelledError:
            pass
        finally:
            await self._shutdown()

    async def _shutdown(self):
        """Graceful shutdown: tutup semua, tulis laporan akhir."""
        logger.info("\n[Shutdown] Menghentikan bot...")
        self._running = False
        self.fetcher.stop()
        self.dashboard.stop()

        if self._stat_task:
            self._stat_task.cancel()
        
        if self._dashboard_task:
            self._dashboard_task.cancel()

        # Tulis summary sesi
        stats = self.trader.get_stats()
        await self.trader._logger.write_session_summary(stats)

        # Print stats akhir
        self.trader.print_stats()
        logger.info("[Shutdown] Bot berhenti. Semua log tersimpan.")


# ═══════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════
def main():
    setup_logging()

    bot = MLFBot(symbols=WATCHLIST)

    # Handle Ctrl+C dengan graceful shutdown
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _handle_shutdown(signum, frame):
        logger.info("\n[Signal] Ctrl+C diterima. Memulai shutdown...")
        for task in asyncio.all_tasks(loop):
            task.cancel()

    signal.signal(signal.SIGINT,  _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    try:
        loop.run_until_complete(bot.run())
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        # Cleanup semua task yang masih pending
        pending = asyncio.all_tasks(loop)
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()
        logger.info("Event loop ditutup. Selamat tinggal!")


if __name__ == "__main__":
    main()
