"""
data_fetcher.py — Async DataFetcher untuk MLF Bot

Arsitektur:
  - REST (aiohttp): Bootstrap kline history + REST order book
  - WebSocket (websockets): Live kline stream + live depth stream
  - Semua update di-push ke asyncio.Queue agar non-blocking

Binance Futures streams digunakan:
  - <symbol>@kline_1m          : live candle update
  - <symbol>@depth10@100ms     : order book top-10 setiap 100ms
  - !forceOrder@arr            : liquidation stream (opsional)
"""

import asyncio
import json
import logging
import time
from collections import defaultdict, deque
from typing import Callable, Dict, List, Optional, Deque
from urllib.parse import urlencode

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from config import (
    BINANCE_REST_BASE, BINANCE_WS_COMBINED, BINANCE_WS_BASE,
    ENTRY_CFG, STREAM_CFG, WATCHLIST
)
from core.models import Kline, OrderBookSnapshot

logger = logging.getLogger("MLF.DataFetcher")


# ═══════════════════════════════════════════════════════════════
# KLINE BUFFER
# Menyimpan histori kline per simbol untuk kalkulasi volume avg
# ═══════════════════════════════════════════════════════════════
class KlineBuffer:
    """
    Rolling buffer kline per simbol.
    Menyimpan CLOSED candles untuk kalkulasi statistik.
    """
    def __init__(self, maxlen: int = 60):
        self._data: Dict[str, Deque[Kline]] = defaultdict(
            lambda: deque(maxlen=maxlen)
        )
        # Candle live (belum closed) per simbol
        self._live: Dict[str, Optional[Kline]] = {}
        # CVD tracking per simbol — cumulative volume delta per current candle
        self._cvd: Dict[str, float] = defaultdict(float)
        # FIX: BUG1 - Store snapshot of CVD before reset for evaluation
        self._last_closed_cvd: Dict[str, float] = defaultdict(float)
        # Timestamp tracking untuk reset CVD saat candle baru
        self._cvd_candle_time: Dict[str, int] = {}

    def push_closed(self, kline: Kline):
        self._data[kline.symbol].append(kline)

    def set_live(self, kline: Kline):
        self._live[kline.symbol] = kline

    def get_closed(self, symbol: str) -> List[Kline]:
        return list(self._data[symbol])

    def get_live(self, symbol: str) -> Optional[Kline]:
        return self._live.get(symbol)
    
    def add_cvd_volume(self, symbol: str, buy_volume: float, sell_volume: float):
        """Add volume delta ke CVD untuk simbol (dari aggTrade stream)."""
        delta = buy_volume - sell_volume
        self._cvd[symbol] += delta
    
    def get_cvd(self, symbol: str) -> float:
        """Get current CVD untuk simbol."""
        return self._cvd.get(symbol, 0.0)
    
    def get_last_closed_cvd(self, symbol: str) -> float:
        # FIX: BUG1 - Return snapshot of CVD from previous closed candle
        """Get CVD snapshot from last closed candle (before reset)."""
        return self._last_closed_cvd.get(symbol, 0.0)
    
    def reset_cvd(self, symbol: str):
        # FIX: BUG1 - Snapshot CVD before wiping it
        """Reset CVD saat candle baru dimulai."""
        self._last_closed_cvd[symbol] = self._cvd.get(symbol, 0.0)
        self._cvd[symbol] = 0.0

    def avg_volume(self, symbol: str, window: int = 30) -> float:
        """Rata-rata quote_volume dari N candle terakhir yang closed."""
        candles = self.get_closed(symbol)
        if not candles:
            return 0.0
        recent = candles[-window:]
        return sum(c.quote_volume for c in recent) / len(recent)

    def price_stddev(self, symbol: str, window: int = 30) -> float:
        """Std dev close price untuk z-score kalkulasi."""
        import statistics
        candles = self.get_closed(symbol)
        if len(candles) < 5:
            return 1.0
        closes = [c.close for c in candles[-window:]]
        return statistics.stdev(closes) if len(closes) > 1 else 1.0

    def price_mean(self, symbol: str, window: int = 30) -> float:
        candles = self.get_closed(symbol)
        if not candles:
            return 0.0
        closes = [c.close for c in candles[-window:]]
        return sum(closes) / len(closes)


# ═══════════════════════════════════════════════════════════════
# DATA FETCHER — Core class
# ═══════════════════════════════════════════════════════════════
class DataFetcher:
    """
    Async data fetcher. Non-blocking sepenuhnya.

    Cara pakai:
        fetcher = DataFetcher(symbols=WATCHLIST)
        await fetcher.bootstrap()           # ambil history kline via REST
        asyncio.create_task(fetcher.run())  # mulai semua WebSocket streams
    """

    def __init__(
        self,
        symbols: List[str],
        on_kline_closed: Optional[Callable]   = None,   # callback: (Kline) -> None
        on_liquidation:  Optional[Callable]   = None,   # callback: (dict) -> None
        on_cvd_update:   Optional[Callable]   = None,   # callback: (symbol, cvd) -> None
        on_oi_update:    Optional[Callable]   = None,   # callback: (symbol, oi, timestamp) -> None
    ):
        self.symbols         = [s.upper() for s in symbols]
        self.kline_buf       = KlineBuffer(maxlen=120)
        self.ob_cache: Dict[str, OrderBookSnapshot] = {}
        
        # OI tracking — store current OI per simbol
        self._current_oi: Dict[str, float] = {sym: 0.0 for sym in self.symbols}

        # Callbacks — TradingLogic dan PaperTrader mendaftar ke sini
        self._on_kline_closed = on_kline_closed
        self._on_liquidation  = on_liquidation
        self._on_cvd_update   = on_cvd_update
        self._on_oi_update    = on_oi_update

        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self.msg_count = 0
        self.agg_msg_count = 0  # FIX: BUG4 - Separate counter for aggTrade messages
        self.liq_count = 0

    # ─────────────────────────────────────────
    # REST: Bootstrap historical klines
    # ─────────────────────────────────────────
    async def bootstrap(self):
        """
        Ambil 60 candle 1m terakhir untuk setiap simbol via REST.
        Dipanggil sekali saat startup agar buffer volume sudah terisi.
        """
        logger.info(f"[Bootstrap] Mengambil histori kline untuk {len(self.symbols)} simbol...")
        connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [self._fetch_klines_rest(session, sym) for sym in self.symbols]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        # Count properly: only a positive integer return means success
        success = sum(1 for r in results if isinstance(r, int) and r > 0)
        failed  = [self.symbols[i] for i, r in enumerate(results) if not isinstance(r, int) or r == 0]

        logger.info(f"[Bootstrap] Selesai. {success}/{len(self.symbols)} simbol berhasil.")
        if failed:
            logger.warning(f"[Bootstrap] Simbol tanpa histori ({len(failed)}): {', '.join(failed[:10])}{'...' if len(failed) > 10 else ''}")
            logger.warning("[Bootstrap] Sinyal tidak akan muncul sampai buffer organik terkumpul (~30 menit).")
        else:
            # Log sample buffer sizes to confirm data loaded
            sample = self.symbols[:3]
            for sym in sample:
                buf_size = len(self.kline_buf.get_closed(sym))
                avg_vol  = self.kline_buf.avg_volume(sym)
                logger.info(f"[Bootstrap] {sym}: {buf_size} candles, avg_volume={avg_vol:.1f}")

    async def _fetch_klines_rest(
        self,
        session: aiohttp.ClientSession,
        symbol: str,
        interval: str = "1m",
        limit: int = 60
    ) -> int:
        """Returns number of candles loaded, or 0 on failure."""
        url = f"{BINANCE_REST_BASE}/fapi/v1/klines"
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    logger.warning(f"[REST] {symbol} → HTTP {resp.status}")
                    return 0
                raw = await resp.json()
                for row in raw:
                    k = self._parse_kline_rest(symbol, row)
                    self.kline_buf.push_closed(k)
                return len(raw)
        except Exception as e:
            logger.error(f"[REST] {symbol} → {e}")
            return 0

    def _parse_kline_rest(self, symbol: str, row: list) -> Kline:
        return Kline(
            symbol       = symbol,
            open_time    = int(row[0]),
            open         = float(row[1]),
            high         = float(row[2]),
            low          = float(row[3]),
            close        = float(row[4]),
            volume       = float(row[5]),
            quote_volume = float(row[7]),
            trades       = int(row[8]),
            is_closed    = True,
        )

    # ─────────────────────────────────────────
    # REST: Fetch order book snapshot (on-demand)
    # ─────────────────────────────────────────
    async def fetch_order_book(self, symbol: str, limit: int = 10) -> Optional[OrderBookSnapshot]:
        url = f"{BINANCE_REST_BASE}/fapi/v1/depth"
        params = {"symbol": symbol, "limit": limit}
        try:
            connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=3)) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    snap = OrderBookSnapshot(
                        symbol    = symbol,
                        timestamp = int(time.time() * 1000),
                        bids      = [[float(p), float(q)] for p, q in data.get("bids", [])],
                        asks      = [[float(p), float(q)] for p, q in data.get("asks", [])],
                    )
                    self.ob_cache[symbol] = snap
                    return snap
        except Exception as e:
            logger.error(f"[OB REST] {symbol} → {e}")
            return None

    # ─────────────────────────────────────────
    # REST: Fetch & track Open Interest
    # ─────────────────────────────────────────
    async def update_oi(self, symbol: str, oi_value: float):
        """
        Record OI snapshot untuk quantitative engine.
        Called dari dashboard OI polling atau setiap kline close.
        """
        if symbol not in self._current_oi:
            return
        
        self._current_oi[symbol] = oi_value
        timestamp_ms = int(time.time() * 1000)
        
        # Emit callback ke quantitative engine
        if self._on_oi_update:
            asyncio.create_task(
                self._safe_callback(self._on_oi_update, symbol, oi_value, timestamp_ms)
            )
    
    def get_current_oi(self, symbol: str) -> float:
        """Get current OI untuk simbol."""
        return self._current_oi.get(symbol, 0.0)

    # ─────────────────────────────────────────
    # WebSocket: Kline streams
    # Binance max ~100 streams per koneksi
    # ─────────────────────────────────────────
    async def run(self):
        """Entry point utama. Jalankan semua WS stream secara paralel."""
        self._running = True
        logger.info("[WS] Memulai semua WebSocket stream...")

        # Bagi simbol ke beberapa batch (100 streams per koneksi)
        batch_size = STREAM_CFG.MAX_STREAMS_PER_CONN // 3  # kline + depth + aggTrade
        batches = [
            self.symbols[i:i + batch_size]
            for i in range(0, len(self.symbols), batch_size)
        ]

        tasks = []
        for batch in batches:
            tasks.append(asyncio.create_task(self._run_kline_ws(batch)))
            # AggTrade stream untuk CVD tracking
            tasks.append(asyncio.create_task(self._run_aggTrade_ws(batch)))

        # Liquidation stream (satu stream untuk semua simbol)
        tasks.append(asyncio.create_task(self._run_liquidation_ws()))

        await asyncio.gather(*tasks)

    async def _run_kline_ws(self, symbols: List[str]):
        """
        Combined stream: kline_1m untuk batch simbol.
        Auto-reconnect pada disconnect.
        """
        streams = "/".join(
            f"{sym.lower()}@kline_{STREAM_CFG.KLINE_INTERVAL}"
            for sym in symbols
        )
        url = f"{BINANCE_WS_COMBINED}{streams}"

        while self._running:
            try:
                logger.info(f"[WS Kline] Connecting... ({len(symbols)} symbols)")
                async with websockets.connect(
                    url,
                    ping_interval=STREAM_CFG.PING_INTERVAL_SEC,
                    max_size=10 * 1024 * 1024,
                ) as ws:
                    logger.info(f"[WS Kline] Connected ✓ ({len(symbols)} symbols)")
                    async for raw_msg in ws:
                        if not self._running:
                            break
                        try:
                            msg = json.loads(raw_msg)
                            await self._handle_kline_msg(msg)
                        except json.JSONDecodeError:
                            continue

            except (ConnectionClosedError, ConnectionClosedOK) as e:
                logger.warning(f"[WS Kline] Disconnected: {e}. Reconnect dalam {STREAM_CFG.RECONNECT_DELAY_SEC}s...")
                await asyncio.sleep(STREAM_CFG.RECONNECT_DELAY_SEC)
            except Exception as e:
                logger.error(f"[WS Kline] Error: {e}. Reconnect...")
                await asyncio.sleep(STREAM_CFG.RECONNECT_DELAY_SEC)

    async def _handle_kline_msg(self, msg: dict):
        """Parse pesan kline dari combined stream."""
        self.msg_count += 1
        if self.msg_count == 1:
            logger.info("[WS Kline] First message received successfully! WebSocket data stream is flowing.")
        elif self.msg_count % 1000 == 0:
            logger.info(f"[WS Kline] Total updates received from WebSocket: {self.msg_count}")
        # Combined stream format: {"stream": "...", "data": {...}}
        data = msg.get("data", msg)
        if data.get("e") != "kline":
            return

        k_raw = data["k"]
        symbol = k_raw["s"]
        kline  = Kline(
            symbol       = symbol,
            open_time    = int(k_raw["t"]),
            open         = float(k_raw["o"]),
            high         = float(k_raw["h"]),
            low          = float(k_raw["l"]),
            close        = float(k_raw["c"]),
            volume       = float(k_raw["v"]),
            quote_volume = float(k_raw["q"]),
            trades       = int(k_raw["n"]),
            is_closed    = bool(k_raw["x"]),
        )

        if kline.is_closed:
            # Candle resmi selesai → push ke buffer & trigger callback
            self.kline_buf.push_closed(kline)
            # Reset CVD untuk candle baru
            self.kline_buf.reset_cvd(kline.symbol)
            if self._on_kline_closed:
                # Schedule callback tanpa blocking WS loop
                asyncio.create_task(
                    self._safe_callback(self._on_kline_closed, kline)
                )
        else:
            # Candle live update
            self.kline_buf.set_live(kline)

    # ─────────────────────────────────────────
    # WebSocket: Liquidation Stream
    # !forceOrder@arr — semua likuidasi futures Binance
    # ─────────────────────────────────────────
    async def _run_liquidation_ws(self):
        url = f"{BINANCE_WS_BASE}/ws/!forceOrder@arr"
        while self._running:
            try:
                logger.info("[WS Liq] Connecting liquidation stream...")
                async with websockets.connect(
                    url,
                    ping_interval=STREAM_CFG.PING_INTERVAL_SEC,
                ) as ws:
                    logger.info("[WS Liq] Connected ✓")
                    async for raw_msg in ws:
                        if not self._running:
                            break
                        try:
                            msg = json.loads(raw_msg)
                            await self._handle_liquidation_msg(msg)
                        except json.JSONDecodeError:
                            continue

            except (ConnectionClosedError, ConnectionClosedOK) as e:
                logger.warning(f"[WS Liq] Disconnected: {e}. Reconnect...")
                await asyncio.sleep(STREAM_CFG.RECONNECT_DELAY_SEC)
            except Exception as e:
                logger.error(f"[WS Liq] Error: {e}. Reconnect...")
                await asyncio.sleep(STREAM_CFG.RECONNECT_DELAY_SEC)

    async def _handle_liquidation_msg(self, msg: dict):
        """
        Liquidation event format:
        { "e": "forceOrder", "E": ts, "o": {
            "s": "BTCUSDT", "S": "SELL",
            "q": "0.014", "p": "9910", ...
        }}
        """
        self.liq_count += 1
        if self.liq_count == 1:
            logger.info("[WS Liq] First liquidation message received successfully! Liquidation data stream is flowing.")
        if msg.get("e") != "forceOrder":
            return
        order = msg.get("o", {})
        symbol = order.get("s", "")
        if symbol not in self.symbols:
            return

        liq_event = {
            "symbol":    symbol,
            "side":      order.get("S"),          # SELL=long liq, BUY=short liq
            "qty":       float(order.get("q", 0)),
            "price":     float(order.get("p", 0)),
            "avg_price": float(order.get("ap", 0)),
            "timestamp": int(msg.get("E", 0)),
        }

        if self._on_liquidation:
            asyncio.create_task(
                self._safe_callback(self._on_liquidation, liq_event)
            )

    # ─────────────────────────────────────────
    # WebSocket: AggTrade stream untuk CVD
    # ─────────────────────────────────────────
    async def _run_aggTrade_ws(self, symbols: List[str]):
        """
        Combined stream: aggTrade untuk batch simbol.
        Dipakai untuk menghitung CVD (Cumulative Volume Delta).
        Auto-reconnect pada disconnect.
        """
        streams = "/".join(
            f"{sym.lower()}@aggTrade"
            for sym in symbols
        )
        url = f"{BINANCE_WS_COMBINED}{streams}"

        while self._running:
            try:
                async with websockets.connect(url) as ws:
                    logger.info(f"[WS AggTrade] Connected ({len(symbols)} symbols)")
                    self.agg_msg_count = 0  # FIX: BUG4 - Use separate counter for aggTrade

                    async for msg in ws:
                        try:
                            self.agg_msg_count += 1  # FIX: BUG4 - Increment aggTrade counter only
                            data = json.loads(msg)
                            await self._handle_aggTrade_msg(data)
                        except json.JSONDecodeError:
                            pass
                        except Exception as e:
                            logger.error(f"[AggTrade] Processing error: {e}")

            except ConnectionClosedOK:
                logger.info("[WS AggTrade] Connection closed normally, reconnecting...")
                await asyncio.sleep(2)
            except ConnectionClosedError as e:
                logger.warning(f"[WS AggTrade] Connection error: {e}, reconnecting...")
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"[WS AggTrade] Unexpected error: {e}")
                await asyncio.sleep(5)

    async def _handle_aggTrade_msg(self, msg: dict):
        """
        Handle aggTrade message format:
        {
            "e": "aggTrade",
            "E": timestamp,
            "s": "BTCUSDT",
            "a": aggTradeId,
            "p": price,
            "q": quantity,
            "f": firstTradeId,
            "l": lastTradeId,
            "T": tradeTime,
            "m": isBuyerMaker  ← FALSE = buyer initiated (BUY), TRUE = seller initiated (SELL)
        }
        
        CVD calculation: buy_initiated_volume - sell_initiated_volume
        """
        if msg.get("e") != "aggTrade":
            return
        
        symbol = msg.get("s", "").upper()
        if symbol not in self.symbols:
            return
        
        is_buyer_maker = msg.get("m", False)  # False = buyer initiated, True = seller initiated
        qty = float(msg.get("q", 0))
        
        # Track volume: if buyer initiated, add to buy volume; else add to sell volume
        if not is_buyer_maker:
            # Buyer initiated (price took) = BUY pressure
            self.kline_buf.add_cvd_volume(symbol, buy_volume=qty, sell_volume=0.0)
        else:
            # Seller initiated (price taker) = SELL pressure
            self.kline_buf.add_cvd_volume(symbol, buy_volume=0.0, sell_volume=qty)
        
        # Emit CVD update callback
        cvd = self.kline_buf.get_cvd(symbol)
        if self._on_cvd_update:
            asyncio.create_task(
                self._safe_callback(self._on_cvd_update, symbol, cvd)
            )

    # ─────────────────────────────────────────
    # Helper
    # ─────────────────────────────────────────
    async def _safe_callback(self, callback: Callable, *args):
        """Jalankan callback, tangkap exception agar tidak crash WS loop."""
        try:
            result = callback(*args)
            if asyncio.iscoroutine(result):
                await result
        except Exception as e:
            logger.error(f"[Callback Error] {e}", exc_info=True)

    def stop(self):
        self._running = False
        logger.info("[DataFetcher] Stop signal dikirim.")
