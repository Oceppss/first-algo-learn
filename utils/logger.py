"""
utils/logger.py — TradeLogger: Logging sistem untuk MLF Bot

Output:
  - logs/trades.csv        : semua trade tertutup (CSV, append)
  - logs/equity.jsonl      : snapshot balance per event (JSON Lines)
  - logs/session_summary.json : ringkasan sesi (ditulis saat shutdown)
  - Console: formatted log dengan emoji (via logging)
"""

import asyncio
import csv
import json
import logging
import os
import time
from pathlib import Path

from config import PAPER_CFG


def setup_logging():
    """Setup logging handler untuk console output yang rapi."""
    Path(PAPER_CFG.LOG_DIR).mkdir(parents=True, exist_ok=True)

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-7s | %(name)-20s | %(message)s",
        datefmt="%H:%M:%S",
    )

    # Console handler
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)

    # File handler (semua level)
    log_path = os.path.join(PAPER_CFG.LOG_DIR, "bot.log")
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)

    root = logging.getLogger("MLF")
    root.setLevel(logging.DEBUG)
    root.addHandler(console)
    root.addHandler(file_handler)
    root.propagate = False


class TradeLogger:
    """
    Async-safe logger untuk trade events.
    Menggunakan asyncio.Lock untuk mencegah race condition saat
    beberapa posisi ditutup hampir bersamaan.
    """

    def __init__(self):
        self._lock = asyncio.Lock()
        Path(PAPER_CFG.LOG_DIR).mkdir(parents=True, exist_ok=True)
        self._init_csv()

    def _init_csv(self):
        """Buat CSV dengan header jika belum ada."""
        if not os.path.exists(PAPER_CFG.TRADE_LOG_FILE):
            headers = [
                "id", "symbol", "side",
                "entry_price", "exit_price", "size_usd",
                "pnl_usd", "pnl_pct", "duration_sec", "close_reason",
                "entry_time", "exit_time",
                # Signal metadata
                "price_move_pct", "volume_ratio", "wick_ratio", "obi", "z_score",
            ]
            with open(PAPER_CFG.TRADE_LOG_FILE, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
                writer.writeheader()

    async def log_position_open(self, position):
        """Log saat posisi dibuka (opsional, bisa dipakai untuk auditing)."""
        async with self._lock:
            log_path = os.path.join(PAPER_CFG.LOG_DIR, "positions_open.jsonl")
            entry = {
                "event": "OPEN",
                "ts":    int(time.time() * 1000),
                **position.to_dict(),
            }
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")

    async def log_trade_closed(self, trade):
        """Append closed trade ke CSV."""
        async with self._lock:
            row = trade.to_csv_row()
            file_exists = os.path.exists(PAPER_CFG.TRADE_LOG_FILE)
            with open(PAPER_CFG.TRADE_LOG_FILE, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=list(row.keys()),
                    extrasaction="ignore"
                )
                if not file_exists:
                    writer.writeheader()
                writer.writerow(row)

    async def log_equity(self, balance: float, timestamp: int):
        """Append equity snapshot ke JSONL file."""
        async with self._lock:
            entry = {"ts": timestamp, "balance": round(balance, 2)}
            with open(PAPER_CFG.EQUITY_LOG_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")

    async def write_session_summary(self, stats: dict):
        """Tulis ringkasan sesi saat shutdown."""
        async with self._lock:
            stats["session_end_ts"] = int(time.time() * 1000)
            with open(PAPER_CFG.SUMMARY_LOG_FILE, "w", encoding="utf-8") as f:
                json.dump(stats, f, indent=2)
            logging.getLogger("MLF.Logger").info(
                f"Session summary ditulis ke {PAPER_CFG.SUMMARY_LOG_FILE}"
            )
