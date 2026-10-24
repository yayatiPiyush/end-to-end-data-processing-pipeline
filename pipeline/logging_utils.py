"""Logging setup: one format for console and file, every line tagged with run id."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

FORMAT = "%(asctime)s %(levelname)-7s run=%(run_id)s %(name)s: %(message)s"


class RunIdFilter(logging.Filter):
    """Injects the current run id so log lines from one run can be grepped together."""

    def __init__(self) -> None:
        super().__init__()
        self.run_id = "-"

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = self.run_id
        return True


RUN_ID_FILTER = RunIdFilter()


def setup_logging(logs_dir: Path | None = None, level: str = "INFO") -> None:
    root = logging.getLogger()
    root.setLevel(level.upper())
    for h in list(root.handlers):          # idempotent when called twice (tests)
        root.removeHandler(h)
        h.close()
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stderr)]
    if logs_dir is not None:
        logs_dir.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(logs_dir / "pipeline.log", encoding="utf-8"))
    for h in handlers:
        h.setFormatter(logging.Formatter(FORMAT))
        h.addFilter(RUN_ID_FILTER)
        root.addHandler(h)
