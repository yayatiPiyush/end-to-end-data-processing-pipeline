"""Duplicate handling, in two distinct steps.

1. **Exact duplicates** (byte-identical records, e.g. an export job that
   retried a page) are dropped *before* validation: they carry no
   information, and counting them as separate rejects would inflate the
   error rate.

2. **Versions** (same business key, different content, e.g. an order
   re-exported after its status changed) are resolved *after* validation, so
   the newest **valid** version wins. "Newest" is decided by the source's own
   ``updated_at``; ties (or missing timestamps) fall back to the later
   position in the file, which is how the export appends changes.

   Trade-off: if the newest version is invalid, an older valid one is loaded
   and the invalid newer version sits in quarantine for review. The
   alternative (load nothing for that key) loses a known-good record.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class DedupeStats:
    exact_duplicates: int = 0
    superseded_versions: int = 0
    timestamp_ties: int = 0


def drop_exact_duplicates(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Keep the first occurrence of each identical raw record."""
    dup = df["_raw"].duplicated(keep="first")
    return df.loc[~dup], int(dup.sum())


def resolve_versions(df: pd.DataFrame, key: str) -> tuple[pd.DataFrame, int, int]:
    """Keep one row per ``key``: latest ``source_updated_at``, then latest line.

    Returns (deduplicated frame, rows superseded, keys whose winner was decided
    only by file position because timestamps tied).
    """
    if df.empty:
        return df, 0, 0
    ordered = df.sort_values(["source_updated_at", "_source_line"],
                             na_position="first", kind="mergesort")
    winners = ordered.drop_duplicates(subset=[key], keep="last")
    superseded = len(df) - len(winners)

    ts_max = df.groupby(key)["source_updated_at"].transform("max")
    at_max = df[df["source_updated_at"] == ts_max]
    ties = int((at_max.groupby(key).size() > 1).sum())
    return winners.sort_values("_source_line"), superseded, ties
