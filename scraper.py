#!/usr/bin/env python3
"""
Google Play Store review scraper for com.google.android.apps.kids.familylink

Usage:
    python scraper.py [--max-minutes N]

Fetches reviews in batches, deduplicates by review_id, appends to reviews.csv,
and tracks continuation tokens / run state in state.json.
"""

import argparse
import csv
import json
import logging
import os
import random
import signal
import time
from collections import namedtuple
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from typing import Any, Optional

from google_play_scraper import Sort
from google_play_scraper import reviews as gps_reviews

# ── ContinuationToken import (version-agnostic) ──────────────────────────────
try:
    from google_play_scraper.features.reviews import ContinuationToken
except ImportError:
    try:
        from google_play_scraper.utils.request import ContinuationToken  # type: ignore
    except ImportError:
        ContinuationToken = namedtuple(  # type: ignore
            "ContinuationToken",
            ["token", "lang", "country", "sort", "count", "filter_score_with"],
        )

# ── Sort options (defensive: HELPFULNESS may not exist in all versions) ───────
try:
    _MOST_RELEVANT_SORT = Sort.HELPFULNESS
except AttributeError:
    _MOST_RELEVANT_SORT = Sort.RATING  # type: ignore  # closest fallback

SORT_OPTIONS: list[tuple[Any, str]] = [
    (_MOST_RELEVANT_SORT, "MOST_RELEVANT"),
    (Sort.NEWEST, "NEWEST"),
]

# ── Constants ─────────────────────────────────────────────────────────────────
APP_ID = "com.google.android.apps.kids.familylink"
REVIEWS_FILE = "reviews.csv"
STATE_FILE = "state.json"
LOG_FILE = "scrape.log"

BATCH_SIZE = 200
LANGUAGES = ["en", "bn", "ar", "tr", "id", "ms"]  # en first (highest priority)

# Best-effort country mapping for each language
LANG_COUNTRY: dict[str, str] = {
    "en": "us",
    "bn": "bd",
    "ar": "eg",
    "tr": "tr",
    "id": "id",
    "ms": "my",
}

CSV_COLUMNS = [
    "review_id",
    "author_name",
    "rating",
    "review_text",
    "thumbs_up_count",
    "review_date",
    "reply_text",
    "reply_date",
    "sort_order",
    "language",
    "country",
    "fetched_at",
]

# Delays (seconds)
DELAY_BATCH_MIN = 5
DELAY_BATCH_MAX = 15
DELAY_LANG_MIN = 30
DELAY_LANG_MAX = 60

# Retry / backoff
BACKOFF_INITIAL = 60
BACKOFF_MAX = 600  # 10 minutes
MAX_RETRIES = 3

# Log rotation
LOG_MAX_BYTES = 5 * 1024 * 1024  # 5 MB

# Reserve this many seconds at end of run for cleanup / state save
TIMEOUT_BUFFER = 30


# ── Token serialization ───────────────────────────────────────────────────────

def serialize_token(token: Any) -> Optional[dict]:
    """Convert a ContinuationToken namedtuple to a JSON-serialisable dict."""
    if token is None:
        return None
    if hasattr(token, "_asdict"):
        d = dict(token._asdict())
        if "sort" in d:
            d["sort"] = int(d["sort"])
        return d
    return None


def deserialize_token(data: Optional[dict]) -> Any:
    """Reconstruct a ContinuationToken from a previously serialised dict."""
    if not data:
        return None
    try:
        return ContinuationToken(
            token=data.get("token"),
            lang=data.get("lang", "en"),
            country=data.get("country", "us"),
            sort=data.get("sort", 0),
            count=data.get("count", BATCH_SIZE),
            filter_score_with=data.get("filter_score_with"),
        )
    except Exception:
        return None


# ── Logging ───────────────────────────────────────────────────────────────────

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("scraper")
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )

    fh = RotatingFileHandler(
        LOG_FILE, maxBytes=LOG_MAX_BYTES, backupCount=1, mode="a", encoding="utf-8"
    )
    fh.setFormatter(fmt)

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


# ── State helpers ─────────────────────────────────────────────────────────────

def load_state() -> dict:
    """Load state.json; return clean defaults on missing / corrupt file."""
    defaults: dict = {
        "last_run": None,
        "total_fetched": 0,
        "sort_index": 0,
        "tokens": {},
    }
    if not os.path.exists(STATE_FILE):
        return defaults
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as fh:
            state = json.load(fh)
        for k, v in defaults.items():
            state.setdefault(k, v)
        return state
    except (json.JSONDecodeError, OSError):
        return defaults


def save_state(state: dict) -> None:
    """Atomically write state to STATE_FILE via a temp file."""
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2)
    os.replace(tmp, STATE_FILE)


# ── CSV helpers ───────────────────────────────────────────────────────────────

def load_existing_ids() -> set:
    """Read only the review_id column from reviews.csv into a set."""
    ids: set = set()
    if not os.path.exists(REVIEWS_FILE):
        return ids
    try:
        with open(REVIEWS_FILE, "r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                rid = (row.get("review_id") or "").strip()
                if rid:
                    ids.add(rid)
    except OSError:
        pass
    return ids


def ensure_header() -> None:
    """Write CSV header if and only if reviews.csv does not yet exist."""
    if not os.path.exists(REVIEWS_FILE):
        with open(REVIEWS_FILE, "w", encoding="utf-8", newline="") as fh:
            csv.DictWriter(fh, fieldnames=CSV_COLUMNS).writeheader()


def append_rows(rows: list[dict]) -> None:
    """Append rows to reviews.csv (append-only, never overwrites)."""
    with open(REVIEWS_FILE, "a", encoding="utf-8", newline="") as fh:
        csv.DictWriter(fh, fieldnames=CSV_COLUMNS).writerows(rows)


# ── Review parsing ────────────────────────────────────────────────────────────

def _fmt_dt(dt: Any) -> str:
    if dt is None:
        return ""
    if hasattr(dt, "isoformat"):
        return dt.isoformat()
    return str(dt)


def parse_review(raw: dict, sort_name: str, lang: str, country: str) -> dict:
    def clean(text: Any) -> str:
        return (text or "").replace("\n", " ").replace("\r", " ").strip()

    return {
        "review_id": (raw.get("reviewId") or "").strip(),
        "author_name": clean(raw.get("userName")),
        "rating": raw.get("score", ""),
        "review_text": clean(raw.get("content")),
        "thumbs_up_count": raw.get("thumbsUpCount", 0),
        "review_date": _fmt_dt(raw.get("at")),
        "reply_text": clean(raw.get("replyContent")),
        "reply_date": _fmt_dt(raw.get("repliedAt")),
        "sort_order": sort_name,
        "language": lang,
        "country": country,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


# ── Scraper ───────────────────────────────────────────────────────────────────

class Scraper:
    def __init__(self, max_minutes: int, logger: logging.Logger) -> None:
        self.max_minutes = max_minutes
        self.logger = logger
        self.start_time = time.monotonic()
        self.shutdown_requested = False

        self.state = load_state()
        self.existing_ids = load_existing_ids()
        self.new_count = 0
        self.skipped_count = 0

        signal.signal(signal.SIGTERM, self._on_signal)
        signal.signal(signal.SIGINT, self._on_signal)

    # ── Time helpers ──────────────────────────────────────────────────────────

    def _elapsed(self) -> float:
        return time.monotonic() - self.start_time

    def _remaining(self) -> float:
        return self.max_minutes * 60.0 - self._elapsed()

    def _timed_out(self) -> bool:
        """True when less than TIMEOUT_BUFFER seconds remain."""
        return self._remaining() <= TIMEOUT_BUFFER

    def _sleep(self, seconds: float) -> bool:
        """
        Interruptible sleep that checks shutdown/timeout every second.
        Returns True if the full duration elapsed; False if interrupted.
        """
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if self.shutdown_requested or self._timed_out():
                return False
            time.sleep(min(1.0, deadline - time.monotonic()))
        return True

    # ── Signal handler ────────────────────────────────────────────────────────

    def _on_signal(self, signum: int, _frame: Any) -> None:
        self.logger.info(f"Signal {signum} received — requesting graceful shutdown.")
        self.shutdown_requested = True

    # ── Fetch with retry / backoff ────────────────────────────────────────────

    def _fetch_batch(
        self, sort_enum: Any, lang: str, country: str, token: Any
    ) -> tuple[list, Any]:
        """
        Fetch one batch of BATCH_SIZE reviews with exponential backoff.
        Returns (reviews_list, next_continuation_token).
        On final failure, returns ([], original_token) so we don't lose position.
        """
        backoff = BACKOFF_INITIAL
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                result, next_token = gps_reviews(
                    APP_ID,
                    lang=lang,
                    country=country,
                    sort=sort_enum,
                    count=BATCH_SIZE,
                    continuation_token=token,
                )
                return result, next_token
            except Exception as exc:
                self.logger.warning(
                    f"Attempt {attempt}/{MAX_RETRIES} failed "
                    f"[lang={lang}, country={country}]: {exc}"
                )
                if attempt == MAX_RETRIES:
                    self.logger.error("Max retries exceeded — skipping this batch.")
                    return [], token  # preserve position for next run

                wait = min(backoff * (2 ** (attempt - 1)), BACKOFF_MAX)
                self.logger.info(f"Backing off {wait:.0f}s before retry {attempt + 1}…")
                if not self._sleep(wait):
                    return [], token  # shutdown / timeout during backoff

        return [], token  # unreachable but satisfies type checkers

    # ── Main run loop ─────────────────────────────────────────────────────────

    def run(self) -> None:
        ensure_header()

        sort_index: int = self.state.get("sort_index", 0) % len(SORT_OPTIONS)
        sort_enum, sort_name = SORT_OPTIONS[sort_index]

        self.logger.info(
            f"{'=' * 20} Run started {'=' * 20}"
        )
        self.logger.info(
            f"sort={sort_name}  max_minutes={self.max_minutes}  "
            f"existing_ids={len(self.existing_ids)}  "
            f"state_total_fetched={self.state['total_fetched']}"
        )

        try:
            for lang_idx, lang in enumerate(LANGUAGES):
                if self.shutdown_requested or self._timed_out():
                    self.logger.info("Stopping: shutdown or timeout reached.")
                    break

                country = LANG_COUNTRY.get(lang, "us")
                token_key = f"{sort_name}:{lang}"
                saved_token_data = self.state.get("tokens", {}).get(token_key)
                token = deserialize_token(saved_token_data)

                self.logger.info(
                    f"── Language {lang_idx + 1}/{len(LANGUAGES)}: "
                    f"lang={lang}  country={country}  sort={sort_name}  "
                    f"token={'resume' if token else 'start'}"
                )

                batch_num = 0
                while not self.shutdown_requested and not self._timed_out():
                    batch_num += 1
                    self.logger.info(
                        f"  Batch {batch_num} | lang={lang} | "
                        f"remaining={self._remaining():.0f}s"
                    )

                    raw_list, next_token = self._fetch_batch(
                        sort_enum, lang, country, token
                    )

                    if not raw_list:
                        self.logger.info(
                            f"  No results returned for lang={lang} — resetting token."
                        )
                        self.state.setdefault("tokens", {})[token_key] = None
                        save_state(self.state)
                        break

                    # Filter duplicates and build rows
                    new_rows: list[dict] = []
                    for raw in raw_list:
                        rid = (raw.get("reviewId") or "").strip()
                        if not rid or rid in self.existing_ids:
                            self.skipped_count += 1
                            continue
                        new_rows.append(parse_review(raw, sort_name, lang, country))
                        self.existing_ids.add(rid)

                    if new_rows:
                        append_rows(new_rows)
                        self.new_count += len(new_rows)
                        self.logger.info(
                            f"    +{len(new_rows)} new rows written  "
                            f"(run total new: {self.new_count}  "
                            f"skipped so far: {self.skipped_count})"
                        )

                    # Persist updated continuation token after every batch
                    token = next_token
                    self.state.setdefault("tokens", {})[token_key] = serialize_token(token)
                    save_state(self.state)

                    if not next_token:
                        self.logger.info(
                            f"  Exhausted results for lang={lang}, sort={sort_name}."
                        )
                        break

                    # Randomised inter-batch delay
                    delay = random.uniform(DELAY_BATCH_MIN, DELAY_BATCH_MAX)
                    self.logger.debug(f"  Sleeping {delay:.1f}s between batches…")
                    if not self._sleep(delay):
                        break  # shutdown or timeout during sleep

                # Randomised inter-language delay (skip after the last language)
                if (
                    lang_idx < len(LANGUAGES) - 1
                    and not self.shutdown_requested
                    and not self._timed_out()
                ):
                    delay = random.uniform(DELAY_LANG_MIN, DELAY_LANG_MAX)
                    self.logger.info(
                        f"Language switch — sleeping {delay:.0f}s "
                        f"(next: {LANGUAGES[lang_idx + 1]})…"
                    )
                    self._sleep(delay)

        finally:
            # ── Always runs: update state and print summary ────────────────────
            self.state["last_run"] = datetime.now(timezone.utc).isoformat()
            self.state["total_fetched"] = (
                self.state.get("total_fetched", 0) + self.new_count
            )
            # Advance sort index so next run uses the other sort order
            self.state["sort_index"] = (sort_index + 1) % len(SORT_OPTIONS)
            save_state(self.state)

            elapsed = self._elapsed()
            total_in_file = len(self.existing_ids)

            lines = [
                "",
                "=" * 64,
                "  SCRAPE RUN SUMMARY",
                "=" * 64,
                f"  Sort order used     : {sort_name}",
                f"  New reviews added   : {self.new_count}",
                f"  Total in file       : {total_in_file}",
                f"  Duplicates skipped  : {self.skipped_count}",
                f"  Run duration        : {elapsed:.0f}s  ({elapsed / 60:.1f} min)",
                "=" * 64,
            ]
            print("\n".join(lines))

            self.logger.info(
                f"Run complete — new={self.new_count}  total={total_in_file}  "
                f"skipped={self.skipped_count}  duration={elapsed:.0f}s"
            )


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Google Play Store review scraper for Family Link"
    )
    parser.add_argument(
        "--max-minutes",
        type=int,
        default=50,
        metavar="N",
        help="Maximum runtime in minutes before graceful exit (default: 50)",
    )
    args = parser.parse_args()

    logger = setup_logging()
    logger.info(
        f"Scraper initialising — app={APP_ID}  max_minutes={args.max_minutes}"
    )

    Scraper(max_minutes=args.max_minutes, logger=logger).run()


if __name__ == "__main__":
    main()
