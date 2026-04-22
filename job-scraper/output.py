import logging
from collections import Counter
from pathlib import Path

import pandas as pd

from models import Job

log = logging.getLogger("job-scraper.output")

CSV_FIELDS = ["title", "company", "location", "date_posted", "source", "url", "keyword_matched"]

_AGG = {
    "title":           "first",
    "company":         "first",
    "location":        "first",
    "date_posted":     "first",
    "source":          "first",
    "keyword_matched": lambda x: ", ".join(x.dropna().astype(str).unique()),
}


def _dedup_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return df.groupby("url", as_index=False).agg(_AGG)[CSV_FIELDS]


def write_csv(jobs: list[Job], path: Path):
    df = pd.DataFrame([j.to_dict() for j in jobs]) if jobs else pd.DataFrame(columns=CSV_FIELDS)
    clean = _dedup_df(df)
    before, after = len(df), len(clean)
    if before != after:
        log.info(f"Deduplication: {before} → {after} rows ({before - after} duplicates removed)")
    clean.to_csv(path, index=False, encoding="utf-8-sig")
    log.info(f"Saved {after} jobs -> {path}")


def save_partial(jobs: list[Job], path: Path):
    if not jobs:
        return
    new_df = pd.DataFrame([j.to_dict() for j in jobs])
    try:
        existing_df = pd.read_csv(path)
        combined = pd.concat([existing_df, new_df], ignore_index=True)
    except FileNotFoundError:
        combined = new_df
    clean = _dedup_df(combined)
    clean.to_csv(path, index=False, encoding="utf-8-sig")
    log.info(f"Partial save: {len(clean)} jobs")


def print_summary(jobs: list[Job], raw_count: int):
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    by_source = Counter(j.source for j in jobs)
    for source, count in sorted(by_source.items()):
        print(f"  {source:<12}: {count} jobs")

    print(f"\n  Total raw collected : {raw_count}")
    print(f"  After deduplication: {len(jobs)}")

    companies = Counter(j.company for j in jobs if j.company)
    print("\n  Top 5 companies hiring:")
    for company, count in companies.most_common(5):
        print(f"    {count:>3}x  {company}")
    print("=" * 60)
