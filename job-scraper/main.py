"""
Entry point for the job scraper.
Run: python main.py
"""

import asyncio
import logging
import os
import random
import re
import sys
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv(Path(__file__).parent.parent / ".env")

import scrapers.alljobs as alljobs
import scrapers.drushim as drushim
import scrapers.linkedin as linkedin
import scrapers.comeet as comeet
from filters import apply_requirements
from output import write_csv, save_partial, print_summary
from utils import delay

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("job-scraper")

CONFIG_PATH = Path(__file__).parent / "config.yaml"
OUTPUT_DIR = Path(__file__).parent


def load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


async def run_browser_scrapers(keywords: list[str], comeet_keywords: list[str], cfg: dict, active: set) -> list:
    jobs = []
    li_enabled = "linkedin" in active
    co_enabled = "comeet" in active

    if not li_enabled and not co_enabled:
        return jobs

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)

        for keyword in keywords:
            if li_enabled:
                try:
                    found = await linkedin.scrape(keyword, cfg, browser)
                    jobs.extend(found)
                except Exception as e:
                    log.error(f"[LinkedIn] {keyword!r}: {e}")
                await asyncio.sleep(random.uniform(
                    cfg["scraping"]["delay_min"], cfg["scraping"]["delay_max"]
                ))

        for keyword in comeet_keywords:
            if co_enabled:
                try:
                    found = await comeet.scrape(keyword, cfg, browser)
                    jobs.extend(found)
                except Exception as e:
                    log.error(f"[Comeet] {keyword!r}: {e}")
                await asyncio.sleep(random.uniform(
                    cfg["scraping"]["delay_min"], cfg["scraping"]["delay_max"]
                ))

        await browser.close()

    return jobs


_CONSTRUCTION_KW = ["בינוי", "בנייה", "קבלנ", "מגורים", "תשתיות", "עבודות עפר"]


def _fix_linkedin_location_filter() -> bool:
    """Widen the location filter to keep LinkedIn jobs whose location couldn't be parsed."""
    path = Path(__file__).parent / "filters.py"
    old = "if j.source.lower() in israeli_sources or _is_israeli_location(j.location)"
    new = "if j.source.lower() in israeli_sources or _is_israeli_location(j.location) or not j.location"
    content = path.read_text(encoding="utf-8")
    if old in content and new not in content:
        path.write_text(content.replace(old, new), encoding="utf-8")
        return True
    return False


def _fix_add_title_excludes(config_path: Path, keywords: list[str]) -> bool:
    """Append keywords to title_exclude in config.yaml, preserving all comments."""
    content = config_path.read_text(encoding="utf-8")
    added = []
    for kw in keywords:
        if f'"{kw}"' not in content:
            content = re.sub(
                r'(title_exclude:(?:\s*\n\s+- "[^"]*")+)',
                lambda m, k=kw: m.group(0) + f'\n    - "{k}"',
                content,
            )
            added.append(kw)
    if added:
        config_path.write_text(content, encoding="utf-8")
    return bool(added)


def _run_mini_scrape(keywords: list[str], cfg: dict, active: set) -> list:
    """Re-run 2 keywords through static scrapers only (no API quota used)."""
    jobs = []
    for keyword in keywords:
        if "alljobs" in active:
            try:
                jobs.extend(alljobs.scrape(keyword, cfg))
            except Exception:
                pass
        if "drushim" in active:
            try:
                jobs.extend(drushim.scrape(keyword, cfg))
            except Exception:
                pass
    return jobs


def self_improve(all_jobs: list, filtered_jobs: list, output_path: Path, config_path: Path, active: set):
    """Analyze output for known issues, auto-fix what's possible, verify. Runs after every scrape."""
    print("\n" + "=" * 50)
    print("🔍 SELF-IMPROVEMENT CHECK")
    print("=" * 50)

    df = pd.read_csv(output_path) if output_path.exists() else pd.DataFrame()
    issues_found: list[str] = []
    issues_fixed: list[str] = []

    # A: LinkedIn location filter too aggressive
    li_total = sum(1 for j in all_jobs if j.source == "LinkedIn")
    li_csv = int((df["source"] == "LinkedIn").sum()) if not df.empty and "source" in df else 0
    if li_total > 0 and li_csv / li_total < 0.5:
        issues_found.append(f"LinkedIn filter too aggressive: kept {li_csv}/{li_total}")
        if _fix_linkedin_location_filter():
            issues_fixed.append("linkedin location filter")

    # B: Construction/noise jobs in results
    if not df.empty and "title" in df:
        noise = df[df["title"].str.contains("|".join(_CONSTRUCTION_KW), na=False)]
        if len(noise) > 3:
            issues_found.append(f"{len(noise)} construction jobs in results")
            if _fix_add_title_excludes(config_path, _CONSTRUCTION_KW):
                issues_fixed.append("construction title filter")

    # C: Unparseable date_posted values
    if not df.empty and "date_posted" in df:
        garbage = df[
            df["date_posted"].notna()
            & (df["date_posted"].astype(str) != "")
            & ~df["date_posted"].astype(str).str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)
        ]
        if len(garbage) > 0:
            issues_found.append(f"{len(garbage)} unparseable date_posted values (manual review needed)")

    # D: Missing data above threshold
    if not df.empty:
        for col in ["company", "location", "date_posted"]:
            if col in df:
                pct = (df[col].isna() | (df[col].astype(str) == "")).mean() * 100
                if pct > 70:
                    issues_found.append(f"{col} missing in {pct:.0f}% of rows")

    # E: Duplicate URLs that slipped through
    if not df.empty and "url" in df:
        dupes = len(df) - df["url"].nunique()
        if dupes > 0:
            issues_found.append(f"{dupes} duplicate URLs slipped through")

    # Verify fixes with a mini re-run
    if issues_fixed:
        print(f"  ⚙️  Fixed {len(issues_fixed)} issue(s) — verifying with mini re-run...")
        cfg = load_config()
        mini_jobs = _run_mini_scrape(["Product Manager", "מנהל/ת פרויקט"], cfg, active)
        mini_filtered = apply_requirements(mini_jobs, cfg["requirements"], cfg["platforms"])
        mini_df = (
            pd.DataFrame([j.to_dict() for j in mini_filtered])
            if mini_filtered else pd.DataFrame()
        )
        if "linkedin location filter" in issues_fixed:
            li2 = int((mini_df["source"] == "LinkedIn").sum()) if not mini_df.empty and "source" in mini_df else 0
            print(f"  ✅ LinkedIn: {li_csv} → {li2} jobs after fix")
        if "construction title filter" in issues_fixed and not mini_df.empty and "title" in mini_df:
            noise2 = mini_df[mini_df["title"].str.contains("|".join(_CONSTRUCTION_KW), na=False)]
            noise_before = int(df["title"].str.contains("|".join(_CONSTRUCTION_KW), na=False).sum()) if not df.empty else 0
            print(f"  ✅ Construction filter: {noise_before} → {len(noise2)} noise jobs")

    # Summary
    print(f"\n  📊 Jobs saved:   {len(df)}")
    print(f"  🔎 Issues found: {len(issues_found)}")
    print(f"  🔧 Issues fixed: {len(issues_fixed)}")
    if issues_found:
        for issue in issues_found:
            tag = "✅" if any(w in issue.lower() for f in issues_fixed for w in f.split()) else "⚠️ "
            print(f"     {tag} {issue}")
    if not issues_found:
        print("\n  ✅ All checks passed — ready for full run")
    elif len(issues_found) == len(issues_fixed):
        print("\n  ✅ All issues resolved — re-run to apply fixes")
    else:
        print(f"\n  ⚠️  {len(issues_found) - len(issues_fixed)} issue(s) need manual review")
    print("=" * 50 + "\n")


def main():
    cfg = load_config()
    active = set(cfg.get("active_platforms", []))
    log.info(f"Active platforms: {', '.join(sorted(active)) or 'none'}")

    output_path = OUTPUT_DIR / cfg["output"]["csv_file"]
    partial_path = OUTPUT_DIR / "job_results_partial.csv"
    save_interval = cfg["output"]["partial_save_interval"]

    all_keywords = (
        cfg["keywords"].get("english", []) +
        cfg["keywords"].get("hebrew", [])
    )
    comeet_keywords = cfg.get("comeet_keywords", all_keywords)

    all_jobs = []
    counter = 0

    # --- Static scrapers ---
    for keyword in all_keywords:
        if "alljobs" in active:
            try:
                all_jobs.extend(alljobs.scrape(keyword, cfg))
            except Exception as e:
                log.error(f"[AllJobs] {keyword!r}: {e}")

        if "drushim" in active:
            try:
                all_jobs.extend(drushim.scrape(keyword, cfg))
            except Exception as e:
                log.error(f"[Drushim] {keyword!r}: {e}")

        counter += 1
        if counter % save_interval == 0:
            save_partial(all_jobs, partial_path)

        delay(cfg["scraping"]["delay_min"], cfg["scraping"]["delay_max"])

    # --- Browser-based scrapers ---
    try:
        browser_jobs = asyncio.run(run_browser_scrapers(all_keywords, comeet_keywords, cfg, active))
        all_jobs.extend(browser_jobs)
    except Exception as e:
        log.error(f"Browser scrapers failed: {e}")

    raw_count = len(all_jobs)

    filtered_jobs = apply_requirements(all_jobs, cfg["requirements"], cfg["platforms"])

    write_csv(filtered_jobs, output_path)
    print_summary(filtered_jobs, raw_count)
    self_improve(all_jobs, filtered_jobs, output_path, CONFIG_PATH, active)


if __name__ == "__main__":
    main()
