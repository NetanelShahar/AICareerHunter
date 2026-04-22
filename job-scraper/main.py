"""
Entry point for the job scraper.
Run: python main.py
"""

import asyncio
import logging
import os
import random
import sys
from pathlib import Path

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


if __name__ == "__main__":
    main()
