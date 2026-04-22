import logging
import os
import random
import re
import time

from serpapi import GoogleSearch

from models import Job
from utils import normalize_location

log = logging.getLogger("job-scraper.comeet")

_MAX_PAGES = 3


def _extract_company(url: str) -> str:
    match = re.search(r'comeet\.com/jobs/([^/]+)/', url)
    return match.group(1).replace("-", " ").title() if match else ""


async def scrape(keyword: str, cfg: dict, browser) -> list[Job]:
    """browser arg kept for API compatibility with main.py — not used."""
    jobs = []

    serpapi_key = os.getenv("SERPAPI_KEY", "")
    if not serpapi_key:
        log.warning("SERPAPI_KEY not found in .env — skipping Comeet")
        return jobs

    for page in range(_MAX_PAGES):
        params = {
            "api_key": serpapi_key,
            "engine":  "google",
            "q":       f'site:comeet.com "{keyword}" Israel',
            "hl":      "en",
            "gl":      "il",
            "start":   page * 10,
            "num":     10,
        }
        log.info(f"Comeet {keyword!r}: SerpAPI page {page + 1}")

        try:
            response = GoogleSearch(params).get_dict()
        except Exception as e:
            log.error(f"SerpAPI error on page {page + 1} for {keyword!r}: {e}")
            break

        print(f"SerpAPI response keys: {list(response.keys())}")
        print(f"organic_results count: {len(response.get('organic_results', []))}")
        print(f"error: {response.get('error', 'none')}")

        organic = response.get("organic_results", [])
        if not organic:
            log.info(f"Comeet {keyword!r}: no more results at page {page + 1}, stopping")
            break

        found = 0
        for result in organic:
            url = result.get("link", "")
            if "/jobs/" not in url and "/position/" not in url:
                continue

            title = result.get("title", "")
            if not title:
                continue

            jobs.append(Job(
                title=title,
                company=_extract_company(url),
                location=normalize_location("Israel"),
                date_posted="",
                source="Comeet",
                url=url,
                keyword_matched=keyword,
            ))
            found += 1

        log.info(f"Comeet {keyword!r}: page {page + 1} — {found} jobs from {len(organic)} results")
        if found == 0:
            break

        time.sleep(random.uniform(1, 2))

    return jobs
