import logging
import random
import time

import requests
from bs4 import BeautifulSoup

from models import Job
from utils import normalize_location, parse_date, is_too_old, HEADERS

log = logging.getLogger("job-scraper.linkedin")

_BASE_URL = "https://il.linkedin.com/jobs/search/"
_MAX_PAGES = 10


async def scrape(keyword: str, cfg: dict, browser) -> list[Job]:
    """browser arg kept for API compatibility with main.py — not used."""
    jobs = []
    location  = cfg["platforms"]["linkedin"].get("location", "Israel")
    max_age   = cfg["requirements"]["max_age_days"]
    delay_min = cfg["scraping"]["delay_min"]
    delay_max = cfg["scraping"]["delay_max"]

    for page in range(_MAX_PAGES):
        start = page * 25
        params = {
            "keywords": keyword,
            "location": location,
            "start":    start,
            "f_TPR":    "r2592000",  # last 30 days
        }
        log.info(f"Page {page + 1}: start={start}")

        try:
            resp = requests.get(_BASE_URL, params=params, headers=HEADERS, timeout=15)
            resp.raise_for_status()
        except Exception as e:
            log.error(f"Error on page {page + 1} for {keyword!r}: {e}")
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        cards = soup.find_all("div", class_="base-card")

        if not cards:
            log.info(f"LinkedIn {keyword!r}: no more results at page {page + 1}, stopping")
            break

        found = 0
        for card in cards:
            raw_url     = card.find("a", class_="base-card__full-link")
            job_url     = raw_url["href"].split("?")[0] if raw_url else ""

            title_el    = card.find("h3", class_="base-search-card__title")
            company_el  = card.find("h4", class_="base-search-card__subtitle")
            location_el = card.find("span", class_="job-search-card__location")
            date_tag    = card.find("time")

            if not title_el:
                continue

            raw_date    = date_tag["datetime"] if date_tag else ""
            date_posted = parse_date(raw_date)
            if is_too_old(date_posted, max_age):
                continue

            jobs.append(Job(
                title=title_el.get_text(strip=True),
                company=company_el.get_text(strip=True) if company_el else "",
                location=normalize_location(location_el.get_text(strip=True) if location_el else ""),
                date_posted=date_posted,
                source="LinkedIn",
                url=job_url,
                keyword_matched=keyword,
            ))
            found += 1

        log.info(f"LinkedIn {keyword!r}: page {page + 1} — {found} jobs")
        if found == 0:
            break

        time.sleep(random.uniform(delay_min, delay_max))

    return jobs
