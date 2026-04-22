import logging
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup

from models import Job
from utils import HEADERS, normalize_location, parse_date, is_too_old, delay, with_retry

log = logging.getLogger("job-scraper.drushim")


def _find_container(el, depth=8):
    """Walk up until we find a node containing a company or location sibling."""
    node = el.parent
    for _ in range(depth):
        if node is None:
            break
        texts = list(node.stripped_strings)
        # A proper card container should have at least 3 text nodes
        if len(texts) >= 3:
            return node
        node = node.parent
    return el.parent


def scrape(keyword: str, cfg: dict) -> list[Job]:
    """Drushim uses infinite scroll — one URL returns all results for the keyword."""
    jobs = []
    base = cfg["platforms"]["drushim"]["base_url"]
    max_age = cfg["requirements"]["max_age_days"]

    url = f"{base}/jobs/search/{quote_plus(keyword)}/?ssaen=1"
    log.info(f"Fetching: {url}")

    try:
        resp = with_retry(
            lambda: requests.get(url, headers=HEADERS, timeout=15),
            attempts=cfg["scraping"]["retry_attempts"],
            backoff_base=cfg["scraping"]["retry_backoff_base"],
        )
        resp.raise_for_status()
    except Exception as e:
        log.error(f"Failed for {keyword!r}: {e}")
        return jobs

    soup = BeautifulSoup(resp.content, "lxml")

    # Job detail links follow the pattern /job/{id}/{hash}/
    seen = set()
    job_links = [
        a for a in soup.select('a[href*="/job/"]')
        if a.get("href", "").count("/") >= 3  # filter out short /job/ hrefs
    ]

    if not job_links:
        log.warning(f"No jobs found for {keyword!r}")
        return jobs

    found = 0
    for link in job_links:
        href = link.get("href", "")
        if href in seen:
            continue
        seen.add(href)

        title = link.get_text(strip=True)
        if not title:
            continue

        container = _find_container(link)

        # Company and location: pick text nodes that are not the title and not Hebrew time phrases
        company = ""
        location = ""
        date_text = ""
        if container:
            for s in container.stripped_strings:
                if s == title:
                    continue
                if "לפני" in s or "שעות" in s or "דקות" in s or "ימים" in s:
                    date_text = s
                elif not company:
                    company = s
                elif not location:
                    location = s

        date_posted = parse_date(date_text)
        if is_too_old(date_posted, max_age):
            continue

        jobs.append(Job(
            title=title,
            company=company,
            location=normalize_location(location),
            date_posted=date_posted,
            source="Drushim",
            url=urljoin(base, href),
            keyword_matched=keyword,
        ))
        found += 1

    log.info(f"Found {found} jobs for {keyword!r}")
    delay(cfg["scraping"]["delay_min"], cfg["scraping"]["delay_max"])
    return jobs
