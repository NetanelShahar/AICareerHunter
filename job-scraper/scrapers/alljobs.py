import logging
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup

from models import Job
from utils import HEADERS, normalize_location, parse_date, is_too_old, delay, with_retry

log = logging.getLogger("job-scraper.alljobs")


def _find_container(el, depth=6):
    """Walk up the DOM until we find a node that contains both a company and location link."""
    node = el.parent
    for _ in range(depth):
        if node is None:
            break
        if node.select_one('a[href*="/Employer/"]') or node.select_one('a[href*="city="]'):
            return node
        node = node.parent
    return el.parent


def scrape(keyword: str, cfg: dict) -> list[Job]:
    jobs = []
    base = cfg["platforms"]["alljobs"]["base_url"]
    max_pages = cfg["scraping"]["max_pages_per_keyword"]
    max_age = cfg["requirements"]["max_age_days"]

    for page in range(1, max_pages + 1):
        url = f"{base}/SearchResultsGuest.aspx?page={page}&freetxt={quote_plus(keyword)}"
        log.info(f"Page {page}: {url}")

        try:
            resp = with_retry(
                lambda: requests.get(url, headers=HEADERS, timeout=15),
                attempts=cfg["scraping"]["retry_attempts"],
                backoff_base=cfg["scraping"]["retry_backoff_base"],
            )
            resp.raise_for_status()
        except Exception as e:
            log.error(f"Failed page {page} for {keyword!r}: {e}")
            break

        soup = BeautifulSoup(resp.content, "lxml")
        title_links = soup.select('a[href*="UploadSingle.aspx"]')

        if not title_links:
            log.warning(f"No cards on page {page}")
            break

        found = 0
        for link in title_links:
            title = link.get_text(strip=True)
            if not title:
                continue

            container = _find_container(link)

            company_el = container.select_one('a[href*="/Employer/"]') if container else None
            location_el = container.select_one('a[href*="city="]') if container else None
            # Date appears as plain text (e.g. "לפני 2 שעות"); grab the first short text node
            date_text = ""
            if container:
                for s in container.stripped_strings:
                    if "לפני" in s or ("/" in s and len(s) <= 12):
                        date_text = s
                        break

            date_posted = parse_date(date_text)
            if is_too_old(date_posted, max_age):
                continue

            href = link.get("href", "")
            jobs.append(Job(
                title=title,
                company=company_el.get_text(strip=True) if company_el else "",
                location=normalize_location(location_el.get_text(strip=True) if location_el else ""),
                date_posted=date_posted,
                source="AllJobs",
                url=urljoin(base, href),
                keyword_matched=keyword,
            ))
            found += 1

        log.info(f"Found {found} jobs on page {page}")
        if found == 0:
            break

        delay(cfg["scraping"]["delay_min"], cfg["scraping"]["delay_max"])

    return jobs
