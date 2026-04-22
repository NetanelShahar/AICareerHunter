"""
Applies the requirements section from config.yaml to filter job results.
Add new requirement types here without touching any scraper.
"""

import logging
from models import Job
from utils import is_too_old

log = logging.getLogger("job-scraper.filters")

SENIORITY_KEYWORDS = {
    "junior": ["junior", "jr", "entry", "associate", "graduate", "intern", "0-2"],
    "mid": ["mid", "intermediate", "2-5", "3-5"],
    "senior": ["senior", "sr", "lead", "principal", "staff", "5+", "7+"],
    "lead": ["lead", "principal", "staff", "architect"],
    "director": ["director", "vp", "head of", "chief", "c-level"],
}


def _matches_seniority(title: str, levels: list[str]) -> bool:
    title_lower = title.lower()
    for level in levels:
        for kw in SENIORITY_KEYWORDS.get(level, [level]):
            if kw in title_lower:
                return True
    return False


ISRAELI_SOURCES = {"alljobs", "drushim"}

ISRAELI_LOCATIONS = {
    "israel", "tel aviv-yafo", "jerusalem", "haifa", "beer sheva",
    "netanya", "herzliya", "ramat gan", "petah tikva", "rishon lezion",
    "rishon le-zion", "rehovot", "holon", "bnei brak", "ashdod",
    "ashkelon", "nazareth", "kfar saba", "ra'anana", "raanana",
    "modiin", "modi'in", "givatayim", "remote",  # Remote jobs in Israel context
}


def _is_israeli_location(location: str) -> bool:
    return location.lower() in ISRAELI_LOCATIONS


def apply_requirements(jobs: list[Job], req: dict, platforms: dict = None) -> list[Job]:
    """Filter jobs according to the requirements block in config.yaml."""
    original = len(jobs)
    result = jobs

    # Date filter: keep jobs from last N days, keep those with no parseable date
    max_age = req.get("max_age_days")
    if max_age:
        result = [j for j in result if not is_too_old(j.date_posted, max_age)]
        log.info(f"After max_age_days filter: {len(result)}/{original}")

    # Location filter: skip Israeli platforms, enforce for global ones
    israeli_sources = {
        name for name, pcfg in (platforms or {}).items()
        if pcfg.get("is_israeli", False)
    }
    result = [
        j for j in result
        if j.source.lower() in israeli_sources or _is_israeli_location(j.location)
    ]
    log.info(f"After Israel location filter: {len(result)}/{original}")

    # Seniority filter
    if req.get("seniority"):
        result = [j for j in result if _matches_seniority(j.title, req["seniority"])]
        log.info(f"After seniority filter: {len(result)}/{original}")

    # Title must include
    if req.get("title_must_include"):
        must = [kw.lower() for kw in req["title_must_include"]]
        result = [j for j in result if any(kw in j.title.lower() for kw in must)]
        log.info(f"After title_must_include filter: {len(result)}/{original}")

    # Title exclude
    if req.get("title_exclude"):
        excl = [kw.lower() for kw in req["title_exclude"]]
        result = [j for j in result if not any(kw in j.title.lower() for kw in excl)]
        log.info(f"After title_exclude filter: {len(result)}/{original}")

    # Company include
    if req.get("companies_include"):
        allowed = {c.lower() for c in req["companies_include"]}
        result = [j for j in result if j.company.lower() in allowed]
        log.info(f"After companies_include filter: {len(result)}/{original}")

    # Company exclude
    if req.get("companies_exclude"):
        excl = {c.lower() for c in req["companies_exclude"]}
        result = [j for j in result if j.company.lower() not in excl]
        log.info(f"After companies_exclude filter: {len(result)}/{original}")

    # Domain keywords in title
    if req.get("domain_keywords"):
        kws = [kw.lower() for kw in req["domain_keywords"]]
        result = [j for j in result if any(kw in j.title.lower() for kw in kws)]
        log.info(f"After domain_keywords filter: {len(result)}/{original}")

    return result
