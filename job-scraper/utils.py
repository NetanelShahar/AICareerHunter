import logging
import random
import re
import time
from datetime import datetime, timedelta
from typing import Optional

log = logging.getLogger("job-scraper.utils")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

LOCATION_MAP = {
    "תל אביב": "Tel Aviv-Yafo",
    "tel aviv": "Tel Aviv-Yafo",
    "tel-aviv": "Tel Aviv-Yafo",
    "תל-אביב": "Tel Aviv-Yafo",
    "ירושלים": "Jerusalem",
    "jerusalem": "Jerusalem",
    "חיפה": "Haifa",
    "haifa": "Haifa",
    "באר שבע": "Beer Sheva",
    "beer sheva": "Beer Sheva",
    "נתניה": "Netanya",
    "netanya": "Netanya",
    "הרצליה": "Herzliya",
    "herzliya": "Herzliya",
    "רמת גן": "Ramat Gan",
    "ramat gan": "Ramat Gan",
    "פתח תקווה": "Petah Tikva",
    "petah tikva": "Petah Tikva",
    "ראשון לציון": "Rishon LeZion",
    "rishon lezion": "Rishon LeZion",
    "remote": "Remote",
    "מרחוק": "Remote",
    "היברידי": "Hybrid",
    "hybrid": "Hybrid",
}


def normalize_location(raw: str) -> str:
    key = raw.strip().lower()
    for pattern, normalized in LOCATION_MAP.items():
        if pattern in key:
            return normalized
    return raw.strip().title()


def parse_date(text: str) -> str:
    """Convert relative or absolute date strings to YYYY-MM-DD."""
    text = text.lower().strip()
    today = datetime.now()

    patterns = [
        (r"(\d+)\s*(day|days|יום|ימים)", lambda m: today - timedelta(days=int(m.group(1)))),
        (r"(\d+)\s*(hour|hours|שעה|שעות)", lambda m: today - timedelta(hours=int(m.group(1)))),
        (r"(\d+)\s*(week|weeks|שבוע|שבועות)", lambda m: today - timedelta(weeks=int(m.group(1)))),
        (r"(\d+)\s*(month|months|חודש|חודשים)", lambda m: today - timedelta(days=int(m.group(1)) * 30)),
        (r"today|היום", lambda m: today),
        (r"yesterday|אתמול", lambda m: today - timedelta(days=1)),
    ]

    for pattern, calc in patterns:
        m = re.search(pattern, text)
        if m:
            try:
                return calc(m).strftime("%Y-%m-%d")
            except Exception:
                pass

    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d.%m.%Y", "%B %d, %Y"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass

    return text


def is_too_old(date_str: str, max_days: int) -> bool:
    try:
        posted = datetime.strptime(date_str, "%Y-%m-%d")
        return (datetime.now() - posted).days > max_days
    except Exception:
        return False


def delay(min_s: float, max_s: float):
    time.sleep(random.uniform(min_s, max_s))


def with_retry(fn, attempts: int = 3, backoff_base: float = 2):
    for attempt in range(attempts):
        try:
            return fn()
        except Exception as e:
            if attempt == attempts - 1:
                raise
            wait = backoff_base ** attempt
            log.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait}s...")
            time.sleep(wait)
