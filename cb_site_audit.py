"""
Scrape Brooklyn CB website links from the NYC CAU page and detect whether each
site appears to contain common resources (calendar, minutes, social links, etc.).
"""

import json
import os
import re
import time
from html.parser import HTMLParser
from typing import Dict, List, Tuple
from urllib.parse import urljoin

import requests

CB_LIST_URL = "https://www.nyc.gov/site/cau/community-boards/brooklyn-boards.page"
OUTPUT_PATH = os.path.join("api_outputs", "cb_site_audit.json")
REQUEST_TIMEOUT = 25
REQUEST_HEADERS = {
    # Present as a standard browser to reduce 403/406 responses.
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

KEY_ITEMS = [
    "Calendar",
    "Minutes",
    "Agendas",
    "Meetings",
    "Resolutions",
    "Contact",
    "Instagram",
    "X",
    "Facebook",
    "Youtube",
    "Newsletters",
    "By-Laws",
    "News",
    "Events",
    "Permits and Licenses",
]


class LinkParser(HTMLParser):
    """Lightweight anchor tag collector using stdlib HTML parser."""

    def __init__(self):
        super().__init__()
        self.links: List[Tuple[str, str]] = []
        self._in_a = False
        self._href = None
        self._buf: List[str] = []

    def handle_starttag(self, tag, attrs):
        if tag != "a":
            return
        attr_dict = dict(attrs)
        self._href = attr_dict.get("href")
        self._buf = []
        self._in_a = True

    def handle_data(self, data):
        if self._in_a:
            self._buf.append(data)

    def handle_endtag(self, tag):
        if tag != "a" or not self._in_a:
            return
        text = "".join(self._buf).strip()
        if self._href:
            self.links.append((text, self._href))
        self._in_a = False
        self._href = None
        self._buf = []


def fetch_html(url: str) -> str:
    resp = requests.get(url, timeout=REQUEST_TIMEOUT, headers=REQUEST_HEADERS)
    resp.raise_for_status()
    return resp.text


def extract_cb_links() -> Dict[str, str]:
    """Return mapping of CB number -> website URL from the CAU Brooklyn page."""
    html = fetch_html(CB_LIST_URL)
    parser = LinkParser()
    parser.feed(html)

    cb_links: Dict[str, str] = {}
    for text, href in parser.links:
        match = re.search(r"Brooklyn\s*CB\s*(\d+)", text, re.IGNORECASE)
        if not match:
            continue
        cb_num = match.group(1)
        if href.startswith("#") or not href:
            continue
        absolute = urljoin(CB_LIST_URL, href)
        cb_links.setdefault(cb_num, absolute)

    return cb_links


def detect_features(html: str, url: str) -> Dict[str, bool]:
    """Heuristic presence checks across full HTML text."""
    text = html.lower()
    features = {item: False for item in KEY_ITEMS}

    basic_map = {
        "calendar": ["calendar"],
        "minutes": ["minutes"],
        "agendas": ["agenda"],
        "meetings": ["meeting"],
        "resolutions": ["resolution"],
        "contact": ["contact"],
        "newsletters": ["newsletter"],
        "by-laws": ["by-laws", "bylaws", "by laws"],
        "news": ["news"],
        "events": ["events", "event"],
        "permits and licenses": ["permit", "license"],
    }

    social_map = {
        "instagram": ["instagram.com", "instagram"],
        "x": ["x.com", "twitter.com", "twitter"],
        "facebook": ["facebook.com", "facebook"],
        "youtube": ["youtube.com", "youtu.be", "youtube"],
    }

    for key, terms in basic_map.items():
        features[_canonical_key(key)] = any(term in text for term in terms)

    for key, terms in social_map.items():
        features[_canonical_key(key)] = any(term in text for term in terms)

    return features


def _canonical_key(name: str) -> str:
    """Match KEY_ITEMS label casing."""
    for item in KEY_ITEMS:
        if item.lower() == name.lower():
            return item
    return name


def audit():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    cb_links = extract_cb_links()
    print(f"Found {len(cb_links)} Brooklyn CB sites.")

    results = []
    for cb_num, url in sorted(cb_links.items(), key=lambda x: int(x[0])):
        print(f"Fetching CB{cb_num}: {url}")
        record = {"cb_number": cb_num, "url": url}
        try:
            html = fetch_html(url)
            record["status"] = "ok"
            record["features"] = detect_features(html, url)
        except Exception as exc:  # noqa: BLE001
            record["status"] = "error"
            record["error"] = str(exc)
        results.append(record)
        time.sleep(0.5)  # gentle pacing

    with open(OUTPUT_PATH, "w") as f:
        json.dump({"source": CB_LIST_URL, "results": results}, f, indent=2)

    print(f"\nSaved results to {OUTPUT_PATH}")


if __name__ == "__main__":
    audit()
