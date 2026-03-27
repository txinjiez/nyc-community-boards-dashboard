"""
Scrape Brooklyn CB websites from the NYC CAU master list.
For each site: check scorecard items and extract actual social media URLs.
"""

import json
import os
import re
import time
from html.parser import HTMLParser
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests

MASTER_URL = "https://www.nyc.gov/site/cau/community-boards/brooklyn-boards.page"
OUTPUT_PATH = os.path.join("api_outputs", "cb_site_audit.json")
TIMEOUT = 30

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    # Do NOT set Accept-Encoding manually — requests handles decompression
    # automatically only when it controls that header.
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Upgrade-Insecure-Requests": "1",
}

# Social media domains → scorecard key
SOCIAL_DOMAINS = {
    "instagram.com": "Instagram",
    "facebook.com": "Facebook",
    "twitter.com": "X",
    "x.com": "X",
    "youtube.com": "Youtube",
    "youtu.be": "Youtube",
}

# Text-based scorecard items: key → list of keywords to search in page text
TEXT_ITEMS = {
    "Calendar":            ["calendar"],
    "Minutes":             ["minutes"],
    "Agendas":             ["agenda"],
    "Meetings":            ["meeting"],
    "Resolutions":         ["resolution"],
    "Contact":             ["contact"],
    "Newsletters":         ["newsletter"],
    "By-Laws":             ["by-laws", "bylaws", "by laws"],
    "News":                ["news"],
    "Events":              ["event"],
    "Permits and Licenses":["permit", "license"],
}


class LinkExtractor(HTMLParser):
    """Collect all <a href="..."> links from a page."""

    def __init__(self):
        super().__init__()
        self.links: List[str] = []           # all hrefs
        self.cb_sites: Dict[str, str] = {}   # cb_number -> url (used on master page)

    def handle_starttag(self, tag, attrs):
        if tag != "a":
            return
        attr = dict(attrs)
        href = attr.get("href", "")
        if href and not href.startswith("#"):
            self.links.append(href)


def fetch(url: str) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
    resp.raise_for_status()
    return resp.text


def extract_master_links() -> Dict[str, str]:
    """Return {cb_number: website_url} from the CAU Brooklyn master page."""
    html = fetch(MASTER_URL)
    parser = LinkExtractor()
    parser.feed(html)

    cb_links: Dict[str, str] = {}
    for href in parser.links:
        # Match links that label themselves as Brooklyn CB N
        # They appear as link text like "Brooklyn CB 1" but we parse from the href pattern
        pass

    # Re-parse with text capture
    class CBLinkParser(HTMLParser):
        def __init__(self):
            super().__init__()
            self.results: Dict[str, str] = {}
            self._in_a = False
            self._href = None
            self._buf: List[str] = []

        def handle_starttag(self, tag, attrs):
            if tag != "a":
                return
            self._href = dict(attrs).get("href", "")
            self._buf = []
            self._in_a = True

        def handle_data(self, data):
            if self._in_a:
                self._buf.append(data)

        def handle_endtag(self, tag):
            if tag != "a" or not self._in_a:
                return
            text = "".join(self._buf).strip()
            href = self._href or ""
            m = re.search(r"Brooklyn\s*CB\s*(\d+)", text, re.IGNORECASE)
            if m and href and not href.startswith("#"):
                num = m.group(1)
                absolute = urljoin(MASTER_URL, href)
                self.results.setdefault(num, absolute)
            self._in_a = False
            self._href = None
            self._buf = []

    p = CBLinkParser()
    p.feed(html)
    return p.results


def extract_social_links(html: str, base_url: str) -> Dict[str, Optional[str]]:
    """
    Return {scorecard_key: url} for social platforms found as actual <a href> links.
    Value is the first matching URL found, or None if not present.
    """
    social: Dict[str, Optional[str]] = {v: None for v in set(SOCIAL_DOMAINS.values())}

    class SocialParser(HTMLParser):
        def handle_starttag(self_, tag, attrs):
            if tag != "a":
                return
            href = dict(attrs).get("href", "") or ""
            try:
                domain = urlparse(href).netloc.lower().lstrip("www.")
            except Exception:
                return
            for sd, key in SOCIAL_DOMAINS.items():
                # Exact match or subdomain match (e.g. m.facebook.com)
                # Prevents "x.com" matching "webex.com" as a substring.
                if (domain == sd or domain.endswith("." + sd)) and social[key] is None:
                    social[key] = href

    p = SocialParser()
    p.feed(html)
    return social


def check_text_features(html: str) -> Dict[str, bool]:
    text = html.lower()
    return {key: any(kw in text for kw in kws) for key, kws in TEXT_ITEMS.items()}


def audit_site(cb_num: str, url: str) -> dict:
    print(f"  Fetching CB{cb_num}: {url}")
    record = {"cb_number": cb_num, "url": url}
    try:
        html = fetch(url)
        record["status"] = "ok"

        text_features = check_text_features(html)
        social_links = extract_social_links(html, url)

        # Build features: booleans for text items, actual URL (or None) for social
        record["features"] = {
            **text_features,
            "Instagram": social_links["Instagram"],
            "X":         social_links["X"],
            "Facebook":  social_links["Facebook"],
            "Youtube":   social_links["Youtube"],
        }

    except requests.HTTPError as e:
        record["status"] = "error"
        record["error"] = f"HTTP {e.response.status_code}"
    except Exception as e:
        record["status"] = "error"
        record["error"] = str(e)

    return record


def main():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    print(f"Fetching master list: {MASTER_URL}")
    cb_links = extract_master_links()
    print(f"Found {len(cb_links)} Brooklyn CB sites on master page.\n")

    if len(cb_links) == 0:
        print("ERROR: No CB links found on master page. Aborting.")
        return

    for num, url in sorted(cb_links.items(), key=lambda x: int(x[0])):
        print(f"  CB{num} -> {url}")

    print()
    results = []
    for cb_num, url in sorted(cb_links.items(), key=lambda x: int(x[0])):
        record = audit_site(cb_num, url)
        results.append(record)
        time.sleep(0.75)  # polite pacing

    output = {
        "source": MASTER_URL,
        "scraped_at": __import__("datetime").datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "results": results,
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nSaved {len(results)} records to {OUTPUT_PATH}")

    # Print summary
    print("\n--- Summary ---")
    for r in results:
        if r["status"] != "ok":
            print(f"CB{r['cb_number']}: ERROR - {r.get('error')}")
            continue
        f = r["features"]
        social = [k for k in ["Instagram", "X", "Facebook", "Youtube"] if f.get(k)]
        score = sum(1 for v in f.values() if v)
        print(f"CB{r['cb_number']:>2}: score {score:2}/14  social={social}")


if __name__ == "__main__":
    main()
