"""
Scrape all NYC Community Board websites across all 5 boroughs.
Checks scorecard items and extracts actual social media URLs.
Outputs one JSON per borough to api_outputs/.
"""

import json, os, re, time
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Dict, List, Optional
from urllib.parse import urlparse
import requests

OUTPUT_DIR = "api_outputs"
TIMEOUT = 30
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Upgrade-Insecure-Requests": "1",
}

SOCIAL_DOMAINS = {
    "instagram.com": "Instagram",
    "facebook.com": "Facebook",
    "twitter.com": "X",
    "x.com":        "X",
    "youtube.com":  "Youtube",
    "youtu.be":     "Youtube",
}

TEXT_ITEMS = {
    "Calendar":             ["calendar"],
    "Minutes":              ["minutes"],
    "Agendas":              ["agenda"],
    "Meetings":             ["meeting"],
    "Resolutions":          ["resolution"],
    "Contact":              ["contact"],
    "Newsletters":          ["newsletter"],
    "By-Laws":              ["by-laws", "bylaws", "by laws"],
    "News":                 ["news"],
    "Events":               ["event"],
    "Permits and Licenses": ["permit", "license"],
}

BOROUGHS = {
    "manhattan": {
        "master_url": "https://www.nyc.gov/site/cau/community-boards/manhattan-boards.page",
        "cbs": {
            "1":  "https://manhattancb1.cityofnewyork.us/",
            "2":  "https://cbmanhattan.cityofnewyork.us/cb2/",
            "3":  "http://www.nyc.gov/manhattancb3",
            "4":  "https://cbmanhattan.cityofnewyork.us/cb4/",
            "5":  "http://www.cb5.org",
            "6":  "http://cbsix.org",
            "7":  "http://www.nyc.gov/mcb7",
            "8":  "http://www.cb8m.com/",
            "9":  "http://www.cb9m.org",
            "10": "http://www.nyc.gov/html/mancb10/html/home/home.shtml",
            "11": "http://www.cb11m.org/",
            "12": "https://cbmanhattan.cityofnewyork.us/cb12/",
        },
    },
    "queens": {
        "master_url": "https://www.nyc.gov/site/cau/community-boards/queens-boards.page",
        "cbs": {
            "1":  "http://www1.nyc.gov/site/queenscb1/index.page",
            "2":  "http://www.nyc.gov/html/qnscb2/html/home/home.shtml",
            "3":  "https://queenscb3.cityofnewyork.us/",
            "4":  "http://www1.nyc.gov/site/queenscb4/index.page",
            "5":  "http://www1.nyc.gov/site/queenscb5/index.page",
            "6":  "http://www1.nyc.gov/site/queenscb6/index.page",
            "7":  "https://www1.nyc.gov/site/queenscb7/index.page",
            "8":  "http://www1.nyc.gov/site/queenscb8/index.page",
            "9":  "http://www1.nyc.gov/site/queenscb9/index.page",
            "10": "http://www1.nyc.gov/site/queenscb10/index.page",
            "11": "http://www1.nyc.gov/site/queenscb11/index.page",
            "12": "http://www.nyc.gov/qcb12",
            "13": "http://www1.nyc.gov/site/queenscb13/index.page",
            "14": "http://www1.nyc.gov/site/queenscb14/index.page",
        },
    },
    "bronx": {
        "master_url": "https://www.nyc.gov/site/cau/community-boards/bronx-boards.page",
        "cbs": {
            "1":  "http://www.nyc.gov/html/bxcb1/html/home/home.shtml",
            "2":  "https://www1.nyc.gov/site/bronxcb2/index.page",
            "3":  "https://www1.nyc.gov/site/bronxcb3/index.page",
            "4":  "http://www1.nyc.gov/site/bronxcb4/index.page",
            "5":  "http://www.nyc.gov/html/bxcb5/html/home/home.shtml",
            "6":  "https://cbbronx.cityofnewyork.us/cb6/",
            "7":  "https://www1.nyc.gov/site/bronxcb7/index.page",
            "8":  "https://cbbronx.cityofnewyork.us/cb8/",
            "9":  "http://www.nyc.gov/html/bxcb9/html/home/home.shtml",
            "10": "http://www1.nyc.gov/site/bronxcb10/index.page",
            "11": "http://www.nyc.gov/bxcb11",
            "12": "http://www1.nyc.gov/site/bronxcb12/index.page",
        },
    },
    "staten_island": {
        "master_url": "https://www.nyc.gov/site/communityboards/about/staten-island-boards.page",
        "cbs": {
            "1": "https://www1.nyc.gov/site/statenislandcb1/index.page",
            "2": "https://www1.nyc.gov/site/statenislandcb2/index.page",
            "3": "https://www1.nyc.gov/site/statenislandcb3/index.page",
        },
    },
}


def fetch(url: str) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
    resp.raise_for_status()
    return resp.text


def extract_social_links(html: str) -> Dict[str, Optional[str]]:
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
                if (domain == sd or domain.endswith("." + sd)) and social[key] is None:
                    social[key] = href

    SocialParser().feed(html)
    return social


def check_text_features(html: str) -> Dict[str, bool]:
    text = html.lower()
    return {key: any(kw in text for kw in kws) for key, kws in TEXT_ITEMS.items()}


def audit_site(cb_num: str, url: str) -> dict:
    record = {"cb_number": cb_num, "url": url}
    try:
        html = fetch(url)
        record["status"] = "ok"
        text_features = check_text_features(html)
        social_links = extract_social_links(html)
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


def scrape_borough(borough_key: str, config: dict) -> None:
    print(f"\n{'='*50}")
    print(f"Scraping {borough_key.upper().replace('_', ' ')}")
    print(f"{'='*50}")

    results = []
    for cb_num, url in sorted(config["cbs"].items(), key=lambda x: int(x[0])):
        print(f"  CB{cb_num}: {url}")
        record = audit_site(cb_num, url)
        status = record.get("status", "error")
        if status == "ok":
            f = record["features"]
            social = [k for k in ["Instagram", "X", "Facebook", "Youtube"] if f.get(k)]
            score = sum(1 for v in f.values() if v)
            print(f"    -> ok | score {score}/14 | social={social}")
        else:
            print(f"    -> ERROR: {record.get('error')}")
        results.append(record)
        time.sleep(0.75)

    output = {
        "borough": borough_key,
        "source": config["master_url"],
        "scraped_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "notes": "Homepage keyword scan + social media URL extraction. Subpage URLs and off-site social media require manual research.",
        "results": results,
    }

    out_path = os.path.join(OUTPUT_DIR, f"cb_site_audit_{borough_key}.json")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"  Saved -> {out_path}")


if __name__ == "__main__":
    for borough_key, config in BOROUGHS.items():
        scrape_borough(borough_key, config)
    print("\nAll boroughs done.")
