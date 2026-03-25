"""
Probe subpage URLs for all 4 boroughs using known URL patterns.
For nyc.gov-hosted CBs, tries standard patterns and verifies 200 OK.
For custom-domain CBs, fetches homepage and extracts nav links.
"""
import json, time, requests
from html.parser import HTMLParser
from urllib.parse import urlparse, urljoin

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
TIMEOUT = 20
SOCIAL_DOMAINS = {
    "instagram.com": "Instagram", "facebook.com": "Facebook",
    "twitter.com": "X", "x.com": "X",
    "youtube.com": "Youtube", "youtu.be": "Youtube",
}

def fetch(url):
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
    r.raise_for_status()
    return r.text, r.url  # also return final URL after redirects

def page_exists(url):
    """Returns the final URL if page exists and isn't a redirect-to-homepage, else None."""
    try:
        html, final_url = fetch(url)
        # If redirected far away from expected domain, consider missing
        orig_domain = urlparse(url).netloc
        final_domain = urlparse(final_url).netloc
        if orig_domain and final_domain and orig_domain != final_domain:
            return None
        # Check for "page not found" or empty content signs
        if len(html) < 500:
            return None
        low = html.lower()
        if "page not found" in low or "404" in low[:2000]:
            return None
        return url
    except:
        return None

def extract_social(html):
    social = {v: None for v in set(SOCIAL_DOMAINS.values())}
    class SP(HTMLParser):
        def handle_starttag(self_, tag, attrs):
            if tag != "a": return
            href = dict(attrs).get("href", "") or ""
            try:
                domain = urlparse(href).netloc.lower().lstrip("www.")
            except: return
            for sd, key in SOCIAL_DOMAINS.items():
                if (domain == sd or domain.endswith("." + sd)) and social[key] is None:
                    social[key] = href
    SP().feed(html)
    return social

def extract_nav_links(html, base_url):
    """Extract all nav/menu links from a page."""
    links = {}
    class LP(HTMLParser):
        def __init__(self):
            super().__init__()
            self._in_a = False; self._href = None; self._buf = []
        def handle_starttag(self, tag, attrs):
            if tag != "a": return
            self._href = dict(attrs).get("href", "")
            self._buf = []; self._in_a = True
        def handle_data(self, data):
            if self._in_a: self._buf.append(data)
        def handle_endtag(self, tag):
            if tag != "a" or not self._in_a: return
            text = "".join(self._buf).strip().lower()
            href = self._href or ""
            if href and not href.startswith("#") and not href.startswith("javascript"):
                abs_href = urljoin(base_url, href)
                links[text] = abs_href
            self._in_a = False; self._href = None; self._buf = []
    LP().feed(html)
    return links

# --- nyc.gov CB URL pattern probing ---
# For each item, list candidate URL suffixes to try
NYC_GOV_PATTERNS = {
    "Calendar": [
        "calendar/calendar.page",
        "meetings/calendar.page",
        "about/calendar.page",
        "meetings/board-meetings.page",
    ],
    "Minutes": [
        "minutes/board-meeting-minutes.page",
        "about/minutes.page",
        "meetings/minutes.page",
        "meetings/board-meeting-minutes.page",
        "about/board-meeting-minutes.page",
    ],
    "Agendas": [
        "minutes/board-meeting-agendas.page",
        "meetings/agendas.page",
        "about/agendas.page",
        "meetings/board-meeting-agendas.page",
        "about/board-meeting-agendas.page",
    ],
    "Resolutions": [
        "about/resolutions.page",
        "meetings/resolutions.page",
        "minutes/resolutions.page",
        "resources/resolutions.page",
        "minutes/board-meeting-resolutions.page",
    ],
    "Contact": [
        "about/contact.page",
        "contact/contact.page",
        "about/contact-us.page",
    ],
    "Newsletters": [
        "news/newsletters.page",
        "about/newsletters.page",
        "news/e-blast-archives.page",
        "news/e-blasts.page",
        "resources/newsletters.page",
    ],
    "By-Laws": [
        "about/by-laws.page",
        "about/bylaws.page",
        "resources/by-laws.page",
        "about/by-laws-and-new-york-city-charter.page",
        "about/bylaws-and-rules.page",
        "about/by-laws-and-rules.page",
    ],
    "News": [
        "news/board-news.page",
        "news/news.page",
        "about/news.page",
        "news/press-releases.page",
        "news/community-news.page",
    ],
    "Events": [
        "calendar/calendar.page",
        "events/events.page",
        "news/events.page",
        "news/community-events.page",
    ],
    "Permits and Licenses": [
        "resources/liquor-licenses.page",
        "resources/permits-and-licenses.page",
        "about/permits.page",
        "resources/permits.page",
        "resources/special-permits.page",
        "resources/cannabis-licenses.page",
        "resources/block-party-permits.page",
        "resources/sidewalk-cafe-permits.page",
    ],
}

def probe_nyc_gov_cb(borough_slug, cb_num):
    """Try all URL patterns for a nyc.gov-hosted CB. Returns {item: url or None}."""
    base = f"https://www.nyc.gov/site/{borough_slug}cb{cb_num}/"
    results = {}
    for item, patterns in NYC_GOV_PATTERNS.items():
        found = None
        for pat in patterns:
            url = base + pat
            exists = page_exists(url)
            if exists:
                found = url
                break
            time.sleep(0.15)
        results[item] = found
        time.sleep(0.1)
    return results

def probe_custom_cb(homepage_url):
    """For custom-domain CBs, fetch homepage and look for nav links matching scorecard items."""
    try:
        html, final_url = fetch(homepage_url)
    except:
        return {item: None for item in NYC_GOV_PATTERNS}

    nav = extract_nav_links(html, final_url)
    social = extract_social(html)

    results = {}
    kw_map = {
        "Calendar": ["calendar"],
        "Minutes": ["minutes"],
        "Agendas": ["agenda"],
        "Resolutions": ["resolution"],
        "Contact": ["contact"],
        "Newsletters": ["newsletter", "e-blast", "eblast"],
        "By-Laws": ["by-law", "bylaw", "bylaws"],
        "News": ["news"],
        "Events": ["event"],
        "Permits and Licenses": ["permit", "license"],
    }
    for item, kws in kw_map.items():
        found = None
        for link_text, link_url in nav.items():
            if any(kw in link_text for kw in kws):
                found = link_url
                break
        results[item] = found

    return results, social

# ============================================================
# Define all CBs
# ============================================================
BOROUGHS = {
    "manhattan": {
        "slug": "manhattan",
        "display": "Manhattan",
        "cbs": {
            "1":  {"url": "https://manhattancb1.cityofnewyork.us/",                    "custom": True},
            "2":  {"url": "https://cbmanhattan.cityofnewyork.us/cb2/",                  "custom": True},
            "3":  {"url": "https://www.nyc.gov/site/manhattancb3/index.page",           "custom": False},
            "4":  {"url": "https://cbmanhattan.cityofnewyork.us/cb4/",                  "custom": True},
            "5":  {"url": "http://www.cb5.org",                                          "custom": True},
            "6":  {"url": "http://cbsix.org",                                            "custom": True},
            "7":  {"url": "https://www.nyc.gov/site/manhattancb7/index.page",           "custom": False},
            "8":  {"url": "http://www.cb8m.com/",                                        "custom": True},
            "9":  {"url": "http://www.cb9m.org",                                         "custom": True},
            "10": {"url": "https://www.nyc.gov/site/manhattancb10/index.page",          "custom": False},
            "11": {"url": "http://www.cb11m.org/",                                       "custom": True},
            "12": {"url": "https://cbmanhattan.cityofnewyork.us/cb12/",                 "custom": True},
        }
    },
    "queens": {
        "slug": "queens",
        "display": "Queens",
        "cbs": {
            "1":  {"url": "https://www.nyc.gov/site/queenscb1/index.page",   "custom": False},
            "2":  {"url": "https://www.nyc.gov/site/queenscb2/index.page",   "custom": False},
            "3":  {"url": "https://queenscb3.cityofnewyork.us/",              "custom": True},
            "4":  {"url": "https://www.nyc.gov/site/queenscb4/index.page",   "custom": False},
            "5":  {"url": "https://www.nyc.gov/site/queenscb5/index.page",   "custom": False},
            "6":  {"url": "https://www.nyc.gov/site/queenscb6/index.page",   "custom": False},
            "7":  {"url": "https://www.nyc.gov/site/queenscb7/index.page",   "custom": False},
            "8":  {"url": "https://www.nyc.gov/site/queenscb8/index.page",   "custom": False},
            "9":  {"url": "https://www.nyc.gov/site/queenscb9/index.page",   "custom": False},
            "10": {"url": "https://www.nyc.gov/site/queenscb10/index.page",  "custom": False},
            "11": {"url": "https://www.nyc.gov/site/queenscb11/index.page",  "custom": False},
            "12": {"url": "https://www.nyc.gov/site/queenscb12/index.page",  "custom": False},
            "13": {"url": "https://www.nyc.gov/site/queenscb13/index.page",  "custom": False},
            "14": {"url": "https://www.nyc.gov/site/queenscb14/index.page",  "custom": False},
        }
    },
    "bronx": {
        "slug": "bronx",
        "display": "Bronx",
        "cbs": {
            "1":  {"url": "https://www.nyc.gov/site/bronxcb1/index.page",   "custom": False},
            "2":  {"url": "https://www.nyc.gov/site/bronxcb2/index.page",   "custom": False},
            "3":  {"url": "https://www.nyc.gov/site/bronxcb3/index.page",   "custom": False},
            "4":  {"url": "https://www.nyc.gov/site/bronxcb4/index.page",   "custom": False},
            "5":  {"url": "https://www.nyc.gov/site/bronxcb5/index.page",   "custom": False},
            "6":  {"url": "https://cbbronx.cityofnewyork.us/cb6/",           "custom": True},
            "7":  {"url": "https://www.nyc.gov/site/bronxcb7/index.page",   "custom": False},
            "8":  {"url": "https://cbbronx.cityofnewyork.us/cb8/",           "custom": True},
            "9":  {"url": "https://www.nyc.gov/site/bronxcb9/index.page",   "custom": False},
            "10": {"url": "https://www.nyc.gov/site/bronxcb10/index.page",  "custom": False},
            "11": {"url": "https://www.nyc.gov/site/bronxcb11/index.page",  "custom": False},
            "12": {"url": "https://www.nyc.gov/site/bronxcb12/index.page",  "custom": False},
        }
    },
    "staten_island": {
        "slug": "statenisland",
        "display": "Staten Island",
        "cbs": {
            "1": {"url": "https://www.nyc.gov/site/statenislandcb1/index.page", "custom": False},
            "2": {"url": "https://www.nyc.gov/site/statenislandcb2/index.page", "custom": False},
            "3": {"url": "https://www.nyc.gov/site/statenislandcb3/index.page", "custom": False},
        }
    },
}

if __name__ == "__main__":
    for borough_key, bconf in BOROUGHS.items():
        print(f"\n{'='*50}\n{bconf['display'].upper()}\n{'='*50}")
        results = []
        for cb_num, cbconf in sorted(bconf["cbs"].items(), key=lambda x: int(x[0])):
            print(f"  CB{cb_num}...", end="", flush=True)
            cb_url = cbconf["url"]
            record = {"cb_number": cb_num, "url": cb_url, "status": "ok", "features": {}}

            if cbconf["custom"]:
                res = probe_custom_cb(cb_url)
                if isinstance(res, tuple):
                    feat, social_on_site = res
                else:
                    feat = res
                    social_on_site = {}
                record["features"] = feat
                # Add social as {url, on_site, active} from homepage links
                for social_key, social_url in social_on_site.items():
                    if social_url:
                        record["features"][social_key] = {"url": social_url, "on_site": True, "active": None}
                    else:
                        record["features"][social_key] = None
            else:
                feat = probe_nyc_gov_cb(bconf["slug"], cb_num)
                record["features"] = feat
                # Also fetch homepage to get social links
                try:
                    html, _ = fetch(cb_url)
                    social_on_site = extract_social(html)
                    for social_key, social_url in social_on_site.items():
                        if social_url:
                            record["features"][social_key] = {"url": social_url, "on_site": True, "active": None}
                        else:
                            record["features"][social_key] = None
                except:
                    for k in ["Instagram", "X", "Facebook", "Youtube"]:
                        record["features"][k] = None

            score = sum(1 for v in record["features"].values() if v)
            print(f" {score}/14")
            results.append(record)
            time.sleep(0.5)

        out = {
            "borough": borough_key,
            "display": bconf["display"],
            "results": results,
        }
        with open(f"api_outputs/cb_site_audit_{borough_key}.json", "w") as f:
            json.dump(out, f, indent=2)
        print(f"  Saved api_outputs/cb_site_audit_{borough_key}.json")

    print("\nAll done.")
