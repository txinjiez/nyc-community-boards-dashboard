"""
Patch all borough JSON files with corrections:
- Fix Manhattan CB10 URL and pages
- Fix contact URL patterns (contact/contact.page for some CBs)
- Apply SI-specific fixes
- Add social media manually found
"""
import json, requests, time
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

HEADERS = {"User-Agent": "Mozilla/5.0 Chrome/122.0.0.0", "Accept": "text/html,*/*"}
SOCIAL_DOMAINS = {
    "instagram.com": "Instagram", "facebook.com": "Facebook",
    "twitter.com": "X", "x.com": "X",
    "youtube.com": "Youtube", "youtu.be": "Youtube",
}

def fetch_text(url):
    r = requests.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
    r.raise_for_status()
    return r.text, r.url

def page_ok(url):
    try:
        html, final = fetch_text(url)
        if len(html) < 800: return False
        low = html.lower()
        if "page not found" in low[:3000]: return False
        return True
    except: return False

def get_nav(url):
    try:
        html, final = fetch_text(url)
        links = {}
        class LP(HTMLParser):
            def __init__(self): super().__init__(); self._in=False; self._href=None; self._buf=[]
            def handle_starttag(self,tag,attrs):
                if tag!='a': return
                self._href=dict(attrs).get('href',''); self._buf=[]; self._in=True
            def handle_data(self,d):
                if self._in: self._buf.append(d)
            def handle_endtag(self,tag):
                if tag!='a' or not self._in: return
                t=''.join(self._buf).strip()
                h=self._href or ''
                if h and not h.startswith('#') and 'javascript' not in h:
                    links[t.lower()] = urljoin(final, h)
                self._in=False; self._href=None; self._buf=[]
        LP().feed(html)
        return links, html
    except: return {}, ""

def extract_social(html):
    social = {v: None for v in set(SOCIAL_DOMAINS.values())}
    class SP(HTMLParser):
        def handle_starttag(self_, tag, attrs):
            if tag != "a": return
            href = dict(attrs).get("href", "") or ""
            try: domain = urlparse(href).netloc.lower().lstrip("www.")
            except: return
            for sd, key in SOCIAL_DOMAINS.items():
                if (domain == sd or domain.endswith("." + sd)) and social[key] is None:
                    social[key] = href
    SP().feed(html)
    return social

def social_obj(url, on_site=True, active=None):
    if not url: return None
    return {"url": url, "on_site": on_site, "active": active}

# ============================================================
# MANHATTAN PATCHES
# ============================================================
def patch_manhattan():
    with open("api_outputs/cb_site_audit_manhattan.json") as f:
        data = json.load(f)
    results_by_num = {r["cb_number"]: r for r in data["results"]}

    # CB10: redirects to cbmanhattan.cityofnewyork.us/cb10/
    cb10 = results_by_num["10"]
    cb10["url"] = "https://cbmanhattan.cityofnewyork.us/cb10/"
    nav, html = get_nav("https://cbmanhattan.cityofnewyork.us/cb10/")
    social = extract_social(html)
    cb10["features"].update({
        "Calendar":            "https://cbmanhattan.cityofnewyork.us/cb10/events/",
        "Minutes":             "https://cbmanhattan.cityofnewyork.us/cb10/meetings/",
        "Agendas":             "https://cbmanhattan.cityofnewyork.us/cb10/meetings/",
        "Resolutions":         "https://cbmanhattan.cityofnewyork.us/cb10/resolutions/",
        "Contact":             "https://cbmanhattan.cityofnewyork.us/cb10/contact/",
        "By-Laws":             "https://cbmanhattan.cityofnewyork.us/cb10/about/",
        "Events":              "https://cbmanhattan.cityofnewyork.us/cb10/events/",
    })
    # Add social from homepage
    for k, v in social.items():
        if v: cb10["features"][k] = social_obj(v, on_site=True)

    # CB1 - manhattancb1.cityofnewyork.us - probe subpages
    cb1 = results_by_num["1"]
    nav1, html1 = get_nav("https://manhattancb1.cityofnewyork.us/")
    # Check known patterns for CB1
    minutes_url = None
    for candidate in ["https://manhattancb1.cityofnewyork.us/meeting-documents/",
                      "https://manhattancb1.cityofnewyork.us/minutes/",
                      "https://manhattancb1.cityofnewyork.us/meetings/"]:
        if page_ok(candidate):
            minutes_url = candidate
            break
    bylaws_url = None
    for candidate in ["https://manhattancb1.cityofnewyork.us/by-laws/",
                      "https://manhattancb1.cityofnewyork.us/bylaws/"]:
        if page_ok(candidate):
            bylaws_url = candidate
            break
    news_url = None
    for candidate in ["https://manhattancb1.cityofnewyork.us/news/",
                      "https://manhattancb1.cityofnewyork.us/press-releases/"]:
        if page_ok(candidate):
            news_url = candidate
            break
    newsletters_url = None
    for candidate in ["https://manhattancb1.cityofnewyork.us/newsletters/",
                      "https://manhattancb1.cityofnewyork.us/e-blasts/"]:
        if page_ok(candidate):
            newsletters_url = candidate
            break
    if minutes_url: cb1["features"]["Minutes"] = minutes_url
    if bylaws_url: cb1["features"]["By-Laws"] = bylaws_url
    if news_url: cb1["features"]["News"] = news_url
    if newsletters_url: cb1["features"]["Newsletters"] = newsletters_url

    # CB3 - nyc.gov/site/manhattancb3 - probe more
    cb3 = results_by_num["3"]
    for item, candidates in [
        ("Minutes", ["https://www.nyc.gov/site/manhattancb3/minutes/board-meeting-minutes.page",
                     "https://www.nyc.gov/site/manhattancb3/about/minutes.page"]),
        ("Agendas", ["https://www.nyc.gov/site/manhattancb3/minutes/board-meeting-agendas.page",
                     "https://www.nyc.gov/site/manhattancb3/about/agendas.page"]),
        ("Resolutions", ["https://www.nyc.gov/site/manhattancb3/about/resolutions.page",
                         "https://www.nyc.gov/site/manhattancb3/minutes/board-meeting-resolutions.page"]),
        ("Newsletters", ["https://www.nyc.gov/site/manhattancb3/news/newsletters.page"]),
        ("News", ["https://www.nyc.gov/site/manhattancb3/news/board-news.page",
                  "https://www.nyc.gov/site/manhattancb3/news/news.page"]),
        ("Permits and Licenses", ["https://www.nyc.gov/site/manhattancb3/resources/liquor-licenses.page",
                                   "https://www.nyc.gov/site/manhattancb3/resources/permits.page"]),
    ]:
        if not cb3["features"].get(item):
            for url in candidates:
                if page_ok(url):
                    cb3["features"][item] = url
                    break
            time.sleep(0.2)

    # CB7 - nyc.gov/site/manhattancb7
    cb7 = results_by_num["7"]
    # Fix contact to contact/contact.page
    if not cb7["features"].get("Contact"):
        if page_ok("https://www.nyc.gov/site/manhattancb7/contact/contact.page"):
            cb7["features"]["Contact"] = "https://www.nyc.gov/site/manhattancb7/contact/contact.page"
    for item, candidates in [
        ("Minutes", ["https://www.nyc.gov/site/manhattancb7/minutes/board-meeting-minutes.page"]),
        ("Agendas", ["https://www.nyc.gov/site/manhattancb7/minutes/board-meeting-agendas.page"]),
        ("News", ["https://www.nyc.gov/site/manhattancb7/news/board-news.page"]),
        ("Newsletters", ["https://www.nyc.gov/site/manhattancb7/news/newsletters.page"]),
        ("Permits and Licenses", ["https://www.nyc.gov/site/manhattancb7/resources/liquor-licenses.page"]),
        ("Events", ["https://www.nyc.gov/site/manhattancb7/calendar/calendar.page"]),
    ]:
        if not cb7["features"].get(item):
            for url in candidates:
                if page_ok(url):
                    cb7["features"][item] = url
                    break
            time.sleep(0.2)

    # Social media known for Manhattan CBs (manually researched)
    # CB1
    cb1["features"]["Instagram"] = social_obj("https://www.instagram.com/manhattancb1/", on_site=False, active=None)
    cb1["features"]["Facebook"] = social_obj("https://www.facebook.com/ManhattanCommunityBoard1/", on_site=False, active=None)

    # CB2
    cb2 = results_by_num["2"]
    cb2["features"]["Instagram"] = social_obj("https://www.instagram.com/mancb2/", on_site=False, active=None)

    # CB3
    # Already has social from homepage (Instagram, X, Facebook found)

    # CB4
    cb4 = results_by_num["4"]
    cb4["features"]["Instagram"] = social_obj("https://www.instagram.com/manhattancb4/", on_site=False, active=None)
    cb4["features"]["Facebook"] = social_obj("https://www.facebook.com/manhattancb4/", on_site=False, active=None)

    # CB5
    cb5 = results_by_num["5"]
    cb5["features"]["Instagram"] = social_obj("https://www.instagram.com/mancb5/", on_site=False, active=None)
    cb5["features"]["Facebook"] = social_obj("https://www.facebook.com/ManhattanCommunityBoard5/", on_site=False, active=None)

    # CB7
    cb7["features"]["Instagram"] = social_obj("https://www.instagram.com/mcb7nyc/", on_site=False, active=None)
    cb7["features"]["Facebook"] = social_obj("https://www.facebook.com/MCB7NYC/", on_site=False, active=None)
    cb7["features"]["X"] = social_obj("https://x.com/mcb7nyc", on_site=False, active=None)

    # CB10
    cb10["features"]["Instagram"] = social_obj("https://www.instagram.com/mancb10/", on_site=False, active=None)
    cb10["features"]["Facebook"] = social_obj("https://www.facebook.com/ManhattanCommunityBoard10/", on_site=False, active=None)

    data["results"] = list(results_by_num.values())
    data["results"].sort(key=lambda r: int(r["cb_number"]))
    with open("api_outputs/cb_site_audit_manhattan.json", "w") as f:
        json.dump(data, f, indent=2)
    print("Manhattan patched.")

# ============================================================
# QUEENS PATCHES
# ============================================================
def patch_queens():
    with open("api_outputs/cb_site_audit_queens.json") as f:
        data = json.load(f)
    results_by_num = {r["cb_number"]: r for r in data["results"]}

    # Fix contact URL for CBs using contact/contact.page
    for cb_num in ["10"]:
        cb = results_by_num[cb_num]
        if not cb["features"].get("Contact"):
            url = f"https://www.nyc.gov/site/queenscb{cb_num}/contact/contact.page"
            if page_ok(url):
                cb["features"]["Contact"] = url

    # Fix by-laws for QB11
    cb11 = results_by_num["11"]
    if page_ok("https://www.nyc.gov/site/queenscb11/about/by-laws.page"):
        cb11["features"]["By-Laws"] = "https://www.nyc.gov/site/queenscb11/about/by-laws.page"

    # QB1 - fix minutes/agendas (not found by pattern probe)
    cb1 = results_by_num["1"]
    for item, candidates in [
        ("Minutes", ["https://www.nyc.gov/site/queenscb1/about/minutes.page",
                     "https://www.nyc.gov/site/queenscb1/meetings/minutes.page"]),
        ("Agendas", ["https://www.nyc.gov/site/queenscb1/about/agendas.page",
                     "https://www.nyc.gov/site/queenscb1/meetings/agendas.page"]),
        ("Resolutions", ["https://www.nyc.gov/site/queenscb1/about/resolutions.page"]),
        ("Newsletters", ["https://www.nyc.gov/site/queenscb1/news/newsletters.page"]),
        ("By-Laws", ["https://www.nyc.gov/site/queenscb1/about/by-laws.page"]),
        ("News", ["https://www.nyc.gov/site/queenscb1/news/board-news.page"]),
        ("Permits and Licenses", ["https://www.nyc.gov/site/queenscb1/resources/liquor-licenses.page",
                                   "https://www.nyc.gov/site/queenscb1/resources/permits.page"]),
    ]:
        if not cb1["features"].get(item):
            for url in candidates:
                if page_ok(url):
                    cb1["features"][item] = url
                    break
            time.sleep(0.15)

    # QB2
    cb2 = results_by_num["2"]
    for item, candidates in [
        ("Minutes", ["https://www.nyc.gov/site/queenscb2/about/minutes.page"]),
        ("Agendas", ["https://www.nyc.gov/site/queenscb2/about/agendas.page"]),
        ("Resolutions", ["https://www.nyc.gov/site/queenscb2/about/resolutions.page"]),
        ("Newsletters", ["https://www.nyc.gov/site/queenscb2/news/newsletters.page"]),
        ("News", ["https://www.nyc.gov/site/queenscb2/news/board-news.page"]),
        ("Permits and Licenses", ["https://www.nyc.gov/site/queenscb2/resources/liquor-licenses.page"]),
    ]:
        if not cb2["features"].get(item):
            for url in candidates:
                if page_ok(url):
                    cb2["features"][item] = url
                    break
            time.sleep(0.15)

    # QB7 - probe calendar and more
    cb7 = results_by_num["7"]
    for item, candidates in [
        ("Calendar", ["https://www.nyc.gov/site/queenscb7/calendar/calendar.page",
                      "https://www.nyc.gov/site/queenscb7/meetings/calendar.page"]),
        ("Resolutions", ["https://www.nyc.gov/site/queenscb7/about/resolutions.page"]),
        ("By-Laws", ["https://www.nyc.gov/site/queenscb7/about/by-laws.page"]),
        ("News", ["https://www.nyc.gov/site/queenscb7/news/board-news.page"]),
        ("Newsletters", ["https://www.nyc.gov/site/queenscb7/news/newsletters.page"]),
        ("Permits and Licenses", ["https://www.nyc.gov/site/queenscb7/resources/liquor-licenses.page"]),
    ]:
        if not cb7["features"].get(item):
            for url in candidates:
                if page_ok(url):
                    cb7["features"][item] = url
                    break
            time.sleep(0.15)

    # QB14
    cb14 = results_by_num["14"]
    for item, candidates in [
        ("Minutes", ["https://www.nyc.gov/site/queenscb14/about/minutes.page",
                     "https://www.nyc.gov/site/queenscb14/meetings/minutes.page"]),
        ("Agendas", ["https://www.nyc.gov/site/queenscb14/about/agendas.page"]),
        ("Resolutions", ["https://www.nyc.gov/site/queenscb14/about/resolutions.page"]),
        ("By-Laws", ["https://www.nyc.gov/site/queenscb14/about/by-laws.page"]),
        ("News", ["https://www.nyc.gov/site/queenscb14/news/board-news.page"]),
        ("Permits and Licenses", ["https://www.nyc.gov/site/queenscb14/resources/liquor-licenses.page"]),
    ]:
        if not cb14["features"].get(item):
            for url in candidates:
                if page_ok(url):
                    cb14["features"][item] = url
                    break
            time.sleep(0.15)

    # Social media known for Queens CBs
    cb4 = results_by_num["4"]
    cb4["features"]["Facebook"] = social_obj("https://www.facebook.com/queenscb4/", on_site=False, active=None)
    cb4["features"]["Instagram"] = social_obj("https://www.instagram.com/queenscb4nyc/", on_site=False, active=None)

    cb7 = results_by_num["7"]
    cb7["features"]["Facebook"] = social_obj("https://www.facebook.com/QueensCB7/", on_site=False, active=None)

    cb10 = results_by_num["10"]
    cb10["features"]["Facebook"] = social_obj("https://www.facebook.com/QueensCommunityBoard10/", on_site=False, active=None)

    cb11 = results_by_num["11"]
    cb11["features"]["Facebook"] = social_obj("https://www.facebook.com/queenscommunityboard11/", on_site=False, active=None)

    cb13 = results_by_num["13"]
    cb13["features"]["Facebook"] = social_obj("https://www.facebook.com/QueensCommunityBoard13/", on_site=False, active=None)

    data["results"] = list(results_by_num.values())
    data["results"].sort(key=lambda r: int(r["cb_number"]))
    with open("api_outputs/cb_site_audit_queens.json", "w") as f:
        json.dump(data, f, indent=2)
    print("Queens patched.")

# ============================================================
# BRONX PATCHES
# ============================================================
def patch_bronx():
    with open("api_outputs/cb_site_audit_bronx.json") as f:
        data = json.load(f)
    results_by_num = {r["cb_number"]: r for r in data["results"]}

    # BX1 - fix contact
    cb1 = results_by_num["1"]
    if not cb1["features"].get("Contact"):
        if page_ok("https://www.nyc.gov/site/bronxcb1/about/contact.page"):
            cb1["features"]["Contact"] = "https://www.nyc.gov/site/bronxcb1/about/contact.page"
    for item, candidates in [
        ("Minutes", ["https://www.nyc.gov/site/bronxcb1/minutes/board-meeting-minutes.page",
                     "https://www.nyc.gov/site/bronxcb1/about/minutes.page"]),
        ("Agendas", ["https://www.nyc.gov/site/bronxcb1/minutes/board-meeting-agendas.page"]),
        ("Resolutions", ["https://www.nyc.gov/site/bronxcb1/about/resolutions.page"]),
        ("Newsletters", ["https://www.nyc.gov/site/bronxcb1/news/newsletters.page"]),
        ("By-Laws", ["https://www.nyc.gov/site/bronxcb1/about/by-laws.page"]),
        ("News", ["https://www.nyc.gov/site/bronxcb1/news/board-news.page"]),
        ("Permits and Licenses", ["https://www.nyc.gov/site/bronxcb1/resources/liquor-licenses.page"]),
    ]:
        if not cb1["features"].get(item):
            for url in candidates:
                if page_ok(url):
                    cb1["features"][item] = url
                    break
            time.sleep(0.15)

    # BX3 - add agendas (confirmed exists)
    cb3 = results_by_num["3"]
    if not cb3["features"].get("Agendas"):
        cb3["features"]["Agendas"] = "https://www.nyc.gov/site/bronxcb3/minutes/board-meeting-agendas.page"

    # BX9
    cb9 = results_by_num["9"]
    if not cb9["features"].get("Contact"):
        if page_ok("https://www.nyc.gov/site/bronxcb9/about/contact.page"):
            cb9["features"]["Contact"] = "https://www.nyc.gov/site/bronxcb9/about/contact.page"
    for item, candidates in [
        ("Minutes", ["https://www.nyc.gov/site/bronxcb9/minutes/board-meeting-minutes.page"]),
        ("Agendas", ["https://www.nyc.gov/site/bronxcb9/minutes/board-meeting-agendas.page"]),
        ("Resolutions", ["https://www.nyc.gov/site/bronxcb9/about/resolutions.page"]),
        ("Newsletters", ["https://www.nyc.gov/site/bronxcb9/news/newsletters.page"]),
        ("By-Laws", ["https://www.nyc.gov/site/bronxcb9/about/by-laws.page"]),
        ("News", ["https://www.nyc.gov/site/bronxcb9/news/board-news.page"]),
        ("Permits and Licenses", ["https://www.nyc.gov/site/bronxcb9/resources/liquor-licenses.page"]),
    ]:
        if not cb9["features"].get(item):
            for url in candidates:
                if page_ok(url):
                    cb9["features"][item] = url
                    break
            time.sleep(0.15)

    # BX7
    cb7 = results_by_num["7"]
    for item, candidates in [
        ("Minutes", ["https://www.nyc.gov/site/bronxcb7/minutes/board-meeting-minutes.page",
                     "https://www.nyc.gov/site/bronxcb7/about/minutes.page"]),
        ("Agendas", ["https://www.nyc.gov/site/bronxcb7/minutes/board-meeting-agendas.page"]),
        ("Resolutions", ["https://www.nyc.gov/site/bronxcb7/about/resolutions.page"]),
        ("Newsletters", ["https://www.nyc.gov/site/bronxcb7/news/newsletters.page"]),
        ("By-Laws", ["https://www.nyc.gov/site/bronxcb7/about/by-laws.page"]),
        ("News", ["https://www.nyc.gov/site/bronxcb7/news/board-news.page"]),
        ("Permits and Licenses", ["https://www.nyc.gov/site/bronxcb7/resources/liquor-licenses.page"]),
    ]:
        if not cb7["features"].get(item):
            for url in candidates:
                if page_ok(url):
                    cb7["features"][item] = url
                    break
            time.sleep(0.15)

    # BX11
    cb11 = results_by_num["11"]
    for item, candidates in [
        ("Resolutions", ["https://www.nyc.gov/site/bronxcb11/about/resolutions.page"]),
        ("Newsletters", ["https://www.nyc.gov/site/bronxcb11/news/newsletters.page"]),
        ("By-Laws", ["https://www.nyc.gov/site/bronxcb11/about/by-laws.page"]),
        ("News", ["https://www.nyc.gov/site/bronxcb11/news/board-news.page"]),
        ("Events", ["https://www.nyc.gov/site/bronxcb11/calendar/calendar.page"]),
        ("Permits and Licenses", ["https://www.nyc.gov/site/bronxcb11/resources/liquor-licenses.page"]),
    ]:
        if not cb11["features"].get(item):
            for url in candidates:
                if page_ok(url):
                    cb11["features"][item] = url
                    break
            time.sleep(0.15)

    # BX12
    cb12 = results_by_num["12"]
    for item, candidates in [
        ("Minutes", ["https://www.nyc.gov/site/bronxcb12/minutes/board-meeting-minutes.page"]),
        ("Resolutions", ["https://www.nyc.gov/site/bronxcb12/about/resolutions.page"]),
        ("Newsletters", ["https://www.nyc.gov/site/bronxcb12/news/newsletters.page"]),
        ("News", ["https://www.nyc.gov/site/bronxcb12/news/board-news.page"]),
        ("Permits and Licenses", ["https://www.nyc.gov/site/bronxcb12/resources/liquor-licenses.page"]),
    ]:
        if not cb12["features"].get(item):
            for url in candidates:
                if page_ok(url):
                    cb12["features"][item] = url
                    break
            time.sleep(0.15)

    # BX6 (cbbronx.cityofnewyork.us/cb6/) - probe
    cb6 = results_by_num["6"]
    nav6, html6 = get_nav("https://cbbronx.cityofnewyork.us/cb6/")
    for t, u in nav6.items():
        if "minute" in t and not cb6["features"].get("Minutes"): cb6["features"]["Minutes"] = u
        if "agenda" in t and not cb6["features"].get("Agendas"): cb6["features"]["Agendas"] = u
        if "resolution" in t and not cb6["features"].get("Resolutions"): cb6["features"]["Resolutions"] = u
        if "newsletter" in t and not cb6["features"].get("Newsletters"): cb6["features"]["Newsletters"] = u
        if "by-law" in t or "bylaw" in t and not cb6["features"].get("By-Laws"): cb6["features"]["By-Laws"] = u
        if "permit" in t or "license" in t and not cb6["features"].get("Permits and Licenses"): cb6["features"]["Permits and Licenses"] = u

    # Social media known for Bronx CBs
    cb1["features"]["Facebook"] = social_obj("https://www.facebook.com/BronxCommunityBoard1/", on_site=False, active=None)

    cb3 = results_by_num["3"]
    cb3["features"]["Facebook"] = social_obj("https://www.facebook.com/BronxCommunityBoard3/", on_site=False, active=None)
    cb3["features"]["Instagram"] = social_obj("https://www.instagram.com/bronxcb3/", on_site=False, active=None)

    cb4 = results_by_num["4"]
    cb4["features"]["Facebook"] = social_obj("https://www.facebook.com/BronxCB4/", on_site=False, active=None)
    cb4["features"]["Instagram"] = social_obj("https://www.instagram.com/bronxcb4/", on_site=False, active=None)

    cb6 = results_by_num["6"]
    cb6["features"]["Facebook"] = social_obj("https://www.facebook.com/BronxCommunityBoard6/", on_site=False, active=None)
    cb6["features"]["Instagram"] = social_obj("https://www.instagram.com/bronxcb6/", on_site=False, active=None)

    cb9 = results_by_num["9"]
    cb9["features"]["Facebook"] = social_obj("https://www.facebook.com/BronxCommunityBoard9/", on_site=False, active=None)

    cb11 = results_by_num["11"]
    cb11["features"]["Facebook"] = social_obj("https://www.facebook.com/BronxCommunityBoard11/", on_site=False, active=None)
    cb11["features"]["Instagram"] = social_obj("https://www.instagram.com/bronxcb11/", on_site=False, active=None)

    data["results"] = list(results_by_num.values())
    data["results"].sort(key=lambda r: int(r["cb_number"]))
    with open("api_outputs/cb_site_audit_bronx.json", "w") as f:
        json.dump(data, f, indent=2)
    print("Bronx patched.")

# ============================================================
# STATEN ISLAND PATCHES
# ============================================================
def patch_staten_island():
    with open("api_outputs/cb_site_audit_staten_island.json") as f:
        data = json.load(f)
    results_by_num = {r["cb_number"]: r for r in data["results"]}

    # SI1 - contact/contact.page confirmed, meetings/minutes.page confirmed
    cb1 = results_by_num["1"]
    cb1["features"]["Contact"] = "https://www.nyc.gov/site/statenislandcb1/contact/contact.page"
    cb1["features"]["Minutes"] = "https://www.nyc.gov/site/statenislandcb1/meetings/minutes.page"
    for item, candidates in [
        ("Calendar", ["https://www.nyc.gov/site/statenislandcb1/meetings/calendar.page",
                      "https://www.nyc.gov/site/statenislandcb1/calendar/events.page"]),
        ("Agendas", ["https://www.nyc.gov/site/statenislandcb1/meetings/agendas.page",
                     "https://www.nyc.gov/site/statenislandcb1/meetings/board-meeting-agendas.page"]),
        ("Resolutions", ["https://www.nyc.gov/site/statenislandcb1/meetings/resolutions.page",
                         "https://www.nyc.gov/site/statenislandcb1/about/resolutions.page"]),
        ("Newsletters", ["https://www.nyc.gov/site/statenislandcb1/news/newsletters.page"]),
        ("Events", ["https://www.nyc.gov/site/statenislandcb1/news/events.page"]),
        ("Permits and Licenses", ["https://www.nyc.gov/site/statenislandcb1/resources/liquor-licenses.page",
                                   "https://www.nyc.gov/site/statenislandcb1/resources/permits.page"]),
    ]:
        if not cb1["features"].get(item):
            for url in candidates:
                if page_ok(url):
                    cb1["features"][item] = url
                    break
            time.sleep(0.15)

    # SI2
    cb2 = results_by_num["2"]
    cb2["features"]["Contact"] = "https://www.nyc.gov/site/statenislandcb2/contact/contact.page"
    for item, candidates in [
        ("Minutes", ["https://www.nyc.gov/site/statenislandcb2/meetings/minutes.page",
                     "https://www.nyc.gov/site/statenislandcb2/about/minutes.page"]),
        ("Agendas", ["https://www.nyc.gov/site/statenislandcb2/meetings/agendas.page"]),
        ("Resolutions", ["https://www.nyc.gov/site/statenislandcb2/about/resolutions.page"]),
        ("Newsletters", ["https://www.nyc.gov/site/statenislandcb2/news/newsletters.page"]),
        ("By-Laws", ["https://www.nyc.gov/site/statenislandcb2/about/by-laws.page"]),
        ("News", ["https://www.nyc.gov/site/statenislandcb2/news/board-news.page"]),
        ("Events", ["https://www.nyc.gov/site/statenislandcb2/news/events.page"]),
        ("Permits and Licenses", ["https://www.nyc.gov/site/statenislandcb2/resources/liquor-licenses.page"]),
    ]:
        if not cb2["features"].get(item):
            for url in candidates:
                if page_ok(url):
                    cb2["features"][item] = url
                    break
            time.sleep(0.15)

    # SI3
    cb3 = results_by_num["3"]
    cb3["features"]["Contact"] = "https://www.nyc.gov/site/statenislandcb3/contact/contact.page"
    for item, candidates in [
        ("Minutes", ["https://www.nyc.gov/site/statenislandcb3/meetings/minutes.page",
                     "https://www.nyc.gov/site/statenislandcb3/about/minutes.page"]),
        ("Agendas", ["https://www.nyc.gov/site/statenislandcb3/meetings/agendas.page"]),
        ("Resolutions", ["https://www.nyc.gov/site/statenislandcb3/about/resolutions.page"]),
        ("Newsletters", ["https://www.nyc.gov/site/statenislandcb3/news/newsletters.page"]),
        ("By-Laws", ["https://www.nyc.gov/site/statenislandcb3/about/by-laws.page"]),
        ("News", ["https://www.nyc.gov/site/statenislandcb3/news/board-news.page"]),
        ("Events", ["https://www.nyc.gov/site/statenislandcb3/news/events.page"]),
        ("Permits and Licenses", ["https://www.nyc.gov/site/statenislandcb3/resources/liquor-licenses.page",
                                   "https://www.nyc.gov/site/statenislandcb3/resources/permits.page"]),
    ]:
        if not cb3["features"].get(item):
            for url in candidates:
                if page_ok(url):
                    cb3["features"][item] = url
                    break
            time.sleep(0.15)

    # Social for Staten Island
    cb1["features"]["Instagram"] = social_obj("https://www.instagram.com/statenislandcb1/", on_site=False, active=None)
    cb1["features"]["Facebook"] = social_obj("https://www.facebook.com/StatenIslandCB1/", on_site=False, active=None)

    cb3 = results_by_num["3"]
    cb3["features"]["Instagram"] = social_obj("https://www.instagram.com/statenislandcb3/", on_site=False, active=None)

    data["results"] = list(results_by_num.values())
    data["results"].sort(key=lambda r: int(r["cb_number"]))
    with open("api_outputs/cb_site_audit_staten_island.json", "w") as f:
        json.dump(data, f, indent=2)
    print("Staten Island patched.")

if __name__ == "__main__":
    print("Patching Manhattan...")
    patch_manhattan()
    print("Patching Queens...")
    patch_queens()
    print("Patching Bronx...")
    patch_bronx()
    print("Patching Staten Island...")
    patch_staten_island()
    print("All done.")
