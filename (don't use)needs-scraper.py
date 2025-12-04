import os
import re
import sqlite3
import requests
import pdfplumber
from bs4 import BeautifulSoup

# -----------------------------
# CONFIG
# -----------------------------
BASE = "https://github.com/NYCPlanning/labs-cd-needs-statements/tree/master"
RAW_PREFIX = "https://github.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/pdf"
}

# -----------------------------
# DATABASE SETUP
# -----------------------------
def init_db():
    conn = sqlite3.connect("dns_data.db")
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dns_pdfs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            borough TEXT,
            cb_number INTEGER,
            pdf_url TEXT,
            text TEXT
        )
    """)
    conn.commit()
    return conn


# -----------------------------
# GET ALL PDF LINKS
# -----------------------------
def get_raw_pdf_links():
    """Use GitHub API to recursively get all PDF files"""
    api_url = "https://api.github.com/repos/NYCPlanning/labs-cd-needs-statements/git/trees/master?recursive=1"
    headers = {"Accept": "application/vnd.github.v3+json"}

    r = requests.get(api_url, headers=headers)
    r.raise_for_status()
    data = r.json()

    pdf_links = []

    if "tree" in data:
        for item in data["tree"]:
            if item["path"].endswith(".pdf"):
                # Convert to raw download URL
                raw_url = f"https://raw.githubusercontent.com/NYCPlanning/labs-cd-needs-statements/master/{item['path']}"
                pdf_links.append(raw_url)

    return pdf_links


# -----------------------------
# DOWNLOAD PDF
# -----------------------------
def download_pdf(url):
    os.makedirs("dns_pdfs", exist_ok=True)
    filename = url.split("/")[-1]
    path = f"dns_pdfs/{filename}"

    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()

    with open(path, "wb") as f:
        f.write(r.content)

    return path


# -----------------------------
# EXTRACT PDF TEXT
# -----------------------------
def extract_text(path):
    text = ""
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            try:
                text += page.extract_text() + "\n"
            except:
                pass
    return text


# -----------------------------
# INSERT INTO DATABASE
# -----------------------------
def save_to_db(conn, borough, cb_number, pdf_url, text):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dns_pdfs (borough, cb_number, pdf_url, text)
        VALUES (?, ?, ?, ?)
    """, (borough, cb_number, pdf_url, text))
    conn.commit()


# -----------------------------
# FILTER BROOKLYN 2024-2026
# -----------------------------
def filter_brooklyn_2024_2026(pdf_urls):
    """Filter for Brooklyn needs statements from 2024, 2025, and 2026"""
    filtered = []
    for url in pdf_urls:
        # Check if it's Brooklyn (BK) and from fiscal years 2024, 2025, or 2026
        if 'BK' in url and any(year in url for year in ['FY2024', 'FY2025', 'FY2026', '2024', '2025', '2026']):
            filtered.append(url)
    return filtered


# -----------------------------
# MAIN SCRAPER LOGIC
# -----------------------------
def run():
    conn = init_db()
    all_pdf_urls = get_raw_pdf_links()

    # Filter for Brooklyn 2024-2026 only
    pdf_urls = filter_brooklyn_2024_2026(all_pdf_urls)

    print(f"Found {len(all_pdf_urls)} total PDFs")
    print(f"Filtered to {len(pdf_urls)} Brooklyn 2024-2026 PDFs")
    print("\nBrooklyn PDFs to download:")
    for url in pdf_urls:
        print(f"  - {url.split('/')[-1]}")
    print()

    for i, url in enumerate(pdf_urls, 1):
        print(f"[{i}/{len(pdf_urls)}] Downloading: {url.split('/')[-1]}")

        # extract borough + district ID from filename
        match = re.findall(r'BK(\d{2})', url)
        if match:
            borough = "BK"
            cb_num = int(match[0])
        else:
            borough, cb_num = ("UNKNOWN", None)

        path = download_pdf(url)
        text = extract_text(path)
        save_to_db(conn, borough, cb_num, url, text)

        print(f"  ✓ Saved: Brooklyn CB{cb_num}\n")

if __name__ == "__main__":
    run()

