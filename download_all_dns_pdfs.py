"""
Download all needs statements PDFs for all NYC boroughs (FY2024, FY2025, FY2026)
from https://github.com/NYCPlanning/labs-cd-needs-statements/tree/master
into dns_pdfs/
"""
import os
import time
import requests

REPO = "NYCPlanning/labs-cd-needs-statements"
BRANCH = "master"
OUT_DIR = "dns_pdfs"
YEARS = ["FY2024", "FY2025", "FY2026"]
BOROUGHS = ["BK", "MN", "QN", "BX", "SI"]

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/vnd.github.v3+json",
}

os.makedirs(OUT_DIR, exist_ok=True)

def get_tree():
    url = f"https://api.github.com/repos/{REPO}/git/trees/{BRANCH}?recursive=1"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json().get("tree", [])

def download_pdf(raw_url, dest_path):
    r = requests.get(raw_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
    r.raise_for_status()
    with open(dest_path, "wb") as f:
        f.write(r.content)

if __name__ == "__main__":
    print("Fetching repo file tree...")
    tree = get_tree()

    pdf_files = [
        item for item in tree
        if item["type"] == "blob" and item["path"].lower().endswith(".pdf")
    ]
    print(f"Found {len(pdf_files)} PDFs in repo")

    # Filter: borough in BOROUGHS, year in YEARS
    to_download = []
    for item in pdf_files:
        name = os.path.basename(item["path"])
        year_match = any(y in name for y in YEARS)
        borough_match = any(f"_{b}" in name or name.startswith(b) for b in BOROUGHS)
        if year_match and borough_match:
            dest = os.path.join(OUT_DIR, name)
            raw_url = f"https://raw.githubusercontent.com/{REPO}/{BRANCH}/{item['path']}"
            to_download.append((name, raw_url, dest))

    print(f"Matching PDFs: {len(to_download)}")

    skipped = 0
    downloaded = 0
    errors = 0
    for name, raw_url, dest in sorted(to_download):
        if os.path.exists(dest):
            skipped += 1
            continue
        print(f"  Downloading {name}...")
        try:
            download_pdf(raw_url, dest)
            downloaded += 1
            time.sleep(0.3)
        except Exception as e:
            print(f"    ERROR: {e}")
            errors += 1

    print(f"\nDone. Downloaded: {downloaded}, Skipped (existing): {skipped}, Errors: {errors}")
    print(f"Total PDFs in {OUT_DIR}/: {len(os.listdir(OUT_DIR))}")
