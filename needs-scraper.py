import requests
from bs4 import BeautifulSoup

BASE = "https://github.com/NYCPlanning/labs-cd-needs-statements/tree/master"

def get_raw_pdf_links():
    r = requests.get(BASE)
    soup = BeautifulSoup(r.text, "html.parser")

    pdf_links = []
    for a in soup.select("a.js-navigation-open"):
        href = a.get("href", "")
        if href.endswith(".pdf"):
            # Convert GitHub blob link to raw link
            raw = href.replace("/blob/", "/raw/")
            full = "https://github.com" + raw
            pdf_links.append(full)

    return pdf_links

pdf_urls = get_raw_pdf_links()
print(pdf_urls[:5])

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible)",
    "Accept": "application/pdf"
}

def download_pdf(url, save_dir="dns_pdfs"):
    os.makedirs(save_dir, exist_ok=True)
    filename = url.split("/")[-1]
    path = f"{save_dir}/{filename}"

    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()

    with open(path, "wb") as f:
        f.write(r.content)

    return path

