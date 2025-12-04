import requests

def get_raw_pdf_links():
    api_url = "https://api.github.com/repos/NYCPlanning/labs-cd-needs-statements/contents"

    r = requests.get(api_url)
    r.raise_for_status()
    items = r.json()

    pdf_links = []

    # This repo contains folders for each borough's DNS directory
    for folder in items:
        if folder["type"] == "dir":
            subfolder_url = folder["url"]
            subfolder_items = requests.get(subfolder_url).json()

            for f in subfolder_items:
                if f["name"].lower().endswith(".pdf"):
                    # f["download_url"] gives the raw PDF file
                    pdf_links.append(f["download_url"])

    return pdf_links

pdf_urls = get_raw_pdf_links()
print(pdf_urls)
print(len(pdf_urls))
