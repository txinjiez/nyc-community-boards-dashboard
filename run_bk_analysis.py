"""
Run OpenAI GPT-4 needs statement analysis for Brooklyn CBs 1-18.
Reads PDFs from dns_pdfs/, outputs to api_outputs/openai/needs_comparison_BK_CB{N}.json
Compares ALL available fiscal years (FY2024, FY2025, FY2026, FY2027).
"""
import os
import json
import pdfplumber
from openai import OpenAI

client = OpenAI()
PDF_DIR = "dns_pdfs"
OUT_DIR = "api_outputs/openai"
os.makedirs(OUT_DIR, exist_ok=True)

ALL_YEARS = ["FY2024", "FY2025", "FY2026", "FY2027"]

# CBs to process
CB_RANGE = range(1, 19)

def read_pdf(path):
    text = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                text.append(t)
    return "\n\n".join(text)

def get_texts(cb_num):
    """Return {FY2024: text, ...} for all available years for a BK CB."""
    cb_str = f"{cb_num:02d}"
    texts = {}
    for year in ALL_YEARS:
        fname = f"{year}_Statement_BK{cb_str}.pdf"
        path = os.path.join(PDF_DIR, fname)
        if os.path.exists(path):
            texts[year] = read_pdf(path)
        else:
            print(f"  NOTE: {fname} not found, skipping year.")
    if len(texts) < 2:
        print(f"  Need at least 2 years for comparison, only found {list(texts.keys())}")
        return None
    return texts

def analyze(cb_num, texts):
    years = list(texts.keys())
    year_range = f"{years[0]} → {years[-1]}"
    num_years = len(years)

    doc_sections = "\n\n".join([
        f"===== {yr} DOCUMENT BEGINS =====\n{texts[yr][:12000]}\n===== {yr} DOCUMENT ENDS ====="
        for yr in years
    ])

    year_list_str = ", ".join(years)
    year_arrow_str = " → ".join(years)

    summary_table_schema = "\n".join([
        f'    "{yr}": "One-sentence summary of {yr} document\'s main focus"{"," if yr != years[-1] else ""}'
        for yr in years
    ])

    notable_quotes_schema = "\n".join([
        f'''    {{
      "year": "{yr}",
      "quote": "Exact quote from {yr} document",
      "significance": "Why this quote matters for year-over-year comparison"
    }}{"," if yr != years[-1] else ""}'''
        for yr in years
    ])

    prompt = f"""CRITICAL INSTRUCTIONS: You MUST compare {num_years} different years of documents. DO NOT extract or summarize a single document. You are doing COMPARATIVE ANALYSIS ONLY.

Brooklyn Community Board {cb_num} - YEAR-OVER-YEAR COMPARISON ({year_range})

YOU HAVE {num_years} DOCUMENTS BELOW: {year_list_str}

YOUR JOB: Compare how things CHANGED from {year_arrow_str}.

ANALYSIS TASKS:
1. Policy Changes: What priorities appeared, disappeared, or shifted emphasis across the years?
2. Agency Response Evolution: Did city agencies become more supportive, dismissive, or neutral? How did their tone/responses change?
3. Emphasis Shifts: Which themes grew stronger? Which faded? What's new in {years[-1]} that wasn't in {years[0]}?
4. Structural Changes: Did document format, length, or organization change?

CRITICAL: Cite specific text from EACH year when you compare. Say things like "In {years[0]} the document stated X, but by {years[-1]} it shifted to Y."

REQUIRED JSON OUTPUT STRUCTURE:
{{
  "community_board": "BK {cb_num}",
  "summary_table": {{
{summary_table_schema}
  }},
  "narrative_comparison": {{
    "policy_changes": "2-3 paragraphs comparing how policy priorities evolved. Cite specific examples from each year.",
    "agency_response_evolution": "2 paragraphs on how city agency responses changed. Quote or reference agency statements from different years.",
    "emphasis_shifts": "2 paragraphs on what themes grew, shrank, or disappeared. Be specific about which year emphasized what.",
    "structural_changes": "1 paragraph on document format/structure differences across years."
  }},
  "notable_quotes": [
{notable_quotes_schema}
  ],
  "final_interpretation": "2-3 sentence assessment of the overall {num_years}-year trajectory. What's the big story here?"
}}

{doc_sections}

NOW: Output ONLY the JSON comparison analysis. Do NOT summarize a single year. COMPARE ALL {num_years} YEARS.
"""
    resp = client.chat.completions.create(
        model="gpt-4.1",
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
    )
    return resp.choices[0].message.content

def needs_rerun(out_path, available_years):
    """Return True if the output file is missing or doesn't contain all available years."""
    if not os.path.exists(out_path):
        return True
    try:
        with open(out_path) as f:
            data = json.load(f)
        existing_years = set(data.get("summary_table", {}).keys())
        return not all(yr in existing_years for yr in available_years)
    except Exception:
        return True

if __name__ == "__main__":
    for cb_num in CB_RANGE:
        out_path = os.path.join(OUT_DIR, f"needs_comparison_BK_CB{cb_num}.json")

        texts = get_texts(cb_num)
        if texts is None:
            print(f"CB{cb_num}: insufficient PDFs, skipping.")
            continue

        available_years = list(texts.keys())
        if not needs_rerun(out_path, available_years):
            print(f"CB{cb_num}: already has all years {available_years}, skipping.")
            continue

        print(f"\nAnalyzing Brooklyn CB{cb_num} ({' → '.join(available_years)})...")
        try:
            result = analyze(cb_num, texts)
            parsed = json.loads(result)
            with open(out_path, "w") as f:
                json.dump(parsed, f, indent=2)
            print(f"  Saved {out_path}")
        except Exception as e:
            print(f"  ERROR: {e}")

    print("\nDone.")
