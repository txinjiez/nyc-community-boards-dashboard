"""
Run OpenAI GPT-4 needs statement analysis for Brooklyn CBs 6-18.
Reads PDFs from dns_pdfs/, outputs to api_outputs/openai/needs_comparison_BK_CB{N}.json
"""
import os
import json
import pdfplumber
from openai import OpenAI

client = OpenAI()
PDF_DIR = "dns_pdfs"
OUT_DIR = "api_outputs/openai"
os.makedirs(OUT_DIR, exist_ok=True)

# CBs to process (skip already-done 1-5)
CB_RANGE = range(6, 19)

def read_pdf(path):
    text = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                text.append(t)
    return "\n\n".join(text)

def get_texts(cb_num):
    """Return {FY2024: text, FY2025: text, FY2026: text} for a BK CB."""
    cb_str = f"{cb_num:02d}"
    texts = {}
    for year in ["FY2024", "FY2025", "FY2026"]:
        fname = f"{year}_Statement_BK{cb_str}.pdf"
        path = os.path.join(PDF_DIR, fname)
        if not os.path.exists(path):
            print(f"  MISSING: {fname}")
            return None
        texts[year] = read_pdf(path)
    return texts

def analyze(cb_num, fy2024, fy2025, fy2026):
    prompt = f"""CRITICAL INSTRUCTIONS: You MUST compare THREE different years of documents. DO NOT extract or summarize a single document. You are doing COMPARATIVE ANALYSIS ONLY.

Brooklyn Community Board {cb_num} - YEAR-OVER-YEAR COMPARISON

YOU HAVE THREE DOCUMENTS BELOW:
- Document 1: FY2024 needs statement
- Document 2: FY2025 needs statement
- Document 3: FY2026 needs statement

YOUR JOB: Compare how things CHANGED from 2024 → 2025 → 2026.

ANALYSIS TASKS:
1. Policy Changes: What priorities appeared, disappeared, or shifted emphasis across the 3 years?
2. Agency Response Evolution: Did city agencies become more supportive, dismissive, or neutral? How did their tone/responses change?
3. Emphasis Shifts: Which themes grew stronger? Which faded? What's new in 2026 that wasn't in 2024?
4. Structural Changes: Did document format, length, or organization change?

CRITICAL: Cite specific text from EACH year when you compare. Say things like "In FY2024 the document stated X, but by FY2026 it shifted to Y."

REQUIRED JSON OUTPUT STRUCTURE:
{{
  "community_board": "BK {cb_num}",
  "summary_table": {{
    "FY2024": "One-sentence summary of FY2024 document's main focus",
    "FY2025": "One-sentence summary of FY2025 document's main focus",
    "FY2026": "One-sentence summary of FY2026 document's main focus"
  }},
  "narrative_comparison": {{
    "policy_changes": "2-3 paragraphs comparing how policy priorities evolved from 2024 to 2026. Cite specific examples from each year.",
    "agency_response_evolution": "2 paragraphs on how city agency responses changed. Quote or reference agency statements from different years.",
    "emphasis_shifts": "2 paragraphs on what themes grew, shrank, or disappeared. Be specific about which year emphasized what.",
    "structural_changes": "1 paragraph on document format/structure differences across years."
  }},
  "notable_quotes": [
    {{
      "year": "FY2024",
      "quote": "Exact quote from FY2024 document",
      "significance": "Why this quote matters for year-over-year comparison"
    }},
    {{
      "year": "FY2025",
      "quote": "Exact quote from FY2025 document",
      "significance": "How this differs from or continues FY2024 themes"
    }},
    {{
      "year": "FY2026",
      "quote": "Exact quote from FY2026 document",
      "significance": "How this represents evolution from prior years"
    }}
  ],
  "final_interpretation": "2-3 sentence assessment of the overall 3-year trajectory. What's the big story here?"
}}

===== FY2024 DOCUMENT BEGINS =====
{fy2024[:15000]}
===== FY2024 DOCUMENT ENDS =====

===== FY2025 DOCUMENT BEGINS =====
{fy2025[:15000]}
===== FY2025 DOCUMENT ENDS =====

===== FY2026 DOCUMENT BEGINS =====
{fy2026[:15000]}
===== FY2026 DOCUMENT ENDS =====

NOW: Output ONLY the JSON comparison analysis. Do NOT summarize a single year. COMPARE ALL THREE YEARS.
"""
    resp = client.chat.completions.create(
        model="gpt-4.1",
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
    )
    return resp.choices[0].message.content

if __name__ == "__main__":
    for cb_num in CB_RANGE:
        out_path = os.path.join(OUT_DIR, f"needs_comparison_BK_CB{cb_num}.json")
        if os.path.exists(out_path):
            print(f"CB{cb_num}: already exists, skipping.")
            continue

        print(f"\nAnalyzing Brooklyn CB{cb_num}...")
        texts = get_texts(cb_num)
        if texts is None:
            print(f"  Skipping CB{cb_num} — missing PDFs.")
            continue

        try:
            result = analyze(cb_num, texts["FY2024"], texts["FY2025"], texts["FY2026"])
            # Validate JSON
            parsed = json.loads(result)
            with open(out_path, "w") as f:
                json.dump(parsed, f, indent=2)
            print(f"  Saved {out_path}")
        except Exception as e:
            print(f"  ERROR: {e}")

    print("\nDone.")
