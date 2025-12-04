import sqlite3
import requests
import json
import os

OLLAMA_API = "http://localhost:11434/api/generate"
MODEL = "deepseek-r1"
OUTPUT_DIR = "deepseek"

def get_statements_by_cb():
    """Get all statements grouped by community board number"""
    conn = sqlite3.connect("dns_data.db")
    cur = conn.cursor()

    # Get distinct CB numbers
    cb_numbers = cur.execute(
        "SELECT DISTINCT cb_number FROM dns_pdfs WHERE borough = 'BK' ORDER BY cb_number"
    ).fetchall()

    results = {}
    for (cb_num,) in cb_numbers:
        # Get all years for this CB
        rows = cur.execute("""
            SELECT pdf_url, text
            FROM dns_pdfs
            WHERE borough = 'BK' AND cb_number = ?
            ORDER BY pdf_url
        """, (cb_num,)).fetchall()

        # Parse fiscal years from URLs
        statements = {}
        for url, text in rows:
            if 'FY2024' in url or '2024' in url:
                statements['FY2024'] = text
            elif 'FY2025' in url or '2025' in url:
                statements['FY2025'] = text
            elif 'FY2026' in url or '2026' in url:
                statements['FY2026'] = text

        # Only include CBs with all 3 years
        if len(statements) == 3:
            results[cb_num] = statements

    conn.close()
    return results


def analyze_multi_year(cb_number, fy2024, fy2025, fy2026):
    """Analyze changes across three fiscal years for a community board"""
    prompt = f"""CRITICAL INSTRUCTIONS: You MUST compare THREE different years of documents. DO NOT extract or summarize a single document. You are doing COMPARATIVE ANALYSIS ONLY.

Brooklyn Community Board {cb_number} - YEAR-OVER-YEAR COMPARISON

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
  "community_board": "{cb_number}",
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

    payload = {
        "model": MODEL,
        "prompt": prompt,
        "stream": False
    }

    response = requests.post(OLLAMA_API, json=payload)
    response.raise_for_status()

    result = response.json()
    return result["response"]


def run():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    cb_data = get_statements_by_cb()

    print(f"Found {len(cb_data)} Brooklyn Community Boards with all 3 years\n")
    print(f"Using model: {MODEL}\n")

    # Process only first 5 CBs
    for i, (cb_num, statements) in enumerate(cb_data.items()):
        if i >= 5:
            break

        print(f"\nAnalyzing Brooklyn CB{cb_num} (FY2024-2026)...")

        output = analyze_multi_year(
            cb_num,
            statements['FY2024'],
            statements['FY2025'],
            statements['FY2026']
        )

        filename = f"{OUTPUT_DIR}/needs_comparison_BK_CB{cb_num}.json"
        with open(filename, "w") as f:
            f.write(output)

        print(f"  ✓ Saved {filename}")


if __name__ == "__main__":
    run()
