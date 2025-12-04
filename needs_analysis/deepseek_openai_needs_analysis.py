import argparse
import os
import sqlite3
import requests


def get_statements_by_cb(borough: str = "BK"):
    """Fetch statements grouped by community board number for the given borough."""
    conn = sqlite3.connect("dns_data.db")
    cur = conn.cursor()

    cb_numbers = cur.execute(
        "SELECT DISTINCT cb_number FROM dns_pdfs WHERE borough = ? ORDER BY cb_number",
        (borough,),
    ).fetchall()

    results = {}
    for (cb_num,) in cb_numbers:
        rows = cur.execute(
            """
            SELECT pdf_url, text
            FROM dns_pdfs
            WHERE borough = ? AND cb_number = ?
            ORDER BY pdf_url
            """,
            (borough, cb_num),
        ).fetchall()

        statements = {}
        for url, text in rows:
            if "FY2024" in url or "2024" in url:
                statements["FY2024"] = text
            elif "FY2025" in url or "2025" in url:
                statements["FY2025"] = text
            elif "FY2026" in url or "2026" in url:
                statements["FY2026"] = text

        if len(statements) == 3:
            results[cb_num] = statements

    conn.close()
    return results


def build_prompt(cb_number, fy2024, fy2025, fy2026, borough):
    """Construct the comparison prompt shared by both providers."""
    return f"""CRITICAL INSTRUCTIONS: You MUST compare THREE different years of documents. DO NOT extract or summarize a single document. You are doing COMPARATIVE ANALYSIS ONLY.

{borough} Community Board {cb_number} - YEAR-OVER-YEAR COMPARISON

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
  "community_board": "{borough} {cb_number}",
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


def call_llm(prompt: str, provider: str):
    """Send the prompt to OpenAI or DeepSeek chat completion API."""
    provider = provider.lower()

    if provider == "openai":
        api_key_env = "OPENAI_API_KEY"
        api_key = os.getenv(api_key_env)
        base_url = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1")
        model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    elif provider == "deepseek":
        api_key_env = "DEEPSEEK_API_KEY"
        api_key = os.getenv(api_key_env)
        base_url = os.getenv("DEEPSEEK_API_BASE", "https://api.deepseek.com/v1")
        model = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")
    else:
        raise ValueError("Provider must be 'openai' or 'deepseek'.")

    if not api_key:
        raise RuntimeError(f"Missing API key. Set {api_key_env} in your environment.")

    endpoint = f"{base_url.rstrip('/')}/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
    }

    # Some hosted models (e.g., gpt-5-mini) only support default temperature.
    if not model.startswith("gpt-5"):
        payload["temperature"] = 0.2

    response = requests.post(endpoint, headers=headers, json=payload, timeout=90)
    response.raise_for_status()
    data = response.json()

    try:
        return data["choices"][0]["message"]["content"]
    except (KeyError, IndexError) as exc:
        raise RuntimeError(f"Unexpected response from {provider}: {data}") from exc


def run(provider: str, max_cbs: int, borough: str):
    output_dir = os.path.join("api_outputs", provider.lower())
    os.makedirs(output_dir, exist_ok=True)

    cb_data = get_statements_by_cb(borough=borough)

    print(f"Found {len(cb_data)} {borough} Community Boards with all 3 years\n")
    print(f"Using provider: {provider}\n")

    for i, (cb_num, statements) in enumerate(cb_data.items()):
        if i >= max_cbs:
            break

        print(f"\nAnalyzing {borough} CB{cb_num} (FY2024-2026)...")

        prompt = build_prompt(
            cb_num,
            statements["FY2024"],
            statements["FY2025"],
            statements["FY2026"],
            borough,
        )

        output = call_llm(prompt, provider)

        filename = os.path.join(output_dir, f"needs_comparison_{borough}_CB{cb_num}.json")
        with open(filename, "w") as f:
            f.write(output)

        print(f"  ✓ Saved {filename}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compare multi-year needs statements using OpenAI or DeepSeek APIs."
    )
    parser.add_argument(
        "--provider",
        choices=["openai", "deepseek"],
        default="openai",
        help="LLM provider to call",
    )
    parser.add_argument(
        "--max-cbs",
        type=int,
        default=5,
        help="Limit the number of community boards to process",
    )
    parser.add_argument(
        "--borough",
        default="BK",
        help="Borough code to analyze (e.g., BK, BX, MN, QN, SI)",
    )
    args = parser.parse_args()

    run(args.provider, args.max_cbs, args.borough)
