"""
Extract Manhattan community board demographic data from PDFs.
2022 and 2023 are in one combined PDF; 2024 and 2025 are separate.
Uses pdfplumber for text pages and GPT-4o vision for chart pages.
NO hallucination: only report what is verifiably in the PDFs.
"""

import json
import os
import base64
import sys
import io
from pathlib import Path
import pdfplumber
from pdf2image import convert_from_path
from openai import OpenAI

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

BASE_DIR = Path(__file__).parent

# Manhattan PDFs
PDF_FILES = {
    "2022_2023": "manhattan_demographics/MBPO_CB_Demographics_Report_2022_and_2023.pdf",
    "2024": "manhattan_demographics/Manhattan-CB-Demographics-Report-2024.pdf",
    "2025": "manhattan_demographics/Manhattan-CB-Demographics-Report-2025-Final.pdf",
}

# Page ranges to extract (0-indexed).
# 2022+2023 PDF: pages 1-17 cover intro, applications, and charts for both years
# 2024 PDF: pages 1-11 cover intro, applications, and charts
# 2025 PDF: pages 1-11 cover intro, applications, text stats, and charts
PAGE_RANGES = {
    "2022_2023": list(range(0, 18)),
    "2024": list(range(0, 12)),
    "2025": list(range(0, 12)),
}


def pdf_page_to_base64(pdf_path: str, page_num: int, dpi: int = 200) -> str:
    images = convert_from_path(pdf_path, dpi=dpi, first_page=page_num + 1, last_page=page_num + 1)
    if not images:
        return None
    buf = io.BytesIO()
    images[0].save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode("utf-8")


def extract_text_from_page(pdf_path: str, page_num: int) -> str:
    with pdfplumber.open(pdf_path) as pdf:
        if page_num >= len(pdf.pages):
            return ""
        return pdf.pages[page_num].extract_text() or ""


def has_meaningful_text(text: str) -> bool:
    keywords = ['%', 'female', 'male', 'black', 'hispanic', 'asian', 'white',
                'age', 'gender', 'race', 'first-time', 'appoint', 'latino',
                'new member', 'returning', 'application', 'interview']
    hits = sum(1 for k in keywords if k in text.lower())
    return hits >= 3 and len(text.strip()) > 150


def extract_page_data_with_vision(pdf_path: str, page_num: int, label: str) -> str:
    print(f"  [vision] {label} page {page_num + 1}...")
    img_b64 = pdf_page_to_base64(pdf_path, page_num)
    if not img_b64:
        return ""

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": (
                            f"This is page {page_num + 1} from the Manhattan Community Boards Demographic Report ({label}).\n\n"
                            "Extract ALL demographic statistics visible on this page. Be extremely precise:\n"
                            "- Report exact percentages and counts as shown\n"
                            "- Read bar charts, pie charts, tables, and infographics carefully\n"
                            "- Include gender breakdown, race/ethnicity breakdown, age distribution, first-time vs returning\n"
                            "- If a number is not clearly visible, say 'unclear' — DO NOT estimate\n"
                            "- If only a chart title is visible but no numbers, say so\n\n"
                            "Format your response as structured text, one stat per line."
                        )
                    },
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/png;base64,{img_b64}", "detail": "high"}
                    }
                ]
            }
        ],
        max_tokens=1500,
        temperature=0.0,
    )
    return response.choices[0].message.content


def collect_pdf_data(pdf_key: str) -> str:
    pdf_path = str(BASE_DIR / PDF_FILES[pdf_key])
    pages = PAGE_RANGES[pdf_key]
    print(f"\n=== Extracting {pdf_key} ({len(pages)} pages) ===")

    parts = []
    for page_num in pages:
        text = extract_text_from_page(pdf_path, page_num)
        if has_meaningful_text(text):
            print(f"  [text]   {pdf_key} page {page_num + 1} ({len(text)} chars)")
            parts.append(f"--- Page {page_num + 1} (text) ---\n{text}")
        else:
            extracted = extract_page_data_with_vision(pdf_path, page_num, pdf_key)
            if extracted.strip():
                parts.append(f"--- Page {page_num + 1} (vision) ---\n{extracted}")

    return "\n\n".join(parts)


def compile_final_json(extractions: dict) -> dict:
    print("\n=== Compiling final Manhattan JSON ===")

    combined = "\n\n".join([
        f"{'='*60}\nFILE: {key}\n{'='*60}\n{data}"
        for key, data in extractions.items()
    ])

    system_prompt = """You are a precise data analyst. You will receive demographic data extracted from Manhattan Community Board Demographic Reports.

The source files are:
- FILE 2022_2023: Combined report covering both 2022 and 2023 appointment cycles
- FILE 2024: Report covering the 2024 appointment cycle
- FILE 2025: Report covering the 2025 appointment cycle

Your task: compile this into structured JSON with ONLY numbers and facts that actually appear in the source data.

CRITICAL RULES:
- DO NOT invent, estimate, or interpolate any numbers
- If a value was not found in the source, use null
- Only use "unclear" if the source explicitly said so
- All data must be traceable back to the source text or vision extraction
- Note: Manhattan has 12 community boards (not 18 like Brooklyn)

Output ONLY valid JSON, no prose."""

    user_prompt = f"""Here is the extracted demographic data from the Manhattan reports:

{combined}

Produce a JSON with this exact structure (use null for missing values):

{{
  "borough": "Manhattan",
  "num_community_boards": 12,
  "categories_detected": ["list of demographic categories found"],
  "gender_balance": {{
    "year_values": {{
      "2022": {{"Female": "XX%", "Male": "XX%", "Other/Non-conforming": "XX%", "note": "..."}},
      "2023": {{"Female": "XX%", "Male": "XX%", "Other/Non-conforming": "XX%", "note": "..."}},
      "2024": {{"Female": "XX%", "Male": "XX%", "Other/Non-conforming": "XX%", "note": "..."}},
      "2025": {{"Female": "XX%", "Male": "XX%", "Other/Non-conforming": "XX%", "note": "..."}}
    }},
    "trend_summary": "one sentence describing the trend"
  }},
  "race_ethnicity_breakdown": {{
    "year_values": {{
      "2022": {{"note": "describe what was available"}},
      "2023": {{"note": "describe what was available"}},
      "2024": {{"note": "describe what was available"}},
      "2025": {{"Latino_Hispanic": "XX%", "non_white": "XX%", "note": "..."}}
    }},
    "trend_summary": "one sentence"
  }},
  "age_distribution": {{
    "year_values": {{
      "2022": {{"note": "describe what was available"}},
      "2023": {{"note": "describe what was available"}},
      "2024": {{"note": "describe what was available"}},
      "2025": {{"note": "describe what was available"}}
    }},
    "trend_summary": "one sentence"
  }},
  "first_time_vs_returning": {{
    "year_values": {{
      "2022": {{"First-Time": "XX%", "Returning": "XX%", "first_time_count": N, "returning_count": N, "total_appointed": N}},
      "2023": {{"First-Time": "XX%", "Returning": "XX%", "first_time_count": N, "returning_count": N, "total_appointed": N}},
      "2024": {{"First-Time": "XX%", "Returning": "XX%", "first_time_count": N, "returning_count": N, "total_appointed": N}},
      "2025": {{"First-Time": "XX%", "Returning": "XX%", "first_time_count": N, "returning_count": N, "total_appointed": N}}
    }},
    "trend_summary": "one sentence"
  }},
  "applications": {{
    "year_values": {{
      "2022": {{"total_applications": N}},
      "2023": {{"total_applications": N}},
      "2024": {{"total_applications": N}},
      "2025": {{"total_applications": N}}
    }}
  }},
  "year_to_year_trends": {{
    "2022_2023": {{"summary": "...", "changes": {{}}}},
    "2023_2024": {{"summary": "...", "changes": {{}}}},
    "2024_2025": {{"summary": "...", "changes": {{}}}}
  }},
  "overall_2022_2025_trends": {{
    "summary": "paragraph describing 4-year arc",
    "major_increases": ["string descriptions"],
    "major_decreases": ["string descriptions"]
  }},
  "data_quality_notes": "notes on missing data or caveats"
}}"""

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        max_tokens=4000,
        temperature=0.0,
    )

    raw = response.choices[0].message.content.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1]
        raw = raw.rsplit("```", 1)[0]

    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"JSON parse error: {e}")
        print(f"Raw response (first 500): {raw[:500]}")
        sys.exit(1)


def main():
    output_dir = BASE_DIR / "api_outputs" / "demographics"
    output_dir.mkdir(exist_ok=True, parents=True)

    extractions = {}
    for key in ["2022_2023", "2024", "2025"]:
        cache_path = output_dir / f"raw_extraction_manhattan_{key}.txt"
        if cache_path.exists():
            print(f"[cache] Loading {key} from {cache_path}")
            extractions[key] = cache_path.read_text()
        else:
            data = collect_pdf_data(key)
            extractions[key] = data
            cache_path.write_text(data)
            print(f"Saved {key} extraction to {cache_path}")

    final = compile_final_json(extractions)

    out_path = output_dir / "manhattan_demographics_analysis.json"
    out_path.write_text(json.dumps(final, indent=2))
    print(f"\nSaved final analysis to {out_path}")

    if "categories_detected" in final:
        print(f"Categories: {', '.join(final['categories_detected'])}")

    return final


if __name__ == "__main__":
    main()
