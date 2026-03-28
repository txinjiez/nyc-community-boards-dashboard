"""
Extract actual demographic data from Brooklyn CB PDFs using OpenAI vision.
Pages with charts are converted to images and read by GPT-4o.
Text pages are extracted directly via pdfplumber.
NO hallucination: only report what is verifiably in the PDFs.
"""

import json
import os
import base64
import sys
from pathlib import Path
import pdfplumber
from pdf2image import convert_from_path
from openai import OpenAI

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

# Demographic summary page ranges (0-indexed) for each year
# These are the pages containing borough-wide aggregate statistics
PAGE_RANGES = {
    "2022": list(range(4, 12)),    # pages 5-12 (applications + demographics)
    "2023": list(range(5, 11)),    # pages 6-11
    "2024": list(range(7, 22)),    # pages 8-22 (appointments + demographics)
    "2025": list(range(7, 24)),    # pages 8-24
}

PDF_FILES = {
    "2022": "brooklyn_demographics/brooklyn_demographics_2022.pdf",
    "2023": "brooklyn_demographics/2023-Brooklyn-Demographics.pdf",
    "2024": "brooklyn_demographics/2024-Brooklyn-Demographics.pdf",
    "2025": "brooklyn_demographics/2025-Brooklyn-Demographics.pdf",
}

BASE_DIR = Path(__file__).parent


def pdf_page_to_base64(pdf_path: str, page_num: int, dpi: int = 200) -> str:
    """Convert a single PDF page to base64-encoded PNG."""
    images = convert_from_path(pdf_path, dpi=dpi, first_page=page_num + 1, last_page=page_num + 1)
    if not images:
        return None
    import io
    buf = io.BytesIO()
    images[0].save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode("utf-8")


def extract_text_from_page(pdf_path: str, page_num: int) -> str:
    """Extract text from a PDF page using pdfplumber."""
    with pdfplumber.open(pdf_path) as pdf:
        if page_num >= len(pdf.pages):
            return ""
        return pdf.pages[page_num].extract_text() or ""


def has_meaningful_text(text: str) -> bool:
    """Return True if the page has enough readable text to skip vision."""
    # Normalize doubled characters (2024/2025 PDF artifact)
    normalized = ""
    i = 0
    while i < len(text):
        if i + 1 < len(text) and text[i] == text[i + 1] and text[i].isalpha():
            normalized += text[i]
            i += 2
        else:
            normalized += text[i]
            i += 1
    # Only use text if it contains actual demographic keywords WITH data
    keywords = ['%', 'percent', 'female', 'male', 'black', 'hispanic', 'asian',
                'white', 'age', 'gender', 'race', 'first-time', 'appoint', 'members']
    has_keywords = sum(1 for k in keywords if k in normalized.lower()) >= 3
    return has_keywords and len(normalized.strip()) > 200


def extract_page_data_with_vision(pdf_path: str, page_num: int, year: str) -> str:
    """Use GPT-4o vision to extract all demographic data from a PDF page image."""
    print(f"  [vision] {year} page {page_num + 1}...")
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
                            f"This is page {page_num + 1} from the {year} Brooklyn Community Boards Demographic Report.\n\n"
                            "Extract ALL demographic statistics that appear on this page. Be extremely precise:\n"
                            "- Report exact percentages and counts as shown (e.g. '52% female', '182 first-time appointments')\n"
                            "- Read bar charts, pie charts, tables, and infographics carefully\n"
                            "- Include gender breakdown, race/ethnicity breakdown, age distribution, first-time vs returning\n"
                            "- Include borough comparison data if present\n"
                            "- If a number is not clearly visible, say 'unclear' — DO NOT estimate\n"
                            "- If a chart category label is visible but number is not, say 'label visible, value unclear'\n\n"
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


def extract_page_data_from_text(text: str, year: str, page_num: int) -> str:
    """Use extracted text directly when it's clean enough."""
    # Normalize doubled chars (2024/2025 artifact)
    normalized = ""
    i = 0
    while i < len(text):
        if i + 1 < len(text) and text[i] == text[i + 1] and text[i].isalpha():
            normalized += text[i]
            i += 2
        else:
            normalized += text[i]
            i += 1
    return f"[Text extraction from {year} page {page_num + 1}]\n{normalized}"


def collect_year_data(year: str) -> str:
    """Collect all demographic data from a year's PDF."""
    pdf_path = str(BASE_DIR / PDF_FILES[year])
    pages = PAGE_RANGES[year]
    print(f"\n=== Extracting {year} ({len(pages)} pages) ===")

    year_data_parts = []

    for page_num in pages:
        text = extract_text_from_page(pdf_path, page_num)

        if has_meaningful_text(text):
            # Use text extraction (normalize doubled chars first)
            normalized = ""
            i = 0
            while i < len(text):
                if i + 1 < len(text) and text[i] == text[i + 1] and text[i].isalpha():
                    normalized += text[i]
                    i += 2
                else:
                    normalized += text[i]
                    i += 1
            print(f"  [text]   {year} page {page_num + 1} ({len(normalized)} chars)")
            year_data_parts.append(f"--- Page {page_num + 1} (text) ---\n{normalized}")
        else:
            # Use vision for image-heavy pages (charts)
            extracted = extract_page_data_with_vision(pdf_path, page_num, year)
            if extracted.strip():
                year_data_parts.append(f"--- Page {page_num + 1} (vision) ---\n{extracted}")

    return "\n\n".join(year_data_parts)


def compile_final_json(year_extractions: dict) -> dict:
    """Use GPT-4o to compile all extracted data into the final structured JSON."""
    print("\n=== Compiling final analysis JSON ===")

    combined = "\n\n".join([
        f"{'='*60}\nYEAR {year}\n{'='*60}\n{data}"
        for year, data in sorted(year_extractions.items())
    ])

    system_prompt = """You are a precise data analyst. You will receive demographic data extracted from Brooklyn Community Board Demographic Reports for 2022, 2023, 2024, and 2025.

Your task: compile this into a structured JSON with ONLY numbers and facts that actually appear in the source data.

CRITICAL RULES:
- DO NOT invent, estimate, or interpolate any numbers
- If a value was not found in the source, use null
- Only use "unclear" if the source explicitly said so
- Percentages must be reported as strings like "52%" or as numbers like 52.0
- All data must be traceable back to the source text

Output ONLY valid JSON, no prose."""

    user_prompt = f"""Here is the extracted demographic data from all 4 years:

{combined}

Produce a JSON with this exact structure (use null for missing values):

{{
  "categories_detected": ["list of demographic categories found across all years"],
  "gender_balance": {{
    "year_values": {{
      "2022": {{"Female": "XX%", "Male": "XX%", "Other/Non-conforming": "XX%"}},
      "2023": {{"Female": "XX%", "Male": "XX%", "Other/Non-conforming": "XX%"}},
      "2024": {{"Female": "XX%", "Male": "XX%", "Other/Non-conforming": "XX%"}},
      "2025": {{"Female": "XX%", "Male": "XX%", "Other/Non-conforming": "XX%"}}
    }},
    "trend_summary": "one sentence describing the trend"
  }},
  "race_ethnicity_breakdown": {{
    "year_values": {{
      "2022": {{"Black": "XX%", "Hispanic": "XX%", "Asian": "XX%", "White": "XX%", "Other": "XX%"}},
      "2023": {{"Black": "XX%", "Hispanic": "XX%", "Asian": "XX%", "White": "XX%", "Other": "XX%"}},
      "2024": {{"Black": "XX%", "Hispanic": "XX%", "Asian": "XX%", "White": "XX%", "Other": "XX%"}},
      "2025": {{"Black": "XX%", "Hispanic": "XX%", "Asian": "XX%", "White": "XX%", "Other": "XX%"}}
    }},
    "trend_summary": "one sentence describing the trend"
  }},
  "age_distribution": {{
    "year_values": {{
      "2022": {{"18-29": "XX%", "30-44": "XX%", "45-64": "XX%", "65+": "XX%"}},
      "2023": {{"18-29": "XX%", "30-44": "XX%", "45-64": "XX%", "65+": "XX%"}},
      "2024": {{"18-29": "XX%", "30-44": "XX%", "45-64": "XX%", "65+": "XX%"}},
      "2025": {{"18-29": "XX%", "30-44": "XX%", "45-64": "XX%", "65+": "XX%"}}
    }},
    "trend_summary": "one sentence describing the trend"
  }},
  "first_time_vs_returning": {{
    "year_values": {{
      "2022": {{"First-Time": "XX%", "Returning": "XX%", "first_time_count": N, "returning_count": N, "total_appointed": N}},
      "2023": {{"First-Time": "XX%", "Returning": "XX%", "first_time_count": N, "returning_count": N, "total_appointed": N}},
      "2024": {{"First-Time": "XX%", "Returning": "XX%", "first_time_count": N, "returning_count": N, "total_appointed": N}},
      "2025": {{"First-Time": "XX%", "Returning": "XX%", "first_time_count": N, "returning_count": N, "total_appointed": N}}
    }},
    "trend_summary": "one sentence describing the trend"
  }},
  "year_to_year_trends": {{
    "2022_2023": {{
      "summary": "paragraph summarizing key changes",
      "changes": {{
        "Gender": {{"Female": "change description", "Male": "change description"}},
        "Race_Ethnicity": {{"Black": "change", "Hispanic": "change", "Asian": "change"}},
        "Age": "change description",
        "First_Time_Appointees": "change description"
      }}
    }},
    "2023_2024": {{
      "summary": "paragraph",
      "changes": {{}}
    }},
    "2024_2025": {{
      "summary": "paragraph",
      "changes": {{}}
    }}
  }},
  "overall_2022_2025_trends": {{
    "summary": "paragraph describing 4-year arc",
    "major_increases": [
      {{"category": "name", "change": "description", "significance": "why it matters"}}
    ],
    "major_decreases": [
      {{"category": "name", "change": "description", "significance": "why it matters"}}
    ]
  }},
  "community_board_changes": {{
    "boards_with_biggest_changes": [
      {{"board": "Brooklyn CB#", "change_type": "type", "details": "description"}}
    ],
    "notes": "any notes about methodology or data quality"
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

    raw = response.choices[0].message.content
    # Strip markdown code fences if present
    raw = raw.strip()
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

    year_extractions = {}

    for year in ["2022", "2023", "2024", "2025"]:
        cache_path = output_dir / f"raw_extraction_{year}.txt"

        if cache_path.exists():
            print(f"[cache] Loading {year} extraction from {cache_path}")
            year_extractions[year] = cache_path.read_text()
        else:
            data = collect_year_data(year)
            year_extractions[year] = data
            cache_path.write_text(data)
            print(f"Saved {year} extraction to {cache_path}")

    final = compile_final_json(year_extractions)

    out_path = output_dir / "brooklyn_demographics_analysis.json"
    out_path.write_text(json.dumps(final, indent=2))
    print(f"\nSaved final analysis to {out_path}")

    if "categories_detected" in final:
        print(f"Categories: {', '.join(final['categories_detected'])}")

    return final


if __name__ == "__main__":
    main()
