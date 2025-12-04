import json
import os
import requests
import base64
from pathlib import Path
import PyPDF2
from pdf2image import convert_from_path
import io

# PDF URLs
PDF_URLS = {
    "2022": "https://www.brooklynbp.nyc.gov/wp-content/uploads/2025/02/2022-Brooklyn-Community-Board-Final-2022.pdf",
    "2023": "https://www.brooklynbp.nyc.gov/wp-content/uploads/2025/02/2023-Brooklyn-Demographic-Report-1.pdf",
    "2024": "https://www.brooklynbp.nyc.gov/wp-content/uploads/2025/02/2024-Demographic-Report.Option-2.pdf",
    "2025": "https://www.brooklynbp.nyc.gov/wp-content/uploads/2025/07/2025-Brooklyn-Community-Boards-Demographic-Report.pdf"
}

SYSTEM_PROMPT = """You are an expert data analyst specializing in demographic trend analysis and multi-year comparisons. Your job is to extract and compare demographic metrics from Brooklyn Community Board Demographic Reports across four years (2022, 2023, 2024, 2025). You must produce clean, valid JSON only. No commentary, no markdown.

INPUT
You will receive:
* Extracted text, tables, or summaries from each year's demographic report.
* Each year will be labeled explicitly, e.g. YEAR_2022, YEAR_2023, etc.

TASK
Analyze change-over-time trends in demographic representation of community-board appointees. Your analysis must:

1. Identify all demographic categories that appear in the documents, such as:
    * Gender
    * Race/Ethnicity
    * Age ranges
    * First-time vs returning appointees
    * Borough-wide comparisons
    * Any category consistently reported across years

2. Compute or describe direction-of-change trends for each category:
    * increase / decrease / stable
    * percent-point change where possible
    * noteworthy shifts or anomalies

3. Compare across years:
    * 2022 → 2023
    * 2023 → 2024
    * 2024 → 2025
    * 2022 → 2025 overall trend line

4. Highlight largest changes:
    * Identify which demographic categories saw the biggest increases or decreases.
    * Identify which community boards (if data is available by board) changed most.

5. Assess gender balance trends: Example target insights:
    * "53% female in 2024 new appointees → 52% female in 2025 first-time appointees"
    * Summaries of similar changes across other categories.

6. Flag missing or inconsistent data:
    * If a category does not appear in all years
    * If definitions change between reports

REQUIRED OUTPUT FORMAT
You must output valid JSON with the following structure:

{
  "categories_detected": [...],
  "year_to_year_trends": {
    "2022_2023": { "summary": "", "changes": {} },
    "2023_2024": { "summary": "", "changes": {} },
    "2024_2025": { "summary": "", "changes": {} }
  },
  "overall_2022_2025_trends": {
    "summary": "",
    "major_increases": [],
    "major_decreases": []
  },
  "gender_balance": {
    "year_values": {},
    "trend_summary": ""
  },
  "community_board_changes": {
    "boards_with_biggest_changes": [],
    "notes": ""
  },
  "data_quality_notes": ""
}

RULES
* Output JSON only — no prose outside JSON.
* When numeric percentages are missing, infer direction of change from text only ("increased/decreased/stayed the same").
* Be conservative: do not invent numbers.
* Ensure the JSON is syntactically valid."""


def download_pdf(url, year):
    """Download PDF to local directory"""
    output_dir = Path("brooklyn_demographics")
    output_dir.mkdir(exist_ok=True)

    output_path = output_dir / f"brooklyn_demographics_{year}.pdf"

    if output_path.exists():
        print(f"✓ {year} PDF already downloaded")
        return str(output_path)

    print(f"📥 Downloading {year} PDF...")
    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(output_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    print(f"✓ Downloaded {year} PDF")
    return str(output_path)


def convert_pdf_to_images(pdf_path, year):
    """Convert PDF pages to images for vision model"""
    print(f"🖼️  Converting {year} PDF to images...")

    images_dir = Path("brooklyn_demographics") / f"images_{year}"
    images_dir.mkdir(exist_ok=True, parents=True)

    # Convert PDF to images (first 10 pages to avoid overwhelming the model)
    images = convert_from_path(pdf_path, dpi=200, first_page=1, last_page=10)

    image_paths = []
    for i, image in enumerate(images):
        image_path = images_dir / f"page_{i+1}.png"
        image.save(image_path, 'PNG')
        image_paths.append(str(image_path))

    print(f"✓ Converted {len(image_paths)} pages to images")
    return image_paths


def encode_image_to_base64(image_path):
    """Encode image to base64 for Ollama vision model"""
    with open(image_path, 'rb') as f:
        return base64.b64encode(f.read()).decode('utf-8')


def extract_with_vision_model(image_paths, year):
    """Use Llama3.2-vision to extract demographic data from images"""
    print(f"🔍 Extracting data from {year} images using Llama3.2-vision...")

    # Prepare images (limit to first 5 pages for each year)
    images_base64 = [encode_image_to_base64(path) for path in image_paths[:5]]

    # Create prompt for this specific year
    user_prompt = f"""YEAR_{year}

Analyze these pages from the {year} Brooklyn Community Board Demographic Report.
Extract all demographic statistics about appointees including:
- Gender breakdown
- Race/Ethnicity breakdown
- Age distribution
- First-time vs returning appointees
- Any other demographic categories

Present the data clearly with percentages and counts where available."""

    # Call Ollama API
    response = requests.post(
        "http://localhost:11434/api/generate",
        json={
            "model": "llama3.2-vision:11b-instruct-q4_K_M",
            "prompt": user_prompt,
            "images": images_base64,
            "stream": False,
            "options": {
                "temperature": 0.1,
                "num_predict": 2000
            }
        }
    )

    if response.status_code != 200:
        raise Exception(f"Ollama API error: {response.text}")

    result = response.json()
    extracted_text = result.get('response', '')

    print(f"✓ Extracted {len(extracted_text)} characters from {year}")
    return extracted_text


def analyze_all_years_with_llm(year_extractions):
    """Use Llama3.2 to analyze all years together and produce final JSON"""
    print("\n🧠 Analyzing trends across all years...")

    # Combine all year extractions
    combined_input = "\n\n".join([
        f"YEAR_{year}:\n{text}"
        for year, text in year_extractions.items()
    ])

    full_prompt = f"{SYSTEM_PROMPT}\n\nDATA:\n{combined_input}\n\nGenerate the JSON analysis now:"

    # Call Ollama for analysis
    response = requests.post(
        "http://localhost:11434/api/generate",
        json={
            "model": "llama3.2:latest",
            "prompt": full_prompt,
            "stream": False,
            "options": {
                "temperature": 0.1,
                "num_predict": 4000
            }
        }
    )

    if response.status_code != 200:
        raise Exception(f"Ollama API error: {response.text}")

    result = response.json()
    analysis_text = result.get('response', '')

    # Try to extract JSON from response
    try:
        # Look for JSON in the response
        start_idx = analysis_text.find('{')
        end_idx = analysis_text.rfind('}') + 1

        if start_idx >= 0 and end_idx > start_idx:
            json_str = analysis_text[start_idx:end_idx]
            analysis_json = json.loads(json_str)
            return analysis_json
        else:
            raise ValueError("No JSON found in response")

    except json.JSONDecodeError as e:
        print(f"⚠️  Failed to parse JSON: {e}")
        print(f"Raw response:\n{analysis_text}")
        # Return a basic structure
        return {
            "error": "Failed to parse JSON",
            "raw_response": analysis_text[:1000]
        }


def main():
    """Main execution pipeline"""
    print("🚀 Starting Brooklyn Demographics Analysis\n")

    # Create output directory
    output_dir = Path("api_outputs/demographics")
    output_dir.mkdir(exist_ok=True, parents=True)

    year_extractions = {}

    # Process each year
    for year, url in PDF_URLS.items():
        print(f"\n{'='*60}")
        print(f"Processing {year}")
        print(f"{'='*60}")

        # Download PDF
        pdf_path = download_pdf(url, year)

        # Convert to images
        image_paths = convert_pdf_to_images(pdf_path, year)

        # Extract with vision model
        extracted_text = extract_with_vision_model(image_paths, year)
        year_extractions[year] = extracted_text

        # Save individual year extraction
        with open(output_dir / f"extraction_{year}.txt", 'w') as f:
            f.write(extracted_text)

    # Analyze all years together
    print(f"\n{'='*60}")
    print("Final Analysis")
    print(f"{'='*60}")

    final_analysis = analyze_all_years_with_llm(year_extractions)

    # Save final analysis
    output_file = output_dir / "brooklyn_demographics_analysis.json"
    with open(output_file, 'w') as f:
        json.dump(final_analysis, f, indent=2)

    print(f"\n✅ Analysis complete! Saved to {output_file}")

    # Print summary
    if "categories_detected" in final_analysis:
        print(f"\n📊 Categories detected: {', '.join(final_analysis['categories_detected'])}")

    return final_analysis


if __name__ == "__main__":
    main()
