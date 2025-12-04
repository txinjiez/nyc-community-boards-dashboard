import json
import os
from openai import OpenAI

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def extract_themes_for_cb(cb_number):
    """
    Use GPT to extract themes from a CB's comparison JSON
    """
    # Load the comparison data
    input_file = f"api_outputs/openai/needs_comparison_BK_CB{cb_number}.json"

    with open(input_file, 'r') as f:
        data = json.load(f)

    # Create prompt for theme extraction
    prompt = f"""Analyze this 3-year community board needs comparison and extract the KEY THEMES that appear across the years.

COMPARISON DATA:
{json.dumps(data, indent=2)}

INSTRUCTIONS:
1. Identify 4-6 major policy themes that appear across FY2024, FY2025, and FY2026
2. For each theme, assign a "depth score" (0-5) for each fiscal year based on:
   - 0 = Not mentioned at all
   - 1 = Basic mention only
   - 2 = Mentioned with examples
   - 3 = Policy asks or commitments requested
   - 4 = Data/evidence cited
   - 5 = Structured recommendations or frameworks proposed

3. Provide a SHORT label for each theme (2-4 words max)

IMPORTANT RULES:
- Extract themes from the actual narrative text, NOT from generic categories
- If a theme is mentioned in summary_table or notable_quotes but not in narratives, still count it
- Score based on the depth of discussion in policy_changes and emphasis_shifts
- Look for themes that EVOLVED across years (e.g., "Traffic" → "Transportation & Mobility" → "Vision Zero")

OUTPUT FORMAT (valid JSON only):
{{
  "cb_number": {cb_number},
  "themes": [
    {{
      "name": "Theme Name",
      "fy2024": <score 0-5>,
      "fy2025": <score 0-5>,
      "fy2026": <score 0-5>,
      "evolution_note": "One sentence describing how this theme changed"
    }}
  ]
}}

Return ONLY valid JSON, no other text."""

    # Call OpenAI
    response = client.chat.completions.create(
        model="gpt-4o-2024-08-06",
        messages=[
            {"role": "system", "content": "You are an expert policy analyst specializing in municipal governance and community needs assessment. You extract themes and score their emphasis based on textual evidence."},
            {"role": "user", "content": prompt}
        ],
        response_format={"type": "json_object"},
        temperature=0.3
    )

    result = json.loads(response.choices[0].message.content)
    return result

def main():
    """Extract themes for all 5 Brooklyn CBs"""
    all_themes = {}

    for cb_num in range(1, 6):
        print(f"\n🔍 Extracting themes for Brooklyn CB{cb_num}...")

        try:
            themes_data = extract_themes_for_cb(cb_num)
            all_themes[f"CB{cb_num}"] = themes_data
            print(f"  ✓ Found {len(themes_data['themes'])} themes")

            # Print themes for verification
            for theme in themes_data['themes']:
                print(f"    - {theme['name']}: FY24={theme['fy2024']} FY25={theme['fy2025']} FY26={theme['fy2026']}")

        except Exception as e:
            print(f"  ✗ Error: {e}")
            all_themes[f"CB{cb_num}"] = {"error": str(e)}

    # Save to file
    output_file = "api_outputs/openai/themes_extracted.json"
    with open(output_file, 'w') as f:
        json.dump(all_themes, f, indent=2)

    print(f"\n✅ Themes extracted and saved to {output_file}")

if __name__ == "__main__":
    main()
