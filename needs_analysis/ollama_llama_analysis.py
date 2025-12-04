import sqlite3
import requests
import json

OLLAMA_API = "http://localhost:11434/api/generate"
MODEL = "llama3.2"

def get_all_statements():
    conn = sqlite3.connect("dns_data.db")
    cur = conn.cursor()
    rows = cur.execute("SELECT id, borough, cb_number, text FROM dns_pdfs").fetchall()
    conn.close()
    return rows

# Comprehensive LLM prompt
def analyze(text, borough, cb_number):
    prompt = f"""
You are analyzing a New York City Community District Needs Statement.

Borough: {borough}
Community Board: {cb_number}

TASKS:

1. Concise Summary
Provide a clear 5–7 sentence summary capturing the district's most important issues, populations of concern, and overarching challenges.

2. Top Policy Themes
Identify the major themes present in the document.
For each theme, include:
- theme name
- 2–3 sentence explanation
- representative quotes
- relevant statistics (if present)

3. Unmet Needs, Disparities, and Service Gaps
Extract all sentences that:
- describe unmet needs
- indicate disparities or inequities
- highlight service gaps
- reference at-risk or underserved populations

Return each sentence with:
- theme category
- affected populations
- any referenced data

4. Priority Ranking of Urgent Needs
Rank the 10 most urgent needs in the district.
For each ranked need, include:
- need description
- justification
- population affected
- severity level (high / medium / low)
- indicators or evidence cited

5. Cross-Sectional Analysis
Analyze the document for the following:

a. Contradictions or Tensions
Identify any conflicting statements, mismatched data points, or tensions between different sections (e.g., housing vs. economic development).

b. Needs Appearing Across Multiple Sections
List needs that recur throughout the document, and explain why they are cross-cutting.

c. Needs Mentioned Without Supporting Data
Identify claims lacking evidence and note what data would strengthen them.

6. Language and Framing Analysis
Assess how the document expresses urgency and inequity.
Identify:
- where the text uses crisis or emergency language
- where needs are framed as routine or ongoing
- where systemic inequity is explicitly or implicitly described

Provide:
- quote
- interpretation
- theme classification

TEXT:
{text}

Return your analysis in structured JSON with keys:
summary, themes, unmet_needs, priority_ranking, cross_sectional_analysis, language_framing_analysis
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
    rows = get_all_statements()

    # Process only first 5 records
    for (id_, borough, cb, text) in rows[:5]:
        print(f"\nAnalyzing {borough} CB{cb}...")
        output = analyze(text, borough, cb)

        with open(f"ollama_analysis_CB{borough}{cb}.json", "w") as f:
            f.write(output)

        print(f"Saved ollama_analysis_CB{borough}{cb}.json!")

if __name__ == "__main__":
    run()
