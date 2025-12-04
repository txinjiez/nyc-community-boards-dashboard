import sqlite3
from openai import OpenAI

client = OpenAI()

def get_all_statements():
    conn = sqlite3.connect("dns_data.db")
    cur = conn.cursor()
    rows = cur.execute("SELECT id, borough, cb_number, text FROM dns").fetchall()
    conn.close()
    return rows

# Example LLM prompt
def analyze(text, borough, cb_number):
    prompt = f"""
You are analyzing a New York City Community District Needs Statement.

Borough: {borough}
Community Board: {cb_number}

TASKS:
1. Provide a concise summary (5–7 sentences).
2. Identify the top policy themes (e.g., housing, infrastructure, parks, youth services).
3. Extract all sentences that describe unmet needs, disparities, or service gaps.
5. Write a "priority ranking" of the district’s most urgent needs with justification.

TEXT:
{text}

Return your analysis in structured JSON with keys:
summary, themes, unmet_needs, mentions, priority_ranking
"""

    resp = client.chat.completions.create(
        model="gpt-4.1",
        messages=[{"role": "user", "content": prompt}]
    )

    return resp.choices[0].message.content


def run():
    rows = get_all_statements()

    for (id_, borough, cb, text) in rows:
        print(f"\nAnalyzing {borough} CB{cb}...")
        output = analyze(text, borough, cb)

        with open(f"analysis_CB{borough}{cb}.json", "w") as f:
            f.write(output)

        print("Saved!")

if __name__ == "__main__":
    run()
