import sqlite3
import requests
import json

OLLAMA_API = "http://localhost:11434/api/generate"
MODEL = "llama3.2"
CHUNK_SIZE = 12000  # characters per chunk

def get_statements_by_cb():
    """Get all statements grouped by community board number"""
    conn = sqlite3.connect("dns_data.db")
    cur = conn.cursor()

    cb_numbers = cur.execute(
        "SELECT DISTINCT cb_number FROM dns_pdfs WHERE borough = 'BK' ORDER BY cb_number"
    ).fetchall()

    results = {}
    for (cb_num,) in cb_numbers:
        rows = cur.execute("""
            SELECT pdf_url, text
            FROM dns_pdfs
            WHERE borough = 'BK' AND cb_number = ?
            ORDER BY pdf_url
        """, (cb_num,)).fetchall()

        statements = {}
        for url, text in rows:
            if 'FY2024' in url or '2024' in url:
                statements['FY2024'] = text
            elif 'FY2025' in url or '2025' in url:
                statements['FY2025'] = text
            elif 'FY2026' in url or '2026' in url:
                statements['FY2026'] = text

        if len(statements) == 3:
            results[cb_num] = statements

    conn.close()
    return results


def chunk_text(text, chunk_size=CHUNK_SIZE):
    """Break text into chunks of approximately chunk_size characters"""
    chunks = []
    current_chunk = ""

    # Split by paragraphs (double newline) to try to keep semantic units together
    paragraphs = text.split('\n\n')

    for para in paragraphs:
        if len(current_chunk) + len(para) + 2 <= chunk_size:
            current_chunk += para + '\n\n'
        else:
            if current_chunk:
                chunks.append(current_chunk.strip())
            current_chunk = para + '\n\n'

    if current_chunk:
        chunks.append(current_chunk.strip())

    return chunks


def compare_chunk(cb_number, chunk_index, total_chunks, fy2024_chunk, fy2025_chunk, fy2026_chunk):
    """Compare a specific chunk across three years"""
    prompt = f"""CRITICAL: You are comparing THREE different years of the SAME SECTION of a document. DO NOT summarize one year - COMPARE ALL THREE.

Brooklyn Community Board {cb_number} - SECTION {chunk_index + 1} of {total_chunks}

YOU HAVE THREE TEXT SECTIONS BELOW (same section from different years):
- FY2024 section
- FY2025 section
- FY2026 section

YOUR JOB: Compare how THIS SECTION changed from 2024 → 2025 → 2026.

Look for:
- New priorities or concerns that appear
- Priorities that disappear or diminish
- Changes in language, tone, or framing
- Changes in data, statistics, or evidence cited
- Changes in agency responses or commitments

REQUIRED JSON OUTPUT:
{{
  "section_number": {chunk_index + 1},
  "changes_detected": [
    {{
      "type": "new_priority|removed_priority|emphasis_shift|tone_change|data_change|agency_response",
      "description": "What specifically changed",
      "fy2024_text": "Relevant quote from FY2024 (or 'Not mentioned' if new)",
      "fy2025_text": "Relevant quote from FY2025",
      "fy2026_text": "Relevant quote from FY2026 (or 'No longer mentioned' if removed)"
    }}
  ],
  "section_summary": "1-2 sentence summary of main changes in this section across the 3 years"
}}

===== FY2024 SECTION =====
{fy2024_chunk}

===== FY2025 SECTION =====
{fy2025_chunk}

===== FY2026 SECTION =====
{fy2026_chunk}

Output ONLY the JSON. COMPARE all three years. Identify specific changes.
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


def synthesize_comparisons(cb_number, chunk_analyses):
    """Synthesize all chunk comparisons into a final comprehensive analysis"""
    prompt = f"""You have analyzed multiple sections of Community Board {cb_number}'s needs statements across FY2024, FY2025, and FY2026.

Below are the section-by-section change analyses. Your job: synthesize these into ONE comprehensive comparison.

SECTION ANALYSES:
{json.dumps(chunk_analyses, indent=2)}

Create a FINAL COMPREHENSIVE COMPARISON with this structure:
{{
  "community_board": "{cb_number}",
  "summary_table": {{
    "FY2024": "Overall characterization of the FY2024 document",
    "FY2025": "Overall characterization of the FY2025 document",
    "FY2026": "Overall characterization of the FY2026 document"
  }},
  "narrative_comparison": {{
    "policy_changes": "2-3 paragraphs synthesizing how policy priorities evolved across sections",
    "agency_response_evolution": "2 paragraphs on agency response changes observed across sections",
    "emphasis_shifts": "2 paragraphs on themes that grew/shrank across the documents",
    "structural_changes": "1 paragraph on format/structure differences"
  }},
  "notable_quotes": [
    {{
      "year": "FY2024",
      "quote": "Important quote from the sections",
      "significance": "Why this matters for comparison"
    }},
    {{
      "year": "FY2025",
      "quote": "Important quote showing change",
      "significance": "How this shows evolution"
    }},
    {{
      "year": "FY2026",
      "quote": "Important quote from latest year",
      "significance": "How this represents current state"
    }}
  ],
  "final_interpretation": "2-3 sentences: What's the big story of change from 2024 to 2026?"
}}

Output ONLY the JSON.
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


def analyze_multi_year_chunked(cb_number, fy2024, fy2025, fy2026):
    """Analyze changes across three years using chunked comparison"""
    print(f"  Chunking documents...")

    # Chunk all three years
    fy2024_chunks = chunk_text(fy2024)
    fy2025_chunks = chunk_text(fy2025)
    fy2026_chunks = chunk_text(fy2026)

    # Use the maximum number of chunks to ensure we cover all content
    max_chunks = max(len(fy2024_chunks), len(fy2025_chunks), len(fy2026_chunks))

    print(f"  FY2024: {len(fy2024_chunks)} chunks | FY2025: {len(fy2025_chunks)} chunks | FY2026: {len(fy2026_chunks)} chunks")
    print(f"  Comparing {max_chunks} sections...")

    chunk_analyses = []

    for i in range(max_chunks):
        # Get chunk or empty string if this year has fewer chunks
        chunk_2024 = fy2024_chunks[i] if i < len(fy2024_chunks) else "[No content in this section]"
        chunk_2025 = fy2025_chunks[i] if i < len(fy2025_chunks) else "[No content in this section]"
        chunk_2026 = fy2026_chunks[i] if i < len(fy2026_chunks) else "[No content in this section]"

        print(f"    Analyzing section {i + 1}/{max_chunks}...")

        try:
            analysis = compare_chunk(cb_number, i, max_chunks, chunk_2024, chunk_2025, chunk_2026)
            chunk_analyses.append({
                "section": i + 1,
                "analysis": analysis
            })
        except Exception as e:
            print(f"      Warning: Section {i + 1} analysis failed: {e}")
            chunk_analyses.append({
                "section": i + 1,
                "analysis": json.dumps({"error": str(e)})
            })

    print(f"  Synthesizing {len(chunk_analyses)} section analyses into final comparison...")

    final_comparison = synthesize_comparisons(cb_number, chunk_analyses)

    return final_comparison


def run():
    cb_data = get_statements_by_cb()

    print(f"Found {len(cb_data)} Brooklyn Community Boards with all 3 years\n")

    # Process only first 5 CBs
    for i, (cb_num, statements) in enumerate(cb_data.items()):
        if i >= 5:
            break

        print(f"\nAnalyzing Brooklyn CB{cb_num} (FY2024-2026)...")

        output = analyze_multi_year_chunked(
            cb_num,
            statements['FY2024'],
            statements['FY2025'],
            statements['FY2026']
        )

        filename = f"needs_comparison_BK_CB{cb_num}.json"
        with open(filename, "w") as f:
            f.write(output)

        print(f"  ✓ Saved {filename}\n")


if __name__ == "__main__":
    run()
