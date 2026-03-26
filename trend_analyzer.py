import os
import json
import uuid
from datetime import datetime, timezone
from google.cloud import bigquery
from groq import Groq
from pydantic import BaseModel, ValidationError
from typing import List, Optional

# 1. Authenticate with GCP and Groq
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-key.json"
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")

if not GROQ_API_KEY:
    raise ValueError("CRITICAL ERROR: GROQ_API_KEY environment variable is missing!")

bq_client = bigquery.Client()
PROJECT_ID = bq_client.project
groq_client = Groq(api_key=GROQ_API_KEY)

INSIGHTS_TABLE = f"{PROJECT_ID}.book_scraping.ai_market_insights"

# 2. Pydantic Bouncer for the Strategy AI
class MarketInsight(BaseModel):
    micro_trends: str
    top_acquisition_targets: List[str]
    strategic_reasoning: str

# 3. Fetch Macro Data (Category Level)
def get_macro_gaps():
    """Fetches the top 10 categories where the library is missing the most books."""
    query = f"""
        SELECT category, collection_gap_volume 
        FROM `{PROJECT_ID}.book_scraping.v_gap_category_level`
        WHERE collection_gap_volume > 0
        LIMIT 10
    """
    results = bq_client.query(query).result()
    summary = "MACRO CATEGORY GAPS (Highest Volume Missing):\n"
    for row in results:
        summary += f"- {row.category}: Missing {row.collection_gap_volume} books\n"
    return summary

# 4. Fetch Micro Data (Book Level)
def get_micro_gaps():
    """Fetches the top 50 highest-priority missing books based on Bestsellers, Reviews, and Freshness."""
    query = f"""
        SELECT title, author_name, category, publish_date, rating_score, review_count, is_bestseller
        FROM `{PROJECT_ID}.book_scraping.v_gap_book_level`
        LIMIT 50
    """
    results = bq_client.query(query).result()
    summary = "MICRO BOOK GAPS (Top 50 Priority Targets):\n"
    for row in results:
        bestseller_tag = "[BESTSELLER]" if row.is_bestseller else ""
        reviews = f"({row.review_count} reviews, {row.rating_score} stars)" if row.review_count else "(New/No Reviews)"
        summary += f"- {row.title} by {row.author_name} | {row.category} | {row.publish_date} {bestseller_tag} {reviews}\n"
    return summary

# 5. The AI Strategy Agent
def generate_acquisition_strategy(macro_data, micro_data):
    prompt = f"""
    You are an expert Library Acquisitions Strategist. Your goal is to advise the library on exactly which books to buy to close market gaps and maximize reader engagement.
    
    I am providing you with two datasets:
    1. MACRO GAPS: Which broad categories the library is lagging behind the market in.
    2. MICRO GAPS: A curated list of 50 specific high-priority books the library DOES NOT own, ranked by Bestseller status, Review counts, and Recency.
    
    Your task:
    1. Identify 1-2 specific 'micro_trends' by looking at how the titles and categories overlap in the data.
    2. Recommend the EXACT top 3 book titles from the Micro list that the library must acquire immediately. Prioritize Bestsellers and high-review items within high-gap categories.
    3. Provide a brief strategic justification.
    
    Return ONLY a valid JSON object.
    
    Required JSON Schema:
    {{
      "micro_trends": "Describe the specific niche trends...",
      "top_acquisition_targets": ["Exact Title 1", "Exact Title 2", "Exact Title 3"],
      "strategic_reasoning": "A 2-3 sentence explanation of why these specific books bridge the biggest gaps."
    }}
    
    DATASETS:
    {macro_data}
    
    {micro_data}
    """
    try:
        chat_completion = groq_client.chat.completions.create(
            messages=[
                {"role": "system", "content": "You output strict JSON only."},
                {"role": "user", "content": prompt}
            ],
            model="llama-3.3-70b-versatile", 
            temperature=0.3, # A little creativity for trend spotting
            response_format={"type": "json_object"} 
        )
        return chat_completion.choices[0].message.content
    except Exception as e:
        print(f"      [!] Groq Strategy API Error: {e}")
        return "{}"

# 6. Worker Execution
def run_trend_analyzer():
    print("==================================================")
    print("  STARTING LAYER 5: HYBRID TREND ANALYZER         ")
    print("==================================================")
    
    print("[*] Fetching Macro & Micro Gap Data from BigQuery...")
    macro_data = get_macro_gaps()
    micro_data = get_micro_gaps()
    
    if "Missing" not in macro_data or "Priority" not in micro_data:
        print("[!] Not enough data to run analysis. Ensure the scraper has populated the views.")
        return
        
    print("[*] Data gathered. Sending to Llama 3.1 for strategic analysis...")
    json_str = generate_acquisition_strategy(macro_data, micro_data)
    
    try:
        raw_record = json.loads(json_str)
        clean_record = MarketInsight(**raw_record)
        
        if clean_record.top_acquisition_targets:
            print(f"\n[+] Identified Micro-Trends:\n    {clean_record.micro_trends}")
            print(f"\n[+] Top Acquisition Targets:\n    " + "\n    ".join(clean_record.top_acquisition_targets))
            print(f"\n[+] Strategic Reasoning:\n    {clean_record.strategic_reasoning}")
            
            # Save the insight to BigQuery for the Dashboard
            row_to_insert = [{
                "insight_id": str(uuid.uuid4()),
                "analyzed_at": datetime.now(timezone.utc).isoformat(),
                "trending_categories": clean_record.micro_trends, # Mapping trend text to your existing schema
                "business_recommendation": f"BUY: {', '.join(clean_record.top_acquisition_targets)}. REASON: {clean_record.strategic_reasoning}"
            }]
            
            job = bq_client.load_table_from_json(row_to_insert, INSIGHTS_TABLE)
            job.result()
            print("\n[+] Successfully saved market insights to BigQuery!")
        else:
            print("\n[!] AI failed to generate a recommendation.")
            
    except ValidationError as e:
        print(f"\n[!] Pydantic rejected the AI's format: {e}")
    except json.JSONDecodeError:
        print("\n[!] Failed to decode JSON from AI.")

if __name__ == "__main__":
    run_trend_analyzer()
