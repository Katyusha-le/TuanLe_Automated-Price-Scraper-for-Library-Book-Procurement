import os
import json
import asyncio
from datetime import datetime, timezone
from google.cloud import bigquery
from groq import Groq
from pydantic import BaseModel, ValidationError
from typing import Optional

# 1. Authenticate
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-key.json"
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")

bq_client = bigquery.Client()
PROJECT_ID = bq_client.project
groq_client = Groq(api_key=GROQ_API_KEY)

CATEGORIES_TABLE = f"{PROJECT_ID}.book_scraping.ai_book_categories"

# 2. Pydantic Bouncer
class UDCClassification(BaseModel):
    udc_code: Optional[str] = None
    udc_name: Optional[str] = None

# 3. Fetch Uncategorized Books
def get_uncategorized_books(limit=50):
    """Finds books in the clean view that aren't in the AI categories table yet."""
    query = f"""
        WITH CleanedWithID AS (
            SELECT 
                CAST(FARM_FINGERPRINT(title) AS STRING) AS book_id, 
                title, 
                overview
            FROM `{PROJECT_ID}.book_scraping.v_library_cleaned`
            WHERE overview IS NOT NULL AND overview != ''
        )
        SELECT v.book_id, v.title, v.overview
        FROM CleanedWithID v
        LEFT JOIN `{CATEGORIES_TABLE}` c ON v.book_id = c.book_id
        WHERE c.book_id IS NULL 
        LIMIT {limit}
    """
    return list(bq_client.query(query).result())

# 4. The AI Librarian
def classify_book_with_ai(title, overview):
    prompt = f"""
    You are an expert Head Librarian. Classify the following book using the Universal Decimal Classification (UDC) system.
    Analyze the title and overview, then assign the SINGLE most accurate Top-Level UDC class from this strict list:
    
    0 - Science and Knowledge. Organization. Computer Science. Information.
    1 - Philosophy. Psychology.
    2 - Religion. Theology.
    3 - Social Sciences (Economics, Law, Education).
    5 - Mathematics. Natural Sciences.
    6 - Applied Sciences. Medicine. Technology.
    7 - The Arts. Recreation. Entertainment. Sport.
    8 - Language. Linguistics. Literature.
    9 - Geography. Biography. History.
    
    Return ONLY a valid JSON object.
    Required JSON Schema:
    {{
      "udc_code": "The single digit code (e.g., '8')",
      "udc_name": "The short name (e.g., 'Literature')"
    }}
    
    Book Title: {title}
    Book Overview: {overview}
    """
    try:
        chat_completion = groq_client.chat.completions.create(
            messages=[
                {"role": "system", "content": "You output strict JSON only."},
                {"role": "user", "content": prompt}
            ],
            model="llama-3.1-8b-instant", 
            temperature=0, 
            response_format={"type": "json_object"} 
        )
        return chat_completion.choices[0].message.content
    except Exception as e:
        print(f"      [!] Groq API Error: {e}")
        return "{}"

# 5. Worker Loop
def run_categorizer():
    print("==================================================")
    print("  STARTING LAYER 4: AI LIBRARIAN (CATEGORIZER)  ")
    print("==================================================")
    
    books = get_uncategorized_books(limit=50)
    
    if not books:
        print("[LIBRARIAN] No new books to categorize. Shutting down.")
        return
        
    print(f"[LIBRARIAN] Found {len(books)} books needing classification.")
    
    rows_to_insert = []
    timestamp = datetime.now(timezone.utc).isoformat()
    
    for book in books:
        print(f"\n[*] Reading: {book.title}")
        
        json_str = classify_book_with_ai(book.title, book.overview)
        
        try:
            raw_record = json.loads(json_str)
            clean_record = UDCClassification(**raw_record)
            
            if clean_record.udc_code:
                print(f" -> Classified as: {clean_record.udc_code} - {clean_record.udc_name}")
                rows_to_insert.append({
                    "book_id": book.book_id,
                    "udc_code": clean_record.udc_code,
                    "udc_name": clean_record.udc_name,
                    "categorized_at": timestamp
                })
            else:
                print(" -> [!] AI failed to determine a category.")
                
        except ValidationError:
            print(" -> [!] Pydantic rejected the AI's format.")
            
    if rows_to_insert:
        try:
            # Swapped to the Free-Tier compatible Load Job API
            job = bq_client.load_table_from_json(rows_to_insert, CATEGORIES_TABLE)
            job.result()  # Wait for the job to complete
            print(f"\n[+] Successfully categorized and saved {len(rows_to_insert)} books!")
        except Exception as e:
            print(f"[!] BigQuery Insert Error: {e}")

if __name__ == "__main__":
    run_categorizer()
