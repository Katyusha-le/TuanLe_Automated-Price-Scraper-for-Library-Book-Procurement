import asyncio
import json
from playwright.async_api import async_playwright
from google.cloud import bigquery
from groq import Groq
from datetime import datetime, timezone
import os

# 1. Authenticate with GCP and Groq
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-key.json"

# Fetch the secret key injected by GitHub Actions
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
if not GROQ_API_KEY:
    raise ValueError("CRITICAL ERROR: GROQ_API_KEY environment variable is missing!")

# Connect to APIs
bq_client = bigquery.Client()
PROJECT_ID = bq_client.project
groq_client = Groq(api_key=GROQ_API_KEY)

QUEUE_TABLE = f"`{PROJECT_ID}.book_scraping.harvested_links`"
DESTINATION_TABLE = f"{PROJECT_ID}.book_scraping.library_database"

# ---------------------------------------------------------
# CORE FUNCTIONS
# ---------------------------------------------------------
async def scrape_dynamic_text(url):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent="Mozilla/5.0")
        page = await context.new_page()
        await page.goto(url, wait_until="domcontentloaded")
        for _ in range(3):
            await page.mouse.wheel(0, 1500) 
            await page.wait_for_timeout(1000) 
        try:
            await page.click("text='Xem Thêm'", timeout=1000)
            await page.wait_for_timeout(1000)
        except:
            pass 
        raw_text = await page.locator("body").inner_text()
        await browser.close()
        return raw_text[:30000]

def clean_data_with_ai(raw_text):
    prompt = f"""
    You are an expert librarian data assistant. Extract the following information from the raw Vietnamese text and return ONLY a valid JSON object. 
    If a piece of information is missing, use null. Preserve all Vietnamese accents perfectly.
    
    Required JSON Schema:
    {{
      "title": "Book Title", "author": "Author Name", "publisher": "Publisher Name",
      "publish_date": "YYYY-MM-DD or MM/YYYY", "cover_type": "Hardcover or Paperback",
      "page_count": 300, "standard_price_vnd": 150000, "current_price_vnd": 120000,
      "overview": "Summary...", "keywords": ["keyword1", "keyword2"]
    }}
    
    Raw text: {raw_text}
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

def log_status(url, new_status):
    row = [{
        "url": url, 
        "status": new_status, 
        "harvest_date": datetime.now(timezone.utc).isoformat()
    }]
    bq_client.load_table_from_json(row, f"{PROJECT_ID}.book_scraping.harvested_links").result()

# ---------------------------------------------------------
# THE WORKER LOOP
# ---------------------------------------------------------
async def run_extractor_worker():
    print("==================================================")
    print("  STARTING PHASE 2: EXTRACTOR WORKER (CONSUMER) ")
    print("==================================================")
    
    while True:
        query = f"""
            WITH LatestStatus AS (
                SELECT url, status, 
                ROW_NUMBER() OVER(PARTITION BY url ORDER BY harvest_date DESC) as rn
                FROM {QUEUE_TABLE}
            )
            SELECT url FROM LatestStatus 
            WHERE rn = 1 AND status = 'PENDING' 
            LIMIT 1
        """
        query_job = bq_client.query(query)
        results = list(query_job.result())
        
        if not results:
            print("[WORKER] The queue is empty! All books processed. Shutting down.")
            break
            
        target_url = results[0].url
        print(f"\n[WORKER] Picked up job from queue: {target_url}")
        
        raw_html_text = await scrape_dynamic_text(target_url)
        print("-> Text scraped. Sending to Llama 3.1...")
        clean_json_str = clean_data_with_ai(raw_html_text)
        
        try:
            book_record = json.loads(clean_json_str)
            
            if book_record.get("title") is None:
                print("-> SKIPPED: AI found no valid data. Marking as FAILED in queue.")
                log_status(target_url, 'FAILED')
                continue
                
            try:
                job = bq_client.load_table_from_json([book_record], DESTINATION_TABLE)
                job.result()
                print("-> Successfully saved book to BigQuery library_database!")
                log_status(target_url, 'COMPLETED')
                
            except Exception as e:
                print(f"-> [!] Database Insert Error: {e}")
                log_status(target_url, 'FAILED')
                
        except json.JSONDecodeError:
            print("-> [!] JSON Error. Marking as FAILED in queue.")
            log_status(target_url, 'FAILED')

        print("-> Pausing 2 seconds for API limits...")
        await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(run_extractor_worker())
