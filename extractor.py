import asyncio
import json
import os
import random
from datetime import datetime, timezone
from playwright.async_api import async_playwright
from playwright_stealth import stealth_async
from google.cloud import bigquery
from groq import Groq
from pydantic import BaseModel, ValidationError
from typing import Optional, List
import markdownify

# 1. Authenticate with GCP and Groq
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-key.json"

GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
if not GROQ_API_KEY:
    raise ValueError("CRITICAL ERROR: GROQ_API_KEY environment variable is missing!")

# Connect to APIs
bq_client = bigquery.Client()
PROJECT_ID = bq_client.project
groq_client = Groq(api_key=GROQ_API_KEY)

# 2. Update to the New Database Architecture
FRONTIER_TABLE = f"{PROJECT_ID}.book_scraping.crawl_frontier"
DESTINATION_TABLE = f"{PROJECT_ID}.book_scraping.library_database"

# ---------------------------------------------------------
# MODULE 4: THE DATA BOUNCER (PYDANTIC)
# ---------------------------------------------------------
class BookData(BaseModel):
    title: Optional[str] = None
    author: Optional[str] = None
    publisher: Optional[str] = None
    publish_date: Optional[str] = None
    cover_type: Optional[str] = None
    page_count: Optional[int] = None
    standard_price_vnd: Optional[int] = None
    current_price_vnd: Optional[int] = None
    overview: Optional[str] = None
    keywords: Optional[List[str]] = None

# ---------------------------------------------------------
# STATE MANAGEMENT (Free-Tier Load Job)
# ---------------------------------------------------------
def update_link_status(url, domain, status, new_retry_count=0):
    """Uses the Free-Tier compatible Load Job API."""
    timestamp = datetime.now(timezone.utc).isoformat()
    row = [{
        "url": url,
        "domain": domain,
        "status": status,
        "discovered_at": timestamp, 
        "last_visited_at": timestamp,
        "retry_count": new_retry_count
    }]
    try:
        job = bq_client.load_table_from_json(row, FRONTIER_TABLE)
        job.result()
    except Exception as e:
        print(f"      [!] BigQuery State Update Error: {e}")

def get_batch_unvisited_links(limit=50):
    """Fetches a batch of UNVISITED links at once to avoid constant DB querying."""
    query = f"""
        WITH LatestStatus AS (
            SELECT url, domain, status, retry_count,
            ROW_NUMBER() OVER(PARTITION BY url ORDER BY discovered_at DESC) as rn
            FROM `{FRONTIER_TABLE}`
        )
        SELECT url, domain, retry_count FROM LatestStatus 
        WHERE rn = 1 AND status = 'UNVISITED' 
        LIMIT {limit}
    """
    return list(bq_client.query(query).result())

# ---------------------------------------------------------
# CORE FUNCTIONS (Now with Stealth & Cloudflare bypass) 
# ---------------------------------------------------------
async def scrape_dynamic_text(url):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        
        # 1. ADDED: User Agent Rotation & Stealth to bypass Fahasa Cloudflare
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ]
        context = await browser.new_context(user_agent=random.choice(user_agents))
        page = await context.new_page()
        await stealth_async(page)
        
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=45000)

            # 2. CRITICAL FIX: Wait for the Cloudflare challenge to clear
            await page.wait_for_timeout(5000)
            
            # 3. DIAGNOSTIC: Check if we are still trapped on a security page
            page_title = await page.title()
            if "Just a moment" in page_title or "Access Denied" in page_title or "Cloudflare" in page_title:
                print(f"      [!] Trapped by Cloudflare. Bot sees: '{page_title}'")
                await browser.close()
                return "" # Return empty so it skips AI processing
            
            # 4. Scroll and expand logic
            for _ in range(3):
                await page.mouse.wheel(0, 1500) 
                await page.wait_for_timeout(1000) 
            try:
                await page.click("text='Xem Thêm'", timeout=1000)
                await page.wait_for_timeout(1000)
            except:
                pass 

            # 5. AGGRESSIVE PRUNING: Destroy the mega-menus, headers, and sidebars to save space
            cleanup_script = """
                document.querySelectorAll('script, style, nav, footer, img, header, aside, iframe, svg, .menu, .header, #header').forEach(el => el.remove());
            """
            await page.evaluate(cleanup_script)
            
            # CHANGED: Grab inner_html instead of inner_text so we preserve structural tags (like tables)
            raw_html = await page.locator("body").inner_html()
            
        except Exception as e:
            print(f"      [!] Playwright failed to load page: {e}")
            raw_html = ""
            
        await browser.close()
        
        # CHANGED: Convert HTML to Markdown. This turns HTML tables into Markdown tables, making it easy for the LLM to read.
        if raw_html:
            md_text = markdownify.markdownify(raw_html, strip=['a', 'img']).strip()
            # Compress empty lines to pack more data into the AI's token limit
            compressed_md = "\n".join([line.strip() for line in md_text.splitlines() if line.strip()])
            return compressed_md[:15000]
        
        return ""

def clean_data_with_ai(raw_text):
    # CHANGED: A completely universal prompt. It relies on structural deduction instead of specific keywords.
    prompt = f"""
    You are a universal data extraction AI. You are receiving the Semantic Markdown structure of an international product page. 
    Identify the core book product entity and extract its details. Translate whatever local language is used into our standard English JSON schema.
    
    UNIVERSAL LOGIC RULES:
    - Deduce attributes based on their context in the Markdown (e.g., look at Markdown tables, lists, or headings near the title).
    - Author/Publisher: Infer from contextual words regardless of language.
    - Dates: Look for year or date formats. Return as string.
    - Prices: Deduce original vs. current price based on context. Usually, the standard price is the higher number, and the current selling price is the lower number. Return pure integers (strip all currencies/symbols).
    - If an attribute is truly missing from the page, use null.
    - Keep the 'overview' concise (maximum 3 sentences) to save space.
    
    Required JSON Schema:
    {{
      "title": "Book Title", "author": "Author Name", "publisher": "Publisher Name",
      "publish_date": "YYYY-MM-DD or MM/YYYY", "cover_type": "Hardcover or Paperback",
      "page_count": 300, "standard_price_vnd": 150000, "current_price_vnd": 120000,
      "overview": "Short Summary...", "keywords": ["keyword1", "keyword2"]
    }}
    
    Markdown text: {raw_text}
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

# ---------------------------------------------------------
# THE WORKER LOOP (Batch Processing + Polling)
# ---------------------------------------------------------
async def run_extractor_worker():
    print("==================================================")
    print("  STARTING LAYER 3: EXTRACTOR WORKER (CONSUMER) ")
    print("==================================================")
    
    max_empty_retries = 6  
    empty_retries = 0

    while True:
        print(f"[WORKER] Fetching a batch of UNVISITED links... (Attempt {empty_retries+1}/{max_empty_retries+1})")
        batch = get_batch_unvisited_links(limit=50)
        
        if not batch:
            if empty_retries < max_empty_retries:
                print("[WORKER] Queue empty. Waiting 10s for BigQuery to settle...")
                await asyncio.sleep(10)
                empty_retries += 1
                continue
            else:
                print("[WORKER] Queue has been empty for over 60 seconds. All books processed. Shutting down.")
                break
                
        # Reset the retry counter if we successfully found links
        empty_retries = 0
        
        print(f"[WORKER] Found {len(batch)} links to process in this batch.")

        for target in batch:
            target_url = target.url
            target_domain = target.domain
            retry_count = target.retry_count
            
            print(f"\n[*] Processing: {target_url}")
            
            # Lock the row instantly
            update_link_status(target_url, target_domain, 'IN_PROGRESS', retry_count)
            
            raw_html_text = await scrape_dynamic_text(target_url)
            
            if not raw_html_text:
                print("-> SKIPPED: Could not extract text from page (Likely blocked or timeout).")
                update_link_status(target_url, target_domain, "FAILED", retry_count + 1)
                continue
                
            print("-> Text scraped. Sending to Llama 3.1...")
            clean_json_str = clean_data_with_ai(raw_html_text)
            
            try:
                raw_record = json.loads(clean_json_str)
                clean_record = BookData(**raw_record)
                book_dict = clean_record.model_dump()
                
                if book_dict.get("title") is None:
                    print("-> SKIPPED: AI found no valid title. Marking as FAILED.")
                    update_link_status(target_url, target_domain, 'FAILED', retry_count + 1)
                    continue
                    
                try:
                    # Use Free-Tier compatible API for the final database too
                    job = bq_client.load_table_from_json([book_dict], DESTINATION_TABLE)
                    job.result()
                    print("-> Successfully saved book to BigQuery library_database!")
                    update_link_status(target_url, target_domain, 'VISITED', retry_count)
                
                except Exception as e:
                    print(f"-> [!] Database Insert Error: {e}")
                    update_link_status(target_url, target_domain, 'FAILED', retry_count + 1)
                    
            except ValidationError as e:
                print(f"-> [!] Pydantic rejected the AI's data format. Marking as FAILED.")
                update_link_status(target_url, target_domain, 'FAILED', retry_count + 1)
                
            except json.JSONDecodeError:
                print("-> [!] JSON Error from Groq. Marking as FAILED.")
                update_link_status(target_url, target_domain, 'FAILED', retry_count + 1)

            print("-> Pausing 2 seconds for API limits...")
            await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(run_extractor_worker())
