import asyncio
from playwright.async_api import async_playwright
from google.cloud import bigquery
from datetime import datetime, timezone
import os

# 1. Authenticate with Google Cloud (The YAML file creates this JSON file for us)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-key.json"

# Connect to BigQuery
bq_client = bigquery.Client()
PROJECT_ID = bq_client.project
DATASET_ID = f"{PROJECT_ID}.book_scraping"
QUEUE_TABLE_ID = f"{DATASET_ID}.harvested_links"

# --- CONFIGURATION ---
CATEGORY_URL = "https://tiki.vn/nha-sach-tiki/c8322?from=header_keyword"
BASE_DOMAIN = "https://tiki.vn"

async def harvest_and_queue_links(category_url, max_links=5):
    print(f"\n[HARVESTER] Launching to scan: {category_url}")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent="Mozilla/5.0")
        page = await context.new_page()
        
        await page.goto(category_url, wait_until="domcontentloaded")
        await page.wait_for_timeout(4000)
        await page.mouse.wheel(0, 1000)
        await page.wait_for_timeout(1000)
        
        hrefs = await page.eval_on_selector_all("a", "elements => elements.map(e => e.getAttribute('href'))")
        
        book_links = []
        for href in hrefs:
            if href and '.html' in href and 'spid=' in href:
                full_url = href if href.startswith('http') else BASE_DOMAIN + href
                if full_url not in book_links:
                    book_links.append(full_url)
                if len(book_links) >= max_links:
                    break
        await browser.close()
        
        print(f"[HARVESTER] Found {len(book_links)} books. Pushing to BigQuery Queue...")
        
        rows_to_insert = [
            {
                "url": link, 
                "status": "PENDING", 
                "harvest_date": datetime.now(timezone.utc).isoformat()
            } 
            for link in book_links
        ]
        
        try:
            job = bq_client.load_table_from_json(rows_to_insert, QUEUE_TABLE_ID)
            job.result() 
            print("[HARVESTER] Success! Links added to the database queue.")
        except Exception as e:
            print(f"[!] Database Error: {e}")

if __name__ == "__main__":
    print("==================================================")
    print("  STARTING PHASE 2: HARVESTER (PRODUCER) ")
    print("==================================================")
    asyncio.run(harvest_and_queue_links(CATEGORY_URL, max_links=5))
