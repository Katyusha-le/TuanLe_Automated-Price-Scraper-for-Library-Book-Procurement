import asyncio
import json
import os
from datetime import datetime, timezone
from playwright.async_api import async_playwright
from google.cloud import bigquery
import random
from playwright_stealth import stealth_async

# 1. Authenticate with GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-key.json"
bq_client = bigquery.Client()
PROJECT_ID = bq_client.project

# Point to the new memory bank
FRONTIER_TABLE = f"{PROJECT_ID}.book_scraping.crawl_frontier"

def load_config():
    with open("sites_config.json", "r") as f:
        return json.load(f)

async def run_discovery():
    print("==================================================")
    print("  STARTING LAYER 1: DISCOVERY BOT (SPIDER)")
    print("==================================================")
    
    config = load_config()
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        
        # 1. Rotate User-Agents to look like standard desktop browsers
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ]
        
        context = await browser.new_context(
            user_agent=random.choice(user_agents),
            viewport={'width': 1920, 'height': 1080}
        )
        page = await context.new_page()
        
        # 2. Inject the Stealth plugin to hide automation fingerprints
        await stealth_async(page)
        
        # Loop through each bookstore in the config file
        for site_name, site_data in config.items():
            print(f"\n[*] Scanning site: {site_name.upper()}")
            domain = site_data["domain"]
            book_link_selector = site_data["selectors"]["book_link"]
            
            for seed_url in site_data["seed_urls"]:
                print(f" -> Visiting seed: {seed_url}")
                try:
                    await page.goto(seed_url, wait_until="domcontentloaded", timeout=60000)
                    
                    print(" -> Waiting for network to settle...")
                    await page.wait_for_timeout(5000)
                    
                    # 1. SCROLL FIRST: Wakes up Fahasa and triggers Tiki's lazy loaders
                    print(" -> Scrolling to trigger lazy loading...")
                    for _ in range(3):
                        await page.mouse.wheel(0, 1000)
                        await page.wait_for_timeout(1000)
                    
                    # 2. THE ACTIVE HUNTER LOOP: Wait up to 10 seconds for the books to render
                    elements = []
                    for attempt in range(10):
                        elements = await page.locator(book_link_selector).element_handles()
                        if len(elements) > 0:
                            break
                        await page.wait_for_timeout(1000)
                        
                    # 3. DIAGNOSTIC DEBUGGER: If it's still 0, scan the page for what Tiki is hiding
                    if len(elements) == 0:
                        print(f" -> [!] Could not find '{book_link_selector}'. Layout has changed.")
                        all_links = await page.locator("a").element_handles()
                        debug_links = []
                        for link in all_links:
                            href = await link.get_attribute("href")
                            # Look for any link that resembles a product or book
                            if href and (".html" in href or "/p" in href or "spid" in href):
                                debug_links.append(href)
                        print(f" -> [DEBUG] Here are actual product links currently on the page: {debug_links[:5]}")
                    
                    # 4. EXTRACT LINKS: Pull the URLs out of the elements the Hunter found
                    found_links = []
                    for el in elements:
                        href = await el.get_attribute("href")
                        if href:
                            if href.startswith("/"):
                                href = f"https://{domain}{href}"
                            href = href.split("?")[0] 
                            
                            if href not in found_links:
                                found_links.append(href)
                                
                    print(f" -> Found {len(found_links)} unique book links.")
                    
                    # 2. Push the UNVISITED links to BigQuery
                    if found_links:
                        rows_to_insert = []
                        timestamp = datetime.now(timezone.utc).isoformat()
                        
                        for link in found_links:
                            rows_to_insert.append({
                                "url": link,
                                "domain": domain,
                                "status": "UNVISITED",
                                "discovered_at": timestamp,
                                "last_visited_at": None,
                                "retry_count": 0
                            })
                        
                        # Load data into the table
                        job = bq_client.load_table_from_json(rows_to_insert, FRONTIER_TABLE)
                        job.result() 
                        print(f" -> Successfully saved {len(found_links)} links to crawl_frontier!")
                            
                except Exception as e:
                    print(f" -> [!] Error scanning {seed_url}: {e}")
                    
        await browser.close()

if __name__ == "__main__":
    asyncio.run(run_discovery())
