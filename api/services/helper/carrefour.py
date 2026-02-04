import time
import random
import re
import asyncio
from urllib.parse import urlparse
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
import os
# ----------------- HELPERS -----------------

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

async def human_delay(a=2, b=5):
    await asyncio.sleep(random.uniform(a, b))

async def is_blocked(page):
    try:
        content = (await page.inner_text("body")).lower()
        return "technical issues at our end" in content
    except:
        return False

async def safe_goto(page, url, retries=3):
    for attempt in range(1, retries + 1):
        log(f"🌐 Navigating (attempt {attempt}) → {url}")
        try:
            await page.goto(url, wait_until="networkidle", timeout=60000)
            await human_delay(4, 7)
            if not await is_blocked(page):
                return True
            log("⚠️ BLOCK PAGE detected — cooling down...")
            await human_delay(12, 18)
        except PlaywrightTimeout:
            log("⏱ Page timeout — retrying")
    return False

# ----------------- SCRAPERS -----------------

def normalize_to_bulk_price(price_str, unit_qty_str):
    try:
        price = float(re.sub(r'[^\d.]', '', price_str))
        match = re.search(r'(\d+(?:\.\d+)?)\s*([a-zA-Z]+)', unit_qty_str.lower())
        if not match:
            return price, "Unit"
        value = float(match.group(1))
        unit = match.group(2)
        if unit in ['g', 'gm', 'grams']:
            return (price / value) * 1000, "KG"
        elif unit in ['ml', 'milliliter']:
            return (price / value) * 1000, "Litre"
        elif unit in ['kg', 'l', 'liter']:
            return price / value, unit.upper()
        return price, "Unit"
    except:
        return 0.0, "Unknown"

async def extract_subcategories(page, domain_base):
    log("🔍 Checking for subcategories")
    subcats = []
    try:
        await page.wait_for_selector('a[href^="/mafpak/en/c/"]', timeout=5000)
    except PlaywrightTimeout:
        return []

    links = await page.query_selector_all('a[href^="/mafpak/en/c/"]')
    for link in links:
        try:
            name_el = await link.query_selector("div.text-primary")
            if not name_el: continue
            name = (await name_el.inner_text()).strip()
            href = await link.get_attribute("href")
            # Combine domain with relative href
            subcats.append((name, domain_base + href))
        except:
            continue
    return subcats

async def scrape_items_to_list(page, category, subcategory, domain_base):
    log(f"🛒 Scraping items | {subcategory or 'DIRECT'}")
    items_list = []
    try:
        await page.wait_for_selector('div.relative.w-\\[134px\\]', timeout=10000)
        cards = await page.query_selector_all('div.relative.w-\\[134px\\]')
        
        for card in cards:
            try:
                name_el = await card.query_selector('div.line-clamp-2 span')
                name = (await name_el.inner_text()).strip() if name_el else "N/A"

                p_main = await card.query_selector('div.text-lg.font-bold')
                p_dec = await card.query_selector('div.text-2xs.font-bold')
                price_val = f"{(await p_main.inner_text()).strip()}{(await p_dec.inner_text()).strip()}" if p_main else "0"

                big_qty_el = await card.query_selector('div.text-gray-500.truncate')
                big_qty = (await big_qty_el.inner_text()).strip() if big_qty_el else ""
                unit_qty = big_qty.split('-')[0].strip() if '-' in big_qty else big_qty

                bulk_price, base_unit = normalize_to_bulk_price(price_val, unit_qty)

                link_el = await card.query_selector('a[href*="/p/"]')
                item_url = domain_base + (await link_el.get_attribute("href")) if link_el else ""

                items_list.append({
                    "Category": category,
                    "Subcategory": subcategory,
                    "Item_Name": name,
                    "Price": price_val,
                    "Currency": "PKR",
                    "Big_Qty": big_qty,
                    "Unit_Qty": unit_qty,
                    "Base_Unit_Price": f"{bulk_price:.2f}",
                    "Base_Unit": base_unit,
                    "Item_URL": item_url
                })
            except:
                continue
    except:
        pass
    return items_list

# ----------------- MAIN ORCHESTRATOR -----------------

async def run_carrefour_scraper(target_url: str):
    # 1. Dynamically determine the base domain
    parsed_uri = urlparse(target_url)
    domain_base = f"{parsed_uri.scheme}://{parsed_uri.netloc}"
    
    # 2. Get your Browserless Token from Vercel Environment Variables
    # Ensure you have added BROWSERLESS_TOKEN in Vercel Settings
    browser_token = os.getenv("BROWSERLESS_TOKEN")
    
    if not browser_token:
        raise Exception("BROWSERLESS_TOKEN is not set in environment variables")

    all_data = []

    async with async_playwright() as p:
        # Connect to the remote Browserless instance instead of launching locally
        # This solves the 'Executable doesn't exist' error on Vercel
        # 1. Add specific flags to the connection URL to disable HTTP/2
        # We append '&--disable-http2' to the WebSocket string
        connection_url = (
            f"wss://chrome.browserless.io?token={browser_token}"
            f"&--disable-http2" 
            f"&--disable-blink-features=AutomationControlled"
        )

        browser = await p.chromium.connect_over_cdp(connection_url)
        
        # 2. Set a high-quality User-Agent and Extra Headers in the context
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            }
        )
        # Browserless handles the user agent and stealth automatically, 
        # but we use a context to keep the session clean
        context = await browser.new_context()
        page = await context.new_page()

        try:
            if await safe_goto(page, target_url):
                # Detect category from breadcrumbs
                try:
                    cat_el = await page.query_selector('li[data-testid="breadcrumb-item"]:nth-child(2)')
                    category = (await cat_el.inner_text()).strip() if cat_el else "General"
                except:
                    category = "General"

                # Use derived domain_base for subcategory links
                subcats = await extract_subcategories(page, domain_base)

                if subcats:
                    for sub_name, sub_url in subcats:
                        if await safe_goto(page, sub_url):
                            data = await scrape_items_to_list(page, category, sub_name, domain_base)
                            all_data.extend(data)
                else:
                    data = await scrape_items_to_list(page, category, "DIRECT", domain_base)
                    all_data.extend(data)
        finally:
            # Always close the connection to avoid wasting Browserless minutes
            await browser.close()
            
        return all_data

async def run_carrefour_scraper_PlaywrightOld(target_url: str):
    # Dynamically determine the base domain (e.g., https://www.carrefour.pk)
    parsed_uri = urlparse(target_url)
    domain_base = f"{parsed_uri.scheme}://{parsed_uri.netloc}"
    
    all_data = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True, 
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        if await safe_goto(page, target_url):
            # Detect category from breadcrumbs
            try:
                cat_el = await page.query_selector('li[data-testid="breadcrumb-item"]:nth-child(2)')
                category = (await cat_el.inner_text()).strip() if cat_el else "General"
            except:
                category = "General"

            # Use derived domain_base for subcategory links
            subcats = await extract_subcategories(page, domain_base)

            if subcats:
                for sub_name, sub_url in subcats:
                    if await safe_goto(page, sub_url):
                        data = await scrape_items_to_list(page, category, sub_name, domain_base)
                        all_data.extend(data)
            else:
                data = await scrape_items_to_list(page, category, "DIRECT", domain_base)
                all_data.extend(data)

        await browser.close()
        return all_data