import time
import random
import re
import asyncio
import os
from urllib.parse import urlparse
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup

# ----------------- HELPERS -----------------

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

async def human_delay(a=2, b=4):
    await asyncio.sleep(random.uniform(a, b))

async def is_blocked(page):
    try:
        # Check specifically for the "Distil" or "Access Denied" text
        content = (await page.content()).lower()
        # If it finds 'access denied' or 'captcha', it's definitely a block
        return any(x in content for x in ["access denied", "distil_identification_block", "please verify you are a human"])
    except:
        return False

async def safe_goto(page, url, retries=2):
    for attempt in range(1, retries + 1):
        log(f"🌐 Navigating (attempt {attempt}) → {url}")
        try:
            # Using domcontentloaded is faster for BeautifulSoup extraction
            await page.goto(url, wait_until="domcontentloaded", timeout=35000)
            await asyncio.sleep(2) # Minimal wait for JS
            
            if not await is_blocked(page):
                return True
            
            log("⚠️ BLOCK PAGE detected — cooling down...")
            await asyncio.sleep(5)
        except PlaywrightTimeout:
            log("⏱ Page timeout — retrying")
    return False

# ----------------- EXTRACTION LOGIC -----------------

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
        # Wait specifically for category links
        await page.wait_for_selector('a[href^="/mafpak/en/c/"]', timeout=5000)
        html = await page.content()
        soup = BeautifulSoup(html, 'html.parser')
        
        links = soup.find_all('a', href=re.compile(r'^/mafpak/en/c/'))
        for link in links:
            name_el = link.find("div", class_="text-primary")
            if name_el:
                name = name_el.get_text(strip=True)
                href = link.get('href')
                subcats.append((name, domain_base + href))
    except:
        log("ℹ️ No subcategories found via selector")
    return subcats

async def scrape_items_to_list(page, category, subcategory, domain_base):
    log(f"🛒 Scraping items | {subcategory or 'DIRECT'}")
    
    # 1. TRIGGER LOAD: Wait for item container and do a fast scroll
    try:
        await page.wait_for_selector('div[class*="max-w-[134px]"]', timeout=8000)
        # Scroll halfway to trigger lazy-load grid
        await page.evaluate("window.scrollTo(0, 2000)")
        await asyncio.sleep(1.5)
    except:
        return []

    # 2. GET HTML & PARSE WITH BEAUTIFUL SOUP
    html_content = await page.content()
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Target containers based on the 'items panel.txt' patterns
    cards = soup.find_all('div', class_=lambda x: x and 'max-w-[134px]' in x)
    items_list = []

    for card in cards:
        try:
            # Name
            name_span = card.find('div', class_='line-clamp-2')
            name = name_span.get_text(strip=True) if name_span else None
            if not name: continue

            # Price
            p_main = card.find('div', class_='text-lg font-bold')
            p_dec = card.find('div', class_='text-2xs font-bold')
            price_val = f"{p_main.text.strip()}{p_dec.text.strip()}" if p_main else "0"

            # Quantity
            big_qty_el = card.find('div', class_='text-gray-500 truncate')
            big_qty = big_qty_el.get_text(strip=True) if big_qty_el else ""
            unit_qty = big_qty.split('-')[0].strip() if '-' in big_qty else big_qty

            bulk_price, base_unit = normalize_to_bulk_price(price_val, unit_qty)

            # URL
            link_el = card.find('a', href=re.compile(r'/p/'))
            item_url = domain_base + link_el['href'] if link_el else ""

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
            
    return items_list

# ----------------- MAIN ORCHESTRATOR -----------------

async def run_carrefour_scraper(target_url: str):
    log("🚀 Starting Hybrid BS4 Scraper Orchestrator")
    start_time = time.time()
    
    parsed_uri = urlparse(target_url)
    domain_base = f"{parsed_uri.scheme}://{parsed_uri.netloc}"
    
    browser_token = os.getenv("BROWSERLESS_TOKEN")
    if not browser_token:
        raise Exception("BROWSERLESS_TOKEN is not set")

    all_data = []

    async with async_playwright() as p:
        connection_url = (
            f"wss://chrome.browserless.io?token={browser_token}"
            f"&--disable-http2" 
            f"&--disable-blink-features=AutomationControlled"
            f"&stealth=true"
            f"&--location=asia"
            f"&timeout=50000"
        )

        try:
            browser = await p.chromium.connect_over_cdp(connection_url)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            )
            page = await context.new_page()

            if await safe_goto(page, target_url):
                log("📖 Main Page Loaded")
                
                # Category detection
                try:
                    cat_el = await page.query_selector('li[data-testid="breadcrumb-item"]:nth-child(2)')
                    category = (await cat_el.inner_text()).strip() if cat_el else "General"
                except:
                    category = "General"

                subcats = await extract_subcategories(page, domain_base)
                log(f"📂 Found {len(subcats)} subcategories")

                if subcats:
                    for i, (sub_name, sub_url) in enumerate(subcats, 1):
                        # VERCEL SAFETY: If we've used 45 seconds, wrap up
                        if time.time() - start_time > 45:
                            log("⏳ Time limit approaching, returning current items")
                            break

                        log(f"➡️ [{i}/{len(subcats)}] Processing: {sub_name}")
                        if await safe_goto(page, sub_url):
                            data = await scrape_items_to_list(page, category, sub_name, domain_base)
                            all_data.extend(data)
                            log(f"✅ Extracted {len(data)} items")
                else:
                    log("➡️ Scraping main page directly")
                    data = await scrape_items_to_list(page, category, "DIRECT", domain_base)
                    all_data.extend(data)

        except Exception as e:
            log(f"💥 CRITICAL ERROR: {str(e)}")
        finally:
            await browser.close()
            
    log(f"🏁 Finished. Total items: {len(all_data)}")
    return all_data