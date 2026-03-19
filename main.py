import asyncio
import json
import os
import time
import argparse
from datetime import date
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
import requests
import urllib.parse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- Config ---
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = "appmbuoYupyA1iB3j"
TABLE_ID = "tblJ7pSMe1p0iTyif"
VIEW_ID = "viwpMtssgHDhyUfEH"

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}

# --- Scraper Logic ---
def format_sc_number(val_str):
    """Converts 25.1K to 25100, 3.8M to 3800000, etc."""
    if not val_str:
        return 0
    val_str = val_str.lower().strip().replace(',', '')
    try:
        if 'k' in val_str:
            num = float(val_str.replace('k', ''))
            return int(num * 1000)
        elif 'm' in val_str:
            num = float(val_str.replace('m', ''))
            return int(num * 1000000)
        return int(float(val_str))
    except (ValueError, TypeError):
        return 0

async def scrape_soundcloud(browser_context, url):
    """Scrapes a single SoundCloud profile."""
    if not url or "soundcloud.com" not in url:
        return None

    page = await browser_context.new_page()
    # Apply stealth
    await Stealth().apply_stealth_async(page)
    
    data = {}
    try:
        print(f"  --> Navigating to {url}")
        # Wait for base load
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(6) # Allow dynamic content to stabilize
        except Exception as e:
            print(f"      [ERROR] Could not load {url}: {e}")
            return None

        # 1. Handle Cookie Consent
        try:
            cookie_btn = page.locator("button#onetrust-accept-btn-handler, button:has-text('Accept'), button:has-text('I accept')")
            if await cookie_btn.count() > 0 and await cookie_btn.is_visible():
                await cookie_btn.click()
                await asyncio.sleep(1)
        except:
            pass

        # 2. Extract Stats (Using precision where possible)
        try:
            stat_selectors = {
                'Followers': "a.infoStats__statLink[href$='/followers'], a[href$='/followers']",
                'Following': "a.infoStats__statLink[href$='/following'], a[href$='/following']",
                'Tracks': "a.infoStats__statLink[href$='/tracks'], a[href$='/tracks']"
            }
            for label, sel in stat_selectors.items():
                link = page.locator(sel).first
                if await link.count() > 0:
                    # 1. Precise count from title
                    title_attr = await link.get_attribute("title")
                    if title_attr:
                         count_str = title_attr.split(' ')[0]
                         data[f'Soc Soundcloud {label}'] = str(format_sc_number(count_str))
                    else:
                         # 2. Display count (e.g. 25.1K)
                         val_el = link.locator(".infoStats__statValue, div, span").first
                         if await val_el.count() > 0:
                             data[f'Soc Soundcloud {label}'] = str(format_sc_number(await val_el.inner_text()))
        except:
            pass

        # 3. Bio & Expand
        try:
            show_more = page.locator("a.truncatedUserDescription__collapse, button.truncatedUserDescription__collapse")
            if await show_more.count() > 0 and await show_more.is_visible():
                await show_more.click()
                await asyncio.sleep(0.5)
            
            bio_el = page.locator(".truncatedUserDescription")
            if await bio_el.count() > 0:
                data['Soc Soundcloud Bio'] = await bio_el.inner_text()
        except:
            pass

        # 4. Verified Badge (Airtable expects TRUE/FALSE if text, or bool if checkbox)
        is_verified = await page.locator(".verifiedBadge, .verifiedBadge__icon").count() > 0
        data['Soc Soundcloud Verified'] = "TRUE" if is_verified else "FALSE"

        # 5. Location (Avoiding Name overlap)
        try:
            loc_els = page.locator("h3.profileHeaderInfo__additional, .profileHeaderInfo__additional")
            name_el = page.locator("h2.profileHeaderInfo__userName")
            name_text = ""
            if await name_el.count() > 0:
                name_text = (await name_el.inner_text()).strip()

            count = await loc_els.count()
            for i in range(count):
                txt = (await loc_els.nth(i).inner_text()).strip()
                if txt and txt != name_text:
                    data['Soc Soundcloud Location'] = txt
                    break
        except:
            pass

        # 6. Profile Image
        try:
            img_el = page.locator(".profileHeader__avatar span, .profileHeaderInfo__avatar span").first
            if await img_el.count() > 0:
                style = await img_el.get_attribute("style")
                if style and "url(" in style:
                    img_url = style.split('url("')[1].split('")')[0].replace('"', '')
                    data['Soc Soundcloud Image'] = img_url
        except:
            pass

        # 7. Social Links (Comma Separated & Unquoted)
        socials = []
        try:
            # Find all potential social links
            links = page.locator("a.web-profile, a.sc-link-secondary[href*='gate.sc']")
            count = await links.count()
            for i in range(count):
                href = await links.nth(i).get_attribute("href")
                if href:
                    if "gate.sc" in href:
                        try:
                            parsed = urllib.parse.urlparse(href)
                            query = urllib.parse.parse_qs(parsed.query)
                            actual = query.get('url', [href])[0]
                            socials.append(urllib.parse.unquote(actual).strip())
                        except:
                            socials.append(href.strip())
                    else:
                        socials.append(href.strip())
            
            if socials:
                # Deduplicate while preserving order, then join
                data['Soc Soundcloud Socials'] = ", ".join(list(dict.fromkeys(socials)))
        except:
            pass

    except Exception as e:
        print(f"      [ERROR] Scrape logic failed for {url}: {e}")
        return None
    finally:
        await page.close()
    
    return data

# --- Airtable Helpers ---
def update_records_bulk(records_batch):
    if not records_batch:
        return True
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}"
    r = requests.patch(url, headers=AIRTABLE_HEADERS, json={"records": records_batch}, timeout=15)
    if r.status_code != 200:
        print(f"      [ERROR] Airtable update failed: {r.text}")
        return False
    return True

# --- Main Logic ---
async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()

    if not args.all and not args.limit:
        # Default to a high number if neither is set, to process "all pending"
        args.limit = 50000 

    print(f"Starting SoundCloud Enrichment...")
    
    # --- Discover valid Airtable fields to avoid unknown field errors ---
    print("  [DEBUG] Discovering Airtable columns...")
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}"
    params = {
        "pageSize": 100,
        "filterByFormula": "AND({Soc Soundcloud} != '', {Last Check} = '')"
    }
    if args.all:
        params["filterByFormula"] = "{Soc Soundcloud} != ''"

    disco_r = requests.get(url, headers=AIRTABLE_HEADERS, params={"maxRecords": 1})
    actual_fields = set()
    if disco_r.status_code == 200:
         recs = disco_r.json().get('records', [])
         if recs:
             actual_fields = set(recs[0].get('fields', {}).keys())
    
    # Ensure common ones even if hidden
    actual_fields.update({"Last Check", "Soc Soundcloud Followers", "Soc Soundcloud Following", "Soc Soundcloud Tracks", "Soc Soundcloud Bio", "Soc Soundcloud Verified", "Soc Soundcloud Socials", "Soc Soundcloud Location", "Soc Soundcloud Image"})

    processed_count = 0
    batch_queue = []
    processed_ids = set() # Track to avoid infinite loops on failed updates
    today_str = date.today().isoformat()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            viewport={'width': 1920, 'height': 1080}
        )

        while True:
            r = requests.get(url, headers=AIRTABLE_HEADERS, params=params)
            data = r.json()
            if "error" in data:
                print(f"      [ERROR] Airtable API error: {data['error'].get('message')}")
                break
                
            records = data.get("records", [])
            if not records:
                break
                
            print(f"  [DEBUG] Fetched {len(records)} records from Airtable.")

            new_work_this_page = 0
            for record in records:
                rec_id = record["id"]
                if rec_id in processed_ids:
                    continue # Already tried this record in this run

                fields = record.get("fields", {})
                sc_url = fields.get("Soc Soundcloud")
                name = fields.get("Name", "Unknown")

                processed_count += 1
                new_work_this_page += 1
                processed_ids.add(rec_id)
                print(f"[{processed_count}] Processing {name} ({sc_url})")
                
                if not sc_url:
                    print("      [SKIP] No SoundCloud URL.")
                    continue

                sc_data = await scrape_soundcloud(context, sc_url)
                if sc_data:
                    # Filter for only existing Airtable fields
                    final_fields = {"Last Check": today_str}
                    for k, v in sc_data.items():
                        if k in actual_fields:
                            final_fields[k] = v
                    
                    batch_queue.append({"id": rec_id, "fields": final_fields})
                    
                    fol = final_fields.get('Soc Soundcloud Followers', '0')
                    loc = final_fields.get('Soc Soundcloud Location', 'Not found')
                    print(f"      --> Found: {fol} followers | Loc: {loc}")

                # Batch update
                if len(batch_queue) >= 10:
                    update_records_bulk(batch_queue)
                    batch_queue = []
                    print("  [INFO] Batch updated.")

                if args.limit and processed_count >= args.limit:
                    break

            if args.limit and processed_count >= args.limit:
                break
            
            # Paging Logic:
            # If we are using the "Todo" filter, we stay on Page 1 because items drop out.
            # If we are using --all, the result set stays the same, so we MUST use offset.
            if args.all:
                if "offset" in data:
                    params["offset"] = data["offset"]
                else:
                    break
            else:
                # Todo mode: if we didn't find any "new" work on this page, try the next
                # otherwise just hit the first page again to get the items that shifted forward.
                if new_work_this_page == 0 and "offset" in data:
                     params["offset"] = data["offset"]
                else:
                     # Clean offset for fresh Page 1 fetch
                     if "offset" in params:
                         del params["offset"]
                     # If the whole page was already processed and no offset left, we're done.
                     if new_work_this_page == 0:
                         break

        # Final flush
        if batch_queue:
            update_records_bulk(batch_queue)
            print("  [INFO] Final batch updated.")

        await browser.close()

    print(f"Finished. Processed {processed_count} records.")

if __name__ == "__main__":
    asyncio.run(main())
