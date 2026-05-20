import asyncio
import os
import time
import argparse
from datetime import datetime
from urllib.parse import urlparse, unquote_plus, parse_qs
from supabase import create_client

# ── Config ─────────────────────────────────────────────────────────────────────
SUPABASE_URL     = os.environ['SUPABASE_URL']
SUPABASE_KEY     = os.environ['SUPABASE_SERVICE_KEY']

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Helpers ────────────────────────────────────────────────────────────────────
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

# ── Scraper Logic ──────────────────────────────────────────────────────────────
async def scrape_soundcloud(browser_context, url):
    """Scrapes a single SoundCloud profile."""
    if not url or "soundcloud.com" not in url:
        return None

    page = await browser_context.new_page()
    # Apply stealth
    from playwright_stealth import Stealth
    await Stealth().apply_stealth_async(page)

    data = {}
    try:
        print(f"  --> Navigating to {url}")
        # Wait for base load
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(6)  # Allow dynamic content to stabilize
        except Exception as e:
            print(f"      [ERROR] Could not load {url}: {e}", flush=True)
            return None

        # 1. Handle Cookie Consent
        try:
            cookie_btn = page.locator("button#onetrust-accept-btn-handler, button:has-text('Accept'), button:has-text('I accept')")
            if await cookie_btn.count() > 0 and await cookie_btn.is_visible():
                await cookie_btn.click()
                await asyncio.sleep(1)
        except Exception as e:
            pass  # Silent fail on cookie handling

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
                        data[f'sc_{label.lower()}'] = str(format_sc_number(count_str))
                    else:
                        # 2. Display count (e.g. 25.1K)
                        val_el = link.locator(".infoStats__statValue, div, span").first
                        if await val_el.count() > 0:
                            txt = await val_el.inner_text()
                            data[f'sc_{label.lower()}'] = str(format_sc_number(txt))
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
                data['sc_bio'] = await bio_el.inner_text()
        except:
            pass

        # 4. Verified Badge
        try:
            is_verified = await page.locator(".verifiedBadge, .verifiedBadge__icon").count() > 0
            data['sc_verified'] = is_verified
        except:
            pass

        # 5. Location
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
                    data['sc_location'] = txt
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
                    data['sc_image'] = img_url
        except:
            pass

        # 7. Social Links (Comma Separated)
        socials = []
        try:
            links = page.locator("a.web-profile, a.sc-link-secondary[href*='gate.sc']")
            count = await links.count()
            for i in range(count):
                href = await links.nth(i).get_attribute("href")
                if href:
                    if "gate.sc" in href:
                        try:
                            parsed = urlparse(href)
                            query = parse_qs(parsed.query)
                            actual = query.get('url', [href])[0]
                            decoded = unquote_plus(actual)
                            socials.append(decoded.strip())
                        except:
                            socials.append(href.strip())
                    else:
                        socials.append(href.strip())

            if socials:
                # Deduplicate while preserving order
                data['sc_socials'] = ", ".join(list(dict.fromkeys(socials)))
        except:
            pass

    except Exception as e:
        print(f"      [FATAL] Scrape logic failed for {url}: {e}", flush=True)
        return None
    finally:
        await page.close()

    return data


# ── Record Processor ───────────────────────────────────────────────────────────
def process_soundcloud_record(record: dict, scraped_data: dict) -> bool:
    """Update one hb_socials record in Supabase with SoundCloud data."""
    social_id = record["id"]
    now = datetime.utcnow().isoformat()

    # Build update object
    update_data = {
        "check_soundcloud_enrichment": now,
        "updated_at": now,
    }

    # Map scraped fields to hb_socials columns
    if "sc_followers" in scraped_data:
        try:
            update_data["followers"] = int(scraped_data["sc_followers"])
        except (ValueError, TypeError):
            pass

    if "sc_bio" in scraped_data and scraped_data["sc_bio"]:
        update_data["description"] = scraped_data["sc_bio"]

    if "sc_image" in scraped_data and scraped_data["sc_image"]:
        update_data["image"] = scraped_data["sc_image"]

    if "sc_verified" in scraped_data:
        update_data["verified"] = bool(scraped_data["sc_verified"])

    # Capture additional fields in history_json (preserve old data)
    history_json = record.get("history_json") or {}
    if "sc_following" in scraped_data:
        history_json["sc_following"] = scraped_data["sc_following"]
    if "sc_tracks" in scraped_data:
        history_json["sc_tracks"] = scraped_data["sc_tracks"]
    if "sc_socials" in scraped_data:
        history_json["sc_socials"] = scraped_data["sc_socials"]
    if "sc_location" in scraped_data:
        history_json["sc_location"] = scraped_data["sc_location"]

    if history_json:
        update_data["history_json"] = history_json

    # Perform update
    try:
        supabase.table('hb_socials').update(update_data).eq('id', social_id).execute()
        return True
    except Exception as e:
        print(f"  [FAIL] Supabase update failed: {e}", flush=True)
        return False


# ── Main Workflow ──────────────────────────────────────────────────────────────
async def main():
    parser = argparse.ArgumentParser(description="Enrich SoundCloud profiles in Supabase.")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()

    limit = args.limit if not args.all else None
    if not args.all and not args.limit:
        limit = 5

    LIMIT = limit or 500
    print(f"Starting SoundCloud Supabase Enrichment (limit={LIMIT})...", flush=True)

    # Fetch stale SoundCloud records
    response = (
        supabase.table('hb_socials')
        .select('id, social_url, name, identifier, history_json')
        .eq('type', 'SOUNDCLOUD')
        .not_.is_('social_url', 'null')
        .order('check_soundcloud_enrichment', desc=False, nullsfirst=True)
        .limit(LIMIT)
        .execute()
    )

    records = response.data or []
    print(f"Fetched {len(records)} records.", flush=True)

    processed = 0

    # Launch browser once for all profiles
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            viewport={'width': 1920, 'height': 1080}
        )

        for i, record in enumerate(records):
            identifier = record.get("name") or record.get("identifier") or record.get("social_url") or record.get("id")
            social_url = (record.get("social_url") or "").strip()
            print(f"[{i+1}/{len(records)}] {identifier}", end=" ", flush=True)

            if not social_url:
                print("  [SKIP] No SoundCloud URL.", flush=True)
                continue

            try:
                scraped_data = await scrape_soundcloud(context, social_url)
                if scraped_data and process_soundcloud_record(record, scraped_data):
                    fol = scraped_data.get('sc_followers', 'n/a')
                    print(f"  OK (followers: {fol})", flush=True)
                    processed += 1
                else:
                    print(f"  [FAIL] Scraping or update failed.", flush=True)
            except Exception as e:
                print(f"  [FAIL] Uncaught error: {e}", flush=True)

            time.sleep(1.2)  # Polite scraping

        await browser.close()

    print(f"\nDone. Processed {processed} records.", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
