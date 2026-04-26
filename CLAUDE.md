# CLAUDE.md — `yl-hb-sc` (SoundCloud enrichment via Airtable)

Conventions shared across the `yl-hb-*` fleet live in
[`SCRAPER-CLAUDE-TEMPLATE.md`](../SCRAPER-CLAUDE-TEMPLATE.md). **This repo
diverges from the template** because it doesn't talk to Supabase at all
and uses Playwright (not Puppeteer).

## What this repo does

Scrapes **SoundCloud** artist pages with Playwright (stealth) for
followers/following/tracks (extracted from `title=` attributes for
exactness), verified status, full biography (auto-expands "Show more"),
and cleaned `gate.sc` redirect socials. Bulk-PATCHes results back into
Airtable.

## Stack

**Custom variant — Python + Playwright + Airtable.** No Supabase. Single
script `main.py`, single workflow.

## Repo layout

```
main.py
requirements.txt                      # playwright, playwright-stealth, requests, python-dotenv
README.md
.github/workflows/
  enrich.yml
```

## Auth

> Convention divergence: no `SUPABASE_*` env vars.

```
AIRTABLE_API_KEY        # required
```

Airtable base / table / view IDs are hardcoded:

```
BASE_ID  = appmbuoYupyA1iB3j
TABLE_ID = tblJ7pSMe1p0iTyif
VIEW_ID  = viwpMtssgHDhyUfEH
```

`load_dotenv` is called twice — once for the local `.env` and once for the
parent directory's `.env` (this lets the script find auth from a parent
monorepo when nested).

## Workflow lifecycle convention

> Convention divergence: no `log_workflow_run`. Dashboard won't see runs.

## Tables this repo touches

Airtable only.

## Running locally

```bash
pip install -r requirements.txt
playwright install chromium --with-deps
export AIRTABLE_API_KEY=...
python main.py
```

## Per-repo gotchas

- **Playwright stealth is mandatory.** SoundCloud blocks bare browsers.
  `playwright-stealth` is already in `requirements.txt` — keep it.
- **Followers/following/tracks counts come from `title=` attributes**
  (which contain the exact integer), not the rendered text (which shows
  rounded "1.2k"). Parsing the rendered text would silently lose precision.
- **The "Show more" biography expander must run before scraping the bio**
  — otherwise truncated bios get persisted. Don't reorder that step.
- **`gate.sc` URLs are SoundCloud's outbound redirect wrapper.** The
  cleaning step decodes the percent-encoded target and strips the wrapper
  so the social fields end up with the real destination URL.
- **Chromium-with-deps is required in CI** for Playwright; the workflow
  installs it explicitly.

## Conventions Claude should follow when editing this repo

- **Use Playwright, not Puppeteer.** This repo is the only Python+browser
  scraper in the fleet; switching to Puppeteer would mean rewriting in JS.
- **Don't add a Supabase client unless the data layer is intentionally
  migrating off Airtable.**
- **Preserve the dual-`.env` load pattern** — it's how this script finds
  auth when nested under a parent repo.

## Related repos

- `yl-hb-ig`, `yl-hb-sk`, `yl-hb-tw` — sibling Airtable-only scrapers.
- The remaining `yl-hb-*` repos write to Supabase.
