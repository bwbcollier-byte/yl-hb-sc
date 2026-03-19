# SoundCloud Enrichment (`yl-hb-sc`)

Automated pipeline for enriching Airtable artist profiles with comprehensive SoundCloud metadata.

## Features
- **Precise Followers/Following/Tracks**: Extracts exact counts from HTML `title` attributes.
- **Verified Status**: Monitors for verified badges.
- **Biography**: Automatically expands "Show more" to capture full text.
- **Cleaned Socials**: Decodes and unquotes `gate.sc` redirects to direct links.
- **Micro-Repo Structure**: Optimized for scheduled execution via GitHub Actions.

## Setup & Local Usage

1. **Clone & Install**:
   ```bash
   pip install -r requirements.txt
   playwright install chromium --with-deps
   ```

2. **Environment**:
   Ensure `.env` contains `AIRTABLE_API_KEY`.

3. **Run Locally**:
   ```bash
   python main.py
   ```

## Workflow Details
- **GitHub Action**: Scheduled to run every 6 hours (`.github/workflows/enrich.yml`).
- **Secret Management**: Requires `AIRTABLE_API_KEY` to be defined in GitHub repository secrets.

---
*Created by Antigravity*
