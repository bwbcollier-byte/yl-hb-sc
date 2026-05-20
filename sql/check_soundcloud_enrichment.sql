-- Add check_soundcloud_enrichment column to hb_socials if not exists
-- This column tracks when each SoundCloud profile was last refreshed
-- Run via: supabase db push (or manually via Supabase dashboard)

ALTER TABLE hb_socials
ADD COLUMN check_soundcloud_enrichment TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_hb_socials_check_soundcloud_enrichment
  ON hb_socials (check_soundcloud_enrichment NULLS FIRST)
  WHERE type = 'SOUNDCLOUD';
