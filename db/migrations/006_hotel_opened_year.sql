-- Migration 006 — hotel_master.opened_year
-- Part of: TravelLens Phase 5/6 hardening — entity-count prompt rule (B-? sibling)
-- File:    db/migrations/006_hotel_opened_year.sql
--
-- What this creates:
--   Adds the `opened_year` column to hotel_master. The column holds the
--   year each hotel opened to guests, as a SMALLINT.
--
-- Why:
--   The 7B SQL-generating LLM was answering "hotels per year" by counting
--   booking rows in fact_bookings — the L-010 dimension-vs-fact confusion.
--   Giving hotel_master its own opened_year column makes the correct
--   query trivially expressible (SELECT opened_year, COUNT(*) FROM
--   hotel_master GROUP BY opened_year) and removes any reason for the
--   model to ever join fact_bookings for that question.
--
--   Population logic — including the hard constraint that opened_year must
--   be <= the hotel's earliest booking year — lives in the populate
--   script, not in this migration. Migrations stay schema-only and
--   idempotent; data population is a separate concern.
--
-- Run with:
--   docker exec travellens-postgres psql -U travellens -d travellens \
--     -f /db/migrations/006_hotel_opened_year.sql
--
-- Idempotent — safe to re-run (ADD COLUMN IF NOT EXISTS).

ALTER TABLE hotel_master
    ADD COLUMN IF NOT EXISTS opened_year SMALLINT;

-- Verify
SELECT 'migration 006 applied'        AS status,
       COUNT(*)                       AS hotel_count,
       COUNT(opened_year)             AS with_opened_year,
       MIN(opened_year)               AS min_year,
       MAX(opened_year)               AS max_year
FROM hotel_master;
