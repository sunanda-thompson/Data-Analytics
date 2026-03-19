-- ============================================================
-- 08_date_functions.sql
-- Topic: Date Math, EXTRACT, DATE_TRUNC, CAST, TIME ZONES,
--        SPLIT_PART, TRIM, LENGTH, CONCAT, COALESCE
-- Database: PostgreSQL
-- ============================================================
-- Dates are one of the trickiest parts of SQL.
-- Almost every business question involves time:
--   "How many orders this month?"
--   "How many days since the last purchase?"
--   "Show me data for Q4 only"
-- ============================================================


-- ============================================================
-- SECTION 1: CURRENT DATE AND TIMESTAMP
-- ============================================================

SELECT current_date;        -- today's date (no time)
SELECT current_timestamp;   -- today's date + current time with time zone


-- ============================================================
-- SECTION 2: DATE ARITHMETIC
-- ============================================================
-- You can add or subtract days, months, years from a date
-- using the INTERVAL keyword.

SELECT
    t.transaction_id,
    t.transaction_date,
    (current_date - t.transaction_date) AS days_since_transaction,
    t.transaction_date + interval '1 day'   AS next_day,
    t.transaction_date + interval '1 month' AS next_month,
    t.transaction_date - interval '7 days'  AS one_week_ago
FROM transactions t;


-- ============================================================
-- SECTION 3: EXTRACTING PARTS OF A DATE
-- ============================================================
-- DATE_PART lets you pull out just the day, month, or year.

SELECT
    t.trans_dt,
    date_part('day',   t.trans_dt) AS day_of_month,
    date_part('month', t.trans_dt) AS month_number,
    date_part('year',  t.trans_dt) AS year_number
FROM transactions t;


-- ============================================================
-- SECTION 4: DATE_TRUNC — ROLLING DATES TO A PERIOD START
-- ============================================================
-- DATE_TRUNC rounds a date down to the start of a period.
-- Useful for monthly/quarterly/yearly reporting.
--
-- 'month' → first day of the month
-- 'year'  → January 1st of that year
-- 'week'  → Monday of that week

-- Roll every date to the first of its month
SELECT
    t.trans_dt,
    cast(date_trunc('month', t.trans_dt) AS DATE) AS first_day_of_month
FROM transactions t;

-- Get the LAST day of the month (trickier — no direct function)
-- Logic: go to first of this month → jump to first of next month → back one day
SELECT
    t.trans_dt,
    cast(
        date_trunc('month', t.trans_dt)  -- first day of this month
        + interval '1 month'             -- first day of next month
        - interval '1 day'               -- last day of this month
    AS DATE) AS last_day_of_month
FROM transactions t;

-- Last day of LAST month (one more step back)
SELECT
    t.trans_dt,
    cast(
        date_trunc('month', t.trans_dt)  -- first day of this month
        - interval '1 day'               -- last day of last month
    AS DATE) AS last_day_of_last_month
FROM transactions t;


-- ============================================================
-- SECTION 5: CAST — CHANGING DATA TYPES
-- ============================================================
-- CAST converts a value from one data type to another.
-- This is essential when you want to do math on text,
-- or compare a timestamp to a plain date.

-- Cast a text zip_code to an integer (so you can do math on it)
SELECT
    zip_code,
    cast(zip_code AS INT)       AS zip_as_int,
    cast(zip_code AS INT) + 2   AS zip_plus_two
FROM customers;

-- Cast a timestamp to just a date (removes the time portion)
SELECT
    current_timestamp,
    cast(current_timestamp AS DATE) AS today_date_only
FROM customers;

-- CAST is also written with the :: shorthand in PostgreSQL:
SELECT current_timestamp::DATE AS today_date_only;


-- ============================================================
-- SECTION 6: TIME ZONES
-- ============================================================
-- Databases often store timestamps in UTC.
-- When you want to display or calculate with a local time,
-- you need to convert using AT TIME ZONE.
--
-- Always match the time zone to YOUR time zone when doing
-- date calculations, otherwise your math will be off by hours.

SELECT
    f.food_id,
    f.item_name,
    f.price_last_updated_ts,
    f.price_last_updated_ts AT TIME ZONE 'America/Chicago' AS price_updated_cst
FROM foods f;

-- Days since price was last updated (using local time):
SELECT
    f.food_id,
    f.item_name,
    current_date - cast(
        (f.price_last_updated_ts AT TIME ZONE 'America/Chicago') AS DATE
    ) AS days_since_last_update
FROM foods f;

-- Filter to items not updated in 30+ days:
SELECT *
FROM (
    SELECT
        f.food_id,
        f.item_name,
        f.price_last_updated_ts AT TIME ZONE 'America/Chicago' AS cst_ts,
        current_date - cast(
            (f.price_last_updated_ts AT TIME ZONE 'America/Chicago') AS DATE
        ) AS days_since_last_update
    FROM foods f
) f
WHERE f.days_since_last_update > 30;


-- ============================================================
-- SECTION 7: SPLIT_PART — PARSING DATE STRINGS
-- ============================================================
-- Sometimes data comes in as a string like '2023M01' instead
-- of a proper date. SPLIT_PART lets you break it apart.
--
-- SPLIT_PART(string, delimiter, position)
--   string    → the text to split
--   delimiter → the character(s) to split on
--   position  → 1 for left side, 2 for right side

SELECT
    split_part('2023M01', 'M', 1) AS year_part,   -- returns '2023'
    split_part('2023M01', 'M', 2) AS month_part;  -- returns '01'

-- Convert the split parts into a proper date:
WITH parsed AS (
    SELECT
        split_part('2023M01', 'M', 1) AS yr,
        split_part('2023M01', 'M', 2) AS mo
)
SELECT
    cast(concat(yr, '-', mo, '-01') AS DATE) AS first_of_month
FROM parsed;
-- Returns: 2023-01-01


-- ============================================================
-- SECTION 8: TRIM & LENGTH — CLEANING WHITESPACE
-- ============================================================
-- Real-world data often has invisible whitespace characters
-- at the start or end of text. This causes problems:
-- WHERE name = 'Brandon' won't match ' Brandon ' (with spaces).

-- Create a test case to see the problem:
DROP TABLE IF EXISTS whitespace_test;
CREATE TEMP TABLE whitespace_test AS (
    SELECT ' Brandon Southern ' AS person_name
);

-- The column looks fine visually, but...
SELECT person_name FROM whitespace_test;

-- ...the length reveals hidden characters:
SELECT
    person_name,
    length(person_name)         AS raw_length,        -- 20 characters (with spaces)
    trim(person_name)           AS cleaned_name,
    length(trim(person_name))   AS cleaned_length      -- 16 characters (actual name)
FROM whitespace_test;

-- WHERE clause won't match because of the extra spaces:
SELECT * FROM whitespace_test WHERE person_name = 'Brandon Southern';      -- 0 rows

-- Fix: wrap in TRIM before comparing:
SELECT * FROM whitespace_test WHERE trim(person_name) = 'Brandon Southern'; -- 1 row
