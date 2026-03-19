-- ============================================================
-- 05_ctes_temp_tables.sql
-- Topic: CTEs, Temp Tables, Data Params, Subqueries
-- Database: PostgreSQL
-- ============================================================
-- As queries get more complex, subqueries become hard to read
-- and debug. CTEs and temp tables solve this by breaking a
-- complicated query into smaller, named, readable steps.
--
-- Think of it like showing your work in math class —
-- instead of one giant calculation, you break it into steps.
-- ============================================================


-- ============================================================
-- SECTION 1: THE PROBLEM — MESSY NESTED SUBQUERIES
-- ============================================================
-- This query classifies customers as 'new', 'dormant', 'churned',
-- or 'active' based on their transaction history.
-- It works, but it's hard to read and even harder to debug.
-- Every time you reference max(trans_dt), you repeat the same code.

SELECT
    t.customer_id,
    t.transaction_id,
    t.trans_dt,
    t.min_cust_trans_dt,
    CASE
        WHEN t.min_cust_trans_dt >= (SELECT max(trans_dt) FROM transactions) - interval '30 days'
            THEN 'new'
        WHEN t.has_trans_in_last_60d = 1 AND t.has_trans_in_last_30d = 0
            THEN 'dormant'
        WHEN t.has_trans_in_last_60d = 0 AND t.has_trans_in_last_30d = 0
            THEN 'churned'
        ELSE 'active'
    END AS customer_status
FROM (
    SELECT
        t.customer_id,
        t.transaction_id,
        t.trans_dt,
        min(t.trans_dt) OVER (PARTITION BY t.customer_id) AS min_cust_trans_dt,
        CASE WHEN t.trans_dt >= (SELECT max(trans_dt) FROM transactions) - interval '90 days' THEN 1 ELSE 0 END AS has_trans_in_last_90d,
        CASE WHEN t.trans_dt >= (SELECT max(trans_dt) FROM transactions) - interval '60 days' THEN 1 ELSE 0 END AS has_trans_in_last_60d,
        CASE WHEN t.trans_dt >= (SELECT max(trans_dt) FROM transactions) - interval '30 days' THEN 1 ELSE 0 END AS has_trans_in_last_30d
    FROM transactions t
) t
WHERE has_trans_in_last_90d = 1;


-- ============================================================
-- SECTION 2: BETTER — USING A CTE (Common Table Expression)
-- ============================================================
-- A CTE uses the WITH keyword to give a name to a subquery.
-- You define it once, then reference it by name below.
--
-- CTEs exist ONLY for the duration of the query they're in.
-- They don't save data anywhere — they just make code cleaner.
--
-- DRY Principle (Don't Repeat Yourself): if you write the same
-- code in multiple places, extract it into a CTE.

WITH trans_w_attributes AS (
    -- Step 1: Enrich each transaction with flags and the customer's first date
    SELECT
        t.customer_id,
        t.transaction_id,
        t.trans_dt,
        min(t.trans_dt) OVER (PARTITION BY t.customer_id) AS min_cust_trans_dt,

        -- Flag: did this transaction happen in the last 90 days?
        CASE WHEN t.trans_dt >= (SELECT max(trans_dt) FROM transactions) - interval '90 days'
             THEN 1 ELSE 0 END AS has_trans_in_last_90d,

        -- Flag: did this transaction happen in the last 60 days?
        CASE WHEN t.trans_dt >= (SELECT max(trans_dt) FROM transactions) - interval '60 days'
             THEN 1 ELSE 0 END AS has_trans_in_last_60d,

        -- Flag: did this transaction happen in the last 30 days?
        CASE WHEN t.trans_dt >= (SELECT max(trans_dt) FROM transactions) - interval '30 days'
             THEN 1 ELSE 0 END AS has_trans_in_last_30d
    FROM transactions t
)

-- Step 2: Use those flags to classify each customer
SELECT
    t.customer_id,
    t.transaction_id,
    t.trans_dt,
    t.min_cust_trans_dt,
    CASE
        WHEN t.min_cust_trans_dt >= (SELECT max(trans_dt) FROM transactions) - interval '30 days'
            THEN 'new'
        WHEN t.has_trans_in_last_60d = 1 AND t.has_trans_in_last_30d = 0
            THEN 'dormant'
        WHEN t.has_trans_in_last_60d = 0 AND t.has_trans_in_last_30d = 0
            THEN 'churned'
        ELSE 'active'
    END AS customer_status
FROM trans_w_attributes t
WHERE has_trans_in_last_90d = 1;


-- ============================================================
-- SECTION 3: EVEN BETTER — ADDING A DATA PARAMS TEMP TABLE
-- ============================================================
-- Notice the query still repeats (SELECT max(trans_dt) FROM transactions)
-- multiple times. We can extract that into its own temp table
-- so we only calculate it once.
--
-- TEMP TABLE: saved in memory for the current session.
-- Cleared automatically when you disconnect. Not stored in the database.

-- Step 1: Create a temp table with today's date (the max transaction date)
-- This gives us a single source of truth for the date parameter.
CREATE TEMP TABLE data_params AS (
    SELECT max(trans_dt) AS today_date
    FROM transactions
);

-- Step 2: Create a temp table with the enriched transactions
CREATE TEMP TABLE trans_w_attributes AS (
    SELECT
        t.customer_id,
        t.transaction_id,
        t.trans_dt,
        min(t.trans_dt) OVER (PARTITION BY t.customer_id) AS min_cust_trans_dt,

        CASE WHEN t.trans_dt >= (SELECT d.today_date FROM data_params d) - interval '90 days'
             THEN 1 ELSE 0 END AS has_trans_in_last_90d,

        CASE WHEN t.trans_dt >= (SELECT d.today_date FROM data_params d) - interval '60 days'
             THEN 1 ELSE 0 END AS has_trans_in_last_60d,

        CASE WHEN t.trans_dt >= (SELECT d.today_date FROM data_params d) - interval '30 days'
             THEN 1 ELSE 0 END AS has_trans_in_last_30d
    FROM transactions t
);

-- Step 3: Final query — clean and easy to read
SELECT
    t.customer_id,
    t.transaction_id,
    t.trans_dt,
    t.min_cust_trans_dt,
    CASE
        WHEN t.min_cust_trans_dt >= (SELECT d.today_date FROM data_params d) - interval '30 days'
            THEN 'new'
        WHEN t.has_trans_in_last_60d = 1 AND t.has_trans_in_last_30d = 0
            THEN 'dormant'
        WHEN t.has_trans_in_last_60d = 0 AND t.has_trans_in_last_30d = 0
            THEN 'churned'
        ELSE 'active'
    END AS customer_status
FROM trans_w_attributes t
WHERE has_trans_in_last_90d = 1;


-- ============================================================
-- SECTION 4: CTE vs TEMP TABLE — WHEN TO USE WHICH
-- ============================================================
--
-- CTE:
--   ✅ Makes a single query more readable
--   ✅ Zero setup required — just write WITH
--   ❌ Disappears after the query runs (can't reference it later)
--   ❌ Recalculated every time it's referenced
--
-- Temp Table:
--   ✅ Persists for the whole session — you can run it once, query many times
--   ✅ Useful for debugging: you can query the temp table mid-analysis
--   ❌ Slightly more setup required
--   ❌ Disappears when you disconnect
--
-- General guidance:
--   Use CTEs for readability in a single query.
--   Use temp tables when you need to reference results multiple times
--   or when you want to inspect intermediate results while debugging.
