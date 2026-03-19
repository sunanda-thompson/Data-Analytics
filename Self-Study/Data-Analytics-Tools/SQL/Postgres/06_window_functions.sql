-- ============================================================
-- 06_window_functions.sql
-- Topic: Window Functions — ROW_NUMBER, RANK, DENSE_RANK,
--        NTILE, SUM OVER, AVG OVER, LAG, LEAD
-- Database: PostgreSQL
-- ============================================================
-- Window functions perform calculations ACROSS a set of related
-- rows — without collapsing them into one row like GROUP BY does.
--
-- The key difference:
--   GROUP BY → reduces many rows into one summary row per group
--   Window   → keeps all rows, but adds a calculated column
--              based on surrounding rows
--
-- All window functions use the OVER() clause.
-- PARTITION BY inside OVER() works like GROUP BY — it defines
-- the "window" (the group of rows to calculate over).
-- ============================================================


-- ============================================================
-- SECTION 1: MIN / MAX / AVG / COUNT OVER A WINDOW
-- ============================================================
-- Instead of a single summary value for the whole table,
-- these return the value PER group — on every row.

SELECT
    t.*,
    min(t.items_in_trans) OVER (PARTITION BY t.trans_dt) AS min_items_that_day,
    max(t.items_in_trans) OVER (PARTITION BY t.trans_dt) AS max_items_that_day,
    avg(t.items_in_trans) OVER (PARTITION BY t.trans_dt) AS avg_items_that_day,
    count(t.items_in_trans) OVER (PARTITION BY t.trans_dt) AS total_trans_that_day
FROM transactions t;

-- Each row still shows its own data, but also includes the
-- daily min/max/avg/count in the same result.


-- ============================================================
-- SECTION 2: ROW_NUMBER
-- ============================================================
-- Assigns a unique sequential number to each row.
-- Useful for getting the "1st, 2nd, 3rd..." record.

-- Number all rows ordered by date
SELECT
    t.*,
    row_number() OVER (ORDER BY t.trans_dt) AS row_num
FROM transactions t;

-- Number transactions PER CUSTOMER (reset to 1 for each customer)
-- So customer 1 gets row 1, 2, 3... and customer 2 also starts at 1
SELECT
    t.*,
    row_number() OVER (PARTITION BY t.customer_id ORDER BY t.trans_dt) AS trans_number_for_this_customer
FROM transactions t;


-- ============================================================
-- SECTION 3: RANK vs DENSE_RANK
-- ============================================================
-- Both rank rows, but they handle ties differently.
--
-- RANK:       ties get the same number, then skips → 1, 1, 3, 4
-- DENSE_RANK: ties get the same number, NO gaps  → 1, 1, 2, 3

SELECT
    t.*,
    row_number()  OVER (ORDER BY t.items_in_trans DESC) AS row_num,
    rank()        OVER (ORDER BY t.items_in_trans DESC) AS rank_with_gaps,
    dense_rank()  OVER (ORDER BY t.items_in_trans DESC) AS rank_no_gaps
FROM transactions t
WHERE t.trans_dt = '2022-01-12';

-- Use RANK when gaps matter (e.g. sports standings)
-- Use DENSE_RANK when you want consecutive numbers despite ties


-- ============================================================
-- SECTION 4: NTILE — BUCKETS (QUARTILES, PERCENTILES)
-- ============================================================
-- NTILE(n) divides rows into n equal buckets and assigns
-- each row a bucket number from 1 to n.
--
-- ntile(4) = quartiles (Q1, Q2, Q3, Q4)
-- ntile(100) = percentiles (1st through 100th)

-- Assign each transaction to a quartile based on items
SELECT
    t.*,
    ntile(4)   OVER (ORDER BY t.items_in_trans) AS quartile,
    ntile(100) OVER (ORDER BY t.items_in_trans) AS percentile
FROM transactions t
WHERE t.trans_dt = '2022-01-12';

-- Summarize: show the minimum value for each quartile
WITH quartile_data AS (
    SELECT
        t.*,
        ntile(4) OVER (ORDER BY t.items_in_trans) AS quartile
    FROM transactions t
    WHERE t.trans_dt = '2022-01-12'
)
SELECT
    quartile,
    min(items_in_trans) AS min_value,
    max(items_in_trans) AS max_value
FROM quartile_data
GROUP BY quartile
ORDER BY quartile;


-- ============================================================
-- SECTION 5: RUNNING TOTALS (SUM OVER with ORDER BY)
-- ============================================================
-- Adding ORDER BY inside OVER() changes the behavior:
-- instead of the total for the whole partition, you get
-- a running (cumulative) total up to the current row.

-- Running total of daily sales, oldest to newest
WITH daily_totals AS (
    SELECT
        t.trans_dt,
        sum(p.price) AS total_sales
    FROM transactions t
    JOIN transaction_items ti ON t.transaction_id = ti.transaction_id
    JOIN products p            ON p.product_id = ti.product_id
    GROUP BY t.trans_dt
)
SELECT
    trans_dt,
    total_sales,
    sum(total_sales) OVER (ORDER BY trans_dt) AS running_total
FROM daily_totals
ORDER BY trans_dt;

-- The explicit syntax below is equivalent — it shows what's
-- happening under the hood:
-- "Sum all rows from the beginning up to the current row"
SELECT
    trans_dt,
    total_sales,
    sum(total_sales) OVER (
        ORDER BY trans_dt
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM daily_totals
ORDER BY trans_dt;

-- Trailing 3-day average (current row + 2 preceding rows)
SELECT
    trans_dt,
    total_sales,
    cast(
        avg(total_sales) OVER (
            ORDER BY trans_dt
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )
    AS INT) AS trailing_3d_avg
FROM daily_totals
ORDER BY trans_dt;


-- ============================================================
-- SECTION 6: LAG and LEAD
-- ============================================================
-- LAG looks at the PREVIOUS row's value.
-- LEAD looks at the NEXT row's value.
-- Both return NULL for the first/last row where there's no neighbor.
--
-- Use case: "What was the transaction ID before this one?"

-- LAG: look back (NULL appears at the top — no "previous" for row 1)
SELECT
    t.*,
    lag(t.transaction_id) OVER (ORDER BY t.trans_dt) AS previous_transaction_id
FROM transactions t;

-- LEAD: look forward (NULL appears at the bottom — no "next" for last row)
SELECT
    t.*,
    lead(t.transaction_id) OVER (ORDER BY t.trans_dt) AS next_transaction_id
FROM transactions t;

-- Alternative to LAG/LEAD using ROW_NUMBER + self-join:
WITH numbered AS (
    SELECT
        t.*,
        row_number() OVER (ORDER BY t.trans_dt, t.transaction_id) AS row_num
    FROM transactions t
)
SELECT
    t.*,
    p.transaction_id AS prior_transaction_id
FROM numbered t
LEFT JOIN numbered p
    ON (t.row_num - 1) = p.row_num  -- join to the row that is one position earlier
ORDER BY t.row_num;
