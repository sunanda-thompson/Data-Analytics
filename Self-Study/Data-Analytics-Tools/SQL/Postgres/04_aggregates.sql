-- ============================================================
-- 04_aggregates.sql
-- Topic: COUNT, SUM, AVG, MIN, MAX, GROUP BY, HAVING, ORDER BY
-- Database: PostgreSQL
-- ============================================================
-- Aggregate functions collapse many rows into a single summary
-- value. For example: instead of seeing 1,000 individual
-- transaction amounts, you can see the total, average, or count.
--
-- They almost always go hand-in-hand with GROUP BY.
-- ============================================================


-- ============================================================
-- SECTION 1: BASIC AGGREGATE FUNCTIONS
-- ============================================================
-- These functions summarize an entire column into one number.

SELECT
    count(*)                    AS total_rows,        -- counts every row
    count(address_2)            AS rows_with_address2, -- counts only non-NULL values
    sum(yearly_income)          AS total_income,
    avg(yearly_income)          AS average_income,
    min(yearly_income)          AS lowest_income,
    max(yearly_income)          AS highest_income
FROM customers;

-- count(*) vs count(column):
--   count(*) counts ALL rows including those with NULLs
--   count(column) counts only rows where that column is NOT NULL


-- ============================================================
-- SECTION 2: MATH ON AGGREGATE RESULTS
-- ============================================================
-- You can apply math directly to aggregate functions.

-- Give everyone a 5% raise: multiply total income by 1.05
SELECT
    sum(yearly_income) * 1.05  AS total_with_5pct_raise,
    sum(yearly_income) * 0.95  AS total_with_5pct_cut
FROM customers;

-- Manually calculate average (to verify AVG function):
SELECT
    (sum(yearly_income) / count(*))  AS manual_average,
    avg(yearly_income)               AS built_in_average
FROM customers;


-- ============================================================
-- SECTION 3: GROUP BY
-- ============================================================
-- GROUP BY splits your data into groups based on a column,
-- then applies the aggregate function to each group separately.
--
-- Rule: every column in SELECT that is NOT an aggregate
-- function MUST also appear in GROUP BY.
--
-- Example: instead of total income for all customers,
-- get total income broken down by state.

SELECT
    state_abv,
    count(*)                AS customer_count,
    sum(yearly_income)      AS total_income,
    avg(yearly_income)      AS avg_income
FROM customers
GROUP BY state_abv;     -- one row of results per unique state

-- Group by multiple columns
SELECT
    state_abv,
    gender,
    count(*)    AS customer_count
FROM customers
GROUP BY state_abv, gender;  -- one row per unique state + gender combination


-- ============================================================
-- SECTION 4: ORDER BY
-- ============================================================
-- ORDER BY sorts your results. Default is ascending (A→Z, 0→9).

-- Sort by customer count, highest first
SELECT
    state_abv,
    count(*) AS customer_count
FROM customers
GROUP BY state_abv
ORDER BY customer_count DESC;   -- DESC = descending (highest first)

-- Sort by multiple columns: first by state, then by count within each state
SELECT
    state_abv,
    gender,
    count(*) AS customer_count
FROM customers
GROUP BY state_abv, gender
ORDER BY state_abv ASC, customer_count DESC;


-- ============================================================
-- SECTION 5: HAVING (filtering on aggregates)
-- ============================================================
-- HAVING filters AFTER grouping, the way WHERE filters before.
--
-- Key difference:
--   WHERE  → filters individual rows BEFORE the aggregation
--   HAVING → filters groups AFTER the aggregation
--
-- You cannot use WHERE to filter on an aggregate result
-- like count(*) or sum() — that's what HAVING is for.

-- ❌ This will throw an error — you can't use WHERE on an aggregate:
-- SELECT state_abv, count(*) AS customer_count
-- FROM customers
-- WHERE count(*) > 100
-- GROUP BY state_abv;

-- ✅ Correct: use HAVING to filter after grouping
SELECT
    state_abv,
    count(*) AS customer_count
FROM customers
GROUP BY state_abv
HAVING count(*) > 100;

-- Combining WHERE and HAVING:
-- WHERE filters rows first (before grouping)
-- HAVING filters groups second (after grouping)
SELECT
    state_abv,
    count(*)       AS customer_count,
    avg(yearly_income) AS avg_income
FROM customers
WHERE yearly_income > 20000          -- filter out rows with low income first
GROUP BY state_abv
HAVING count(*) > 50                  -- then only show states with 50+ remaining customers
ORDER BY customer_count DESC;


-- ============================================================
-- SECTION 6: FINDING DUPLICATES
-- ============================================================
-- A common use of GROUP BY + HAVING is to find duplicate records.
-- If a name appears more than once, something might be wrong.

-- Find customer names that appear more than once
SELECT
    customer_name,
    count(*) AS record_count
FROM customers
GROUP BY customer_name
HAVING count(*) > 1
ORDER BY record_count DESC;


-- ============================================================
-- SECTION 7: CONCAT + COALESCE (useful with aggregates)
-- ============================================================
-- CONCAT combines text from multiple columns into one string
SELECT
    concat(first_name, ' - ', city) AS name_and_city
FROM customers;

-- COALESCE returns the first non-NULL value from a list.
-- Useful for replacing NULLs with a readable fallback.
SELECT
    customer_name,
    address_2,
    coalesce(address_2, 'No second address provided') AS address_2_cleaned
FROM customers;
