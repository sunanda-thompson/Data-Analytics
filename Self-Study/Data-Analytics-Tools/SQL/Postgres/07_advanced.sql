-- ============================================================
-- 07_advanced.sql
-- Topic: Views, Indexes, Partitions, User-Defined Functions,
--        Stored Procedures, Constraints, UNION, INTERSECT
-- Database: PostgreSQL
-- ============================================================


-- ============================================================
-- SECTION 1: VIEWS
-- ============================================================
-- A VIEW is a saved query that behaves like a table.
-- It stores the INSTRUCTIONS (the SQL), not the data itself.
-- Every time you query a view, it reruns the underlying query
-- and returns fresh data.
--
-- Benefits:
--   - Simplifies complex queries for yourself and teammates
--   - Data always stays current (no sync issues)
--   - Hides complexity — users just SELECT from the view

-- Example: create a view that filters products to PS4/PS5 only
CREATE VIEW ps_products_vw AS (
    SELECT *
    FROM products
    WHERE description LIKE '%PlayStation 5%'
       OR description LIKE '%PS5%'
       OR description LIKE '%PS4%'
       OR description LIKE '%PlayStation 4%'
);

-- Now anyone can query it like a regular table — no filter needed
SELECT * FROM ps_products_vw;

-- Compare: a temp table would go out of sync if the products table changes.
-- A view always reflects current data automatically.

-- IMPORTANT difference:
-- Temp table  → stores a snapshot of data at creation time
-- View        → stores instructions; reruns every time you query it


-- ============================================================
-- SECTION 2: CREATE TABLE AS SELECT (CTAS)
-- ============================================================
-- Useful when you want to save a filtered copy of a table
-- as its own permanent table for repeated use.

CREATE TABLE ps_products AS (
    SELECT *
    FROM products
    WHERE description LIKE '%PlayStation 5%'
       OR description LIKE '%PS5%'
       OR description LIKE '%PS4%'
       OR description LIKE '%PlayStation 4%'
);

SELECT * FROM ps_products;


-- ============================================================
-- SECTION 3: UPDATE FROM SELECT
-- ============================================================
-- If data in the source table changes and you need to sync
-- a derived table, use UPDATE with a subquery.

-- First, simulate stale data by nulling out prices:
UPDATE ps_products SET price = NULL;

-- Verify the update:
SELECT * FROM ps_products;

-- Now resync the prices from the original products table:
UPDATE ps_products
SET price = p.price
FROM (
    SELECT product_id, price
    FROM products
) p
WHERE ps_products.product_id = p.product_id;
-- Note: we use the full table name (not an alias) in the WHERE clause
-- because the database needs to know which 'product_id' we mean.


-- ============================================================
-- SECTION 4: UNION, UNION ALL, INTERSECT
-- ============================================================
-- These combine results from multiple SELECT statements.
--
-- UNION ALL: combines ALL rows from both queries, including duplicates
-- UNION:     combines rows and REMOVES duplicates
-- INTERSECT: returns ONLY rows that appear in BOTH queries

-- UNION ALL — stack foods and drinks into one result
SELECT
    food_id  AS item_id,
    item_name,
    package_price,
    'food'   AS source_table
FROM foods

UNION ALL

SELECT
    drink_id AS item_id,
    item_name,
    package_price,
    'drink'  AS source_table
FROM drinks;

-- UNION — combine employee names from two tables, no duplicates
SELECT first_name FROM employees
UNION
SELECT first_name FROM friends;

-- INTERSECT — only names that appear in BOTH tables
SELECT food_name FROM all_foods
INTERSECT
SELECT food_name FROM favorite_foods;


-- ============================================================
-- SECTION 5: INDEXES
-- ============================================================
-- An index is like a book's index: it stores a pre-sorted
-- reference so the database can find matching rows quickly
-- without scanning every single row.
--
-- When to create one: on columns you frequently filter on
--                     (e.g. WHERE product_id = 1744)
--
-- Trade-off:
--   ✅ Faster SELECT queries
--   ❌ Slower INSERT/UPDATE/DELETE (index must be updated too)
--   ❌ Takes up storage space
--   ❌ Needs to be rebuilt when data changes significantly
-- Do NOT index every column blindly.

CREATE INDEX transaction_items_product_id_idx
ON transaction_items (product_id);

-- Run a query — should be faster after the index is created:
SELECT * FROM transaction_items WHERE product_id = 1744;

-- Remove an index if it's not helping:
DROP INDEX transaction_items_product_id_idx;


-- ============================================================
-- SECTION 6: PARTITIONS
-- ============================================================
-- Partitioning physically divides a large table into smaller
-- chunks based on a column value (usually a date).
-- When you query with a date filter, PostgreSQL only looks
-- at the relevant partition instead of scanning all billions of rows.
--
-- Benefit: much faster queries on large tables
-- Cost: more setup work upfront

-- Create the parent partitioned table:
CREATE TABLE partitioned_transactions (
    customer_id    BIGINT,
    trans_dt       DATE,
    transaction_id BIGINT,
    items_in_trans INTEGER,
    store_id       BIGINT
) PARTITION BY RANGE (trans_dt);

-- Create yearly child partitions:
-- Note: upper bound is EXCLUSIVE — 2022-01-01 goes into the 2022 partition
CREATE TABLE transactions_2020 PARTITION OF partitioned_transactions
FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');

CREATE TABLE transactions_2021 PARTITION OF partitioned_transactions
FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');

CREATE TABLE transactions_2022 PARTITION OF partitioned_transactions
FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');

-- Insert data (goes automatically to the right partition)
INSERT INTO partitioned_transactions
SELECT * FROM transactions;

-- Query via the parent table — PostgreSQL routes to correct partition
SELECT * FROM partitioned_transactions WHERE trans_dt = '2021-02-01';

-- Or query a specific partition directly
SELECT * FROM transactions_2021;


-- ============================================================
-- SECTION 7: CONSTRAINTS
-- ============================================================
-- Constraints are rules on a column that prevent invalid data
-- from being inserted. They protect data integrity.
-- Example: only allow last_name = 'Belcher' in a family table.
--
-- ⚠️ Trade-off: every insert/update must be checked against the rule,
-- which adds load to the database. Use sparingly in production.

CREATE TABLE bobs_family (
    customer_id BIGSERIAL PRIMARY KEY,
    first_name  VARCHAR(255),
    last_name   VARCHAR(255) CHECK (last_name = 'Belcher')
);

INSERT INTO bobs_family (first_name, last_name) VALUES ('Bob', 'Belcher');    -- ✅ works
INSERT INTO bobs_family (first_name, last_name) VALUES ('Phillip', 'Frond'); -- ❌ violates constraint


-- ============================================================
-- SECTION 8: USER-DEFINED FUNCTIONS
-- ============================================================
-- You can create your own functions to reuse complex logic.
-- This follows the DRY principle: write it once, use it everywhere.
--
-- Example: getting the last day of a month requires 3 steps.
-- Wrapping that in a function means you just call one word.

CREATE FUNCTION last_day_of_month (IN input_date DATE, OUT result DATE)
AS $$
    SELECT cast(
        date_trunc('month', input_date)  -- go to the 1st of this month
        + interval '1 month'             -- jump to the 1st of next month
        - interval '1 day'               -- go back one day = last day of this month
    AS DATE)
$$ LANGUAGE SQL;

-- Test the function:
SELECT last_day_of_month(cast('2023-05-06' AS DATE));
-- Returns: 2023-05-31

-- Use it in a query:
SELECT
    t.*,
    last_day_of_month(t.trans_dt) AS last_day_of_trans_month
FROM transactions t;


-- ============================================================
-- SECTION 9: STORED PROCEDURES
-- ============================================================
-- Stored procedures are like functions but they don't return
-- a value — they just execute a series of actions.
--
-- Use case: automating a multi-step operation, like adding a
-- new customer to two different tables at the same time.

CREATE TABLE family_members (member_id BIGSERIAL PRIMARY KEY, member_name VARCHAR(255));
CREATE TABLE bank_accounts  (member_id BIGSERIAL PRIMARY KEY, member_name VARCHAR(255));

-- This procedure inserts into BOTH tables with one call:
CREATE PROCEDURE add_new_member (name VARCHAR(255))
LANGUAGE SQL
AS $$
    INSERT INTO family_members (member_name) VALUES (name);
    INSERT INTO bank_accounts  (member_name) VALUES (name);
$$;

-- Execute the procedure:
CALL add_new_member('Baby Yoda');

-- Confirm both tables were updated:
SELECT * FROM family_members;
SELECT * FROM bank_accounts;
