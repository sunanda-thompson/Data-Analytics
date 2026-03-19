-- ============================================================
-- 02_filtering.sql
-- Topic: WHERE, AND, OR, IN, BETWEEN, LIKE, NULL, DISTINCT
-- Database: PostgreSQL
-- ============================================================
-- Filtering is how you ask the database to show you only the
-- rows you care about, instead of every single row in a table.
-- ============================================================


-- Setup: recreate the customers table for these examples
DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    customer_id   BIGINT,
    customer_name VARCHAR(255),
    birth_year    INTEGER,
    birth_month   INTEGER,
    gender        VARCHAR(255),
    address       VARCHAR(255),
    address_2     VARCHAR(255),
    city          VARCHAR(255),
    state_abv     CHARACTER(2),
    zip_code      VARCHAR(255),
    per_capita_income_by_zip_code NUMERIC(14,2),
    yearly_income NUMERIC(14,2)
);


-- ============================================================
-- SECTION 1: BASIC WHERE FILTERING
-- ============================================================
-- WHERE lets you filter rows based on a condition.
-- Only rows where the condition is TRUE come back.

-- Show all customers born in March (month 3)
-- Because birth_month is an integer, no quotes needed
SELECT *
FROM customers
WHERE birth_month = 3;

-- Show customers with income above $50,000
SELECT *
FROM customers
WHERE yearly_income > 50000;

-- Show customers NOT from Texas
SELECT *
FROM customers
WHERE state_abv != 'TX';

-- Using >= (greater than or equal to)
SELECT *
FROM customers
WHERE yearly_income >= 75000;


-- ============================================================
-- SECTION 2: AND / OR / NOT
-- ============================================================
-- Combine multiple conditions to narrow or broaden your results.
--
-- AND  → BOTH conditions must be true
-- OR   → AT LEAST ONE condition must be true
-- NOT  → reverses a condition (true becomes false, false becomes true)

-- AND: born in March AND female
SELECT *
FROM customers
WHERE birth_month = 3
  AND lower(gender) = 'female';   -- lower() makes comparison case-insensitive

-- OR: born in March OR income over 75k
SELECT *
FROM customers
WHERE birth_month = 3
   OR yearly_income > 75000;

-- Nested AND + OR: Use parentheses to control order of evaluation
-- (just like math: parentheses are evaluated first)
-- Here: birth year in range AND (either gender)
SELECT *
FROM customers
WHERE birth_year BETWEEN 1961 AND 1980
  AND (lower(gender) = 'female' OR lower(gender) = 'male');


-- ============================================================
-- SECTION 3: BETWEEN
-- ============================================================
-- BETWEEN is shorthand for >= AND <=. Both endpoints are inclusive.

-- Customers born between 1961 and 1980 (both years included)
SELECT *
FROM customers
WHERE birth_year BETWEEN 1961 AND 1980;

-- This is identical to the above — just more verbose:
SELECT *
FROM customers
WHERE birth_year >= 1961
  AND birth_year <= 1980;


-- ============================================================
-- SECTION 4: IN
-- ============================================================
-- IN lets you match against a list of values.
-- It's cleaner than writing multiple OR conditions.
-- Keep the list under ~20 values for readability.

-- Born in March, August, or October
SELECT *
FROM customers
WHERE birth_month IN (3, 8, 10);

-- Works with text too
SELECT *
FROM customers
WHERE gender IN ('Male', 'Female');

-- The long way (equivalent but messier):
SELECT *
FROM customers
WHERE birth_month = 3
   OR birth_month = 8
   OR birth_month = 10;


-- ============================================================
-- SECTION 5: LIKE / ILIKE (Pattern Matching)
-- ============================================================
-- Use when you don't know the exact value but know part of it.
--
-- %  → wildcard: matches anything (any number of characters)
-- _  → matches exactly ONE character
--
-- LIKE  → case-sensitive
-- ILIKE → case-insensitive (PostgreSQL only)

-- Names that start with "Hazel" (anything can follow)
SELECT *
FROM customers
WHERE customer_name LIKE 'Hazel%';

-- Case-insensitive version (safer):
SELECT *
FROM customers
WHERE customer_name ILIKE 'hazel%';

-- Names that contain "son" anywhere
SELECT *
FROM customers
WHERE customer_name ILIKE '%son%';

-- Names that are exactly 4 characters starting with 'a'
SELECT *
FROM customers
WHERE customer_name ILIKE 'a___';  -- 'a' + exactly 3 more characters


-- ============================================================
-- SECTION 6: NULL CHECKS
-- ============================================================
-- NULL means a value is unknown or missing — it's not zero or blank.
-- You cannot use = NULL. You must use IS NULL or IS NOT NULL.

-- Customers who don't have a second address line
SELECT *
FROM customers
WHERE address_2 IS NULL;

-- Customers who DO have a second address line
SELECT *
FROM customers
WHERE address_2 IS NOT NULL;

-- Illustrating the difference between NULL and an empty string:
SELECT
    '' AS blank_cell,   -- has space allocated, but nothing in it
    NULL AS null_cell;  -- no space allocated — truly unknown


-- ============================================================
-- SECTION 7: DISTINCT
-- ============================================================
-- DISTINCT removes duplicate values from your result.
-- Useful for quickly seeing what unique values exist in a column.

-- Shows every gender value including duplicates (one row per customer)
SELECT customer_name FROM customers;

-- Shows only unique gender values (e.g. Male, Female, Non-binary)
SELECT DISTINCT gender FROM customers;


-- ============================================================
-- SECTION 8: UPPER / LOWER (Standardizing Text)
-- ============================================================
-- Real-world data is messy. The same value might appear as
-- "MALE", "Male", or "male" in different rows.
-- Use lower() or upper() to standardize before comparing.

SELECT
    gender,
    lower(gender) AS lower_gender,
    upper(gender) AS upper_gender
FROM customers;

-- Best practice: apply lower() in both the column and the comparison value
SELECT *
FROM customers
WHERE lower(gender) = 'female';


-- ============================================================
-- SECTION 9: SELECTING SPECIFIC COLUMNS (best practice)
-- ============================================================
-- Using SELECT * is fine for quick exploration, but in real work
-- you should name the columns you want. It:
--   - makes your query easier to read
--   - prevents issues if the table structure changes
--   - shows where each column comes from when using multiple tables

-- Using table alias 'c' to be explicit about where columns come from
SELECT
    c.customer_id,
    c.customer_name,
    c.birth_year,
    c.birth_month,
    c.gender,
    c.city,
    c.state_abv,
    c.yearly_income
FROM customers c
WHERE c.birth_month = 3
  AND lower(c.gender) = 'female';
