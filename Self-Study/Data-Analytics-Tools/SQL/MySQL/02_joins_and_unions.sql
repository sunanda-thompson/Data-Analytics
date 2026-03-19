-- ============================================================
-- 02_joins_and_unions.sql
-- Topic: INNER JOIN, LEFT JOIN, RIGHT JOIN, SELF JOIN,
--        Multiple Joins, UNION, UNION ALL, UNION DISTINCT
-- Database: MySQL
-- Dataset: Parks & Recreation
-- ============================================================


-- ============================================================
-- SECTION 1: PREVIEW YOUR TABLES BEFORE JOINING
-- ============================================================
-- Always look at both tables before trying to join them.
-- Understand what column they share in common.

SELECT * FROM employee_demographics;
SELECT * FROM employee_salary;
# These two tables share 'employee_id' — that's our join column.

SELECT * FROM parks_departments;
# This is a reference/lookup table — usually no duplicates, clean data.


-- ============================================================
-- SECTION 2: INNER JOIN
-- ============================================================
-- Returns ONLY rows where the join condition matches in BOTH tables.
-- Any row that doesn't have a matching partner is excluded.
--
-- Use when: you only want records that exist in both tables.

# Basic inner join — using full table names
SELECT *
FROM employee_demographics
INNER JOIN employee_salary
    ON employee_demographics.employee_id = employee_salary.employee_id;
-- NOTE: employee #2 won't appear if there's no entry in employee_demographics

# Using aliases (shorthand — much cleaner to write)
SELECT *
FROM employee_demographics AS dem
INNER JOIN employee_salary AS sal
    ON dem.employee_id = sal.employee_id;

# Return specific columns only (best practice)
SELECT
    dem.employee_id,
    dem.age,
    sal.occupation
FROM employee_demographics AS dem
INNER JOIN employee_salary AS sal
    ON dem.employee_id = sal.employee_id;


-- ============================================================
-- SECTION 3: LEFT JOIN (most commonly used OUTER JOIN)
-- ============================================================
-- Returns ALL rows from the LEFT table.
-- For rows with no match in the right table, NULL fills in.
--
-- Use when: your left table is your "main" dataset and you
-- want to enrich it with data from another table,
-- but still keep records even if there's no match.

SELECT *
FROM employee_demographics AS dem       -- LEFT table (keep all of these)
LEFT OUTER JOIN employee_salary AS sal  -- RIGHT table (join where possible)
    ON dem.employee_id = sal.employee_id;
-- Employee #2 from demographics will appear with NULL salary columns
-- because there's no matching record in employee_salary


-- ============================================================
-- SECTION 4: RIGHT JOIN
-- ============================================================
-- Returns ALL rows from the RIGHT table.
-- NULLs fill in where there's no match on the left side.
--
-- In practice: most people just flip the table order
-- and use LEFT JOIN instead. It reads more naturally.

SELECT *
FROM employee_demographics AS dem       -- LEFT table
RIGHT OUTER JOIN employee_salary AS sal -- RIGHT table (keep ALL of these)
    ON dem.employee_id = sal.employee_id;
-- If employee_salary has a record with no matching demographics,
-- the demographics columns come back as NULL


-- ============================================================
-- SECTION 5: SELF JOIN
-- ============================================================
-- A table joined to itself. Looks unusual but has real uses.
--
-- Example: a "Secret Santa" assignment where each employee
-- is paired with the next employee (ID + 1).
-- Both "sides" of the join are the same table, just with different aliases.

SELECT *
FROM employee_salary emp1
JOIN employee_salary emp2
    ON emp1.employee_id + 1 = emp2.employee_id;
# Each row shows employee emp1 and the person right after them (emp2)

# With cleaner column names:
SELECT
    emp1.employee_id    AS secret_santa_id,
    emp1.first_name     AS secret_santa_first,
    emp1.last_name      AS secret_santa_last,
    emp2.employee_id    AS recipient_id,
    emp2.first_name     AS recipient_first,
    emp2.last_name      AS recipient_last
FROM employee_salary emp1
JOIN employee_salary emp2
    ON emp1.employee_id + 1 = emp2.employee_id;


-- ============================================================
-- SECTION 6: JOINING MULTIPLE TABLES
-- ============================================================
-- You can chain as many JOINs as you need.
-- Each JOIN adds another table to the result.

SELECT *
FROM employee_demographics AS dem
INNER JOIN employee_salary AS sal
    ON dem.employee_id = sal.employee_id
INNER JOIN parks_departments pd
    ON sal.dept_id = pd.department_id;
-- Result: employee info + salary + department name all in one row


-- ============================================================
-- SECTION 7: UNION and UNION ALL
-- ============================================================
-- UNION stacks the results of two SELECT statements on top of each other.
-- Both queries MUST have the same number of columns, in the same order.
--
-- UNION:     removes duplicate rows from the combined result
-- UNION ALL: keeps ALL rows including duplicates (faster — no dedup step)

# ❌ Bad example — columns don't logically match:
SELECT age, gender FROM employee_demographics
UNION
SELECT first_name, last_name FROM employee_salary;
# This runs but the result makes no sense — age vs first_name in same column

# ✅ Good example — matching columns:
SELECT first_name, last_name FROM employee_demographics
UNION
SELECT first_name, last_name FROM employee_salary;

# UNION DISTINCT — same as UNION (explicit about removing duplicates)
SELECT first_name, last_name FROM employee_demographics
UNION DISTINCT
SELECT first_name, last_name FROM employee_salary;

# UNION ALL — keeps duplicates (use when you don't want deduplication)
SELECT first_name, last_name FROM employee_demographics
UNION ALL
SELECT first_name, last_name FROM employee_salary;


-- ============================================================
-- SECTION 8: UNION WITH LABELS
-- ============================================================
-- A handy technique: add a constant "label" column to each
-- part of the UNION so you know which table each row came from.

# Label employees by age category
SELECT first_name, last_name, 'Old Man'  AS label
FROM employee_demographics
WHERE age > 40 AND gender = 'Male'

UNION

SELECT first_name, last_name, 'Old Lady' AS label
FROM employee_demographics
WHERE age > 40 AND gender = 'Female'

UNION

SELECT first_name, last_name, 'Highly Paid' AS label
FROM employee_salary
WHERE salary > 70000

ORDER BY first_name, last_name;
