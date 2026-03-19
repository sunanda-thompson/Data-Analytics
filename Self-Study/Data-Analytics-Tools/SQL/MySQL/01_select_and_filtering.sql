-- ============================================================
-- 01_select_and_filtering.sql
-- Topic: SELECT, DISTINCT, WHERE, LIKE, GROUP BY,
--        ORDER BY, HAVING, LIMIT, ALIASING
-- Database: MySQL
-- Dataset: Parks & Recreation (employee_demographics, employee_salary)
-- ============================================================
-- MySQL and PostgreSQL are both SQL databases, but there are
-- small syntax differences. The biggest ones you'll notice:
--   - Comments: MySQL uses #, PostgreSQL uses --
--   - ILIKE doesn't exist in MySQL (LIKE is always case-insensitive by default)
--   - LIMIT works differently (offset uses comma syntax)
-- ============================================================


-- ============================================================
-- SECTION 1: SELECT — CHOOSING COLUMNS
-- ============================================================
-- SELECT tells the database WHICH columns to return.
-- * means "all columns."
-- You can also do basic math directly in a SELECT.

-- Return all columns from the table
SELECT *
FROM parks_and_recreation.employee_demographics;

-- Return specific columns and do math on a column
SELECT
    first_name,
    last_name,
    birth_date,
    age,
    age + 10        -- PEMDAS (order of operations) applies here
FROM parks_and_recreation.employee_demographics;

-- Math with parentheses (evaluated first)
SELECT
    first_name,
    last_name,
    age,
    (age + 10) * 10   -- 10 added first, then multiplied by 10
FROM parks_and_recreation.employee_demographics;


-- ============================================================
-- SECTION 2: DISTINCT — REMOVING DUPLICATES
-- ============================================================
-- Without DISTINCT, you get one row per record.
-- With DISTINCT, duplicate values are removed.

-- Shows every row's gender value (many duplicates)
SELECT gender
FROM parks_and_recreation.employee_demographics;

-- Shows only unique gender values (e.g. Male, Female)
SELECT DISTINCT gender
FROM parks_and_recreation.employee_demographics;


-- ============================================================
-- SECTION 3: WHERE — FILTERING ROWS
-- ============================================================
-- WHERE filters which rows are returned.
-- Comparison operators: =, !=, >, >=, <, <=

-- Only return records for Leslie
SELECT *
FROM employee_salary
WHERE first_name = 'Leslie';

-- Salary above 50,000
SELECT *
FROM employee_salary
WHERE salary > 50000;

-- Salary 50,000 or more (>= means "greater than or equal to")
SELECT *
FROM employee_salary
WHERE salary >= 50000;

-- Everyone who is NOT Female
SELECT *
FROM employee_demographics
WHERE gender != 'Female';

-- Born after January 1, 1985
SELECT *
FROM employee_demographics
WHERE birth_date > '1985-01-01';


-- ============================================================
-- SECTION 4: LOGICAL OPERATORS — AND, OR, NOT
-- ============================================================

# AND: BOTH conditions must be true
SELECT *
FROM employee_demographics
WHERE birth_date > '1985-01-01'
  AND gender = 'male';

# OR: AT LEAST ONE condition must be true
SELECT *
FROM employee_demographics
WHERE birth_date > '1985-01-01'
   OR gender = 'male';

# NOT: reverses the condition (true becomes false)
SELECT *
FROM employee_demographics
WHERE birth_date > '1985-01-01'
   OR NOT gender = 'male';

# Nested conditions with parentheses (very important for correctness)
# "Leslie who is 44" OR "anyone over 55"
SELECT *
FROM employee_demographics
WHERE (first_name = 'Leslie' AND age = 44)
   OR age > 55;


-- ============================================================
-- SECTION 5: LIKE — PATTERN MATCHING
-- ============================================================
-- LIKE finds partial matches.
-- % = wildcard (any number of characters)
-- _ = exactly one character

# Names starting with 'Jer' (anything can follow)
SELECT *
FROM employee_demographics
WHERE first_name LIKE 'Jer%';

# Names with 'er' anywhere in them
SELECT *
FROM employee_demographics
WHERE first_name LIKE '%er%';

# Names starting with 'a'
SELECT *
FROM employee_demographics
WHERE first_name LIKE 'a%';

# Names starting with 'a' followed by exactly 2 more characters
SELECT *
FROM employee_demographics
WHERE first_name LIKE 'a__';   -- 'a' + 2 underscores = 3 characters total

# Names starting with 'a' followed by exactly 3 more characters
SELECT *
FROM employee_demographics
WHERE first_name LIKE 'a___';  -- 'a' + 3 underscores = 4 characters total

# Names starting with 'a' then 3 characters then anything after
SELECT *
FROM employee_demographics
WHERE first_name LIKE 'a___%'; -- 'a' + 3 chars + any more after that

# Find anyone born in 1989
SELECT *
FROM employee_demographics
WHERE birth_date LIKE '1989%';


-- ============================================================
-- SECTION 6: GROUP BY + AGGREGATE FUNCTIONS
-- ============================================================
-- GROUP BY collapses rows into groups.
-- Aggregate functions (AVG, MAX, MIN, COUNT) summarize each group.
-- Rule: every column in SELECT that is NOT an aggregate function
-- must appear in GROUP BY.

# Count employees per gender
SELECT gender
FROM employee_demographics
GROUP BY gender;

# Average age by gender
SELECT gender, AVG(age)   # AVG is an aggregate function
FROM employee_demographics
GROUP BY gender;

# Multiple aggregates at once
SELECT
    gender,
    AVG(age)  AS avg_age,
    MAX(age)  AS max_age,
    MIN(age)  AS min_age,
    COUNT(age) AS count
FROM employee_demographics
GROUP BY gender;


-- ============================================================
-- SECTION 7: ORDER BY
-- ============================================================
-- ORDER BY sorts the result set.
-- Default is ASC (A→Z, smallest→largest).
-- Add DESC to flip it.

# Alphabetical by first name (default = ASC)
SELECT *
FROM employee_demographics
ORDER BY first_name;

# Reverse alphabetical
SELECT *
FROM employee_demographics
ORDER BY first_name DESC;

# Sort by gender first, then by age within each gender
SELECT *
FROM employee_demographics
ORDER BY gender, age;   -- gender has few unique values; age provides the fine-grained sort

# Sort by gender ASC, then age DESC
SELECT *
FROM employee_demographics
ORDER BY gender, age DESC;


-- ============================================================
-- SECTION 8: HAVING vs WHERE
-- ============================================================
-- WHERE filters BEFORE grouping (on individual rows)
-- HAVING filters AFTER grouping (on aggregate results)
-- You cannot use WHERE on an aggregate function — use HAVING.

# ❌ This throws an error:
# SELECT gender, AVG(age)
# FROM employee_demographics
# WHERE AVG(age) > 40      -- can't use aggregate in WHERE
# GROUP BY gender;

# ✅ Correct — use HAVING:
SELECT gender, AVG(age) AS avg_age
FROM employee_demographics
GROUP BY gender
HAVING AVG(age) > 40;

# Combining WHERE (filter rows first) + HAVING (filter groups second)
SELECT occupation, AVG(salary) AS avg_salary
FROM employee_salary
WHERE occupation LIKE '%manager%'      -- only include managers
GROUP BY occupation
HAVING AVG(salary) > 75000             -- only show groups with avg salary above 75k
;


-- ============================================================
-- SECTION 9: LIMIT
-- ============================================================
-- LIMIT restricts how many rows are returned.
-- Useful for previewing data or getting top N results.

# Just the first 3 rows
SELECT *
FROM employee_demographics
LIMIT 3;

# The 3 oldest employees
SELECT *
FROM employee_demographics
ORDER BY age DESC
LIMIT 3;

# MySQL-specific: LIMIT offset, count
# "Start at position 2 (skip 2 rows), then return 1 row"
SELECT *
FROM employee_demographics
ORDER BY age DESC
LIMIT 2, 1;


-- ============================================================
-- SECTION 10: ALIASING (Renaming columns in the output)
-- ============================================================
-- AS renames a column in the result set.
-- This makes output cleaner and also lets you reference the
-- alias name in HAVING and ORDER BY in MySQL.

# Without alias — output column is called 'AVG(age)'
SELECT gender, AVG(age)
FROM employee_demographics
GROUP BY gender
HAVING AVG(age) > 40;

# With alias — output column is called 'avg_age', more readable
SELECT gender, AVG(age) AS avg_age
FROM employee_demographics
GROUP BY gender
HAVING avg_age > 40;   -- MySQL allows using the alias in HAVING

# AS is optional in MySQL — both lines below are equivalent:
SELECT gender, AVG(age) AS avg_age FROM employee_demographics GROUP BY gender;
SELECT gender, AVG(age)    avg_age FROM employee_demographics GROUP BY gender;
