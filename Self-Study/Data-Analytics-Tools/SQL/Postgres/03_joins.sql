-- ============================================================
-- 03_joins.sql
-- Topic: INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL OUTER, SELF JOIN
-- Database: PostgreSQL
-- ============================================================
-- JOINs combine data from two or more tables based on a
-- related column (usually an ID).
--
-- Think of it like this: you have a customers table and a states
-- table. The customers table has a state_id number, and the states
-- table translates that number into an abbreviation like "TX".
-- A JOIN lets you pull both together in one result.
-- ============================================================


-- Setup: create and populate tables for these examples

CREATE TABLE customers (
    customer_id BIGINT,
    first_name  VARCHAR(255),
    last_name   VARCHAR(255),
    address_1   VARCHAR(255),
    address_2   VARCHAR(255),
    city        VARCHAR(255),
    zip_code    VARCHAR(255),
    state_id    BIGINT
);

CREATE TABLE states (
    state_id  BIGINT,
    state_abv CHAR(2)
);


-- ============================================================
-- SECTION 1: INNER JOIN
-- ============================================================
-- INNER JOIN returns ONLY the rows where the condition matches
-- in BOTH tables. If a customer has a state_id that doesn't
-- exist in the states table, that customer is excluded.
--
-- Use when: you only want rows that have a match on both sides.

SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.city,
    c.state_id          AS cust_table_state_id,
    s.state_id          AS states_table_state_id,
    s.state_abv
FROM customers c            -- 'c' is an alias for the customers table
JOIN states s               -- 's' is an alias for the states table
    ON c.state_id = s.state_id;   -- the matching condition


-- ============================================================
-- SECTION 2: LEFT JOIN (most commonly used)
-- ============================================================
-- LEFT JOIN returns ALL rows from the LEFT table (customers),
-- and only the matching rows from the RIGHT table (states).
-- If there's no match, the right side columns come back as NULL.
--
-- Use when: you want to keep all records from your main table,
-- even if some don't have a match in the other table.

-- All customers, even ones with no matching state in the states table
SELECT
    c.customer_id,
    c.first_name,
    c.state_id,
    s.state_abv         -- this will be NULL if there's no matching state
FROM customers c
LEFT JOIN states s
    ON c.state_id = s.state_id;


-- ============================================================
-- SECTION 3: RIGHT JOIN
-- ============================================================
-- RIGHT JOIN returns ALL rows from the RIGHT table (states),
-- and only the matching rows from the LEFT table (customers).
-- NULL appears for any customer columns with no match.
--
-- Note: In practice, most people never use RIGHT JOIN.
-- You can always flip the table order and use LEFT JOIN instead.
-- It's just easier to read left to right.

SELECT
    c.customer_id,
    c.first_name,
    s.state_id,
    s.state_abv
FROM customers c
RIGHT JOIN states s
    ON c.state_id = s.state_id;


-- ============================================================
-- SECTION 4: FULL OUTER JOIN
-- ============================================================
-- FULL OUTER JOIN returns ALL rows from BOTH tables.
-- Where there's no match, NULL fills in the gaps.
--
-- Use when: you want a complete picture — every row from
-- both tables, matched where possible, NULLs where not.

-- Create subset tables for a clear example:
CREATE TABLE states_only_tx_mt AS
SELECT * FROM states
WHERE state_abv = 'TX' OR state_abv = 'MT';

CREATE TABLE customers_ca_mt_only AS
SELECT * FROM customers
WHERE state_id = 1 OR state_id = 33;

-- Full picture: all states, all customers — matched where possible
SELECT *
FROM customers_ca_mt_only c
FULL OUTER JOIN states_only_tx_mt s
    ON c.state_id = s.state_id;


-- ============================================================
-- SECTION 5: SELF JOIN
-- ============================================================
-- A SELF JOIN joins a table to itself.
-- The most common use case: an employees table where one column
-- holds an employee's manager_id, and the manager is also
-- an employee in the same table.

CREATE TABLE employees (
    employee_id  BIGINT,
    first_name   VARCHAR(255),
    manager_id   BIGINT,         -- references another employee_id
    address      VARCHAR(255),
    city         VARCHAR(255),
    state_abv    VARCHAR(2),
    zip_code     VARCHAR(20),
    phone_number VARCHAR(20)
);

-- Show only employees WHO HAVE a manager (INNER JOIN version)
-- Each employee is joined to their manager from the same table
SELECT
    e.employee_id,
    e.first_name                AS employee_name,
    e.manager_id,
    mgr.first_name              AS manager_name,
    mgr.phone_number            AS manager_phone
FROM employees e
JOIN employees mgr
    ON e.manager_id = mgr.employee_id;   -- match employee's manager_id to another employee's ID

-- Show ALL employees, including those without a manager (LEFT JOIN version)
-- Employees with no manager will have NULL in the manager columns
SELECT
    e.employee_id,
    e.first_name                AS employee_name,
    mgr.first_name              AS manager_name,
    mgr.phone_number            AS manager_phone
FROM employees e
LEFT JOIN employees mgr
    ON e.manager_id = mgr.employee_id;


-- ============================================================
-- SECTION 6: JOINING MULTIPLE TABLES
-- ============================================================
-- You can chain multiple JOINs in a single query.
-- Each JOIN adds another table to the mix.

-- Example: join customers → states to get abbreviation,
-- then join to a zip_codes table to get income data
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    s.state_abv
FROM customers c
JOIN states s
    ON c.state_id = s.state_id;
-- Add more JOINs below if you have additional related tables


-- ============================================================
-- SECTION 7: CROSS JOIN (avoid unless you have a specific reason)
-- ============================================================
-- CROSS JOIN matches EVERY row in one table with EVERY row
-- in another table. This produces a cartesian product.
-- If customers has 100 rows and states has 50 rows,
-- the result has 5,000 rows (100 × 50).
--
-- This usually happens by accident when you forget to write
-- your ON condition. Rarely useful intentionally.

SELECT *
FROM customers
CROSS JOIN states;
-- ⚠️ This will return a huge number of rows — be careful
