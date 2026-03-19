-- ============================================================
-- 01_basics.sql
-- Topic: Creating Tables, Inserting Data, Updating & Deleting
-- Database: PostgreSQL
-- ============================================================
-- This script covers the DDL (Data Definition Language) and DML
-- (Data Manipulation Language) basics — the building blocks of
-- working with any database.
--
-- DDL = commands that define the structure of your database
--       (CREATE, ALTER, DROP)
-- DML = commands that work with the data inside tables
--       (INSERT, UPDATE, DELETE, SELECT)
-- ============================================================


-- ============================================================
-- SECTION 1: CREATING A TABLE
-- ============================================================
-- CREATE TABLE builds a new table in your database.
-- You must define each column's name and data type.
--
-- Common data types:
--   VARCHAR(255) → text up to 255 characters
--   INT          → whole numbers
--   BIGINT       → very large whole numbers (use for IDs — you never know how many records you'll have)
--   NUMERIC(x,y) → decimal numbers (x = total digits, y = digits after the decimal)
--   DATE         → stores a date (YYYY-MM-DD)
--   TIMESTAMP    → stores a date AND time
--   BOOLEAN      → true or false

CREATE TABLE house_addresses (
    street           VARCHAR(255),
    city             VARCHAR(255),
    state_abv        VARCHAR(2),     -- only 2 characters needed for a state abbreviation
    zip_code         INT,
    house_built_date DATE
);


-- ============================================================
-- SECTION 2: MODIFYING TABLE STRUCTURE (ALTER TABLE)
-- ============================================================
-- After creating a table, you can add or rename columns.

-- Rename a column
ALTER TABLE house_addresses
RENAME zip_code TO zip;

-- Add a new column to an existing table
ALTER TABLE house_addresses
ADD unique_id BIGINT;


-- ============================================================
-- SECTION 3: DROPPING (DELETING) A TABLE
-- ============================================================
-- DROP TABLE permanently removes the table AND all its data.
-- Use with extreme caution in a real environment.

DROP TABLE house_addresses;

-- Safer version: only drops the table if it exists.
-- Avoids an error if the table doesn't exist yet.
DROP TABLE IF EXISTS house_addresses;


-- ============================================================
-- SECTION 4: INSERTING DATA
-- ============================================================
-- INSERT INTO adds new rows to a table.
-- Values must be listed in the same order as the columns,
-- and text values must be wrapped in single quotes.
-- Use NULL for unknown or missing values.

-- First, recreate the table so we have something to insert into:
CREATE TABLE house_addresses (
    street           VARCHAR(255),
    city             VARCHAR(255),
    state_abv        VARCHAR(2),
    zip_code         INT,
    house_built_date DATE
);

INSERT INTO house_addresses
VALUES ('123 Main Street', 'Austin', 'TX', 78704, NULL);

INSERT INTO house_addresses
VALUES ('5800 South Congress', 'Austin', 'TX', 78704, NULL);

INSERT INTO house_addresses
VALUES ('123 Main Street', 'Midland', 'TX', 11111, NULL);

INSERT INTO house_addresses
VALUES ('123 Main Street', 'Dallas', 'TX', 22222, NULL);


-- ============================================================
-- SECTION 5: VIEWING YOUR DATA (SELECT)
-- ============================================================
-- SELECT is how you read data from a table.
-- The * means "all columns."

SELECT *
FROM house_addresses;


-- ============================================================
-- SECTION 6: UPDATING EXISTING RECORDS
-- ============================================================
-- UPDATE changes the value in one or more columns for rows
-- that match your WHERE condition.
--
-- ⚠️ WARNING: If you forget the WHERE clause, EVERY row gets updated.
-- Always run a SELECT first to confirm which rows you're targeting.

-- First, check what you're about to update:
SELECT *
FROM house_addresses
WHERE city = 'Austin';

-- Then run the update:
UPDATE house_addresses
SET zip_code = 78702
WHERE city = 'Austin';


-- ============================================================
-- SECTION 7: DELETING RECORDS
-- ============================================================
-- DELETE removes rows from a table.
--
-- ⚠️ WARNING: Same as UPDATE — missing WHERE deletes ALL rows.
-- Always preview with SELECT before running DELETE.

-- Preview first:
SELECT *
FROM house_addresses
WHERE city = 'Austin';

-- Then delete:
DELETE FROM house_addresses
WHERE city = 'Austin';


-- ============================================================
-- SECTION 8: AUTO-INCREMENTING PRIMARY KEYS
-- ============================================================
-- In real tables, every row needs a unique ID.
-- BIGSERIAL PRIMARY KEY auto-generates this ID for you —
-- you don't have to manually provide a number each time you insert.

CREATE TABLE customers (
    customer_id BIGSERIAL PRIMARY KEY,  -- auto-increments: 1, 2, 3, 4...
    first_name  VARCHAR(255),
    last_name   VARCHAR(255)
);

-- Notice we don't provide a value for customer_id — PostgreSQL handles it
INSERT INTO customers (first_name, last_name) VALUES ('Bob', 'Belcher');
INSERT INTO customers (first_name, last_name) VALUES ('Louise', 'Belcher');
INSERT INTO customers (first_name, last_name) VALUES ('Tina', 'Belcher');

SELECT * FROM customers;

-- Even if you delete all rows and reinsert, the sequence keeps going.
-- It won't restart at 1 — this is intentional to avoid ID collisions.
DELETE FROM customers;

INSERT INTO customers (first_name, last_name) VALUES ('Bob', 'Belcher');

SELECT * FROM customers;
-- customer_id will be 4, not 1 — because the sequence remembers
