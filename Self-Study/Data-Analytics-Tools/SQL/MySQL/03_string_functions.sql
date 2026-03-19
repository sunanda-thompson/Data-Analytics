-- ============================================================
-- 03_string_functions.sql
-- Topic: LENGTH, UPPER, LOWER, TRIM, LEFT, RIGHT,
--        SUBSTRING, REPLACE, LOCATE, CONCAT
-- Database: MySQL
-- ============================================================
-- String functions let you clean, format, and extract pieces
-- of text data. Real-world data is messy — names might be in
-- all caps, phone numbers might have dashes or spaces, etc.
-- These functions help you standardize it.
-- ============================================================


-- ============================================================
-- SECTION 1: LENGTH — count characters in a string
-- ============================================================
-- Returns the number of characters in a string.
-- Useful for validating data (e.g. zip codes should be 5 digits)
-- or finding records with unusually long/short values.

# How many characters is the word 'skyfall'?
SELECT LENGTH('skyfall');    -- returns 7

# Length of every employee's first name
SELECT
    first_name,
    LENGTH(first_name) AS name_length
FROM employee_demographics;

# Sort by name length shortest to longest
# (ORDER BY 2 means ORDER BY the 2nd column in the SELECT list)
SELECT
    first_name,
    LENGTH(first_name) AS name_length
FROM employee_demographics
ORDER BY 2;


-- ============================================================
-- SECTION 2: UPPER and LOWER — standardize text case
-- ============================================================
-- Text comparisons are often case-sensitive.
-- UPPER() and LOWER() let you standardize before comparing.
-- Best practice: apply LOWER() to both sides of a comparison
-- so 'MALE', 'Male', and 'male' all match.

SELECT UPPER('sky');    -- returns 'SKY'
SELECT LOWER('SKY');    -- returns 'sky'

# Make all first names uppercase (e.g. for a mailing label)
SELECT
    first_name,
    UPPER(first_name) AS upper_name
FROM employee_demographics;


-- ============================================================
-- SECTION 3: TRIM — remove whitespace
-- ============================================================
-- Whitespace (spaces, tabs) at the beginning or end of a
-- value can cause WHERE clause mismatches and join failures.
-- TRIM removes it.
--
-- TRIM  = removes spaces from both sides
-- LTRIM = removes spaces from the LEFT side only
-- RTRIM = removes spaces from the RIGHT side only

SELECT TRIM('      sky       ');    -- returns 'sky'
SELECT LTRIM('      sky       ');   -- returns 'sky       '
SELECT RTRIM('      sky       ');   -- returns '      sky'


-- ============================================================
-- SECTION 4: LEFT and RIGHT — extract characters from edges
-- ============================================================
-- LEFT(string, n)  → returns the first n characters
-- RIGHT(string, n) → returns the last n characters
-- Useful for pulling area codes, year from a date string, etc.

SELECT
    first_name,
    LEFT(first_name, 4)    AS first_4_chars,
    RIGHT(first_name, 4)   AS last_4_chars
FROM employee_demographics;


-- ============================================================
-- SECTION 5: SUBSTRING — extract from any position
-- ============================================================
-- SUBSTRING(string, start_position, length)
-- Extracts 'length' characters starting at 'start_position'.
-- Position 1 = first character (SQL is 1-indexed, not 0-indexed).

SELECT
    first_name,
    SUBSTRING(first_name, 3, 2)  AS chars_3_and_4,
    -- "Starting at position 3, give me 2 characters"
    birth_date,
    SUBSTRING(birth_date, 6, 2)  AS birth_month
    -- "Starting at position 6 of the date string, give me 2 characters"
    -- birth_date format: YYYY-MM-DD → position 6 is the month digits
FROM employee_demographics;


-- ============================================================
-- SECTION 6: REPLACE — swap characters
-- ============================================================
-- REPLACE(string, old_value, new_value)
-- Replaces every occurrence of old_value with new_value.
-- Useful for cleaning formatting: removing dashes from phone numbers,
-- replacing abbreviations, etc.

# Replace every 'a' with a 'z' in first names
SELECT
    first_name,
    REPLACE(first_name, 'a', 'z') AS replaced_name
FROM employee_demographics;


-- ============================================================
-- SECTION 7: LOCATE — find the position of a character
-- ============================================================
-- LOCATE(substring, string) returns the position (index) where
-- the substring first appears. Returns 0 if not found.
-- Useful when combined with SUBSTRING to extract dynamic content.

SELECT LOCATE('x', 'Alexander');
-- 'x' appears at position 4 in 'Alexander'


-- ============================================================
-- SECTION 8: CONCAT — combine multiple columns into one string
-- ============================================================
-- CONCAT(value1, separator, value2, ...)
-- Joins multiple values together into one string.
-- Common use: creating a "full name" from first and last name.

SELECT
    first_name,
    last_name,
    CONCAT(first_name, ' ', last_name) AS full_name
FROM employee_demographics;
