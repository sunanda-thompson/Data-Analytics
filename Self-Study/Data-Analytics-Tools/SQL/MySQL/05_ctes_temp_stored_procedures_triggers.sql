-- ============================================================
-- 05_ctes_temp_stored_procedures_triggers.sql
-- Topic: CTEs, Temp Tables, Stored Procedures,
--        Parameters, Triggers, Events
-- Database: MySQL
-- Dataset: Parks & Recreation
-- ============================================================


-- ============================================================
-- SECTION 1: CTEs (Common Table Expressions)
-- ============================================================
-- A CTE lets you give a name to a complex subquery and then
-- reference it by that name below — like a variable in programming.
-- This makes long, nested queries much easier to read and debug.
--
-- Syntax: WITH cte_name AS ( ... ) SELECT ... FROM cte_name
--
-- ⚠️ Important MySQL rule: a CTE can ONLY be used in the SELECT
-- that immediately follows it. You cannot reference it again later.

# Basic CTE: calculate salary stats per gender, then find the average of averages
WITH CTE_Example AS (
    SELECT
        gender,
        AVG(salary)   AS avg_sal,
        MAX(salary)   AS max_sal,
        MIN(salary)   AS min_sal,
        COUNT(salary) AS count_sal
    FROM employee_demographics dem
    JOIN employee_salary sal ON dem.employee_id = sal.employee_id
    GROUP BY gender
)
SELECT AVG(avg_sal) AS average_of_averages
FROM CTE_Example;
# Without CTE, you'd have to nest this subquery inside another SELECT —
# much harder to read.

# Compare: the same logic as a subquery (messy)
SELECT AVG(avg_sal)
FROM (
    SELECT gender, AVG(salary) avg_sal, MAX(salary) max_sal,
           MIN(salary) min_sal, COUNT(salary) count_sal
    FROM employee_demographics dem
    JOIN employee_salary sal ON dem.employee_id = sal.employee_id
    GROUP BY gender
) example_subquery;


# Multiple CTEs — define as many as you need, separated by commas
WITH CTE_Recent_Hires AS (
    SELECT employee_id, gender, birth_date
    FROM employee_demographics
    WHERE birth_date > '1985-01-01'
),
CTE_High_Earners AS (
    SELECT employee_id, salary
    FROM employee_salary
    WHERE salary > 50000
)
# Now join both CTEs together:
SELECT *
FROM CTE_Recent_Hires
JOIN CTE_High_Earners
    ON CTE_Recent_Hires.employee_id = CTE_High_Earners.employee_id;

# You can rename CTE columns inline:
WITH CTE_Example (Gender, AVG_Sal, MAX_Sal, MIN_Sal, COUNT_Sal) AS (
    SELECT gender, AVG(salary), MAX(salary), MIN(salary), COUNT(salary)
    FROM employee_demographics dem
    JOIN employee_salary sal ON dem.employee_id = sal.employee_id
    GROUP BY gender
)
SELECT *
FROM CTE_Example;


-- ============================================================
-- SECTION 2: TEMPORARY TABLES
-- ============================================================
-- Temp tables are saved in memory for the current session.
-- Unlike CTEs, you can query them multiple times.
-- They disappear when you close your connection.
--
-- Use temp tables when:
--   - You need to reference intermediate results multiple times
--   - You want to inspect your data mid-analysis (helpful for debugging)
--   - Your CTE would be referenced more than once

# Create a temp table manually with defined columns
CREATE TEMPORARY TABLE temp_table (
    first_name     VARCHAR(50),
    last_name      VARCHAR(50),
    favorite_movie VARCHAR(100)
);

SELECT * FROM temp_table;   -- empty right now

INSERT INTO temp_table
VALUES ('Alex', 'Freberg', 'Lord of the Rings: The Two Towers');

SELECT * FROM temp_table;   -- now has 1 row


# Create a temp table directly from a SELECT statement (faster + more common)
CREATE TEMPORARY TABLE salary_over_50k
SELECT *
FROM employee_salary
WHERE salary >= 50000;

SELECT * FROM salary_over_50k;
# This temp table stores the result and you can query it anytime
# during this session — run it once, use it many times.


-- ============================================================
-- SECTION 3: STORED PROCEDURES
-- ============================================================
-- A stored procedure is a saved block of SQL code you can
-- run by calling its name — like a macro or a function button.
--
-- Benefits:
--   - Write complex logic once, reuse it with one line
--   - Other team members can call it without knowing the SQL
--   - Consistent execution — no copy-paste errors
--
-- In MySQL Workbench: after creating a procedure, right-click
-- "Stored Procedures" in the Schemas panel and refresh to see it.

# Basic stored procedure (no DELIMITER — not best practice)
CREATE PROCEDURE large_salaries()
SELECT *
FROM employee_salary
WHERE salary >= 50000;

CALL large_salaries();   # Run it like this


# BEST PRACTICE: Use DELIMITER to avoid issues with semicolons inside the procedure
# By default, MySQL uses ; to end statements. Inside a procedure, there are
# multiple ; but we only want the procedure to end at END $$.
# DELIMITER $$ changes the ending character temporarily to $$.

DELIMITER $$
CREATE PROCEDURE large_salaries2()
BEGIN
    SELECT *
    FROM employee_salary
    WHERE salary >= 50000;

    SELECT *
    FROM employee_salary
    WHERE salary >= 10000;
END $$
DELIMITER ;   # Reset back to normal semicolon

CALL large_salaries2();
# Returns two result sets: one for salary >= 50k, one for >= 10k


-- ============================================================
-- SECTION 4: STORED PROCEDURES WITH PARAMETERS
-- ============================================================
-- Parameters let you pass a value INTO the procedure at call time.
-- This makes the procedure flexible — you control the filter.

DELIMITER $$
CREATE PROCEDURE large_salaries3(param_employee_id INT)
BEGIN
    SELECT salary
    FROM employee_salary
    WHERE employee_id = param_employee_id;
END $$
DELIMITER ;

CALL large_salaries3(1);   -- Returns salary for employee_id = 1
CALL large_salaries3(4);   -- Returns salary for employee_id = 4


-- ============================================================
-- SECTION 5: TRIGGERS
-- ============================================================
-- A TRIGGER automatically executes a block of code when a
-- specific event happens on a table (INSERT, UPDATE, or DELETE).
--
-- Use case: when a new hire is added to employee_salary,
-- automatically create their profile in employee_demographics too.
--
-- NEW = the data being inserted (available in INSERT triggers)
-- OLD = the previous data (available in UPDATE and DELETE triggers)

SELECT * FROM employee_demographics;
SELECT * FROM employee_salary;

DELIMITER $$
CREATE TRIGGER employee_insert
    AFTER INSERT ON employee_salary     -- fires AFTER a row is inserted into employee_salary
    FOR EACH ROW                         -- runs once for each inserted row
BEGIN
    INSERT INTO employee_demographics (employee_id, first_name, last_name)
    VALUES (NEW.employee_id, NEW.first_name, NEW.last_name);
    -- NEW.column_name refers to the values just inserted into employee_salary
END $$
DELIMITER ;

# TEST: Insert a new employee into employee_salary
INSERT INTO employee_salary (employee_id, first_name, last_name, occupation, salary, dept_id)
VALUES (13, 'Jean-Ralphio', 'Saperstein', 'Entertainment 720 CEO', 1000000, NULL);

# Check: Jean-Ralphio should now automatically appear in employee_demographics too
SELECT * FROM employee_demographics;


-- ============================================================
-- SECTION 6: EVENTS (Scheduled SQL Jobs)
-- ============================================================
-- An EVENT runs SQL code automatically on a schedule.
-- Think of it like a cron job, but inside the database.
--
-- Use case: automatically delete employees who have retired
-- (age >= 60) every 30 seconds — simulating a daily cleanup job.
--
-- First, check if the event scheduler is turned on:

SHOW VARIABLES LIKE 'event%';
-- If event_scheduler = OFF, events won't run.

SELECT * FROM employee_demographics;

DELIMITER $$
CREATE EVENT delete_retirees
ON SCHEDULE EVERY 30 SECOND     -- runs every 30 seconds
-- For a real scenario: ON SCHEDULE EVERY 1 MONTH
DO
BEGIN
    DELETE
    FROM employee_demographics
    WHERE age >= 60;
END $$
DELIMITER ;

# After the event fires, employees aged 60+ will be removed automatically.
SELECT * FROM employee_demographics;
