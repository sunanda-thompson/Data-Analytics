-- ============================================================
-- 04_case_subqueries_window.sql
-- Topic: CASE Statements, Subqueries, Window Functions
-- Database: MySQL
-- Dataset: Parks & Recreation
-- ============================================================


-- ============================================================
-- SECTION 1: CASE STATEMENTS
-- ============================================================
-- CASE is SQL's version of if/else logic.
-- It evaluates conditions top to bottom and returns the value
-- for the FIRST condition that is true.
--
-- Structure:
--   CASE
--     WHEN condition THEN result
--     WHEN condition THEN result
--     ELSE fallback_result
--   END AS alias

# Classify employees into age groups
SELECT
    first_name,
    last_name,
    age,
    CASE
        WHEN age <= 30 THEN 'Young'
        WHEN age BETWEEN 31 AND 50 THEN 'Mid-Career'
        WHEN age >= 51 THEN 'Senior'
    END AS age_bracket
FROM employee_demographics;

# Calculate new salary based on current salary range
# AND calculate a bonus for employees in the Finance department (dept_id = 6)
SELECT
    first_name,
    last_name,
    salary,
    CASE
        WHEN salary < 50000  THEN salary * 1.05  -- 5% raise
        WHEN salary >= 50000 THEN salary * 1.07  -- 7% raise
    END AS new_salary,
    CASE
        WHEN dept_id = 6 THEN salary * 0.10  -- 10% bonus for Finance
        -- No ELSE here → anyone not in dept 6 gets NULL (no bonus)
    END AS bonus
FROM employee_salary;

-- Reference: dept_id 6 = Finance (confirmed by looking at parks_departments)
SELECT * FROM parks_departments;


-- ============================================================
-- SECTION 2: SUBQUERIES
-- ============================================================
-- A subquery is a query nested inside another query.
-- The inner query runs first, and its result is used
-- by the outer query.
--
-- Three places you can put a subquery:
--   1. In the WHERE clause (most common)
--   2. In the SELECT clause (as a calculated column)
--   3. In the FROM clause (as a virtual table)

# --- Subquery in WHERE clause ---
# "Show employees who work in department 1"
# Instead of knowing the employee IDs, we look them up from employee_salary
SELECT *
FROM employee_demographics
WHERE employee_id IN (
    SELECT employee_id
    FROM employee_salary
    WHERE dept_id = 1
);
# The inner query returns a list of employee_ids in dept 1.
# The outer query then returns the demographic info for those IDs.

# --- Subquery in SELECT clause ---
# Show each employee's salary alongside the overall average salary
SELECT
    first_name,
    salary,
    (SELECT AVG(salary) FROM employee_salary) AS company_avg_salary
FROM employee_salary
GROUP BY first_name, salary;

# --- Subquery in FROM clause (virtual table) ---
# First, the inner query creates a summary by gender:
SELECT gender, AVG(age), MAX(age), MIN(age), COUNT(age)
FROM employee_demographics
GROUP BY gender;

# Then, use it as a virtual table to calculate the average of the max ages:
SELECT AVG(max_age) AS avg_of_max_ages
FROM (
    SELECT
        gender,
        AVG(age)   AS avg_age,
        MAX(age)   AS max_age,
        MIN(age)   AS min_age,
        COUNT(age) AS count_age
    FROM employee_demographics
    GROUP BY gender
) AS aggregated_by_gender;
# "Aggregated_by_gender" is just an alias we give the subquery.
# MySQL requires a name for every subquery in the FROM clause.


-- ============================================================
-- SECTION 3: WINDOW FUNCTIONS
-- ============================================================
-- Window functions perform calculations across rows related
-- to the current row, WITHOUT collapsing them into groups.
--
-- GROUP BY: 2 genders → 2 rows of output
-- OVER():   keeps all rows, adds a calculated column to each

# --- GROUP BY version (collapses rows) ---
SELECT
    gender,
    AVG(salary) AS avg_salary
FROM employee_demographics dem
JOIN employee_salary sal ON dem.employee_id = sal.employee_id
GROUP BY gender;
# Result: 2 rows (one per gender)

# --- Window Function version (keeps all rows) ---
SELECT
    dem.first_name,
    dem.last_name,
    AVG(salary) OVER (PARTITION BY gender) AS avg_salary_for_their_gender
FROM employee_demographics dem
JOIN employee_salary sal ON dem.employee_id = sal.employee_id;
# Result: one row per employee, with their gender's average salary shown on each row

# --- Rolling Total ---
# Cumulative salary total as you go through employees (ordered by employee_id)
SELECT
    dem.first_name,
    dem.last_name,
    gender,
    salary,
    SUM(salary) OVER (PARTITION BY gender ORDER BY dem.employee_id) AS rolling_total_by_gender
FROM employee_demographics dem
JOIN employee_salary sal ON dem.employee_id = sal.employee_id;

# --- ROW_NUMBER, RANK, DENSE_RANK ---
#
# ROW_NUMBER:  always unique — no ties (1, 2, 3, 4, 5)
# RANK:        ties share a rank, then skips → (1, 1, 3, 4)
# DENSE_RANK:  ties share a rank, no gaps   → (1, 1, 2, 3)

SELECT
    dem.first_name,
    dem.last_name,
    gender,
    salary,
    ROW_NUMBER()  OVER (PARTITION BY gender ORDER BY salary DESC) AS row_num,
    RANK()        OVER (PARTITION BY gender ORDER BY salary DESC) AS rank_num,
    DENSE_RANK()  OVER (PARTITION BY gender ORDER BY salary DESC) AS dense_rank_num
FROM employee_demographics dem
JOIN employee_salary sal ON dem.employee_id = sal.employee_id;
# PARTITION BY gender means: rank separately within Male and within Female
# ORDER BY salary DESC means: highest salary gets rank 1
