# 🗄️ SQL — Notes & Reference Guide

SQL (Structured Query Language) is the standard language for talking to databases. If Excel is for analyzing data in a spreadsheet, SQL is for analyzing data stored in a database — which is where most real-world data lives.

This folder contains scripts for two databases:
- **PostgreSQL** — used in the AnalyticsMentor.io curriculum with transaction and customer data
- **MySQL** — used in a beginner series with the fictional Parks & Recreation dataset

---

## ⚡ Quick Reference

➡️ **[SQL Cheat Sheet — PostgreSQL vs MySQL](https://sunanda-thompson.github.io/Data-Analytics/Self-Study/Data-Analytics-Tools/SQL/SQL_Cheat_Sheet.html)** — side-by-side syntax comparison for both databases, with live search. Open this when you need a fast answer on syntax differences.

---

## 🤔 What Is SQL, Really?

Think of a database as a collection of spreadsheets (called *tables*) that are all connected to each other. SQL is how you ask questions about those tables:

- *"Show me all customers from Texas"* → `SELECT * FROM customers WHERE state = 'TX'`
- *"What's the total revenue by month?"* → `SELECT month, SUM(revenue) FROM sales GROUP BY month`

You write instructions, the database runs them, and you get results back.

---

## 📂 Scripts in This Folder

### PostgreSQL

| File | What It Covers |
|------|---------------|
| `01_basics.sql` | Creating tables, inserting data, updating and deleting records |
| `02_filtering.sql` | WHERE, AND, OR, IN, BETWEEN, LIKE, NULL checks |
| `03_joins.sql` | Combining data from multiple tables |
| `04_aggregates.sql` | COUNT, SUM, AVG, GROUP BY, HAVING |
| `05_ctes_temp_tables.sql` | Organizing complex queries with CTEs and temp tables |
| `06_window_functions.sql` | ROW_NUMBER, RANK, running totals, LAG/LEAD |
| `07_advanced.sql` | Views, indexes, partitions, stored procedures, functions |
| `08_date_functions.sql` | Date math, formatting, and time zone handling |

### MySQL

| File | What It Covers |
|------|---------------|
| `01_select_and_filtering.sql` | SELECT, WHERE, LIKE, GROUP BY, ORDER BY, HAVING, LIMIT |
| `02_joins_and_unions.sql` | All JOIN types and combining result sets with UNION |
| `03_string_functions.sql` | UPPER, LOWER, TRIM, SUBSTRING, REPLACE, CONCAT |
| `04_case_subqueries_window.sql` | CASE statements, subqueries, window functions |
| `05_ctes_temp_stored_procedures_triggers.sql` | CTEs, temp tables, stored procedures, triggers, events |

### Reference

| File | What It Covers |
|------|---------------|
| 🔗 [SQL_Cheat_Sheet.html](https://sunanda-thompson.github.io/Data-Analytics/Self-Study/Data-Analytics-Tools/SQL/SQL_Cheat_Sheet.html) | Side-by-side PostgreSQL vs MySQL syntax — all major commands |

---

## 🔑 Key Concepts to Know Before You Start

**Table** — A collection of data organized in rows and columns, like a spreadsheet tab.

**Row** — One record (e.g., one customer, one transaction).

**Column** — One field/attribute (e.g., name, price, date).

**Primary Key** — A unique ID for each row. No two rows can have the same primary key.

**Foreign Key** — A column that references the primary key of another table. This is how tables connect.

**Query** — A question you ask the database, written in SQL.

**NULL** — A missing or unknown value. Not zero, not blank — truly unknown.

---

## 📋 SQL Query Order of Operations

SQL executes clauses in a specific order. This matters when you're trying to filter results:
```
1. FROM        -- which table(s) to pull from
2. JOIN        -- combine with other tables
3. WHERE       -- filter individual rows BEFORE grouping
4. GROUP BY    -- group rows together
5. HAVING      -- filter groups AFTER grouping
6. SELECT      -- choose which columns to show
7. ORDER BY    -- sort the results
8. LIMIT       -- restrict how many rows come back
```

> **Most common beginner mistake:** Trying to use a column alias (created in SELECT) inside a WHERE clause — it hasn't been created yet at that point in execution.
