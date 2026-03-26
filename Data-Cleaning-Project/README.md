# Sales Data Cleaning & PostgreSQL Import Project

## Project Overview

This project demonstrates the end-to-end workflow of a data analyst — taking a deliberately messy, real-world-style sales dataset, cleaning it manually in Excel, loading it into PostgreSQL, and querying it for business insights.

The goal was to understand **why** data needs to be cleaned, **how** to identify and fix every category of data quality issue, and **what** a cleaned dataset should look like before it enters a database.

---

## The Problem

The raw dataset (`messy_sales_data_RAW.xlsx`) contained 21 rows and 25 columns — intentionally filled with the kinds of issues you encounter in real business data:

| Issue | Example |
|---|---|
| Duplicate columns | `customer_name` and `CUSTOMER NAME` both present |
| Inconsistent headers | `Qty`, `quantity`, `Unit Price ($)`, `  email  ` |
| Mixed case values | `Completed`, `completed`, `COMPLETED` |
| Inconsistent country values | `USA`, `US`, `United States`, `United States of America` |
| Phone numbers in two columns, multiple formats | `555-123-4567`, `(555) 234 5678`, `+1-555-678-9012` |
| Prices stored as text with symbols | `$1,299.99`, `€79.99` |
| Discount column with mixed formats | `15%`, `0%`, `0.10`, `150%` |
| Typos in data | `enteprise`, `nroth`, `TEXASS`, `labtop pro 15` |
| Duplicate rows | C001 appeared as an exact duplicate |
| HTML tags in data | `<b>emma wilson</b>` |
| Future dates | `2025-12-31` in a 2024 dataset |
| Negative prices | `-29.99` on a refund row |
| Missing values | Blank names, emails, dates, regions |
| Scientific notation on phone numbers | `5.551235e+09` |

---

## Tools Used

- **Microsoft Excel** — data cleaning and transformation
- **PostgreSQL 18** — database storage and querying
- **pgAdmin 4** — database interface

---

## Files in This Repository

| File | Description |
|---|---|
| `messy_sales_data_RAW.xlsx` | The original uncleaned dataset with all issues intact |
| `messy_sales_data_cleaning.xlsx` | The working file showing the cleaned result |
| `sales_data_clean.csv` | Final export ready for PostgreSQL import |
| `data_cleaning_process.sql` | SQL used to create the table and query the data |
| `DATA_CLEANING_WALKTHROUGH.docx` | Full documented walkthrough of every cleaning step |
| `README.md` | This file |

---

## Cleaning Summary

Starting point: **21 rows, 25 columns**, multiple data quality issues
End result: **17 rows, 18 columns**, clean and import-ready

### What was done:
- Removed 7 duplicate columns
- Standardized all headers to `lowercase_with_underscores`
- Merged two phone number columns into one, normalized to 10-digit format
- Standardized all text to lowercase
- Fixed all typos across product, region, state, customer_type, and status columns
- Unified country values to `USA`
- Converted discount column from mixed `%` text and decimals to consistent decimal format
- Fixed date column to `YYYY-MM-DD` format
- Removed `$` and `€` symbols from price columns and converted to numeric
- Removed 2 exact duplicate rows
- Removed redundant `revenue` column (duplicate of `total`)
- Flagged rows with missing names/emails and missing dates
- Flagged a future date (`2025-12-31`) as suspect data
- Fixed negative price on refund row

---

## PostgreSQL Table Definition

```sql
create table sales_data (
      customer_id       varchar(10)
    , customer_name     text
    , email             text
    , phone_number      varchar(15)
    , purchase_date     date
    , product_name      text
    , quantity          integer
    , unit_price        numeric(10,2)
    , total             numeric(10,2)
    , region            text
    , state             varchar(5)
    , country           varchar(10)
    , sales_rep         text
    , sales_rep_id      varchar(10)
    , customer_type     text
    , discount          numeric(5,2)
    , payment_method    text
    , status            text
)
;
```

---

## Key Learnings

- Data type decisions in Excel directly affect whether PostgreSQL will accept the import
- Phone numbers must be stored as `TEXT`, not numbers — they are identifiers, not values you calculate with
- The `CREATE TABLE` statement must exist before any data can be imported
- Short file paths with no spaces are essential when using pgAdmin's import wizard on Windows
- Cleaning is not just cosmetic — inconsistent values like `USA` vs `United States` become two different groups in any SQL query

---

## What Comes Next

With clean data in PostgreSQL, the next step in the analytics workflow is querying for business insights and connecting to a visualization tool such as Tableau or Power BI to build dashboards.
