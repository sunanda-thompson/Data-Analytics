# Magento E-Commerce Data Analytics Pipeline
### Portfolio Project — Data Analytics Coordinator | Innova

This project simulates the full data workflow of a **Data Analytics Coordinator** at an energy efficiency firm. It covers the complete pipeline from raw e-commerce order exports through validation, cleaning, reconciliation against a payment processor, and generation of settlement-ready files for client billing systems.

---

## 📁 Repository Files

| File | Type | What It Contains |
|---|---|---|
| `raw_data_export.xlsx` | Excel Workbook | All raw simulated data from Magento and Authorize.net across 6 tabs, with problem rows colour-coded |
| `magento_analytics_complete.sql` | SQL (MySQL 8.0+) | Full pipeline in SQL — table creation, data loading, validation queries, cleaning, reconciliation, and reporting |
| `magento_analytics_complete.py` | Python 3.8+ | Full pipeline in Python — data generation, validation, all 8 transformations, reconciliation, and 4 output file exports |
| `process_documentation.docx` | Word Document | Step-by-step process explanation with a full pipeline flowchart, code samples, before/after data examples, and a job requirement mapping |

---

## 🖥️ Simulated Software Systems

This project simulates data exported from the following real-world platforms:

### Magento / Adobe Commerce
**What it is:** Magento (now Adobe Commerce) is one of the most widely used e-commerce platforms in the world. It powers online stores for mid-size and enterprise retailers, handling product catalogs, shopping carts, checkout flows, and order management.

**What was simulated:** The `magento_orders` and `magento_customers` tables replicate the format of a standard Magento order export — including the exact data quality issues you encounter in real exports: currency values stored as formatted strings (`$215.08`), inconsistent SKU codes entered by different users, mixed tax field formats caused by mid-year configuration changes, duplicate rows from overlapping export jobs, and missing invoice numbers from incomplete billing workflows.

**Real equivalent:** In production, this data would be pulled via the Magento REST API (`GET /rest/V1/orders`) or exported directly from the Commerce Manager admin panel under Sales → Orders → Export.

---

### Commerce Manager (Magento Admin)
**What it is:** Commerce Manager is the admin dashboard built into Magento. It is the interface where business users manage orders, customers, inventory, and reports. Data analysts typically pull exports directly from this interface or connect to the underlying MySQL database.

**What was simulated:** The customer table (`magento_customers`) reflects the kind of record you export from the Commerce Manager customer grid — customer IDs, names, emails, US state codes, account creation dates, and loyalty tier designations.

---

### Authorize.net
**What it is:** Authorize.net is a payment gateway and processor owned by Visa. It sits between the customer's credit card and the merchant's bank account, handling authorization, capture, and settlement. It is one of the most common payment processors integrated with Magento stores.

**What was simulated:** The `payment_transactions` table replicates Authorize.net's settlement file format — transaction IDs, order references, settlement dates, gross amounts, processor fees (calculated at the standard 2.9% + $0.30 rate), net amounts, transaction statuses (`settled`, `voided`, `refunded`), and authorization codes. Three intentional "orphan" transactions were injected — settled payments with no matching Magento order — which is a real scenario requiring finance team escalation.

---

### Microsoft Dynamics
**What it is:** Microsoft Dynamics 365 is an enterprise resource planning (ERP) and accounting platform used by many large organizations to manage finances, invoicing, and vendor payments. Utility company clients often use Dynamics as their system of record for tracking incentive payments.

**What was simulated:** The status code mapping in both the SQL and Python files mirrors the Dynamics numeric status schema — `complete → 110 (INVOICED)`, `processing → 100 (IN_PROGRESS)`, `closed → 120`, `pending → 50`, `canceled → 999 (VOID)`. The JSON output file (`settlement_ready.json`) is structured as a REST API payload for Dynamics upload.

---

### ARIBA (SAP Procurement Portal)
**What it is:** SAP Ariba is a procurement and supply chain platform used by large enterprises and utility companies to manage vendor relationships and process payments. When a utility company uses Ariba, settlement files must be formatted and uploaded to match Ariba's exact schema requirements.

**What was simulated:** The schema mapping concept in the transformation pipeline — normalizing field names, enforcing specific date formats, and mapping status codes — directly mirrors what is required when preparing files for Ariba or similar procurement portals.

---

## 📊 What the Excel File Contains (`raw_data_export.xlsx`)

The workbook has 6 tabs, designed to show the raw data exactly as it comes out of Magento and Authorize.net before any cleaning:

| Tab | Contents |
|---|---|
| **LEGEND** | Guide to the workbook — what each tab contains and what each highlight colour means |
| **RAW_Orders** | All 105 raw order rows including 5 intentional duplicates. Problem rows are colour-coded: 🔴 Red = duplicate, 🟠 Orange = bad SKU format, 🩷 Pink = missing invoice, 🟡 Yellow = combined tax format |
| **RAW_Customers** | 50 customer records — the cleanest of the three tables |
| **RAW_Transactions** | 39 payment transactions from the simulated Authorize.net settlement file. 🔴 Red rows = orphan transactions with no matching Magento order |
| **Data_Dictionary** | Definition of every column across all three tables — data type, expected format, and known issues flagged |
| **Issues_Summary** | All 8 data quality problems summarised in one place with affected row counts, business impact, and recommended action |

---

## 🗄️ What the SQL File Contains (`magento_analytics_complete.sql`)

Written in **MySQL 8.0+ syntax**, compatible with MySQL Workbench. Run sections in order from top to bottom.

| Section | What It Does |
|---|---|
| **Section 1** | `CREATE DATABASE` and all 4 table definitions with correct field types, constraints, and comments explaining every column |
| **Section 2** | How to load data via `LOAD DATA INFILE` or `INSERT`, plus row count verification queries |
| **Section 3** | 7 validation queries — duplicate detection, null field audit, tax format check, SKU inconsistency check, currency format check, date format check, orphan transaction detection |
| **Section 4** | `CREATE TABLE magento_orders_clean AS SELECT` — the full staging table with all transformations applied: deduplication via `ROW_NUMBER()`, SKU normalization, currency stripping, tax unification via `COALESCE()`, date conversion via `STR_TO_DATE()`, status code mapping, incentive program mapping, grand total cross-check |
| **Section 5** | Reconciliation queries — Magento vs Authorize.net comparison, orphan transactions via `LEFT JOIN`, unsettled orders, amount mismatch detection |
| **Section 6** | `CREATE TABLE settlement_ready` — final output joining clean orders + settled transactions + customers, filtered to payment-eligible only |
| **Section 7** | Export queries for each output tab: full detail, program summary, monthly summary, issues log |
| **Section 8** | Ad-hoc finance and reporting queries — revenue by status, top SKUs, customer lifetime value, monthly trend, payment method breakdown, state-level revenue |

---

## 🐍 What the Python File Contains (`magento_analytics_complete.py`)

A single runnable Python script covering the entire pipeline in 9 sections. Every transformation includes a comment block explaining the problem, the fix applied, and the result.

```bash
pip install pandas openpyxl
python3 magento_analytics_complete.py
```

| Section | What It Does |
|---|---|
| **Section 1** | Imports and folder setup |
| **Section 2** | Generates synthetic raw data simulating Magento + Authorize.net exports with 6 intentional data quality issues injected |
| **Section 3** | Loads all three raw tables into an in-memory SQLite database for SQL validation queries |
| **Section 4** | 7 validation checks using pandas + SQL — results printed with issue counts, all problems logged to `issues_log` |
| **Section 5** | 8 cleaning transformations using pandas — deduplication, SKU normalization, currency stripping, tax unification, ISO date conversion, status code mapping, incentive mapping, grand total cross-check |
| **Section 6** | Reconciliation — 4 checks comparing clean orders against settled transactions |
| **Section 7** | Builds the settlement-ready DataFrame using `merge()` (inner join + left join) |
| **Section 8** | Exports 4 output files: CSV, JSON (Dynamics API payload), formatted XLSX (4-tab report), validation flags CSV |
| **Section 9** | Final summary report printed to console |

**Output files generated when the script is run:**
```
data/output/settlement_ready.csv       ← flat file for ETL / system import
data/output/settlement_ready.json      ← REST API payload for Microsoft Dynamics
data/output/invoice_report.xlsx        ← 4-tab formatted client invoice report
data/output/validation_flags.csv       ← exceptions log for finance team handoff
```

---

## 📄 What the Word Document Contains (`process_documentation.docx`)

An 8-section reference document explaining the entire project in plain language for both technical and non-technical readers. Includes a full colour-coded pipeline flowchart.

| Section | Contents |
|---|---|
| **Section 1** | Project overview — what the pipeline does, why it matters, tools used |
| **Section 2** | Full colour-coded pipeline flowchart — 7 steps from source systems to client upload with decision points and escalation paths |
| **Section 3** | Data sources and raw data — what comes out of each system, table shapes, all 8 raw data problems with real examples |
| **Section 4** | Validation step — each check explained with the SQL query used, the result found, and why it matters |
| **Section 5** | Cleaning and transformation — all 8 transformations with before/after examples and code in both Python and SQL |
| **Section 6** | Reconciliation — the Magento vs Authorize.net comparison results, orphan transaction breakdown, escalation guidance |
| **Section 7** | Settlement files — how the dataset is assembled, the join logic, settlement totals, all 4 output formats explained |
| **Section 8** | Job requirement mapping — every bullet point from the Innova job description linked to the specific project work that demonstrates it |

---

## ⚡ Data Quality Issues Deliberately Injected

The raw data was built with these intentional problems to simulate what analysts actually receive from real Magento exports:

| # | Issue | Rows Affected | Business Risk |
|---|---|---|---|
| 1 | Duplicate order IDs | 5 pairs (10 rows) | Double-counts revenue and inflates incentive claims |
| 2 | Missing invoice numbers | 11 orders | Orders cannot be submitted for payment without this field |
| 3 | Mixed tax formats (combined vs itemized) | All 100 orders | Client system needs one unified tax field to process invoices |
| 4 | Inconsistent SKU formats (`sku_led_001` vs `SKU-LED-001`) | 24 orders | Same product appears as two separate items in revenue reports |
| 5 | Currency stored as text (`$215.08`) | All 100 orders | Math operations (SUM, AVG) fail or return wrong results |
| 6 | Wrong date format (`MM/DD/YYYY HH:MM`) | All 100 orders | Ambiguous and incompatible with API and system upload schemas |
| 7 | Orphan transactions | 3 transactions ($421.47) | Settled money with no order record — requires finance investigation |
| 8 | Plain-text status codes | All 100 orders | Microsoft Dynamics requires numeric codes, not plain English words |

---

## 🌱 Domain Context

Innova works with utility companies that offer rebates and incentives to customers who purchase energy-efficient products. After a customer buys a smart thermostat or LED lighting kit through the online store, the utility company reimburses Innova a percentage of the sale as an incentive payment — ranging from 15% to 30% of the subtotal depending on the product category.

Clean, properly formatted settlement files are what trigger those reimbursement payments. If the data has errors — wrong amounts, missing invoices, unresolved duplicates — the utility client rejects the submission and payment is delayed. That is the real-world stakes behind every transformation in this pipeline.

---

## 🛠️ Requirements

```bash
# Python dependencies
Python 3.8+
pip install pandas openpyxl

# SQL
MySQL 8.0+ or MySQL Workbench
# Note: The Python script uses SQLite (built-in) — no additional install needed
```
