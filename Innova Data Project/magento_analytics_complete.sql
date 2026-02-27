-- ============================================================
--  MAGENTO DATA ANALYTICS — COMPLETE SQL REFERENCE
--  Innova | Data Analytics Coordinator Portfolio
-- ============================================================
--  DATABASE : MySQL 8.0+ (syntax compatible with MySQL Workbench)
--  PURPOSE  : Full pipeline from table creation → raw data load
--             → validation → cleaning → reconciliation → reporting
--
--  HOW TO USE THIS FILE:
--    1. Open MySQL Workbench (or any MySQL client)
--    2. Run SECTION 1 first to create the database and tables
--    3. Run SECTION 2 to populate them with the raw data
--    4. Run SECTIONS 3–7 in order to validate and clean
--    5. Run SECTION 8 for finance/reporting queries
--
--  Each section is self-contained and heavily commented so you
--  can run individual blocks and understand exactly what each
--  query does and why.
-- ============================================================


-- ============================================================
-- SECTION 1: DATABASE AND TABLE CREATION
-- ============================================================
-- We create one database to hold all three source tables plus
-- a clean staging table and a validation issues log.
-- This mirrors what you'd set up before a Magento integration.
-- ============================================================

-- Create (or reuse) the database
CREATE DATABASE IF NOT EXISTS magento_analytics
    CHARACTER SET utf8mb4        -- supports all Unicode characters
    COLLATE utf8mb4_unicode_ci;  -- case-insensitive comparisons

USE magento_analytics;

-- ─────────────────────────────────────────────────────────────
-- TABLE 1: magento_customers
-- Source: Commerce Manager customer export
-- Fields reflect what a standard Magento customer record holds
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS magento_customers (
    customer_id   VARCHAR(12)  NOT NULL,   -- e.g. CUST-0001
    first_name    VARCHAR(50)  NOT NULL,
    last_name     VARCHAR(50)  NOT NULL,
    email         VARCHAR(100) NOT NULL,
    state         CHAR(2)      NOT NULL,   -- 2-letter US state code
    created_at    DATE         NOT NULL,   -- account creation date
    loyalty_tier  VARCHAR(10)  DEFAULT 'None',  -- bronze / silver / gold / None

    PRIMARY KEY (customer_id)
);

-- ─────────────────────────────────────────────────────────────
-- TABLE 2: magento_orders
-- Source: Magento order export (raw, uncleaned)
-- IMPORTANT: This table intentionally stores data exactly as
-- Magento exports it — including all the messy formatting.
-- Subtotal is VARCHAR because Magento exports it as "$215.08".
-- Tax fields are nullable because Magento uses two formats.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS magento_orders (
    order_id        VARCHAR(15)   NOT NULL,   -- e.g. ORD-00001 (NOT unique — dupes exist!)
    customer_id     VARCHAR(12)   NOT NULL,   -- FK to magento_customers
    order_date      VARCHAR(20)   NOT NULL,   -- raw format: "MM/DD/YYYY HH:MM"
    sku             VARCHAR(25)   NOT NULL,   -- raw SKU — may be in any format
    qty             INT           NOT NULL,
    subtotal        VARCHAR(15)   NOT NULL,   -- stored as "$215.08" — text, not a number!
    state_tax       DECIMAL(8,2)  NULL,       -- only filled when using ITEMIZED tax format
    county_tax      DECIMAL(8,2)  NULL,       -- only filled when using ITEMIZED tax format
    combined_tax    DECIMAL(8,2)  NULL,       -- only filled when using COMBINED tax format
    shipping        DECIMAL(8,2)  NOT NULL DEFAULT 0.00,
    discount        DECIMAL(8,2)  NOT NULL DEFAULT 0.00,
    grand_total     DECIMAL(10,2) NOT NULL,
    status          VARCHAR(15)   NOT NULL,   -- complete / processing / pending / closed / canceled
    payment_method  VARCHAR(20)   NOT NULL,   -- authorizenet / paypal / free
    invoice_number  VARCHAR(12)   NULL        -- NULL on ~10% of orders — a known data quality issue

    -- NOTE: No PRIMARY KEY here because duplicates exist in the raw export.
    --       We'll enforce uniqueness only in the clean staging table.
);

-- ─────────────────────────────────────────────────────────────
-- TABLE 3: payment_transactions
-- Source: Authorize.net settlement file export
-- This represents what the payment processor says was charged.
-- We reconcile this against magento_orders to find gaps.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS payment_transactions (
    transaction_id  VARCHAR(20)   NOT NULL,
    order_id        VARCHAR(15)   NOT NULL,   -- FK to magento_orders (may not exist — orphans!)
    settle_date     DATE          NOT NULL,   -- date money moved to bank account
    gross_amount    DECIMAL(10,2) NOT NULL,   -- total charged to customer
    processor_fee   DECIMAL(8,2)  NOT NULL,   -- Authorize.net fee (2.9% + $0.30)
    net_amount      DECIMAL(10,2) NOT NULL,   -- gross_amount minus processor_fee
    status          VARCHAR(12)   NOT NULL,   -- settled / voided / refunded
    auth_code       VARCHAR(12)   NOT NULL,   -- authorization code from card network

    PRIMARY KEY (transaction_id)
);

-- ─────────────────────────────────────────────────────────────
-- TABLE 4: orders_issues_log
-- Used to record every data quality problem we find during
-- validation. This becomes the "Issues Log" tab in the
-- client's invoice report spreadsheet.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS orders_issues_log (
    log_id         INT           AUTO_INCREMENT PRIMARY KEY,
    issue_type     VARCHAR(30)   NOT NULL,   -- DUPLICATE_ORDER, MISSING_INVOICE, etc.
    order_id       VARCHAR(15),
    transaction_id VARCHAR(20),
    detail         VARCHAR(255)  NOT NULL,
    action_required VARCHAR(150) NOT NULL,
    flagged_at     TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- SECTION 2: LOAD RAW DATA
-- ============================================================
-- In a real Magento integration, data arrives via:
--   a) API pull  → INSERT via Python/ETL script
--   b) CSV export → LOAD DATA INFILE
--   c) Direct DB connection → INSERT INTO ... SELECT ...
--
-- Below are the INSERT statements that match the project data.
-- In practice you would use LOAD DATA INFILE with your CSV path.
-- ============================================================

-- To load from CSV instead of INSERT statements, use:
--   LOAD DATA INFILE '/path/to/magento_customers.csv'
--   INTO TABLE magento_customers
--   FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
--   LINES TERMINATED BY '\n'
--   IGNORE 1 ROWS;  -- skip the header line

-- Sample of what the INSERT data looks like (abbreviated):
-- Full dataset loaded by the Python script via pandas .to_sql()

-- Verify data loaded correctly after insertion:
SELECT 'magento_customers'   AS table_name, COUNT(*) AS row_count FROM magento_customers
UNION ALL
SELECT 'magento_orders',                    COUNT(*)              FROM magento_orders
UNION ALL
SELECT 'payment_transactions',              COUNT(*)              FROM payment_transactions;
-- Expected: 50 customers | 105 orders (incl. 5 dupes) | 39 transactions


-- ============================================================
-- SECTION 3: VALIDATION — FIND ALL DATA QUALITY PROBLEMS
-- ============================================================
-- These queries are run BEFORE any cleaning.
-- The goal is to understand what problems exist and how many
-- rows are affected. Results feed the Issues Log.
-- ============================================================

-- ─────────────────────────────────────────────────────────────
-- 3A. DUPLICATE ORDER IDs
-- Problem: Magento exports sometimes include the same order
--          twice if jobs overlap or exports are run back-to-back.
-- Risk: Double-counting revenue and incentive claims.
-- ─────────────────────────────────────────────────────────────
SELECT
    order_id,
    COUNT(*)          AS occurrences,
    MAX(grand_total)  AS grand_total,
    MAX(status)       AS status
FROM magento_orders
GROUP BY order_id
HAVING COUNT(*) > 1    -- only show IDs that appear more than once
ORDER BY occurrences DESC;
-- RESULT: 5 order_ids each appear twice → 10 duplicate rows

-- Show the full duplicate rows side by side for review:
SELECT *
FROM magento_orders
WHERE order_id IN (
    SELECT order_id
    FROM magento_orders
    GROUP BY order_id
    HAVING COUNT(*) > 1
)
ORDER BY order_id;

-- Log the duplicates to the issues log:
INSERT INTO orders_issues_log (issue_type, order_id, detail, action_required)
SELECT DISTINCT
    'DUPLICATE_ORDER',
    order_id,
    CONCAT('order_id appears ', cnt, ' times in export'),
    'Keep first occurrence only — delete remaining rows'
FROM (
    SELECT order_id, COUNT(*) AS cnt
    FROM magento_orders
    GROUP BY order_id
    HAVING COUNT(*) > 1
) sub;


-- ─────────────────────────────────────────────────────────────
-- 3B. NULL / MISSING FIELD AUDIT
-- Problem: Critical fields may be empty — without invoice_number
--          an order cannot be submitted for payment.
-- ─────────────────────────────────────────────────────────────

-- Column-level null count across the entire orders table:
SELECT
    SUM(CASE WHEN order_id       IS NULL OR order_id = ''       THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN customer_id    IS NULL OR customer_id = ''    THEN 1 ELSE 0 END) AS null_customer_id,
    SUM(CASE WHEN sku             IS NULL OR sku = ''            THEN 1 ELSE 0 END) AS null_sku,
    SUM(CASE WHEN subtotal        IS NULL OR subtotal = ''       THEN 1 ELSE 0 END) AS null_subtotal,
    SUM(CASE WHEN grand_total     IS NULL                        THEN 1 ELSE 0 END) AS null_grand_total,
    SUM(CASE WHEN invoice_number  IS NULL OR invoice_number = '' THEN 1 ELSE 0 END) AS null_invoice_number,
    SUM(CASE WHEN payment_method  IS NULL OR payment_method = '' THEN 1 ELSE 0 END) AS null_payment_method,
    COUNT(*) AS total_rows
FROM magento_orders;
-- RESULT: invoice_number NULL on ~11 rows — all other critical fields populated

-- Get the specific orders missing invoice numbers:
SELECT
    order_id,
    customer_id,
    status,
    grand_total,
    invoice_number
FROM magento_orders
WHERE invoice_number IS NULL OR invoice_number = ''
ORDER BY order_id;

-- Log missing invoice numbers:
INSERT INTO orders_issues_log (issue_type, order_id, detail, action_required)
SELECT DISTINCT
    'MISSING_INVOICE',
    order_id,
    'invoice_number is NULL — order cannot be submitted for payment',
    'Request invoice number from billing/finance team'
FROM magento_orders
WHERE invoice_number IS NULL OR invoice_number = '';


-- ─────────────────────────────────────────────────────────────
-- 3C. TAX FORMAT INCONSISTENCY
-- Problem: Magento stores tax two different ways in the same
--          table. Some orders have a single combined_tax field.
--          Others split it into state_tax + county_tax.
--          The client's system needs ONE unified tax field.
-- ─────────────────────────────────────────────────────────────

-- Count orders using each tax format:
SELECT
    SUM(CASE WHEN combined_tax IS NOT NULL                             THEN 1 ELSE 0 END) AS using_combined_tax,
    SUM(CASE WHEN state_tax IS NOT NULL AND county_tax IS NOT NULL     THEN 1 ELSE 0 END) AS using_itemized_tax,
    SUM(CASE WHEN combined_tax IS NULL AND (state_tax IS NULL OR county_tax IS NULL)
             THEN 1 ELSE 0 END)                                                           AS missing_tax_entirely,
    COUNT(*) AS total_orders
FROM magento_orders;
-- RESULT: ~43 combined, ~57 itemized — both formats coexist in same export

-- Preview orders with combined tax (single field):
SELECT order_id, grand_total, combined_tax, state_tax, county_tax
FROM magento_orders
WHERE combined_tax IS NOT NULL
LIMIT 5;

-- Preview orders with itemized tax (two fields):
SELECT order_id, grand_total, combined_tax, state_tax, county_tax,
       ROUND(state_tax + county_tax, 2) AS itemized_total
FROM magento_orders
WHERE state_tax IS NOT NULL AND county_tax IS NOT NULL
LIMIT 5;


-- ─────────────────────────────────────────────────────────────
-- 3D. SKU FORMAT INCONSISTENCY
-- Problem: The same physical product appears under different
--          SKU codes depending on how it was entered.
--          Example: SKU-LED-001 and sku_led_001 are the same
--          product but will show as separate items in reports.
-- ─────────────────────────────────────────────────────────────

-- Find all distinct raw SKU values and how many orders each has:
SELECT
    sku             AS raw_sku,
    COUNT(*)        AS order_count,
    UPPER(REPLACE(REPLACE(REPLACE(sku, '_', '-'), ' ', '-'), 'SKU-', 'SKU-')) AS normalized_preview
FROM magento_orders
GROUP BY sku
ORDER BY normalized_preview;

-- Find SKU variants that normalize to the same canonical value:
-- (These are the ones that need fixing)
SELECT
    UPPER(REPLACE(REPLACE(sku, '_', '-'), ' ', '-'))  AS normalized_sku,
    COUNT(DISTINCT sku)                                AS variant_count,
    GROUP_CONCAT(DISTINCT sku ORDER BY sku SEPARATOR ' | ') AS raw_variants
FROM magento_orders
GROUP BY UPPER(REPLACE(REPLACE(sku, '_', '-'), ' ', '-'))
HAVING COUNT(DISTINCT sku) > 1;
-- RESULT: SKU-LED-001 appears as 'SKU-LED-001' and 'sku_led_001'


-- ─────────────────────────────────────────────────────────────
-- 3E. CURRENCY FORMAT — SUBTOTAL STORED AS TEXT
-- Problem: Magento exported subtotal as "$215.08" (a string).
--          You cannot run SUM(), AVG(), or any math on a string.
-- ─────────────────────────────────────────────────────────────

-- Show the problem — this SUM will return 0 or error:
SELECT subtotal FROM magento_orders LIMIT 5;
-- Returns: '$215.08', '$497.80', '$261.37', etc. — these are strings

-- Verify by attempting a cast and checking for NULLs:
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN CAST(REPLACE(REPLACE(subtotal,'$',''),',','') AS DECIMAL(10,2)) IS NULL
             THEN 1 ELSE 0 END) AS failed_conversions
FROM magento_orders;
-- RESULT: 0 failed conversions — all can be parsed once $ is removed


-- ─────────────────────────────────────────────────────────────
-- 3F. DATE FORMAT — WRONG FORMAT FOR SYSTEM UPLOAD
-- Problem: Dates stored as "06/22/2024 12:00" (US format).
--          ISO 8601 (YYYY-MM-DDTHH:MM:SS) is required for APIs
--          and system uploads — it's unambiguous and universally sortable.
-- ─────────────────────────────────────────────────────────────

-- Show the raw format:
SELECT order_id, order_date FROM magento_orders LIMIT 5;
-- Returns: '06/22/2024 12:00', '07/08/2024 06:03', etc.

-- Preview the conversion to proper MySQL DATE format:
SELECT
    order_id,
    order_date AS raw_date,
    STR_TO_DATE(order_date, '%m/%d/%Y %H:%i') AS converted_date,
    DATE_FORMAT(STR_TO_DATE(order_date, '%m/%d/%Y %H:%i'), '%Y-%m-%dT%H:%i:%sZ') AS iso_8601
FROM magento_orders
LIMIT 5;


-- ============================================================
-- SECTION 4: CLEANING — CREATE STAGING TABLE
-- ============================================================
-- The staging table (magento_orders_clean) applies ALL fixes
-- in one CREATE TABLE ... SELECT statement.
-- Raw data is NEVER modified — we always create a new clean
-- table and leave the original intact for audit purposes.
-- ============================================================

-- Drop if exists (for re-runs):
DROP TABLE IF EXISTS magento_orders_clean;

-- Create the clean staging table:
CREATE TABLE magento_orders_clean AS
SELECT
    -- ── ORDER IDENTITY ──────────────────────────────────────
    order_id,
    customer_id,

    -- ── DATE NORMALIZATION ───────────────────────────────────
    -- Convert from 'MM/DD/YYYY HH:MM' to proper MySQL DATETIME
    -- STR_TO_DATE parses the string; DATE_FORMAT outputs ISO 8601
    DATE_FORMAT(
        STR_TO_DATE(order_date, '%m/%d/%Y %H:%i'),
        '%Y-%m-%dT%H:%i:%sZ'
    ) AS order_date_iso,

    -- Also store as a plain DATE for grouping queries:
    DATE(STR_TO_DATE(order_date, '%m/%d/%Y %H:%i')) AS order_date,

    -- ── SKU NORMALIZATION ────────────────────────────────────
    -- UPPER converts to uppercase: 'sku_led_001' → 'SKU_LED_001'
    -- REPLACE swaps underscores for hyphens: 'SKU_LED_001' → 'SKU-LED-001'
    -- TRIM removes any leading/trailing whitespace
    UPPER(TRIM(REPLACE(REPLACE(sku, '_', '-'), ' ', '-'))) AS normalized_sku,

    qty,

    -- ── CURRENCY NORMALIZATION ───────────────────────────────
    -- REPLACE strips the $ sign and any commas (for values like $1,247.50)
    -- CAST converts the resulting string to a proper decimal number
    CAST(REPLACE(REPLACE(subtotal, '$', ''), ',', '') AS DECIMAL(10,2)) AS subtotal,

    -- ── TAX FIELD UNIFICATION ────────────────────────────────
    -- Business rule: the client needs ONE total_tax field.
    -- COALESCE returns the first non-NULL value in the list.
    -- If combined_tax exists, use it.
    -- If not, add state_tax + county_tax together.
    -- IFNULL(x, 0) treats NULL as zero so the addition works.
    COALESCE(
        combined_tax,
        ROUND(IFNULL(state_tax, 0) + IFNULL(county_tax, 0), 2)
    ) AS total_tax,

    -- Keep the original tax fields for audit trail:
    state_tax,
    county_tax,
    combined_tax,

    -- Record which tax format was used (useful for debugging):
    CASE
        WHEN combined_tax IS NOT NULL THEN 'combined'
        WHEN state_tax IS NOT NULL AND county_tax IS NOT NULL THEN 'itemized'
        ELSE 'missing'
    END AS tax_source,

    shipping,
    discount,
    grand_total,

    -- ── STATUS CODE MAPPING ──────────────────────────────────
    -- Magento uses plain English; Microsoft Dynamics needs numeric codes
    status AS magento_status,
    CASE status
        WHEN 'complete'   THEN '110'
        WHEN 'processing' THEN '100'
        WHEN 'closed'     THEN '120'
        WHEN 'pending'    THEN '50'
        WHEN 'canceled'   THEN '999'
        ELSE '000'
    END AS dynamics_status_code,

    -- Flag which orders are eligible for payment submission:
    CASE
        WHEN status IN ('complete', 'processing') THEN TRUE
        ELSE FALSE
    END AS payment_eligible,

    payment_method,
    invoice_number,

    -- ── DEDUPLICATION ───────────────────────────────────────
    -- ROW_NUMBER assigns 1 to the first occurrence of each order_id,
    -- 2 to the second, etc. We then delete rows where this > 1.
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY grand_total DESC) AS row_rank

FROM magento_orders;

-- Remove all duplicate rows — keep only the first occurrence per order_id:
DELETE FROM magento_orders_clean WHERE row_rank > 1;

-- Drop the helper column — it was only needed for deduplication:
ALTER TABLE magento_orders_clean DROP COLUMN row_rank;

-- Add a proper primary key now that duplicates are removed:
ALTER TABLE magento_orders_clean ADD PRIMARY KEY (order_id);

-- Verify the clean table:
SELECT
    COUNT(*)                            AS total_orders,
    COUNT(DISTINCT order_id)            AS unique_orders,
    SUM(CASE WHEN invoice_number IS NULL THEN 1 ELSE 0 END) AS missing_invoices,
    SUM(CASE WHEN payment_eligible = TRUE THEN 1 ELSE 0 END) AS payment_eligible_count
FROM magento_orders_clean;
-- RESULT: 100 total = 100 unique (no more dupes), ~11 missing invoices


-- ── Computed column: grand_total cross-check ────────────────
-- Recompute grand_total from its components and compare.
-- Any mismatch > $0.02 indicates a data corruption issue.
SELECT
    order_id,
    grand_total                                              AS recorded_total,
    ROUND(subtotal + total_tax + shipping - discount, 2)    AS recomputed_total,
    ABS(grand_total - ROUND(subtotal + total_tax + shipping - discount, 2)) AS discrepancy
FROM magento_orders_clean
WHERE ABS(grand_total - ROUND(subtotal + total_tax + shipping - discount, 2)) > 0.02
ORDER BY discrepancy DESC;
-- RESULT: 0 rows — all grand totals reconcile correctly


-- ── Incentive program mapping ────────────────────────────────
-- Each normalized SKU maps to an energy efficiency program.
-- This determines the incentive rate used to calculate the
-- reimbursement amount the utility client owes.
-- NOTE: In a real system this would be in a reference table.
--       Shown here as a CASE expression for clarity.
ALTER TABLE magento_orders_clean
    ADD COLUMN incentive_program VARCHAR(30) NULL,
    ADD COLUMN incentive_rate    DECIMAL(5,4) NULL,
    ADD COLUMN incentive_amount  DECIMAL(10,2) NULL;

UPDATE magento_orders_clean
SET
    incentive_program = CASE normalized_sku
        WHEN 'SKU-LED-001'    THEN 'ENERGY_EFF_LIGHTING'
        WHEN 'SKU-THERM-002'  THEN 'SMART_THERMOSTAT'
        WHEN 'SKU-SMART-003'  THEN 'SMART_HOME_PROG'
        WHEN 'SKU-AUDIT-004'  THEN 'HOME_ENERGY_AUDIT'
        WHEN 'SKU-HVAC-005'   THEN 'HVAC_UPGRADE'
        WHEN 'HVAC-005'       THEN 'HVAC_UPGRADE'
        WHEN 'SKU-REBATE-006' THEN 'DIRECT_REBATE'
        WHEN 'THERM002'       THEN 'SMART_THERMOSTAT'
        ELSE 'UNMAPPED'
    END,
    incentive_rate = CASE normalized_sku
        WHEN 'SKU-LED-001'    THEN 0.1500
        WHEN 'SKU-THERM-002'  THEN 0.2000
        WHEN 'SKU-SMART-003'  THEN 0.1800
        WHEN 'SKU-AUDIT-004'  THEN 0.2500
        WHEN 'SKU-HVAC-005'   THEN 0.2200
        WHEN 'HVAC-005'       THEN 0.2200
        WHEN 'SKU-REBATE-006' THEN 0.3000
        WHEN 'THERM002'       THEN 0.2000
        ELSE 0.0000
    END;

-- Calculate incentive_amount = subtotal × incentive_rate:
UPDATE magento_orders_clean
SET incentive_amount = ROUND(subtotal * incentive_rate, 2);

-- Verify: check for any unmapped SKUs (incentive_program = 'UNMAPPED'):
SELECT normalized_sku, COUNT(*) AS cnt
FROM magento_orders_clean
WHERE incentive_program = 'UNMAPPED'
GROUP BY normalized_sku;
-- RESULT: 0 rows — all SKUs successfully mapped to a program


-- ============================================================
-- SECTION 5: RECONCILIATION — MAGENTO vs PAYMENT PROCESSOR
-- ============================================================
-- Now that both tables are clean, we compare them.
-- The core question: does what Magento says happened match
-- what the payment processor (Authorize.net) says happened?
-- Any gaps = money that is unaccounted for.
-- ============================================================

-- ─────────────────────────────────────────────────────────────
-- 5A. HIGH-LEVEL RECONCILIATION SUMMARY
-- Compare order counts and revenue totals between sources.
-- This is the first number finance will ask about.
-- ─────────────────────────────────────────────────────────────
SELECT
    'Magento (complete + processing)' AS source,
    COUNT(DISTINCT order_id)          AS order_count,
    ROUND(SUM(grand_total), 2)        AS total_revenue,
    NULL                              AS total_fees,
    NULL                              AS net_revenue
FROM magento_orders_clean
WHERE magento_status IN ('complete', 'processing')

UNION ALL

SELECT
    'Authorize.net (settled)'         AS source,
    COUNT(DISTINCT order_id)          AS order_count,
    ROUND(SUM(gross_amount), 2)       AS total_revenue,
    ROUND(SUM(processor_fee), 2)      AS total_fees,
    ROUND(SUM(net_amount), 2)         AS net_revenue
FROM payment_transactions
WHERE status = 'settled'

UNION ALL

-- Show the gap between the two sources:
SELECT
    'GAP (Magento minus Processor)'   AS source,
    (SELECT COUNT(DISTINCT order_id) FROM magento_orders_clean WHERE magento_status IN ('complete','processing'))
    - (SELECT COUNT(DISTINCT order_id) FROM payment_transactions WHERE status = 'settled') AS order_count,
    ROUND(
        (SELECT SUM(grand_total) FROM magento_orders_clean WHERE magento_status IN ('complete','processing'))
        - (SELECT SUM(gross_amount) FROM payment_transactions WHERE status = 'settled'),
    2) AS total_revenue,
    NULL AS total_fees,
    NULL AS net_revenue;


-- ─────────────────────────────────────────────────────────────
-- 5B. ORPHAN TRANSACTIONS
-- Problem: Transactions in the processor file with NO matching
--          Magento order. This is money that moved but has no
--          record in the order system.
-- Possible causes: deleted orders, system migration artifact,
--                  fraud, or data from a different channel.
-- ─────────────────────────────────────────────────────────────
SELECT
    t.transaction_id,
    t.order_id,
    t.gross_amount,
    t.processor_fee,
    t.net_amount,
    t.status,
    t.settle_date,
    'No matching Magento order' AS flag
FROM payment_transactions t
LEFT JOIN magento_orders_clean o
    ON t.order_id = o.order_id
WHERE o.order_id IS NULL   -- the LEFT JOIN produced no match → orphan
  AND t.status = 'settled' -- only care about money that actually moved
ORDER BY t.gross_amount DESC;
-- RESULT: 3 orphan transactions totalling $421.47

-- Log orphans to issues log:
INSERT INTO orders_issues_log (issue_type, order_id, transaction_id, detail, action_required)
SELECT
    'ORPHAN_TRANSACTION',
    t.order_id,
    t.transaction_id,
    CONCAT('Settled transaction $', t.gross_amount, ' has no matching Magento order'),
    'Escalate to finance team — investigate whether order was deleted or from another channel'
FROM payment_transactions t
LEFT JOIN magento_orders_clean o ON t.order_id = o.order_id
WHERE o.order_id IS NULL AND t.status = 'settled';


-- ─────────────────────────────────────────────────────────────
-- 5C. UNSETTLED MAGENTO ORDERS
-- Problem: Magento shows complete/processing, but the payment
--          processor has no record of a settlement.
-- Possible causes: voided, pending, or genuinely unsettled.
-- ─────────────────────────────────────────────────────────────
SELECT
    o.order_id,
    o.magento_status,
    o.grand_total,
    o.payment_method,
    o.invoice_number,
    'No matching settled transaction' AS flag
FROM magento_orders_clean o
LEFT JOIN payment_transactions t
    ON o.order_id = t.order_id AND t.status = 'settled'
WHERE o.payment_eligible = TRUE  -- only complete/processing orders
  AND t.order_id IS NULL         -- no settled transaction found
ORDER BY o.grand_total DESC;


-- ─────────────────────────────────────────────────────────────
-- 5D. AMOUNT MISMATCH — same order, different dollar amounts
-- Problem: Both systems have the order, but the amounts differ.
-- This can happen due to post-order adjustments (refunds, fees).
-- ─────────────────────────────────────────────────────────────
SELECT
    o.order_id,
    o.grand_total                   AS magento_total,
    t.gross_amount                  AS processor_total,
    ABS(o.grand_total - t.gross_amount) AS discrepancy,
    CASE
        WHEN o.grand_total > t.gross_amount THEN 'Processor charged less than Magento recorded'
        ELSE 'Processor charged more than Magento recorded'
    END AS direction
FROM magento_orders_clean o
INNER JOIN payment_transactions t ON o.order_id = t.order_id
WHERE ABS(o.grand_total - t.gross_amount) > 0.01   -- allow 1-cent rounding tolerance
ORDER BY discrepancy DESC;


-- ============================================================
-- SECTION 6: BUILD SETTLEMENT-READY OUTPUT TABLE
-- ============================================================
-- This is the final deliverable: the table that gets exported
-- as CSV, JSON, or XLSX and uploaded to the client system
-- (Microsoft Dynamics, ARIBA, etc.).
-- Only includes: payment-eligible orders + settled transactions.
-- ============================================================

DROP TABLE IF EXISTS settlement_ready;

CREATE TABLE settlement_ready AS
SELECT
    -- ── ORDER DETAILS ──────────────────────────────────────
    o.order_id,
    o.invoice_number,
    o.order_date_iso        AS order_timestamp,
    t.settle_date           AS settlement_date,

    -- ── CUSTOMER DETAILS ───────────────────────────────────
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.state,

    -- ── PRODUCT & PROGRAM ──────────────────────────────────
    o.normalized_sku        AS sku_normalized,
    o.incentive_program,
    o.qty,

    -- ── FINANCIALS ─────────────────────────────────────────
    o.subtotal,
    o.total_tax,
    o.tax_source,
    o.shipping,
    o.discount,
    o.grand_total,
    o.incentive_rate,
    o.incentive_amount,
    t.processor_fee         AS processing_fee,
    t.net_amount            AS net_settled_amount,

    -- ── SYSTEM OF RECORD FIELDS ────────────────────────────
    o.dynamics_status_code,
    CASE o.dynamics_status_code
        WHEN '110' THEN 'INVOICED'
        WHEN '100' THEN 'IN_PROGRESS'
        WHEN '120' THEN 'CLOSED'
        WHEN '50'  THEN 'PENDING'
        WHEN '999' THEN 'VOID'
        ELSE 'UNKNOWN'
    END AS status_label,
    t.transaction_id        AS txn_id

FROM magento_orders_clean o
-- Inner join with settled transactions — only orders that have payment confirmed:
INNER JOIN payment_transactions t
    ON o.order_id = t.order_id
    AND t.status = 'settled'
-- Left join customers to enrich with contact info:
LEFT JOIN magento_customers c
    ON o.customer_id = c.customer_id
-- Only include payment-eligible orders:
WHERE o.payment_eligible = TRUE
ORDER BY t.settle_date, o.order_id;

-- Verify the settlement table:
SELECT
    COUNT(*)                              AS records_in_settlement,
    ROUND(SUM(grand_total), 2)            AS total_gross_revenue,
    ROUND(SUM(incentive_amount), 2)       AS total_incentives_payable,
    ROUND(SUM(processing_fee), 2)         AS total_processor_fees,
    ROUND(SUM(net_settled_amount), 2)     AS net_to_client
FROM settlement_ready;
-- RESULT: 19 records | ~$5,191 gross | ~$984 incentives | ~$156 fees


-- ============================================================
-- SECTION 7: EXPORT QUERIES
-- ============================================================
-- These SELECT statements generate the data that gets exported
-- to CSV/XLSX for the client. In practice, run these in your
-- BI tool or pipe through Python to generate the files.
-- ============================================================

-- Full settlement detail (CSV/XLSX Settlement Detail tab):
SELECT * FROM settlement_ready ORDER BY settlement_date, order_id;

-- Program summary (XLSX Program Summary tab):
SELECT
    incentive_program,
    COUNT(*)                         AS order_count,
    ROUND(SUM(subtotal), 2)          AS total_subtotal,
    ROUND(SUM(total_tax), 2)         AS total_tax,
    ROUND(SUM(incentive_amount), 2)  AS total_incentive,
    ROUND(SUM(net_settled_amount), 2) AS net_revenue
FROM settlement_ready
GROUP BY incentive_program
ORDER BY net_revenue DESC;

-- Monthly finance summary (XLSX Monthly Summary tab):
SELECT
    DATE_FORMAT(settlement_date, '%Y-%m')   AS month,
    COUNT(*)                                AS transactions,
    ROUND(SUM(grand_total), 2)              AS gross_revenue,
    ROUND(SUM(processing_fee), 2)           AS processing_fees,
    ROUND(SUM(incentive_amount), 2)         AS incentive_payable,
    ROUND(SUM(net_settled_amount), 2)       AS net_to_client
FROM settlement_ready
GROUP BY DATE_FORMAT(settlement_date, '%Y-%m')
ORDER BY month;

-- Issues log (XLSX Issues Log tab):
SELECT
    issue_type,
    order_id,
    transaction_id,
    detail,
    action_required,
    flagged_at
FROM orders_issues_log
ORDER BY issue_type, order_id;


-- ============================================================
-- SECTION 8: FINANCE & REPORTING QUERIES
-- ============================================================
-- Ad-hoc analytical queries that a Data Analytics Coordinator
-- would run to answer questions from the finance and account
-- management teams.
-- ============================================================

-- Revenue by order status (full orders table):
SELECT
    magento_status,
    dynamics_status_code,
    COUNT(*)                         AS order_count,
    ROUND(SUM(grand_total), 2)       AS total_value,
    ROUND(AVG(grand_total), 2)       AS avg_order_value
FROM magento_orders_clean
GROUP BY magento_status, dynamics_status_code
ORDER BY total_value DESC;

-- Top 5 SKUs by total revenue (normalized):
SELECT
    normalized_sku,
    incentive_program,
    COUNT(*)                         AS order_count,
    SUM(qty)                         AS total_units,
    ROUND(SUM(grand_total), 2)       AS total_revenue,
    ROUND(AVG(grand_total), 2)       AS avg_order_value,
    ROUND(SUM(incentive_amount), 2)  AS total_incentives
FROM magento_orders_clean
GROUP BY normalized_sku, incentive_program
ORDER BY total_revenue DESC
LIMIT 5;

-- Customer lifetime value (from clean orders):
SELECT
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name)  AS customer_name,
    c.state,
    c.loyalty_tier,
    COUNT(o.order_id)                        AS total_orders,
    ROUND(SUM(o.grand_total), 2)             AS lifetime_value,
    ROUND(AVG(o.grand_total), 2)             AS avg_order_value,
    MAX(o.order_date)                        AS last_order_date
FROM magento_customers c
LEFT JOIN magento_orders_clean o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.state, c.loyalty_tier
ORDER BY lifetime_value DESC
LIMIT 10;

-- Monthly trend: new orders and revenue (for executive dashboard):
SELECT
    DATE_FORMAT(order_date, '%Y-%m')    AS month,
    COUNT(*)                             AS new_orders,
    ROUND(SUM(grand_total), 2)           AS gross_revenue,
    ROUND(SUM(incentive_amount), 2)      AS incentives_generated,
    ROUND(AVG(grand_total), 2)           AS avg_order_value,
    SUM(CASE WHEN magento_status = 'complete' THEN 1 ELSE 0 END) AS completed_orders
FROM magento_orders_clean
GROUP BY DATE_FORMAT(order_date, '%Y-%m')
ORDER BY month;

-- Payment method breakdown (useful for processor fee analysis):
SELECT
    payment_method,
    COUNT(*)                        AS order_count,
    ROUND(SUM(grand_total), 2)      AS total_revenue,
    ROUND(AVG(grand_total), 2)      AS avg_value
FROM magento_orders_clean
GROUP BY payment_method
ORDER BY total_revenue DESC;

-- State-level revenue summary (for regional reporting):
SELECT
    c.state,
    COUNT(DISTINCT c.customer_id)   AS unique_customers,
    COUNT(o.order_id)               AS total_orders,
    ROUND(SUM(o.grand_total), 2)    AS total_revenue
FROM magento_customers c
INNER JOIN magento_orders_clean o ON c.customer_id = o.customer_id
GROUP BY c.state
ORDER BY total_revenue DESC;

-- Full issues log with priority ranking:
SELECT
    issue_type,
    COUNT(*) AS issue_count,
    CASE issue_type
        WHEN 'DUPLICATE_ORDER'     THEN '1-HIGH: Inflates revenue counts'
        WHEN 'MISSING_INVOICE'     THEN '2-HIGH: Blocks payment processing'
        WHEN 'ORPHAN_TRANSACTION'  THEN '3-MEDIUM: Unexplained settled money'
        ELSE '4-LOW: Data quality'
    END AS priority_and_impact
FROM orders_issues_log
GROUP BY issue_type
ORDER BY priority_and_impact;
