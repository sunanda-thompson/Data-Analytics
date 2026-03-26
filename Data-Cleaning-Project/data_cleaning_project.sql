-- =====================================================
-- SALES DATA CLEANING PROJECT
-- PostgreSQL Table Creation & Analysis Queries
-- =====================================================




-- =====================================================
-- STEP 1: CREATE THE TABLE
-- =====================================================

/*
	the table must be created before any data can be imported.
	each column is defined with a specific data type that matches
	what was cleaned and prepared in excel.

	data type decisions:
		varchar(n)    → text with a known max length (ids, codes, state abbreviations)
		text          → text with no length limit (names, emails, descriptions)
		date          → calendar date (requires yyyy-mm-dd format on import)
		integer       → whole numbers only (quantity)
		numeric(10,2) → decimal numbers with 2 decimal places (prices, totals)
*/

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


-- =====================================================
-- STEP 2: IMPORT THE DATA
-- =====================================================

/*
	after creating the table, import the cleaned csv file.
	the file must:
		- have no bom (byte order mark) — save as plain csv, not utf-8 bom
		- use yyyy-mm-dd date format
		- be stored at a short, simple file path with no spaces
		- have the same number of columns as the table above

	windows tip: store the file at c:\sql\filename.csv
	the pgadmin import wizard struggles with long folder paths.
*/

copy sales_data
from 'C:/SQL/sales.csv'
delimiter ','
csv header
;


-- =====================================================
-- STEP 3: VERIFY THE IMPORT
-- =====================================================

-- always check your data immediately after importing
select
	*
from
	sales_data
;


-- confirm row count matches expectation (should be 17)
select
	count(*) as total_rows
from
	sales_data
;


-- =====================================================
-- STEP 4: EXPLORATORY ANALYSIS
-- =====================================================

/*
	before answering any business questions, explore the data.
	understand what values exist in each key column.
	never assume — always verify.
*/


-- what statuses exist?
select
	distinct status
from
	sales_data
order by
	status
;


-- what customer types exist?
select
	distinct customer_type
from
	sales_data
order by
	customer_type
;


-- what regions exist?
select
	distinct region
from
	sales_data
order by
	region
;


-- check for any remaining nulls in key columns
select
	  sum(case when customer_name  is null then 1 else 0 end) as null_names
	, sum(case when email          is null then 1 else 0 end) as null_emails
	, sum(case when purchase_date  is null then 1 else 0 end) as null_dates
	, sum(case when phone_number   is null then 1 else 0 end) as null_phones
	, sum(case when sales_rep      is null then 1 else 0 end) as null_sales_reps
from
	sales_data
;


-- =====================================================
-- STEP 5: BUSINESS QUESTIONS
-- =====================================================


-- total revenue and order count by status
select
	  status
	, count(*)        as order_count
	, sum(total)      as total_revenue
from
	sales_data
where
	status is not null
group by
	status
order by
	total_revenue desc
;


-- total revenue by region
select
	  region
	, count(*)        as order_count
	, sum(total)      as total_revenue
	, round(avg(total), 2) as avg_order_value
from
	sales_data
where
	region is not null
group by
	region
order by
	total_revenue desc
;


-- best performing sales reps
select
	  sales_rep
	, count(*)             as total_orders
	, sum(total)           as total_revenue
	, round(avg(total), 2) as avg_order_value
from
	sales_data
where
	sales_rep is not null
group by
	sales_rep
order by
	total_revenue desc
;


-- best selling products by revenue
select
	  product_name
	, count(*)        as times_ordered
	, sum(quantity)   as total_units_sold
	, sum(total)      as total_revenue
from
	sales_data
group by
	product_name
order by
	total_revenue desc
;


-- top 5 customers by total spend
select
	  customer_id
	, customer_name
	, count(*)        as total_orders
	, sum(total)      as total_spent
from
	sales_data
where
	customer_name is not null
group by
	  customer_id
	, customer_name
order by
	total_spent desc
limit 5
;


-- revenue by month
select
	  extract(year  from purchase_date) as year
	, extract(month from purchase_date) as month
	, count(*)                           as total_orders
	, sum(total)                         as monthly_revenue
from
	sales_data
where
	purchase_date is not null
group by
	  extract(year  from purchase_date)
	, extract(month from purchase_date)
order by
	  year
	, month
;


-- revenue after discount applied
select
	  customer_id
	, customer_name
	, product_name
	, total                                        as original_total
	, discount
	, round(total - (total * discount), 2)         as discounted_total
	, round(total * discount, 2)                   as discount_amount
from
	sales_data
where
	discount is not null
	and discount > 0
order by
	discount_amount desc
;


-- order breakdown by customer type
select
	  customer_type
	, count(*)             as total_orders
	, sum(total)           as total_revenue
	, round(avg(total), 2) as avg_order_value
from
	sales_data
where
	customer_type is not null
group by
	customer_type
order by
	total_revenue desc
;


-- what percentage of orders are in each status?
select
	  status
	, count(*) as order_count
	, round(
		count(*) * 100.0 / sum(count(*)) over ()
	  , 1) as pct_of_total
from
	sales_data
where
	status is not null
group by
	status
order by
	order_count desc
;


-- =====================================================
-- STEP 6: SAVE USEFUL RESULTS AS NEW TABLES
-- =====================================================

/*
	create table as select (ctas) saves query results
	as a new table. this does not touch the original data.
	use this to build summary/reporting tables.
*/


-- save completed orders only
create table completed_orders as
	select
		*
	from
		sales_data
	where
		status = 'completed'
;


-- save a revenue summary by region
create table revenue_summary as
	select
		  region
		, count(*)             as total_orders
		, sum(total)           as total_revenue
		, round(avg(total), 2) as avg_order_value
	from
		sales_data
	where
		region is not null
	group by
		region
	order by
		total_revenue desc
;


-- verify
select * from completed_orders;
select * from revenue_summary;
