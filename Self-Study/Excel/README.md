# 📗 Excel — Notes & Reference Guide

Excel is one of the most widely used tools in data analytics. It lets you store, clean, and analyze data without writing any code. This guide walks through everything from the basics to more advanced features, using real Amazon transaction data and a customer dataset for practice.

---

## 📌 Before You Do Anything — Inspect Your Data

Whenever you open a new spreadsheet, don't jump straight into formulas. Take a moment to understand what you're looking at:

- **Make visual assumptions** — What does this data represent? What does each column mean?
- **Check for missing values** — Are there blank cells where there should be a number?
- **Check if values make sense** — Are prices negative? Are dates in the future when they shouldn't be?
- **Look for duplicate rows** — The same record appearing twice will throw off your analysis

Think of this like proofreading before you hand something in.

---

## 1. Worksheet Setup

| Task | How To Do It |
|------|-------------|
| Rename a sheet | Double-click the tab at the bottom → type a new name |
| Select all data at once | Click the small box in the top-left corner (between row 1 and column A) |

---

## 2. Sorting Data

Sorting lets you reorder your rows based on the values in a column — like sorting orders from oldest to newest.

**Steps:**
1. Click the top-left box to select all data
2. Go to **Data → Sort**
3. Check the box that says *"My list has headers"* (so Excel doesn't sort your column titles)
4. Choose which column to sort by and the direction (Oldest → Newest, A → Z, Largest → Smallest)
5. Click **+** to add a second sorting rule (e.g., first by date, then by amount)

---

## 3. Filtering Data

Filters let you temporarily show only the rows that match a condition — like showing only orders from a specific category.

- **Data → Filter** adds a dropdown arrow to each column header
- Click the arrow to choose which values to show or hide
- **Always turn filters off when you're done** (same button) — leaving them on can make you think data is missing

---

## 4. Locking Cell References with `$`

When you copy a formula to other cells, Excel automatically adjusts the cell references. Sometimes you don't want that — for example, if one cell contains a tax rate that every formula should reference.

Use `$` to lock a reference so it doesn't change when copied:

```
= D2 * D14      ← Both references will shift when copied
= D2 * D$14     ← Row 14 stays locked; D will shift
= D2 * $D$14    ← Both column D and row 14 are fully locked
```

> **Rule of thumb:** If a value is used by many formulas (like a rate or a total), lock it with `$`.

---

## 5. Data Types

Excel needs to know whether a cell contains a number, a date, text, etc. If your numbers are stored as text, formulas won't work correctly.

**To change a data type:**
1. Select the column or cells
2. Right-click → **Format Cells**
3. Choose the correct type (Number, Date, Text, Currency, etc.)

Also accessible from the **Home** ribbon → Number format dropdown.

---

## 6. Grouping & Hiding Columns

If you have columns that aren't needed for your current analysis, you can group and collapse them to keep things tidy.

- Select columns (e.g., C through D) → **Data → Group**
- A small `−` button appears above the columns — click it to collapse them
- Click `+` to expand again

This doesn't delete data, it just hides it temporarily.

---

## 7. Formulas

Formulas start with `=` and tell Excel to calculate something. Here are the most important ones:

### Logical Formulas

```excel
-- IF: Returns one value if a condition is true, another if false
=IF(B2 = "underpants", 1, 0)
-- Translation: "If the value in B2 is 'underpants', put 1 here. Otherwise put 0."

-- OR: Returns TRUE if at least one condition is met
=OR(B2 = "power_inverter", B2 = "building_material")
-- Translation: "Is B2 either of these two things?"
```

### Math Formulas

```excel
-- SUM: Adds up a range of cells
=SUM(B2:B4)            -- Adds B2, B3, and B4
=SUM(B:B)              -- Adds the entire column B

-- SUMIF: Adds only the cells that meet a condition
=SUMIF(B:B, "underpants", I:I)
-- Translation: "Look in column B. Every time you find 'underpants', add the
--              corresponding value from column I."
```

### Statistical Formulas

```excel
-- COUNT: Counts how many cells in a range have numbers
=COUNT(I:I)
=COUNT(I2:I1390)

-- COUNTIF: Counts only the cells that meet a condition
=COUNTIF(B:B, "building_material")
-- Translation: "How many times does 'building_material' appear in column B?"

-- AVERAGE: Calculates the mean (can be skewed by extreme values)
=AVERAGE(I2:I1390)

-- MEDIAN: Finds the middle value (better than average when data has outliers)
=MEDIAN(I2:I1390)

-- MODE: Finds the most frequently occurring value
=MODE(I2:I1390)

-- MIN / MAX: Finds the smallest or largest value
=MIN(I2:I1390)
=MAX(I2:I1390)
```

> **Tip:** Press `Shift + Ctrl + ↓` to quickly jump to the last row of a column when selecting a range.

### Quartile Formulas

Quartiles split your data into four equal groups. This is useful for understanding the spread of your data.

```excel
=QUARTILE.INC(P2:P13, 1)    -- 25th percentile (bottom quarter)
=QUARTILE.INC(P2:P13, 2)    -- 50th percentile (the median/middle)
=QUARTILE.INC(P2:P13, 3)    -- 75th percentile (top quarter)
```

---

## 8. Lookups — Finding Data in Another Sheet

Lookup formulas let you search for a value in one table and pull back related information from another — like searching for a state ID and returning the state name.

### VLOOKUP (the classic — looks Vertically)

```excel
=VLOOKUP(H2, states!A:B, 2, FALSE)
```

Breaking this down piece by piece:
- `H2` — The value you're searching for (the State ID in the customers sheet)
- `states!A:B` — The table to search in (columns A and B on the States sheet)
- `2` — Return the value from the 2nd column of that table (the state abbreviation)
- `FALSE` — Only return an exact match (don't guess)

### XLOOKUP (newer, more flexible — recommended)

```excel
=XLOOKUP(H2, states!C:C, states!B:B)
```

- `H2` — The value you're searching for
- `states!C:C` — The column to search in
- `states!B:B` — The column to return the value from

> **Why XLOOKUP over VLOOKUP?** With VLOOKUP, the lookup column must always be the leftmost column. XLOOKUP has no such restriction — you can search and return from any columns.

---

## 9. Charts & Visualizations

| Chart Type | Best Used For |
|------------|---------------|
| **Bar / Column** | Comparing values across categories (e.g. sales by product) |
| **Line** | Showing trends over time (e.g. monthly revenue) |
| **Scatter Plot** | Spotting correlations and outliers (e.g. price vs. quantity sold) |
| **Pareto Chart** | Showing which items drive the most value (80/20 analysis) |

**To create a chart:**
1. Select the data columns you want (hold `Ctrl` to select non-adjacent columns)
2. Go to **Insert → Charts** and pick your type

**Scatter Plot insight:** Clusters of dots show you where most of your data lives. Dots far away from the cluster are *outliers* — worth investigating.

**Pareto Chart insight:** Helps you see that a small number of categories (e.g. 20% of products) often drive the majority of results (e.g. 80% of revenue).

---

## 10. Pivot Tables

Pivot tables are one of the most powerful Excel features. They let you drag and drop columns to instantly summarize and group your data — no formulas required.

**To create one:**
1. Click anywhere in your data
2. **Insert → PivotTable**
3. Drag fields into the Rows, Columns, and Values boxes on the right panel

**To access the field list later:** PivotTable Analyze → Field List

---

## 📂 Practice Files Used

| File | What It's For |
|------|--------------|
| `amazon_transactions.xlsx` | Sorting, filtering, formulas practice |
| `customers_and_states.xlsx` | VLOOKUP / XLOOKUP practice |
| `XLOOKUP_Excel_Tutorial_File.xlsx` | Dedicated XLOOKUP walkthrough |
| `Formula_Excel_Template.xlsx` | Formula practice template |
