# 📊 Tableau — Notes & Dashboard Framework

Tableau is a data visualization tool that lets you build interactive dashboards without writing code. You connect it to a data source (like an Excel file or a database), drag and drop fields, and it generates charts automatically.

This guide covers the **thinking framework** behind building a good dashboard — because the biggest mistake beginners make is opening Tableau before they've thought through what they actually want to show.

---

## 🧠 The Framework: Think Before You Build

A professional dashboard answers specific business questions. Before you open Tableau, you need to understand the business and its data. Otherwise you'll spend hours building something that doesn't actually help anyone.

---

## Step A: Understand the Business

Ask these questions about any business you're building a dashboard for:

| Question | Why It Matters |
|----------|---------------|
| What is the launch date vs. today's date? | Tells you how much historical data you actually have to work with. A 3-month-old company can't show year-over-year trends. |
| What does the business do? | You can't measure success without understanding what success looks like for them. |
| How does the business operate? | Online vs. brick-and-mortar businesses have very different key metrics. An online store cares about web traffic and cart abandonment; a physical store cares about foot traffic and average transaction size. |

---

## Step B: Understand Your Role as a Data Analyst

Your job isn't just to make pretty charts. Here's what you're actually responsible for:

**1. Identify Key Metrics**
What numbers does this business actually care about? Revenue, transactions, units sold, customer count? Don't show 20 metrics — find the 3-5 that drive decisions.

**2. Choose the Right Chart**
A bad chart makes good data confusing. A line chart to show a single number is wrong. A bar chart to show a trend over 3 years is wrong. Matching the chart type to the question is a skill (see chart guide below).

**3. Provide "Double-Click" Metrics**
A good dashboard has layers:
- **Top level:** The headline KPI (e.g. total revenue this month)
- **Second level:** Breakdown behind it (e.g. revenue by store or by product category)

This lets executives see the summary, and analysts dig into the detail — same dashboard, different use.

**4. Ensure Professionalism & Accuracy**
One wrong number on a dashboard and you lose people's trust. Always verify your totals match the source data before sharing.

---

## Step C: Assess Your Data

Before you build anything, inventory what data you have and understand what it contains.

**Example datasets used in this course:**

| File | What It Contains |
|------|-----------------|
| `Store_wbr_trans_data_20230424.xlsx` | Store-level revenue, transaction count, and unit sales — broken down by date and store number |
| `Sales_by_date_and_prod_cat.xlsx` | Company-wide revenue, transaction count, and unit sales — broken down by date and product category |

**Data span:** 3 years of data — enough for year-over-year comparisons, trend lines, and seasonal analysis.

**Key questions to ask about your data:**
- What is the date range?
- What is the granularity? (daily, weekly, monthly records?)
- Are there missing dates or gaps?
- What does each row represent? (one row per transaction? per store per day?)

---

## Step D: Define Your Reporting Time Frame

Not all businesses need the same time granularity. Pick the right level before you build:

| Granularity | Use When |
|-------------|----------|
| **Annual** | Executive summaries, year-over-year performance |
| **Quarterly** | Finance and investor reporting |
| **Monthly** | Standard operations and marketing dashboards |
| **Weekly** | Retail, e-commerce, fast-moving businesses |
| **Daily** | High-frequency monitoring (e.g. campaign performance) |
| **Hourly** | Flash sales, live events, real-time alerts |

> **Rule of thumb:** Match time granularity to how frequently decisions are made using that data. If the sales team reviews numbers weekly, build a weekly dashboard — not daily.

---

## Step E: Chart Selection Guide

Choosing the wrong chart type makes your data harder to understand, not easier. Use this as a reference:

| Chart Type | Best For | Common Mistake |
|------------|----------|---------------|
| **Line Chart** | Trends over time (revenue by month) | Don't use for categories with no natural order |
| **Bar / Column Chart** | Comparing values across categories | Don't use for time trends with many data points |
| **Scatter Plot** | Spotting correlations and outliers | Hard to read without clear axis labels |
| **Pareto Chart** | Finding the 20% of causes driving 80% of the effect | Don't use when all items contribute roughly equally |
| **Map** | Geographic distribution of data | Only use when location actually matters to the insight |
| **Heat Map** | Patterns across two dimensions (e.g. sales by day of week × hour) | Needs enough data to be meaningful |
| **KPI Card / Big Number** | Showing a single headline metric | Don't show without context (vs. prior period) |

---

## Step F: Dashboard Design Principles

Once you're in Tableau, keep these principles in mind:

**Lead with the most important number.** Put the key KPI at the top left — that's where eyes go first.

**Less is more.** A dashboard with 15 charts overwhelms people. Aim for 4-6 focused visuals that tell a complete story.

**Always provide context.** A number without comparison means nothing. "$500K revenue" — is that good or bad? Add a "vs. last month" or "vs. target" comparison.

**Revenue is almost always relevant.** If you're unsure what to show a business, revenue is almost always on the dashboard.

**Test before you share.** Verify that your chart totals match what you'd calculate manually from the source data.

---

## 📂 Practice Files

| File | Description |
|------|-------------|
| `Tableau_Notes.docx` | Original raw notes from the course |
| *(Tableau workbook files to be added as course progresses)* | Dashboard exercises |

---

## 🔗 Resources

- [Tableau Public](https://public.tableau.com) — Free version of Tableau. You can publish and share dashboards here.
- [AnalyticsMentor.io](https://analyticsmentor.io) — Source curriculum
- [Tableau Chart Type Guide](https://www.tableau.com/learn/whitepapers/which-chart-or-graph-is-right-for-you) — Official guide to choosing the right visualization
