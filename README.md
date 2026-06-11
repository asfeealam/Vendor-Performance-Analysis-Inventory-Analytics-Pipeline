# Vendor Performance Analysis: End-to-End Retail Analytics Project

## Project Overview

This project is a company-standard end-to-end Data Analytics solution designed to evaluate vendor performance, optimize inventory management, improve profitability, and support strategic business decision-making in the retail and wholesale industry.

The project simulates a real-world analytics workflow used by Data Analysts and Business Analysts, integrating SQL, Python, ETL pipelines, Statistical Analysis, and Power BI to solve business-critical problems.

Unlike traditional dashboard-only projects, this project focuses on solving actual business challenges through data-driven insights and actionable recommendations.

---

## Business Problem

Retail organizations work with multiple vendors and thousands of products across different categories. Poor vendor performance, inefficient inventory management, and supplier dependency can significantly impact profitability.

The objective of this project was to answer the following business questions:

* Which vendors contribute the most to revenue and profitability?
* Which vendors are underperforming?
* Which brands require promotional or pricing adjustments?
* How much capital is tied up in unsold inventory?
* Does bulk purchasing reduce procurement costs?
* How dependent is the business on a small group of vendors?
* Are there statistically significant differences between high-performing and low-performing vendors?

---

## Project Architecture

Raw Data (CSV Files)

⬇

SQLite Database

⬇

SQL-Based ETL Pipeline

⬇

Aggregated Vendor Performance Table

⬇

Python Analysis & Statistical Testing

⬇

Power BI Dashboard

⬇

Business Recommendations

---

## Dataset

The project uses multiple operational datasets commonly found in retail businesses:

### Purchases

* Vendor purchases
* Purchase quantities
* Purchase costs
* Purchase dates

### Sales

* Sales quantities
* Sales revenue
* Selling prices

### Vendor Invoices

* Freight costs
* Invoice information
* Vendor payment details

### Purchase Prices

* Product-level pricing information

### Inventory Tables

* Beginning inventory
* Ending inventory

---

## Technologies Used

### Database & ETL

* SQL
* SQLite
* SQLAlchemy

### Data Analysis

* Python
* Pandas
* NumPy

### Statistical Analysis

* SciPy
* Hypothesis Testing
* Confidence Intervals

### Visualization

* Matplotlib
* Seaborn
* Power BI

### Automation

* Python Scripting
* Logging Framework

---

## ETL Pipeline

A complete ETL process was developed to transform raw transactional data into an analysis-ready dataset.

### Data Extraction

* Imported multiple CSV files into SQLite
* Created database tables
* Automated ingestion process using Python scripts

### Data Transformation

* Merged multiple business tables
* Created vendor-level aggregated datasets
* Optimized SQL queries for large datasets
* Reduced millions of transactional records into a summarized analytical table

### Data Loading

* Stored processed vendor summary tables back into the database
* Enabled fast reporting and dashboard creation

---

## Feature Engineering

Several business KPIs were created:

### Gross Profit

Gross Profit = Total Sales Dollars − Total Purchase Dollars

### Profit Margin

Profit Margin = Gross Profit ÷ Total Sales Dollars × 100

### Inventory Turnover

Inventory Turnover = Total Sales Quantity ÷ Total Purchase Quantity

### Sales-to-Purchase Ratio

Sales-to-Purchase Ratio = Total Sales Dollars ÷ Total Purchase Dollars

### Unsold Inventory Value

Unsold Inventory = (Purchase Quantity − Sales Quantity) × Purchase Price

---

## Exploratory Data Analysis

Performed extensive EDA to:

* Understand sales patterns
* Identify outliers
* Evaluate profitability distributions
* Analyze vendor behavior
* Assess inventory movement

Techniques used:

* Summary Statistics
* Histograms
* Boxplots
* Correlation Heatmaps
* Scatter Plots

---

## Key Business Questions Solved

### 1. Which Brands Require Promotional Support?

Identified brands with:

* Low sales performance
* High profit margins

Result:

* 198 brands identified for pricing and promotional optimization.

---

### 2. Which Vendors Drive Sales?

Analyzed:

* Total sales revenue
* Total purchase volume
* Vendor contribution

Result:

* Identified top-performing vendors driving business revenue.

---

### 3. Vendor Dependency Analysis

Measured procurement dependency across vendors.

Result:

* Top 10 vendors contributed approximately 66% of total procurement.

Business Risk:

* High supplier concentration risk.

---

### 4. Does Bulk Purchasing Reduce Costs?

Analyzed order sizes against unit purchase costs.

Result:

| Order Size | Avg Unit Cost |
| ---------- | ------------- |
| Small      | ~$39          |
| Medium     | ~$15          |
| Large      | ~$10          |

Insight:

* Bulk purchasing significantly reduces procurement costs.

---

### 5. Inventory Turnover Analysis

Evaluated inventory efficiency across vendors.

Result:

* Identified vendors with low inventory turnover.
* Highlighted slow-moving inventory.

---

### 6. Unsold Inventory Analysis

Calculated inventory value not yet converted into revenue.

Result:

* Approximately $2.7 million tied up in unsold inventory.

Business Impact:

* Working capital inefficiency.

---

## Statistical Analysis

### Confidence Interval Analysis

Compared profit margins between:

* High-performing vendors
* Low-performing vendors

Finding:

* Low-performing vendors exhibited significantly higher profit margins.

Possible Explanation:

* Premium pricing strategy
* Lower operational costs

---

### Hypothesis Testing

Performed Independent T-Test.

#### Null Hypothesis (H0)

There is no significant difference in profit margins between high-performing and low-performing vendors.

#### Alternative Hypothesis (H1)

There is a significant difference in profit margins between high-performing and low-performing vendors.

Result:

* Statistically significant difference observed.

---

## Power BI Dashboard

The dashboard provides executive-level visibility into:

### KPIs

* Total Sales
* Total Purchases
* Gross Profit
* Profit Margin
* Unsold Inventory Value

### Visualizations

* Top Vendors by Sales
* Top Brands by Sales
* Vendor Purchase Contribution
* Vendor Dependency Analysis
* Low Inventory Turnover Vendors
* Low Performing Brands

---

## Key Insights

* Top 10 vendors contribute ~66% of total procurement.
* Approximately $2.7M remains locked in unsold inventory.
* Bulk purchasing reduces unit procurement costs significantly.
* Several high-margin brands suffer from low sales performance.
* Vendor dependency creates supply chain risks.
* Inventory turnover varies substantially across vendors.

---

## Business Recommendations

### Pricing Optimization

Adjust pricing strategies for low-selling, high-margin brands.

### Inventory Management

Reduce excess stock and improve inventory turnover.

### Vendor Diversification

Reduce dependency on a small group of suppliers.

### Bulk Purchasing Strategy

Leverage volume discounts to lower procurement costs.

### Marketing Optimization

Increase visibility of profitable but low-performing brands.

---

## Project Outcomes

This project demonstrates:

* SQL-based data extraction and transformation
* End-to-end ETL pipeline development
* Business-focused analytics
* Statistical hypothesis testing
* Executive dashboard development
* Real-world problem-solving using data

The project mirrors the workflow followed by Data Analysts and Business Analysts in retail, e-commerce, supply chain, and procurement domains.
