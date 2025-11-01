# Data_engineering_portfolio
“Demonstration of data cleaning and relational database structuring for large-scale Scientific datasets (synthetic sample).”


#Project Overview
This repository demonstrates a representative subset of my work on large-scale scientific data cleaning, structuring, and automation performed during my time at one of the company iam working
The uploaded datasets (Anon_Demo_scopus_like.csv and Anon_Cleaned_scopus.csv) contain synthetic 10-row and cleaned samples created solely for demonstration.
The original system processed over 7 million scientific database records sourced from multiple scientific domains, involving complex normalization, entity resolution, and database automation.


#Tech Stack & Tools
Languages: Python (pandas, numpy, regex, mysql.connector)
Database: MySQL (hosted on AWS RDS, managed via Navicat)
Pipeline: Python-based ETL scripts for parsing, deduplication, affiliation extraction, and domain mapping
Automation: SQL triggers + Python loops for batch inserts and logging
Visualization: Power BI dashboards for performance tracking (92% inbox placement, 300% faster automation)

| Component                          | Description                                                                                      |
| ---------------------------------- | ------------------------------------------------------------------------------------------------ |
| `Anon_Demo_Github_scopus_like.csv` | Unprocessed, messy Scopus-style export to simulate real-world raw data.                          |
| `Anon_Cleaned_scopus.csv`          | Cleaned and structured dataset showing normalized fields, deduplication, and entity consistency. |
| `scopus_cleaning_demo.py`          | Python script demonstrating the cleaning, standardization, and database-loading logic.           |
| `schema.sql`                       | Example of the relational schema used in MySQL to store and query structured publication data.   |

This project replicates the logic and structure of my original IAAM pipeline — designed to clean, normalize, and relationally map millions of Scopus records into a searchable MySQL database for analytics and outreach automation.

All data here is synthetic and anonymized, intended purely to showcase technical architecture, data handling logic, and workflow reproducibility.
