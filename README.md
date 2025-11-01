# Scopus Data Cleaning & Affiliation Structuring (Demo)

This repository is a **representative demo** of my work on large-scale **scientific data cleaning, structuring, and automation** at the *International Association of Advanced Materials (IAAM)*.  

While this demo uses small anonymized datasets (`~10 rows`), the **production pipeline handled over 7 million Scopus records**, processed and stored in **MySQL (AWS RDS)** using **Navicat** as the GUI.

> **Note:** All data here is synthetic and anonymized. Logic and structure are identical to the real IAAM implementation.

---

## Project Overview

This project showcases the end-to-end workflow of transforming messy Scopus exports into a clean, relational database structure that can be queried, analyzed, and automated for large-scale scientific outreach.

**Core operations:**
- Clean raw Scopus data dumps (`scopus_sample.csv`)
- Standardize author names, affiliations, and metadata
- Extract **country, city, and ZIP** from textual affiliations
- Structure data into MySQL-ready format (`structured_affiliation_insert.py`)
- Compare **raw vs cleaned** datasets for audit and visualization

---

## Tech Stack

- **Languages:** Python (pandas, numpy, regex, mysql.connector)  
- **Database:** MySQL (AWS RDS), managed via Navicat  
- **Pipeline:** Python-based ETL scripts (cleaning → parsing → structuring)  
- **Visualization:** Power BI dashboards for QA and performance metrics  

---

##  Pipeline Flow & File Logic

###  Raw Input
- **`scopus_sample.csv`**  
  Raw, messy Scopus-style export (authors + affiliations mixed, special characters, inconsistent delimiters).

---

### Cleaning Layer
- **`Special_Characters_Cleaning_Code.py`**  
  Removes encoding errors, extra semicolons, and non-ASCII junk.
  
- **`authors_cleaning.py`**  
  Standardizes author fields — spacing, delimiters, and case formatting.

---

### Parsing & Extraction Layer
- **`authors_seprataion_from_affiliation.py`**  
  Splits authors from affiliations when combined in one field (common in Scopus exports).  

- **`Affiliations_Cleaning_Step_1.py`**  
  First-level normalization — trims, standardizes, and deduplicates affiliation text.

- **`Affiliations_First_Country.py`**  
  Extracts the *first country occurrence* from each affiliation (for country-level mapping).

- **`City_ZIP_Extraction.py`**  
  Extracts *city* and *ZIP codes* for location-level insights and regional analytics.

---

### Structuring & Database Layer
- **`structured_affiliation_insert.py`**  
  Converts cleaned data into SQL insert statements and formats for MySQL import.  
  This mimics the real IAAM setup using **AWS RDS + Navicat**.

---

### Outputs
- **`Anon_Cleaned_scopus.csv`** – Cleaned, structured, and anonymized version.  
- **`scopus_sample.csv`** – Raw messy version (for before/after comparison).

---

## Example Pipeline Run (Local)

```bash
# 1. Clean the raw Scopus data
python Special_Characters_Cleaning_Code.py

# 2. Normalize author fields
python authors_cleaning.py

# 3. Separate authors from affiliations
python authors_seprataion_from_affiliation.py

# 4. Clean and extract metadata from affiliations
python Affiliations_Cleaning_Step_1.py
python Affiliations_First_Country.py
python City_ZIP_Extraction.py

# 5. Prepare structured data for SQL insert
python structured_affiliation_insert.py


Data Processing Architecture (Mermaid Diagram)

graph TD
    A[Raw Scopus Export<br>(scopus_sample.csv)] --> B[ Special Character Cleaning<br>(Special_Characters_Cleaning_Code.py)]
    B --> C[Author Normalization<br>(authors_cleaning.py)]
    C --> D[Author–Affiliation Separation<br>(authors_seprataion_from_affiliation.py)]
    D --> E[Affiliation Cleaning Step 1<br>(Affiliations_Cleaning_Step_1.py)]
    E --> F[Country Extraction<br>(Affiliations_First_Country.py)]
    F --> G[City & ZIP Extraction<br>(City_ZIP_Extraction.py)]
    G --> H[Structured Data Insert<br>(structured_affiliation_insert.py)]
    H --> I[Cleaned Output<br>(Anon_Cleaned_scopus.csv)]

Relational Schema Overview (MySQL)

Below is the simplified entity-relationship model of the final MySQL database design.
This schema was implemented in AWS RDS, managed through Navicat, and served as the backbone for structured analytics and automation.


erDiagram
    AUTHORS {
        int author_id PK
        string author_name
        string author_email
        string scopus_id
        int affiliation_id FK
    }

    AFFILIATIONS {
        int affiliation_id PK
        string affiliation_name
        string department
        string city
        string zip
        int country_id FK
    }

    COUNTRY {
        int country_id PK
        string country_name
        string region
    }

    PUBLISHERS {
        int publisher_id PK
        string publisher_name
    }

    ARTICLES {
        int article_id PK
        string title
        int year
        int publisher_id FK
        int author_id FK
        int affiliation_id FK
    }

    AUTHORS ||--o{ ARTICLES : writes
    AFFILIATIONS ||--o{ AUTHORS : hosts
    COUNTRY ||--o{ AFFILIATIONS : located_in
    PUBLISHERS ||--o{ ARTICLES : publishes

