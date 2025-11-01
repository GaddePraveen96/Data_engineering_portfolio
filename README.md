# Data_engineering_portfolio
“Demonstration of data cleaning and relational database structuring for large-scale Scientific datasets (synthetic sample).”


#Scopus Data Cleaning & Affiliation Structuring (Demo)

This repo is a representative demo of the data-engineering work I did at an organization i worked to clean and structure large-scale Author exports from publication databases.
The files here use small, anonymized CSVs, but the actual pipeline was built for ~7,000,000 rows and ran on MySQL (AWS RDS) + Navicat. This version is safe to make public.

1. What this repo shows

How to take a messy Scopus dump (scopus_sample.csv)

Clean special/unicode/junk characters

Separate authors from affiliations (Scopus loves to put them together 🙄)

Normalize and extract country, city, ZIP from affiliation strings

Prepare data for relational DB insert (structured_affiliation_insert.py)

Show a before → after comparison (Anon_Cleaned_scopus.csv)

2. Files and flow
  Raw input

  scopus_sample.csv
  Small sample of the original Scopus export. Columns and messiness are kept close to real Scopus (authors bunched, affiliations long, publisher strings, etc.).

#Cleaning layer

Special_Characters_Cleaning_Code.py

First pass cleaning.

Removes weird encodings, extra semicolons, non-ASCII stuff that breaks parsing.

This is the “make it processable” step.

authors_cleaning.py

Normalizes author strings (spacing, delimiter consistency, name order).

Prepares author text for the next step where we separate authors from affiliations.


authors_seprataion_from_affiliation.py

Scopus sometimes mixes authors + affiliation in the same field.

This script splits those into author-level data and affiliation-level data.

Output from here is more “tabular” and easier to load.

Affiliations_Cleaning_Step_1.py

First standardization of affiliations.

Removes duplicate institute strings, trims “Dept. of…”, harmonizes separators.

Goal: turn human-written affiliation strings into machine-readable segments.

Affiliations_First_Country.py

Looks at the affiliation string and tries to extract the country reliably.

This is needed because later, in the 7M-row version, we mapped countries to outreach rules / email domains.

City_ZIP_Extraction.py

Pulls city / ZIP / location from the same affiliation text.

Useful when building location-wise analytics or routing to country-specific campaigns.
  

structured_affiliation_insert.py

Takes the cleaned + parsed data and formats it for inserts into MySQL.

This is the part I used with Navicat + AWS RDS in the original IAAM system.

In the real setup, this went into relational tables (authors, affiliations, countries, publishers).


Outputs / demo data

Anon_Cleaned_scopus.csv

This is the cleaned version, with names anonymized.

You can show a before/after using:

scopus_sample.csv → raw

Anon_Cleaned_scopus.csv → structured/cleaned



Why this matters (for recruiters)

This is not “just Python.” It’s data engineering for messy scientific data.

You can see I understand:

extraction → cleaning → normalization → structuring → DB load

working with semi-structured exports (Scopus, Web of Science style)

building it in separate scripts so it can scale / be scheduled

This is exactly what you need in pharma / regulatory / R&D orgs that run on bad Excel exports.
