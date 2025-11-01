import pandas as pd
import pymysql

# === Step 1: Connect to DB ===
try:
    conn = pymysql.connect(
        #host='',
        #port=,
        #user='',
        #password='', 
        #database='',
        #charset='',
        #autocommit=True
    )
except Exception as e:
    print(" DB connection failed:", e)
    exit()

# === Step 2: Create table without primary key ===
create_table_query = """
CREATE TABLE IF NOT EXISTS authors_extracted_from_affiliations (
    Raw_Data_ID INT,
    Abbreviated_Author_Name TEXT,
    Affiliation_Raw TEXT
);
"""


try:
    with conn.cursor() as cursor:
        cursor.execute(create_table_query)
    print("Table `authors_extracted_from_affiliations` is ready.")
except Exception as e:
    print(" Failed to create table:", e)
    exit()

# === Step 3: Load Source Data ===
df = pd.read_sql("SELECT raw_source_id, Authors with affiliations FROM raw_data_ascii_cleaned;", conn)
print(f" Loaded {len(df)} rows.")

# === Step 4: Extract Author + Affiliation Segments ===
extracted_rows = []

for _, row in df.iterrows():
    raw_id = row['raw_source_ID']
    full_text = str(row['Authors with affiliations'])

    segments = [seg.strip() for seg in full_text.split(';') if seg.strip()]

    for segment in segments:
        if ',' in segment:
            author, affiliation = segment.split(',', 1)
            extracted_rows.append((raw_id, author.strip(), affiliation.strip()))

print(f" Extracted {len(extracted_rows)} author–affiliation pairs. Inserting into table...")

# === Step 5: Insert into Table ===
insert_query = """
INSERT INTO authors_extracted_from_affiliations (Raw_Data_ID, Author_Name, Affiliation_Raw)
VALUES (%s, %s, %s)
"""

try:
    with conn.cursor() as cursor:
        cursor.executemany(insert_query, extracted_rows)
    print("Inserted successfully into `authors_extracted_from_affiliations`.")
except Exception as e:
    print("Failed to insert data:", e)
