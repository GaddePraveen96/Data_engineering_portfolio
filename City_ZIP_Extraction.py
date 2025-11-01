import pymysql
import pandas as pd
import re

# === CONFIG ===
BATCH_SIZE = 50000
DB_CONFIG = {
    #"host": "",
    #"port":,
    #"user": "",
    #"password": "",  # Update this
    #"database": ""
}

# === CITY EXTRACTION LOGIC (FROM YOUR CSV SCRIPT) ===
extended_blacklist = [
    'university', 'institute', 'school', 'department', 'faculty', 'college', 'center', 'centre',
    'academy', 'hospital', 'ministry', 'laboratory', 'key laboratory', 'universidade',
    'management and transportation engineering', 'universiti', 'ltd', 'company', 'co.', 'inc',
    'poligono industrial de cataboi', 'institut parisien de chimie moleculaire',
    'mce risalpur campus', 'national innovation platform for industry-education integration of energy storage technology',
    'Koneru Lakshmaiah Education Foundation', 'universitas', 'manufacturing', 'transport',
    'Blueglownano Technologies Limited', 'Instituto de Fisica e Matematica'
]

manual_city_map = {
    'national innovation platform for industry-education integration of energy storage technology': 'Harbin',
    'State Ethnic Affairs Commission': 'Yinchuan',
    'Electronic and Systems Engineering': 'UKM Bangi',
}

def extract_best_city(affiliation, country):
    affiliation_lower = str(affiliation).lower()
    
    for keyword, city in manual_city_map.items():
        if keyword in affiliation_lower:
            return city

    parts = [p.strip() for p in str(affiliation).split(",") if p.strip()]
    if parts and parts[-1].lower() == str(country).lower():
        parts.pop()

    cleaned_parts = []
    for p in parts:
        if re.fullmatch(r"[A-Z]{0,2}?\d{4,6}[A-Z]?", p):
            continue
        if re.fullmatch(r"\d{4,6}", p):
            continue
        cleaned_parts.append(p)

    for part in reversed(cleaned_parts):
        part_lower = part.lower()
        if not any(bl in part_lower for bl in extended_blacklist) and not re.search(r'\d', part):
            return part.strip()

    for part in cleaned_parts:
        part_lower = part.lower()
        if not any(bl in part_lower for bl in extended_blacklist) and not re.search(r'\d', part):
            return part.strip()

    return ""

# === ZIP Extraction (you can enhance later) ===
def extract_zip(text):
    zip_match = re.search(r"\b(?:[A-Z]{1,2}-)?\d{4,7}\b", text)
    po_box_match = re.search(r"P\.?O\.?\s?(Box)?\s?\d{2,}", text, re.IGNORECASE)
    return po_box_match.group() if po_box_match else (zip_match.group() if zip_match else None)

# === DB CONNECTION ===
conn = pymysql.connect(**DB_CONFIG)
cursor = conn.cursor()

# === CREATE OUTPUT TABLE ===
cursor.execute("""
CREATE TABLE IF NOT EXISTS affiliations_city_zip_extracted (
    Raw_Data_ID INT,
    Author_Name TEXT,
    Original_Affiliation TEXT,
    Cleaned_First_Affiliation TEXT,
    Extracted_Country TEXT,
    Extracted_City TEXT,
    ZIPcode_POBox TEXT
)
""")

# === GET TOTAL ROW COUNT ===
cursor.execute("SELECT COUNT(*) FROM affiliations_first_extracted")
total_rows = cursor.fetchone()[0]
print(f"Total rows in source table: {total_rows}")

offset = 0
inserted_rows = 0

while offset < total_rows:
    print(f"\n🔍 [DEBUG] Fetching batch at offset {offset}")
    query = f"""
        SELECT Raw_Data_ID, Author_Name, Original_Affiliation, Cleaned_First_Affiliation, Extracted_Country
        FROM affiliations_first_extracted
        LIMIT {BATCH_SIZE} OFFSET {offset}
    """
    df = pd.read_sql_query(query, conn)

    # Skip header or bad rows
    df = df[df['Raw_Data_ID'] != 'Raw_Data_ID']
    df = df[pd.to_numeric(df['Raw_Data_ID'], errors='coerce').notnull()]
    df['Raw_Data_ID'] = df['Raw_Data_ID'].astype(int)

    if df.empty:
        print(f"Empty or invalid batch at offset {offset}")
        offset += BATCH_SIZE
        continue

    # Apply your logic
    df["Extracted_City"] = df.apply(
        lambda row: extract_best_city(row["Cleaned_First_Affiliation"], row["Extracted_Country"]),
        axis=1
    )
    df["ZIPcode_POBox"] = df["Cleaned_First_Affiliation"].apply(lambda x: extract_zip(str(x)))

    # Insert
    insert_query = """
        INSERT INTO affiliations_city_zip_extracted
        (Raw_Data_ID, Author_Name, Original_Affiliation, Cleaned_First_Affiliation,
         Extracted_Country, Extracted_City, ZIPcode_POBox)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_query, df.values.tolist())
    conn.commit()

    print(f" Inserted rows {offset + 1} to {offset + len(df)}")
    inserted_rows += len(df)
    offset += BATCH_SIZE

print(f"\n DONE. Total rows inserted: {inserted_rows}")
cursor.close()
conn.close()
