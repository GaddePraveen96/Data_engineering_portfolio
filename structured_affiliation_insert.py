import pandas as pd
import re
import unicodedata
import pymysql

# === CONFIG ===
DB_CONFIG = {
    #"host": "",
    #"port",
    #"user": "",
    #"password": "",  # update this
    #"database": "raw_data"
}
CHUNK_SIZE = 50000

# === Connect to DB ===
conn = pymysql.connect(**DB_CONFIG)
cursor = conn.cursor()

# === Create target table if not exists ===
cursor.execute("""
CREATE TABLE IF NOT EXISTS affiliations_structured_entities (
    Raw_Data_ID INT,
    Cleaned_First_Affiliation TEXT,
    Department TEXT,
    School TEXT,
    Institute TEXT,
    Laboratory TEXT,
    University TEXT,
    Company TEXT,
    Other_Org TEXT
)
""")
conn.commit()

# === Parser Function (same logic) ===
def parse_affiliation_with_eth_fix(text):
    if pd.isna(text):
        return pd.Series({
            "Department": "",
            "School": "",
            "Institute": "",
            "Laboratory": "",
            "University": "",
            "Company": "",
            "Other_Org": ""
        })

    text_norm = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("utf-8", "ignore")
    first_affil = text_norm.split(";")[0].strip()
    parts = re.split(r',|;', first_affil.lower())
    parts = [p.strip() for p in parts if p.strip()]

    department = school = institute = laboratory = university = company = other_org = ""
    university_keywords = [
        "university", "universitat", "universite", "universita", "universidade",
        "amrita vishwa vidyapeetham", "academy of sciences"
    ]
    known_universities = [
        "eth zurich", "epfl", "mit", "massachusetts institute of technology",
        "university of texas at el paso", "caltech", "harvard", "stanford"
    ]
    lab_keywords = ["laboratory", "lab"]
    institute_keywords = ["institute", "academy", "research center", "research institute"]
    company_keywords = [
        "company", "co.", "co ltd", "inc", "corp", "gmbh", "ltd",
        "technologies", "solutions", "systems", "group", "industries", "pharma", "limited"
    ]

    college_part = ""

    for part in parts:
        if re.match(r"^\d{2,5}\s", part):
            continue
        if any(kw in part for kw in university_keywords):
            university = part.title()
        elif any(known in part for known in known_universities):
            university = part.upper() if part.strip() in ["eth zurich", "epfl", "mit", "caltech"] else part.title()
        elif not laboratory and any(kw in part for kw in lab_keywords):
            laboratory = part.title()
        elif not institute and any(kw in part for kw in institute_keywords):
            institute = part.title()
        elif not school and "school" in part:
            school = part.title()
        elif not department and any(kw in part for kw in ["department", "faculty"]):
            department = part.title()
        elif not company and any(kw in part for kw in company_keywords):
            company = part.title()
        elif not college_part and "college" in part:
            college_part = part.title()

    if not university and college_part:
        university = college_part
    elif college_part and not school:
        school = college_part

    for part in parts:
        part_title = part.title()
        if part_title not in [department, school, institute, laboratory, university, company] and not other_org:
            other_org = part_title

    return pd.Series({
        "Department": department,
        "School": school,
        "Institute": institute,
        "Laboratory": laboratory,
        "University": university,
        "Company": company,
        "Other_Org": other_org
    })

# === Process in batches ===
offset = 0
while True:
    query = f"""
        SELECT Raw_Data_ID, Cleaned_First_Affiliation
        FROM affiliations_first_extracted
        LIMIT {CHUNK_SIZE} OFFSET {offset}
    """
    df = pd.read_sql(query, conn)

    if df.empty:
        break

    df_structured = df["Cleaned_First_Affiliation"].apply(parse_affiliation_with_eth_fix)
    df_final = pd.concat([df, df_structured], axis=1)

    insert_sql = """
    INSERT INTO affiliations_structured_entities
    (Raw_Data_ID, Cleaned_First_Affiliation, Department, School, Institute, Laboratory, University, Company, Other_Org)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_sql, df_final.to_records(index=False).tolist())
    conn.commit()

    print(f"Processed {offset + len(df)} rows")
    offset += CHUNK_SIZE

# === Cleanup ===
cursor.close()
conn.close()
print(" All done!")
