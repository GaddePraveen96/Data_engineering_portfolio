import pymysql
import pandas as pd
import re

# === Step 1: Connect to MySQL using pymysql ===
conn = pymysql.connect(
    #host= Put host details
    #port= give port details,
    #user=give user details,
    #password='xyz',
    #database='data_base_name',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor,
    autocommit=True
)

cursor = conn.cursor()  #  Add this line

# === Step 2: Count total rows ===
cursor.execute("SELECT COUNT(*) AS total FROM raw_data_ascii_cleaned")
total_rows = cursor.fetchone()['total']
batch_size = 50000

# === Step 3: Create Output Table ===
cursor.execute("DROP TABLE IF EXISTS authors_cleaned_final")
cursor.execute("""
    CREATE TABLE authors_cleaned_final (
        Raw_Data_ID INT,
        Author_First_Name TEXT,
        Author_Last_Name TEXT,
        Author_ID TEXT
    )
""")

# === Step 4: Author Name Parser ===
def parse_author_name(author_str):
    author_str = author_str.strip()
    match = re.match(r'^\s*(?:(.*?),\s*(.*?)|(.+?))\s*(?:\(([^)]+)\))?$', author_str)
    if match:
        if match.group(1) and match.group(2):
            last = match.group(1).strip()
            first = match.group(2).strip()
        elif match.group(3):
            parts = match.group(3).strip().split()
            first = parts[0]
            last = ' '.join(parts[1:]) if len(parts) > 1 else ''
        else:
            return None
        author_id = match.group(4).strip() if match.group(4) else ''
        return first, last, author_id
    return None

# === Step 5: Process in Batches and Insert Cleaned Data ===
for offset in range(0, total_rows, batch_size):
    print(f"Processing rows {offset} to {offset + batch_size}")
    cursor.execute(f"""
        SELECT raw_source_id, Author full names
        FROM raw_data_ascii_cleaned
        LIMIT {batch_size} OFFSET {offset}
    """)
    rows = cursor.fetchall()

    insert_rows = []
    for row in rows:
        raw_data_id = row['raw_source_id']
        authors_raw = row['Author full names']
        if pd.notna(authors_raw):
            authors = [a.strip() for a in authors_raw.split(';') if a.strip()]
            for author in authors:
                parsed = parse_author_name(author)
                if parsed:
                    first, last, author_id = parsed
                    insert_rows.append((raw_data_id, first, last, author_id))

    if insert_rows:
        insert_sql = """
            INSERT INTO authors_cleaned_final
            (Raw_Data_ID, Author_First_Name, Author_Last_Name, Author_ID)
            VALUES (%s, %s, %s, %s)
        """
        cursor.executemany(insert_sql, insert_rows)
        print(f"Inserted {len(insert_rows)} rows.")

# === Step 6: Done ===
print("All author data cleaned and inserted successfully.")
cursor.close()
conn.close()