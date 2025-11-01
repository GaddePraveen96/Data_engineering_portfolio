import pandas as pd
import pymysql
from unidecode import unidecode

# Connection credentials
#host = ''
#port = 
#user = ''
#password = ''
#database = ''

# Cleaning function
def clean_text(text):
    if pd.isna(text):
        return text
    return ''.join(char for char in unidecode(str(text)) if char.isascii())

# Tables mapping
tables = {
    'raw_affiliations': 'affiliations_ascii_cleaned',
    'raw_authors': 'authors_ascii_cleaned',
    'raw_correspondence': 'correspondence_ascii_cleaned'
}

# Main loop (raw → clean → new table)
for raw_table, clean_table in tables.items():
    try:
        print(f"\n🔄 Cleaning `{raw_table}` → `{clean_table}`")

        # 1. Connect directly using PyMySQL
        conn = pymysql.connect(host=host, port=port, user=user, password=password, db=database)
        df = pd.read_sql(f"SELECT * FROM {raw_table}", conn)
        conn.close()

        if df.empty:
            print(f"⚠️ `{raw_table}` is empty. Skipping.\n")
            continue

        # 2. Clean special characters
        df_cleaned = df.apply(lambda col: col.map(clean_text))
        df_cleaned.drop_duplicates(inplace=True)

        if df_cleaned.empty:
            print(f"⚠️ All rows removed after cleaning `{raw_table}`. Skipping.\n")
            continue

        # 3. Save to NEW table using SQL INSERTs
        conn = pymysql.connect(host=host, port=port, user=user, password=password, db=database)
        cursor = conn.cursor()

        # Create new table
        columns = ', '.join([f"`{col}` TEXT" for col in df_cleaned.columns])
        cursor.execute(f"DROP TABLE IF EXISTS `{clean_table}`")
        cursor.execute(f"CREATE TABLE `{clean_table}` ({columns})")

        # Insert cleaned data
        rows = [tuple(x) for x in df_cleaned.to_numpy()]
        placeholders = ', '.join(['%s'] * len(df_cleaned.columns))
        insert_query = f"INSERT INTO `{clean_table}` VALUES ({placeholders})"

        cursor.executemany(insert_query, rows)
        conn.commit()
        conn.close()

        print(f"✅ Saved cleaned table `{clean_table}` with {len(df_cleaned)} rows.")

    except Exception as e:
        print(f"❌ Failed cleaning `{raw_table}`: {e}")
