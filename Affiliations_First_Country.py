import pymysql
import re
import unidecode

# === Step 1: Connect to DB ===
conn = pymysql.connect(
    #host='',
    #port=,  #  lowercase 'port' and proper comma
    #user='',
    #password='',
    #database='raw_data',
    #charset='utf8mb4',
    #autocommit=True
)
cursor = conn.cursor()


# === Step 2: Load Data ===
cursor.execute("SELECT Raw_Data_ID, Author_Name, Affiliation_Raw FROM authors_extracted_from_affiliations")
rows = cursor.fetchall()

# === Step 3: Country List & Pattern ===
country_list = {
    # North America
   "antigua and barbuda": "Antigua and Barbuda",
    "bahamas": "Bahamas",
    "barbados": "Barbados",
    "belize": "Belize",
    "canada": "Canada",
    "costa rica": "Costa Rica",
    "cuba": "Cuba",
    "dominica": "Dominica",
    "dominican republic": "Dominican Republic",
    "el salvador": "El Salvador",
    "grenada": "Grenada",
    "guatemala": "Guatemala",
    "haiti": "Haiti",
    "honduras": "Honduras",
    "jamaica": "Jamaica",
    "mexico": "Mexico",
    "nicaragua": "Nicaragua",
    "panama": "Panama",
    "saint kitts and nevis": "Saint Kitts and Nevis",
    "saint lucia": "Saint Lucia",
    "saint vincent and the grenadines": "Saint Vincent and the Grenadines",
    "trinidad and tobago": "Trinidad and Tobago",
    "united states": "United States",
    "greenland": "Greenland",
    "bermuda": "Bermuda",
    "puerto rico": "Puerto Rico",

    # Europe
    "albania": "Albania",
    "andorra": "Andorra",
    "armenia": "Armenia",
    "austria": "Austria",
    "azerbaijan": "Azerbaijan",
    "belarus": "Belarus",
    "belgium": "Belgium",
    "bosnia and herzegovina": "Bosnia and Herzegovina",
    "bulgaria": "Bulgaria",
    "croatia": "Croatia",
    "cyprus": "Cyprus",
    "czech republic": "Czech Republic",
    "denmark": "Denmark",
    "estonia": "Estonia",
    "finland": "Finland",
    "france": "France",
    "georgia": "Georgia",
    "germany": "Germany",
    "greece": "Greece",
    "hungary": "Hungary",
    "iceland": "Iceland",
    "ireland": "Ireland",
    "italy": "Italy",
    "kazakhstan": "Kazakhstan",
    "kosovo": "Kosovo",
    "latvia": "Latvia",
    "liechtenstein": "Liechtenstein",
    "lithuania": "Lithuania",
    "luxembourg": "Luxembourg",
    "malta": "Malta",
    "moldova": "Moldova",
    "monaco": "Monaco",
    "montenegro": "Montenegro",
    "netherlands": "Netherlands",
    "north macedonia": "North Macedonia",
    "norway": "Norway",
    "poland": "Poland",
    "portugal": "Portugal",
    "romania": "Romania",
    "russia": "Russia",
    "san marino": "San Marino",
    "serbia": "Serbia",
    "slovakia": "Slovakia",
    "slovenia": "Slovenia",
    "spain": "Spain",
    "sweden": "Sweden",
    "switzerland": "Switzerland",
    "turkey": "Turkey",
    "Turkiye": "Turkey",
    "ukraine": "Ukraine",
    "united kingdom": "United Kingdom",
    "vatican city": "Vatican City",

    # Asia
    "afghanistan": "Afghanistan",
    "armenia": "Armenia",
    "azerbaijan": "Azerbaijan",
    "bahrain": "Bahrain",
    "bangladesh": "Bangladesh",
    "bhutan": "Bhutan",
    "brunei": "Brunei",
    "cambodia": "Cambodia",
    "china": "China",
    "cyprus": "Cyprus",
    "georgia": "Georgia",
    "india": "India",
    "indonesia": "Indonesia",
    "iran": "Iran",
    "iraq": "Iraq",
    "israel": "Israel",
    "japan": "Japan",
    "jordan": "Jordan",
    "kazakhstan": "Kazakhstan",
    "kuwait": "Kuwait",
    "kyrgyzstan": "Kyrgyzstan",
    "laos": "Laos",
    "lebanon": "Lebanon",
    "malaysia": "Malaysia",
    "maldives": "Maldives",
    "mongolia": "Mongolia",
    "myanmar": "Myanmar",
    "nepal": "Nepal",
    "north korea": "North Korea",
    "oman": "Oman",
    "pakistan": "Pakistan",
    "palestine": "Palestine",
    "philippines": "Philippines",
    "qatar": "Qatar",
    "Russian Federation": "Russia",
    "saudi arabia": "Saudi Arabia",
    "singapore": "Singapore",
    "south korea": "South Korea",
    "sri lanka": "Sri Lanka",
    "Syrian Arab Republic": "Syria",
    "taiwan": "Taiwan",
    "tajikistan": "Tajikistan",
    "thailand": "Thailand",
    "timor-leste": "Timor-Leste",
    "turkey": "Turkey",
    "turkmenistan": "Turkmenistan",
    "united arab emirates": "United Arab Emirates",
    "uzbekistan": "Uzbekistan",
    "Viet Nam": "Vietnam",
    "yemen": "Yemen",
    "hong kong": "Hong Kong",
    "Macao": "Macau",

    # Africa
    "algeria": "Algeria",
    "angola": "Angola",
    "benin": "Benin",
    "botswana": "Botswana",
    "burkina faso": "Burkina Faso",
    "burundi": "Burundi",
    "cabo verde": "Cabo Verde",
    "cameroon": "Cameroon",
    "central african republic": "Central African Republic",
    "chad": "Chad",
    "comoros": "Comoros",
    "democratic republic of the congo": "Democratic Republic of the Congo",
    "republic of the congo": "Republic of the Congo",
    "djibouti": "Djibouti",
    "egypt": "Egypt",
    "equatorial guinea": "Equatorial Guinea",
    "eritrea": "Eritrea",
    "eswatini": "Eswatini",
    "ethiopia": "Ethiopia",
    "gabon": "Gabon",
    "gambia": "Gambia",
    "ghana": "Ghana",
    "guinea": "Guinea",
    "guinea-bissau": "Guinea-Bissau",
    "kenya": "Kenya",
    "lesotho": "Lesotho",
    "liberia": "Liberia",
    "libya": "Libya",
    "madagascar": "Madagascar",
    "malawi": "Malawi",
    "mali": "Mali",
    "mauritania": "Mauritania",
    "mauritius": "Mauritius",
    "morocco": "Morocco",
    "mozambique": "Mozambique",
    "namibia": "Namibia",
    "niger": "Niger",
    "nigeria": "Nigeria",
    "rwanda": "Rwanda",
    "sao tome and principe": "S\u00e3o Tom\u00e9 and Pr\u00edncipe",
    "senegal": "Senegal",
    "seychelles": "Seychelles",
    "sierra leone": "Sierra Leone",
    "somalia": "Somalia",
    "south africa": "South Africa",
    "south sudan": "South Sudan",
    "sudan": "Sudan",
    "tanzania": "Tanzania",
    "togo": "Togo",
    "tunisia": "Tunisia",
    "uganda": "Uganda",
    "zambia": "Zambia",
    "zimbabwe": "Zimbabwe",
    "sahrawi arab democratic republic": "Sahrawi Arab Democratic Republic",
    "somaliland": "Somaliland",
    "r\u00e9union": "R\u00e9union",
    "mayotte": "Mayotte",
    "canary islands": "Canary Islands",
    "ceuta": "Ceuta",
    "melilla": "Melilla",
    "madeira": "Madeira",

    # Oceania
    "australia": "Australia",
    "fiji": "Fiji",
    "kiribati": "Kiribati",
    "marshall islands": "Marshall Islands",
    "micronesia": "Micronesia",
    "nauru": "Nauru",
    "new zealand": "New Zealand",
    "palau": "Palau",
    "papua new guinea": "Papua New Guinea",
    "samoa": "Samoa",
    "solomon islands": "Solomon Islands",
    "tonga": "Tonga",
    "tuvalu": "Tuvalu",
    "vanuatu": "Vanuatu",
    "new caledonia": "New Caledonia",
    "french polynesia": "French Polynesia",
    "guam": "Guam",

    # South America
    "brazil": "Brazil",
    "argentina": "Argentina",
    "chile": "Chile",
    "colombia": "Colombia",
    "peru": "Peru",
    "venezuela": "Venezuela",
    "uruguay": "Uruguay",
    "paraguay": "Paraguay",
    "bolivia": "Bolivia",
    "ecuador": "Ecuador",

    # Central America and Caribbean
    "cuba": "Cuba",
    "jamaica": "Jamaica",
    "puerto rico": "Puerto Rico",
    "dominican republic": "Dominican Republic",
    "haiti": "Haiti",
    "costa rica": "Costa Rica",
    "panama": "Panama",
    "guatemala": "Guatemala",
    "honduras": "Honduras",
    "nicaragua": "Nicaragua",
    "el salvador": "El Salvador",

    # Others / Territories
    "palestine": "Palestine, State of",
    "hong kong": "Hong Kong",
    "macau": "Macao",
    "vatican": "Holy See",
    "kosovo": "Kosovo"    
}
country_pattern = re.compile(r'\b(' + '|'.join(re.escape(c) for c in country_list.keys()) + r')\b', flags=re.IGNORECASE)

# === Step 4: Utility Functions ===
def normalize(text):
    if not text or text.strip() == "":
        return ""
    return unidecode.unidecode(text).replace('\n', ' ').strip()

def extract_first_affiliation_safely(text):
    normalized = normalize(text)
    chunks = [c.strip() for c in normalized.split(',') if c.strip()]
    country_indices = [i for i, chunk in enumerate(chunks) if country_pattern.search(chunk)]

    if not country_indices:
        return normalized, "Unknown"

    last_idx = country_indices[-1]
    first_aff = ', '.join(chunks[:last_idx + 1])
    match = country_pattern.search(chunks[last_idx])
    country_key = match.group(0).lower()
    extracted_country = country_list.get(country_key, country_key.title())

    return first_aff, extracted_country

# === Step 5: Prepare Output Table ===
cursor.execute("DROP TABLE IF EXISTS affiliations_first_extracted")
cursor.execute("""
    CREATE TABLE affiliations_first_extracted (
        Raw_Data_ID INT,
        Author_Name TEXT,
        Original_Affiliation TEXT,
        Cleaned_First_Affiliation TEXT,
        Extracted_Country TEXT
    )
""")

# === Step 6: Process & Insert Cleaned Affiliations ===
insert_sql = """
INSERT INTO affiliations_first_extracted
(Raw_Data_ID, Author_Name, Original_Affiliation, Cleaned_First_Affiliation, Extracted_Country)
VALUES (%s, %s, %s, %s, %s)
"""

data_to_insert = []

for raw_id, author_name, aff in rows:
    cleaned, country = extract_first_affiliation_safely(aff)
    data_to_insert.append((raw_id, author_name, aff, cleaned, country))

cursor.executemany(insert_sql, data_to_insert)
conn.commit()

cursor.close()
conn.close()
print(" Extracted single affiliations and saved to `affiliations_first_extracted`.")
