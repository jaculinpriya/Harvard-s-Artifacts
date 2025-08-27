import streamlit as st
import requests
import pandas as pd
import sqlite3
import time
import math  
from typing import List, Optional

# ------------------------- Static Config -------------------------
DB_PATH = "harvard_artifacts1.db"
API_BASE = "https://api.harvardartmuseums.org"
API_KEY = "c4686fb9-77b7-4dee-8f64-91e9c369fba4"  

# ------------------------- Helpers & DB Setup -------------------------

def get_db_conn(path: str = DB_PATH):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn

def init_db(conn: sqlite3.Connection):
    mydb = conn.cursor()
    mydb.execute("""
    CREATE TABLE IF NOT EXISTS artifact_metadata (
        id INTEGER PRIMARY KEY,
        title TEXT,
        culture TEXT,
        period TEXT,
        century TEXT,
        medium TEXT,
        dimensions TEXT,
        description TEXT,
        department TEXT,
        classification TEXT,
        accessionyear INTEGER,
        accessionmethod TEXT
    )
    """)
    mydb.execute("""
    CREATE TABLE IF NOT EXISTS artifact_media (
        objectid INTEGER,
        imagecount INTEGER,
        mediacount INTEGER,
        colorcount INTEGER,
        rank INTEGER,
        datebegin INTEGER,
        dateend INTEGER,
        PRIMARY KEY (objectid),
        FOREIGN KEY (objectid) REFERENCES artifact_metadata(id)
    )
    """)
    mydb.execute("""
    CREATE TABLE IF NOT EXISTS artifact_colors (
        objectid INTEGER,
        color TEXT,
        spectrum TEXT,
        hue TEXT,
        percent REAL,
        css3 TEXT
    )
    """)
    conn.commit()

# ------------------------- Harvard API ETL -------------------------

def fetch_from_harvard(endpoint: str, params: dict) -> dict:
    params = params.copy()
    params['apikey'] = API_KEY
    url = f"{API_BASE}/{endpoint}"
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"API request failed: {e}")
        return {}

def fetch_classification_records(classification: str, min_records: int = 250, page_size: int = 100):
    """Fetch artifacts for a classification with paging"""
    records: List[dict] = []
    page = 1
    attempts = 0
    max_attempts = 200 

    with st.spinner(f'Fetching {classification} from Harvard API...'):
        while len(records) < min_records and attempts < max_attempts:
            params = {
                'classification': classification,
                'size': page_size,
                'page': page,
                'hasimage': 1,
            }
            data = fetch_from_harvard('object', params)
            if not data or 'records' not in data:
                break

            batch = data.get('records', [])
            if not batch:
                break

            records.extend(batch)
            totalrecords = data.get('info', {}).get('totalrecords', None)
            page += 1
            attempts += 1
            time.sleep(0.2)  # avoid rate limiting

            if totalrecords is not None and page > math.ceil(totalrecords / page_size):
                break

    return records[:min_records]

# ------------------------- JSON -> Table Mappers -------------------------

def safe_int(v):
    try: return int(v) if v is not None else None
    except: return None

def safe_float(v):
    try: return float(v) if v is not None else None
    except: return None

def guess_hue_from_hex(hexcode: Optional[str]) -> Optional[str]:
    if not hexcode: return None
    h = hexcode.lstrip('#')
    if len(h) < 6: return None
    try:
        r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    except: return None
    mx, mn = max(r, g, b), min(r, g, b)
    if mx == mn: return 'Grey'
    if mx == r: return 'Red' if r - max(g, b) > 10 else 'Orange'
    if mx == g: return 'Green'
    return 'Blue'

def closest_css3(hexcode: Optional[str]) -> Optional[str]:
    return hexcode

def parse_metadata(record: dict) -> dict:
    return {
        'id': record.get('objectid') or record.get('id'),
        'title': record.get('title'),
        'culture': record.get('culture'),
        'period': record.get('period'),
        'century': record.get('century'),
        'medium': record.get('medium'),
        'dimensions': record.get('dimensions'),
        'description': record.get('description'),
        'department': record.get('department'),
        'classification': record.get('classification'),
        'accessionyear': safe_int(record.get('accessionyear')),
        'accessionmethod': record.get('accessionmethod')
    }

def parse_media(record: dict) -> dict:
    images = record.get('images') or []
    imagecount = len(images)
    mediacount = record.get('mediacount') or imagecount
    colors = record.get('colors') or []
    colorcount = len(colors)
    return {
        'objectid': record.get('objectid') or record.get('id'),
        'imagecount': imagecount,
        'mediacount': mediacount,
        'colorcount': colorcount,
        'rank': record.get('rank') or 0,
        'datebegin': safe_int(record.get('datebegin')),
        'dateend': safe_int(record.get('dateend'))
    }

def parse_colors(record: dict):
    entries = []
    objectid = record.get('objectid') or record.get('id')
    for c in record.get('colors') or []:
        hexcode = c.get('hex') or c.get('color')
        entries.append({
            'objectid': objectid,
            'color': hexcode,
            'spectrum': c.get('spectrum') or hexcode,
            'hue': c.get('name') or guess_hue_from_hex(hexcode),
            'percent': safe_float(c.get('percent')),
            'css3': c.get('css3') or closest_css3(hexcode)
        })
    return entries

# ------------------------- Insertion -------------------------

def insert_records_into_db(conn: sqlite3.Connection, records: List[dict], replace: bool = True):
    cur = conn.cursor()
    md, media, colors = [], [], []
    for rec in records:
        m = parse_metadata(rec); md.append(tuple(m.values()))
        n = parse_media(rec); media.append(tuple(n.values()))
        for c in parse_colors(rec): colors.append(tuple(c.values()))
    cur.executemany(
        "REPLACE INTO artifact_metadata VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", md
    )
    for t in media:
        cur.execute("REPLACE INTO artifact_media VALUES (?,?,?,?,?,?,?)", t)
    cur.executemany("INSERT INTO artifact_colors VALUES (?,?,?,?,?,?)", colors)
    conn.commit()
    return len(md), len(media), len(colors)

# ------------------------- Prewritten Queries -------------------------

PREWRITTEN_QUERIES = {
    "Artifacts from 11th century Byzantine":
        "SELECT * FROM artifact_metadata WHERE century='11th century' AND culture='Byzantine';",
    "Unique cultures":
        "SELECT DISTINCT culture FROM artifact_metadata WHERE culture IS NOT NULL;",
    "Artifacts from Archaic Period":
        "SELECT * FROM artifact_metadata WHERE period='Archaic';",
    "Titles ordered by accession year":
        "SELECT title, accessionyear FROM artifact_metadata ORDER BY accessionyear DESC;",
    "Artifacts per department":
        "SELECT department, COUNT(*) FROM artifact_metadata GROUP BY department;",
    "Artifacts with >1 image":
        "SELECT a.title, m.imagecount FROM artifact_media m JOIN artifact_metadata a ON m.objectid=a.id WHERE m.imagecount > 1;",
    "Average rank":
        "SELECT AVG(rank) FROM artifact_media;",
    "Higher colorcount than mediacount":
        "SELECT a.title, m.colorcount, m.mediacount FROM artifact_media m JOIN artifact_metadata a ON m.objectid=a.id WHERE m.colorcount > m.mediacount;",
    "Artifacts 1500-1600":
        "SELECT a.title, m.datebegin, m.dateend FROM artifact_media m JOIN artifact_metadata a ON m.objectid=a.id WHERE m.datebegin>=1500 AND m.dateend<=1600;",
    "Artifacts with no media":
        "SELECT a.title FROM artifact_metadata a LEFT JOIN artifact_media m ON a.id=m.objectid WHERE m.mediacount=0 OR m.mediacount IS NULL;",
    "Distinct hues":
        "SELECT DISTINCT hue FROM artifact_colors;",
    "Top 5 colors":
        "SELECT color, COUNT(*) as cnt FROM artifact_colors GROUP BY color ORDER BY cnt DESC LIMIT 5;",
    "Average % coverage per hue":
        "SELECT hue, AVG(percent) FROM artifact_colors GROUP BY hue;",
    "Colors by artifact ID":
        "SELECT * FROM artifact_colors WHERE objectid=1;",
    "Total color entries":
        "SELECT COUNT(*) FROM artifact_colors;",
    "Titles & hues for Byzantine":
        "SELECT a.title, c.hue FROM artifact_metadata a JOIN artifact_colors c ON a.id=c.objectid WHERE a.culture='Byzantine';",
    "Titles with hues":
        "SELECT a.title, c.hue FROM artifact_metadata a JOIN artifact_colors c ON a.id=c.objectid;",
    "Titles, cultures, ranks where period not null":
        "SELECT a.title, a.culture, m.rank FROM artifact_metadata a JOIN artifact_media m ON a.id=m.objectid WHERE a.period IS NOT NULL;",
    "Top 10 ranked artifacts with Grey":
        "SELECT a.title FROM artifact_metadata a JOIN artifact_colors c ON a.id=c.objectid JOIN artifact_media m ON a.id=m.objectid WHERE c.hue='Grey' ORDER BY m.rank DESC LIMIT 10;",
    "Artifacts per classification with avg media count":
        "SELECT a.classification, COUNT(*), AVG(m.mediacount) FROM artifact_metadata a JOIN artifact_media m ON a.id=m.objectid GROUP BY a.classification;"
}

# ------------------------- UI Enhancer -------------------------

def add_custom_ui():
    st.markdown(
        """
        <style>
        /* Background Gradient */
        .stApp {
            background: linear-gradient(135deg, #f9f9f9, #e6f0ff);
        }
        /* Buttons hover animation */
        div.stButton > button {
            border-radius: 12px;
            transition: all 0.3s ease-in-out;
            font-size: 16px;
            font-weight: bold;
        }
        div.stButton > button:hover {
            transform: scale(1.05);
            background-color: #4CAF50 !important;
            color: white !important;
        }
        /* Table Styling */
        .stDataFrame {
            border-radius: 10px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        }
        </style>
        """,
        unsafe_allow_html=True
    )

# ------------------------- Streamlit UI -------------------------

def main():
    st.set_page_config(page_title="Harvard Artifacts Explorer", layout='wide')
    add_custom_ui()

    # Animated Title
    st.markdown(
        """
        <h2 style="text-align:center; animation: fadeIn 2s;">
        🏛️ Harvard Artifacts Explorer
        </h2>
        <style>
        @keyframes fadeIn {
            from {opacity: 0;}
            to {opacity: 1;}
        }
        body {
            margin: 0;
            background: linear-gradient(-45deg, #e0f7fa, #e1bee7, #ffe0b2, #c8e6c9);
            background-size: 400% 400%;
            animation: gradientBG 15s ease infinite;
        }
        @keyframes gradientBG {
            0% {background-position: 0% 50%;}
            50% {background-position: 100% 50%;}
            100% {background-position: 0% 50%;}
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    conn = get_db_conn(); init_db(conn)

    # Config Section
    st.subheader("⚙️ Configuration")
    col1, col2 = st.columns(2)
    with col1:
        classification = st.selectbox("Choose classification", [
            'Paintings','Sculpture','Coins','Jewelry','Drawings',
            'Furniture','Photographs','Prints','Textiles','Ceramics',
            'Arms and Armor','Manuscripts'
        ])
    with col2:
        min_records = st.number_input("Records to fetch", 100, 2000, 200, step=50)

    # Action Buttons
    st.markdown("### 🚀 Actions")
    colA, colB, colC = st.columns(3)

    # 1️⃣ Fetch Data
    with colA:
        if st.button("📥 Fetch Data"):
            records = fetch_classification_records(classification, min_records)
            if not records:
                st.error("❌ No records found. Check API key or classification.")
            else:
                st.session_state['last_fetched'] = records
                st.success(f"✅ Fetched {len(records)} records.")
                df = pd.json_normalize(records)
                st.dataframe(df.head(50))
                for r in records[:5]:
                    if r.get("primaryimageurl"):
                        st.image(r["primaryimageurl"], caption=r.get("title"))

    # 2️⃣ Show Metadata / Media / Colors
    with colB:
        if st.button("📜 Show Metadata, Media & Colors"):
            recs = st.session_state.get('last_fetched')
            if recs:
                st.success("Showing parsed data...")

                # Metadata
                st.markdown("### 🗂 Metadata (Dict)")
                metadata_list = [parse_metadata(r) for r in recs]
                metadata_dict = {m['id']: m for m in metadata_list}
                st.json(dict(list(metadata_dict.items())[:5]))

                # Media
                st.markdown("### 🎞 Media")
                media_list = [parse_media(r) for r in recs]
                st.json(media_list[:5])

                # Colors
                st.markdown("### 🎨 Colors")
                colors_list = [c for r in recs for c in parse_colors(r)]
                st.json(colors_list[:5])
            else:
                st.warning("⚠️ Fetch data first before viewing metadata/media/colors.")

    # 3️⃣ Insert into DB
    with colC:
        if st.button("💾 Insert into DB"):
            recs = st.session_state.get('last_fetched')
            if recs:
                inserted = insert_records_into_db(conn, recs)
                st.success(
                    f"Inserted into DB: metadata={inserted[0]}, media={inserted[1]}, colors={inserted[2]}"
                )
            else:
                st.warning("⚠️ No data to insert.")

    # Query Explorer
    st.markdown("---")
    st.subheader("🔎 Query Explorer")
    q_choice = st.selectbox("Choose a prewritten query", list(PREWRITTEN_QUERIES.keys()))
    if st.button("Run Query"):
        q = PREWRITTEN_QUERIES[q_choice]
        df = pd.read_sql_query(q, conn)
        st.dataframe(df)

    # Custom SQL
    st.markdown("---")
    st.subheader("📝 Custom SQL")
    sql = st.text_area("Write your SQL (SELECT recommended)")
    if st.button("Execute SQL"):
        try:
            df = pd.read_sql_query(sql, conn)
            st.dataframe(df)
        except Exception as e:
            st.error(f"SQL error: {e}")


if __name__ == "__main__":
    main()
