import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine, text

# ---------------- GOOGLE SHEETS AUTH ----------------
scope = [
"https://spreadsheets.google.com/feeds",
"https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_name(
"Enter your credentials",
scope
)

client = gspread.authorize(creds)

# ---------------- READ GOOGLE SHEET ----------------
sheet = client.open("Copy of BR - Data Structure").worksheet("tech")

df = get_as_dataframe(
sheet,
evaluate_formulas=True,
dtype=str
).dropna(how="all")

# ---------------- CLEAN COLUMN NAMES ----------------
df.columns = (
df.columns
.str.strip()
.str.lower()
.str.replace("\n"," ")
.str.replace(" ","_")
.str.replace(r"[^\w_]","",regex=True)
)

df = df.astype(str)

# ---------------- POSTGRES CONNECTION ----------------
conn_string = (
"Enter your DB link"
)

engine = create_engine(conn_string)

# ---------------- STEP 1: SNAPSHOT CURRENT DATA ----------------
with engine.begin() as conn:

    conn.execute(text("""

        INSERT INTO public.tech_main_history
        (snapshot_at, uuid, data)

        SELECT
        NOW(),
        uuid,
        to_jsonb(tech_main)

        FROM public.tech_main

    """))

print("✅ Snapshot saved in tech_main_history")

# ---------------- STEP 2: REFRESH MAIN TABLE ----------------
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE public.tech_main"))

# PUSH DATA IN CHUNKS OF 500
df.to_sql(
"tech_main",
engine,
schema="public",
if_exists="append",
index=False,
chunksize=500,
method="multi"
)

print("✅ tech_main refreshed with chunk upload (500 rows)")

# ---------------- STEP 3: CHANGE DETECTION ----------------
with engine.begin() as conn:

    result = conn.execute(text("""
    SELECT MAX(snapshot_at) FROM public.tech_main_history
    """))

    last_snapshot = result.scalar()

# -------- COLUMN LEVEL CHANGES --------
    conn.execute(text("""

    INSERT INTO public.tech_main_change_log
    (uuid, account_global_legal_name, cn_unique_key, software_in_use, field_name, old_value, new_value, changed_at)

    SELECT
    t.uuid,
    t.account_global_legal_name,
    t.cn_unique_key,
    t.software_in_use,
    key,
    h.data ->> key,
    to_jsonb(t) ->> key,
    NOW()

    FROM public.tech_main t

    JOIN public.tech_main_history h
    ON t.uuid = h.uuid

    CROSS JOIN LATERAL jsonb_object_keys(h.data) AS key

    WHERE h.snapshot_at = :snapshot_time
    AND (h.data ->> key) IS DISTINCT FROM (to_jsonb(t) ->> key)

    """), {"snapshot_time": last_snapshot})

# -------- NEW ROWS --------
    conn.execute(text("""

    INSERT INTO public.tech_main_change_log
    (uuid, account_global_legal_name, cn_unique_key, software_in_use, field_name, old_value, new_value, changed_at)

    SELECT
    t.uuid,
    t.account_global_legal_name,
    t.cn_unique_key,
    t.software_in_use,
    'NEW_TECH_ROW',
    NULL,
    t.software_in_use,
    NOW()

    FROM public.tech_main t

    LEFT JOIN public.tech_main_history h
    ON t.uuid = h.uuid

    WHERE h.uuid IS NULL

    """))

# -------- DELETED ROWS --------
    conn.execute(text("""

    INSERT INTO public.tech_main_change_log
    (uuid, account_global_legal_name, cn_unique_key, software_in_use, field_name, old_value, new_value, changed_at)

    SELECT
    h.uuid,
    h.data ->> 'account_global_legal_name',
    h.data ->> 'cn_unique_key',
    h.data ->> 'software_in_use',
    'DELETED_TECH_ROW',
    h.data ->> 'software_in_use',
    NULL,
    NOW()

    FROM public.tech_main_history h

    LEFT JOIN public.tech_main t
    ON h.uuid = t.uuid

    WHERE t.uuid IS NULL
    AND h.snapshot_at = :snapshot_time

    """), {"snapshot_time": last_snapshot})

print("✅ Changes logged successfully in tech_main_change_log")
