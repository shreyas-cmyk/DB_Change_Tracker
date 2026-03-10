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
r"C:\Users\RNXT_Shreya\Downloads\still-fact-466904-i7-22099d8cad5c.json",
scope
)

client = gspread.authorize(creds)

# ---------------- READ GOOGLE SHEET ----------------
sheet = client.open("Copy of BR - Data Structure").worksheet("prospects")

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
"postgresql://neondb_owner:npg_GaKJj6dU7cXB@"
"ep-broad-shape-ai2p3a5s-pooler.c-4.us-east-1.aws.neon.tech/"
"neondb?sslmode=require&channel_binding=require"
)

engine = create_engine(conn_string)

# ---------------- STEP 1: SNAPSHOT CURRENT DATA ----------------
with engine.begin() as conn:

    conn.execute(text("""

        INSERT INTO public.prospects_main_history
        (snapshot_at, uuid, data)

        SELECT
        NOW(),
        uuid,
        to_jsonb(prospects_main)

        FROM public.prospects_main

    """))

print("✅ Snapshot saved in prospects_main_history")

# ---------------- STEP 2: REFRESH MAIN TABLE ----------------
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE public.prospects_main"))

# PUSH DATA IN CHUNKS OF 500
df.to_sql(
"prospects_main",
engine,
schema="public",
if_exists="append",
index=False,
chunksize=500,
method="multi"
)

print("✅ prospects_main refreshed with chunk upload (500 rows)")

# ---------------- STEP 3: CHANGE DETECTION ----------------
with engine.begin() as conn:

    result = conn.execute(text("""
    SELECT MAX(snapshot_at) FROM public.prospects_main_history
    """))

    last_snapshot = result.scalar()

# -------- COLUMN LEVEL CHANGES --------
    conn.execute(text("""

    INSERT INTO public.prospect_main_change_log
    (uuid, account_global_legal_name, center_name, prospect_full_name, field_name, old_value, new_value, changed_at)

    SELECT
    p.uuid,
    p.account_global_legal_name,
    p.center_name,
    p.prospect_full_name,
    key,
    h.data ->> key,
    to_jsonb(p) ->> key,
    NOW()

    FROM public.prospects_main p

    JOIN public.prospects_main_history h
    ON p.uuid = h.uuid

    CROSS JOIN LATERAL jsonb_object_keys(h.data) AS key

    WHERE h.snapshot_at = :snapshot_time
    AND (h.data ->> key) IS DISTINCT FROM (to_jsonb(p) ->> key)

    """), {"snapshot_time": last_snapshot})

# -------- NEW ROWS --------
    conn.execute(text("""

    INSERT INTO public.prospect_main_change_log
    (uuid, account_global_legal_name, center_name, prospect_full_name, field_name, old_value, new_value, changed_at)

    SELECT
    p.uuid,
    p.account_global_legal_name,
    p.center_name,
    p.prospect_full_name,
    'NEW_PROSPECT_ROW',
    NULL,
    p.prospect_full_name,
    NOW()

    FROM public.prospects_main p

    LEFT JOIN public.prospects_main_history h
    ON p.uuid = h.uuid

    WHERE h.uuid IS NULL

    """))

# -------- DELETED ROWS --------
    conn.execute(text("""

    INSERT INTO public.prospect_main_change_log
    (uuid, account_global_legal_name, center_name, prospect_full_name, field_name, old_value, new_value, changed_at)

    SELECT
    h.uuid,
    h.data ->> 'account_global_legal_name',
    h.data ->> 'center_name',
    h.data ->> 'prospect_full_name',
    'DELETED_PROSPECT_ROW',
    h.data ->> 'prospect_full_name',
    NULL,
    NOW()

    FROM public.prospects_main_history h

    LEFT JOIN public.prospects_main p
    ON h.uuid = p.uuid

    WHERE p.uuid IS NULL
    AND h.snapshot_at = :snapshot_time

    """), {"snapshot_time": last_snapshot})

print("✅ Changes logged successfully in prospect_main_change_log")