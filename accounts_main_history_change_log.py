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
sheet = client.open("Copy of BR - Data Structure").worksheet("accounts")

df = get_as_dataframe(
sheet,
evaluate_formulas=True,
dtype=str
).dropna(how="all")

# ---------------- NORMALIZE COLUMN NAMES ----------------
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
    
        INSERT INTO public.accounts_main_history
        (snapshot_at, uuid, data)
        
        SELECT
        NOW(),
        uuid,
        to_jsonb(accounts_main)
        
        FROM public.accounts_main
        
    """))

print("✅ Snapshot saved in accounts_main_history")

# ---------------- STEP 2: REFRESH MAIN TABLE ----------------
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE public.accounts_main"))

df.to_sql(
"accounts_main",
engine,
schema="public",
if_exists="append",
index=False,
method="multi"
)

print("✅ accounts_main refreshed")

# ---------------- STEP 3: CHANGE DETECTION ----------------
with engine.begin() as conn:

    result = conn.execute(text("""
    SELECT MAX(snapshot_at) FROM public.accounts_main_history
    """))

    last_snapshot = result.scalar()

# -------- COLUMN LEVEL CHANGES --------
    conn.execute(text("""

    INSERT INTO public.account_main_change_log
    (uuid, account_global_legal_name, field_name, old_value, new_value, changed_at)

    SELECT
    a.uuid,
    a.account_global_legal_name,
    key,
    h.data ->> key,
    to_jsonb(a) ->> key,
    NOW()

    FROM public.accounts_main a

    JOIN public.accounts_main_history h
    ON a.uuid = h.uuid

    CROSS JOIN LATERAL jsonb_object_keys(h.data) AS key

    WHERE h.snapshot_at = :snapshot_time
    AND (h.data ->> key) IS DISTINCT FROM (to_jsonb(a) ->> key)

    """), {"snapshot_time": last_snapshot})

# -------- NEW ACCOUNTS --------
    conn.execute(text("""

    INSERT INTO public.account_main_change_log
    (uuid, account_global_legal_name, field_name, old_value, new_value, changed_at)

    SELECT
    a.uuid,
    a.account_global_legal_name,
    'NEW_ACCOUNT',
    NULL,
    a.account_global_legal_name,
    NOW()

    FROM public.accounts_main a

    LEFT JOIN public.accounts_main_history h
    ON a.uuid = h.uuid

    WHERE h.uuid IS NULL

    """))

# -------- DELETED ACCOUNTS --------
    conn.execute(text("""

    INSERT INTO public.account_main_change_log
    (uuid, account_global_legal_name, field_name, old_value, new_value, changed_at)

    SELECT
    h.uuid,
    h.data ->> 'account_global_legal_name',
    'DELETED_ACCOUNT',
    h.data ->> 'account_global_legal_name',
    NULL,
    NOW()

    FROM public.accounts_main_history h

    LEFT JOIN public.accounts_main a
    ON a.uuid = h.uuid

    WHERE a.uuid IS NULL
    AND h.snapshot_at = :snapshot_time

    """), {"snapshot_time": last_snapshot})

print("✅ Changes logged successfully in account_main_change_log")