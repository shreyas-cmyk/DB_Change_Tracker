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
sheet = client.open("Copy of BR - Data Structure").worksheet("services")

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

        INSERT INTO public.services_main_history
        (snapshot_at, uuid, data)

        SELECT
        NOW(),
        uuid,
        to_jsonb(services_main)

        FROM public.services_main

    """))

print("✅ Snapshot saved in services_main_history")

# ---------------- STEP 2: REFRESH MAIN TABLE ----------------
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE public.services_main"))

df.to_sql(
"services_main",
engine,
schema="public",
if_exists="append",
index=False,
method="multi"
)

print("✅ services_main refreshed")

# ---------------- STEP 3: CHANGE DETECTION ----------------
with engine.begin() as conn:

    result = conn.execute(text("""
    SELECT MAX(snapshot_at) FROM public.services_main_history
    """))

    last_snapshot = result.scalar()

# -------- COLUMN LEVEL CHANGES --------
    conn.execute(text("""

    INSERT INTO public.service_main_change_log
    (uuid, account_global_legal_name, cn_unique_key, center_name, field_name, old_value, new_value, changed_at)

    SELECT
    s.uuid,
    s.account_global_legal_name,
    s.cn_unique_key,
    s.center_name,
    key,
    h.data ->> key,
    to_jsonb(s) ->> key,
    NOW()

    FROM public.services_main s

    JOIN public.services_main_history h
    ON s.uuid = h.uuid

    CROSS JOIN LATERAL jsonb_object_keys(h.data) AS key

    WHERE h.snapshot_at = :snapshot_time
    AND (h.data ->> key) IS DISTINCT FROM (to_jsonb(s) ->> key)

    """), {"snapshot_time": last_snapshot})

# -------- NEW SERVICES --------
    conn.execute(text("""

    INSERT INTO public.service_main_change_log
    (uuid, account_global_legal_name, cn_unique_key, center_name, field_name, old_value, new_value, changed_at)

    SELECT
    s.uuid,
    s.account_global_legal_name,
    s.cn_unique_key,
    s.center_name,
    'NEW_SERVICE_ROW',
    NULL,
    s.primary_service,
    NOW()

    FROM public.services_main s

    LEFT JOIN public.services_main_history h
    ON s.uuid = h.uuid

    WHERE h.uuid IS NULL

    """))

# -------- DELETED SERVICES --------
    conn.execute(text("""

    INSERT INTO public.service_main_change_log
    (uuid, account_global_legal_name, cn_unique_key, center_name, field_name, old_value, new_value, changed_at)

    SELECT
    h.uuid,
    h.data ->> 'account_global_legal_name',
    h.data ->> 'cn_unique_key',
    h.data ->> 'center_name',
    'DELETED_SERVICE_ROW',
    h.data ->> 'primary_service',
    NULL,
    NOW()

    FROM public.services_main_history h

    LEFT JOIN public.services_main s
    ON s.uuid = h.uuid

    WHERE s.uuid IS NULL
    AND h.snapshot_at = :snapshot_time

    """), {"snapshot_time": last_snapshot})

print("✅ Changes logged successfully in service_main_change_log")