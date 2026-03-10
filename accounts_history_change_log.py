import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine, text

# ---------------- STEP 1: GOOGLE SHEETS AUTH ----------------
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_name(
    r"C:\Users\RNXT_Shreya\Downloads\still-fact-466904-i7-22099d8cad5c.json",
    scope
)
client = gspread.authorize(creds)

# ---------------- STEP 2: READ GOOGLE SHEET ----------------
sheet = client.open("accounts_data_br").worksheet("accounts")

df = get_as_dataframe(
    sheet,
    evaluate_formulas=True,
    dtype=str
).dropna(how="all")

# ---------------- STEP 3: NORMALIZE COLUMN NAMES ----------------
df.columns = (
    df.columns
      .str.strip()
      .str.lower()
      .str.replace("\n", " ")
      .str.replace(" ", "_")
      .str.replace(r"[()]", "_", regex=True)
      .str.replace(r"[^\w_]", "", regex=True)
)

df.drop(columns=["id"], errors="ignore", inplace=True)
df = df.astype(str)

# ---------------- STEP 4: POSTGRES CONNECTION ----------------
conn_string = (
    "postgresql://neondb_owner:npg_GaKJj6dU7cXB@"
    "ep-broad-shape-ai2p3a5s-pooler.c-4.us-east-1.aws.neon.tech/"
    "neondb?sslmode=require&channel_binding=require"
)

engine = create_engine(conn_string)

# ---------------- STEP 5: SNAPSHOT CURRENT ACCOUNTS ----------------
with engine.begin() as conn:
    conn.execute(text("""
        INSERT INTO public.accounts_history (snapshot_at, account_global_legal_name, data)
        SELECT 
            NOW(),
            account_global_legal_name,
            to_jsonb(accounts)
        FROM public.accounts;
    """))

print("✅ Snapshot stored in accounts_history.")

# ---------------- STEP 6: TRUNCATE + RELOAD ACCOUNTS ----------------
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE public.accounts"))

df.to_sql(
    "accounts",
    engine,
    schema="public",
    if_exists="append",
    index=False,
    method="multi"
)

print("✅ Accounts table refreshed.")

# ---------------- STEP 7: COMPARE WITH LAST SNAPSHOT ----------------
with engine.begin() as conn:

    result = conn.execute(text("""
        SELECT MAX(snapshot_at) FROM public.accounts_history
    """))
    last_snapshot = result.scalar()

    # Column-level comparison
    conn.execute(text("""
        INSERT INTO public.account_change_log
        (account_global_legal_name, field_name, old_value, new_value, changed_at)

        SELECT 
            a.account_global_legal_name,
            key AS field_name,
            h.data ->> key AS old_value,
            to_jsonb(a) ->> key AS new_value,
            NOW()
        FROM public.accounts a
        JOIN public.accounts_history h
            ON a.account_global_legal_name = h.account_global_legal_name
        CROSS JOIN LATERAL jsonb_object_keys(h.data) AS key
        WHERE h.snapshot_at = :snapshot_time
        AND (h.data ->> key) IS DISTINCT FROM (to_jsonb(a) ->> key);
    """), {"snapshot_time": last_snapshot})

    # Detect NEW accounts
    conn.execute(text("""
        INSERT INTO public.account_change_log
        (account_global_legal_name, field_name, old_value, new_value, changed_at)

        SELECT 
            a.account_global_legal_name,
            'NEW_ACCOUNT',
            NULL,
            a.account_global_legal_name,
            NOW()
        FROM public.accounts a
        LEFT JOIN public.accounts_history h
            ON a.account_global_legal_name = h.account_global_legal_name
        WHERE h.account_global_legal_name IS NULL;
    """))

print("✅ Changes compared and logged successfully.")