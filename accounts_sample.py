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
sheet = client.open(
    "accounts_data_br"
).worksheet("accounts")

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

# ---------------- STEP 4: ALIGN COLUMN NAMES WITH ACCOUNTS TABLE ----------------
df.rename(columns={

    "account_last_update_date": "account_last_update_date",
    "account_nasscom_status": "account_nasscom_status",
    "account_nasscom_member_status": "account_nasscom_member_status",
    "account_global_legal_name": "account_global_legal_name",
    "account_about": "account_about",
    "account_hq_address": "account_hq_address",
    "account_hq_city": "account_hq_city",
    "account_hq_state": "account_hq_state",
    "account_hq_zip_code": "account_hq_zip_code",
    "account_hq_country": "account_hq_country",
    "account_hq_region": "account_hq_region",
    "account_hq_boardline": "account_hq_boardline",
    "account_hq_website": "account_hq_website",
    "account_hq_key_offerings": "account_hq_key_offerings",
    "account_key_offerings_source_link": "account_key_offerings_source_link",
    "account_hq_sub_industry": "account_hq_sub_industry",
    "account_hq_industry": "account_hq_industry",
    "account_primary_category": "account_primary_category",
    "account_primary_nature": "account_primary_nature",
    "account_hq_forbes_2000_rank": "account_hq_forbes_2000_rank",
    "account_hq_fortune_500_rank": "account_hq_fortune_500_rank",
    "account_hq_company_type": "account_hq_company_type",
    "account_hq_revenue": "account_hq_revenue",
    "account_hq_revenue_range": "account_hq_revenue_range",
    "account_hq_fy_end": "account_hq_fy_end",
    "account_hq_revenue_year": "account_hq_revenue_year",
    "account_hq_revenue_source_type": "account_hq_revenue_source_type",
    "account_hq_revenue_source_link": "account_hq_revenue_source_link",
    "account_hq_employee_count": "account_hq_employee_count",
    "account_hq_employee_range": "account_hq_employee_range",
    "account_hq_employee_source_type": "account_hq_employee_source_type",
    "account_hq_employee_source_link": "account_hq_employee_source_link",
    "account_center_employees": "account_center_employees",
    "account_center_employees_range": "account_center_employees_range",
    "years_in_india": "years_in_india",
    "account_first_center_year": "account_first_center_year",
    "account_comments": "account_comments",
    "account_coverage": "account_coverage"

}, inplace=True)

# ---------------- STEP 5: DROP ID COLUMN IF PRESENT ----------------
df.drop(columns=["id"], errors="ignore", inplace=True)

# Ensure all values are TEXT-compatible
df = df.astype(str)

# ---------------- STEP 6: POSTGRES CONNECTION ----------------
conn_string = (
    "postgresql://neondb_owner:npg_GaKJj6dU7cXB@"
    "ep-broad-shape-ai2p3a5s-pooler.c-4.us-east-1.aws.neon.tech/"
    "neondb?sslmode=require&channel_binding=require"
)

engine = create_engine(conn_string)

# ---------------- STEP 7: TRUNCATE ACCOUNTS TABLE ----------------
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE public.accounts RESTART IDENTITY"))

# ---------------- STEP 8: INSERT DATA ----------------
df.to_sql(
    "accounts",
    engine,
    schema="public",
    if_exists="append",
    index=False,
    method="multi"
)

print("✅ Database table 'accounts' successfully refreshed from Google Sheet.")