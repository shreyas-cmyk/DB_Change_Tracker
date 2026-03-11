# Query to create centers table:
  
CREATE TABLE public.centers_main (
uuid TEXT,
last_update_date TEXT,
cn_unique_key TEXT PRIMARY KEY,
account_global_legal_name TEXT,
center_status TEXT,
center_inc_year TEXT,
announced_year TEXT,
announced_month TEXT,
center_inc_year_notes TEXT,
center_inc_year_updated_link TEXT,
center_timeline TEXT,
center_end_year TEXT,
center_account_website TEXT,
center_name TEXT,
center_business_segment TEXT,
center_business_sub_segment TEXT,
center_management_partner TEXT,
center_jv_status TEXT,
center_jv_name TEXT,
center_type TEXT,
center_focus TEXT,
center_source_link TEXT,
center_website TEXT,
center_linkedin TEXT,
center_address TEXT,
center_city TEXT,
center_state TEXT,
center_zip_code TEXT,
center_country TEXT,
center_country_iso2 TEXT,
lat TEXT,
lng TEXT,
center_region TEXT,
center_boardline TEXT,
center_employees TEXT,
center_employees_range TEXT,
center_employees_range_source_link TEXT,
center_services TEXT,
center_first_year TEXT,
center_comments TEXT
);



# Query to create centers history table:
  
CREATE TABLE public.centers_main_history (
snapshot_at TIMESTAMP,
uuid TEXT,
data JSONB
);




# Query to create centers change log table:
  
CREATE TABLE public.center_main_change_log (
uuid TEXT,
cn_unique_key TEXT,
account_global_legal_name TEXT,
field_name TEXT,
old_value TEXT,
new_value TEXT,
changed_at TIMESTAMP
);

