# Query to create prospects table:
  
CREATE TABLE public.prospects_main (
uuid TEXT,
last_update_date TEXT,
account_global_legal_name TEXT,
center_name TEXT,
prospect_full_name TEXT,
prospect_first_name TEXT,
prospect_last_name TEXT,
prospect_title TEXT,
prospect_department TEXT,
prospect_level TEXT,
prospect_linkedin_url TEXT,
prospect_email TEXT,
prospect_city TEXT,
prospect_state TEXT,
prospect_country TEXT
);



# Query to create prospects history table:
  
CREATE TABLE public.prospects_main_history (
snapshot_at TIMESTAMP,
uuid TEXT,
data JSONB
);



# Query to create prospects change log table:
  
CREATE TABLE public.prospect_main_change_log (
uuid TEXT,
account_global_legal_name TEXT,
center_name TEXT,
prospect_full_name TEXT,
field_name TEXT,
old_value TEXT,
new_value TEXT,
changed_at TIMESTAMP
);
