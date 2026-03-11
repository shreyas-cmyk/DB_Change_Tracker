# Query to create accounts table:
  
CREATE TABLE public.accounts_main (
uuid TEXT PRIMARY KEY,
account_last_update_date TEXT,
account_nasscom_status TEXT,
account_nasscom_member_status TEXT,
account_data_coverage TEXT,
account_source TEXT,
account_type TEXT,
account_global_legal_name TEXT,
account_about TEXT,
account_hq_address TEXT,
account_hq_city TEXT,
account_hq_state TEXT,
account_hq_zip_code TEXT,
account_hq_country TEXT,
account_hq_region TEXT,
account_hq_boardline TEXT,
account_hq_website TEXT,
account_hq_linkedin_link TEXT,
account_hq_key_offerings TEXT,
account_key_offerings_source_link TEXT,
account_hq_sub_industry TEXT,
account_hq_industry TEXT,
account_primary_category TEXT,
account_primary_nature TEXT,
account_hq_forbes_2000_rank TEXT,
account_hq_fortune_500_rank TEXT,
account_hq_company_type TEXT,
account_hq_revenue TEXT,
account_hq_revenue_range TEXT,
account_hq_fy_end TEXT,
account_hq_revenue_year TEXT,
account_hq_revenue_source_type TEXT,
account_hq_revenue_source_link TEXT,
account_hq_employee_count TEXT,
account_hq_employee_range TEXT,
account_hq_employee_source_type TEXT,
account_hq_employee_source_link TEXT,
account_center_employees TEXT,
account_center_employees_range TEXT,
years_in_india TEXT,
account_first_center_year TEXT,
account_comments TEXT,
account_coverage TEXT
);



# Query to create accounts history table:
  
CREATE TABLE public.accounts_main_history (
snapshot_at TIMESTAMP,
uuid TEXT,
data JSONB
);



# Query to create accounts change log table:
  
CREATE TABLE public.account_main_change_log (
uuid TEXT,
account_global_legal_name TEXT,
field_name TEXT,
old_value TEXT,
new_value TEXT,
changed_at TIMESTAMP
);

