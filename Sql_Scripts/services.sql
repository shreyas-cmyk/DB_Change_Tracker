# Query to create services table:
  
CREATE TABLE public.services_main (
uuid TEXT,
last_update_date TEXT,
account_global_legal_name TEXT,
cn_unique_key TEXT,
center_name TEXT,
center_type TEXT,
center_focus TEXT,
center_city TEXT,
primary_service TEXT,
focus_region TEXT,
service_it TEXT,
service_erd TEXT,
service_fna TEXT,
service_hr TEXT,
service_procurement TEXT,
service_sales_marketing TEXT,
service_customer_support TEXT,
service_others TEXT,
software_vendor TEXT,
software_in_use TEXT
);



# Query to create services history table:
  
CREATE TABLE public.services_main_history (
snapshot_at TIMESTAMP,
uuid TEXT,
data JSONB
);



# Query to create services change log table:
  
CREATE TABLE public.service_main_change_log (
uuid TEXT,
account_global_legal_name TEXT,
cn_unique_key TEXT,
center_name TEXT,
field_name TEXT,
old_value TEXT,
new_value TEXT,
changed_at TIMESTAMP
);

