# Query to create tech table:

CREATE TABLE public.tech_main (
uuid TEXT,
account_global_legal_name TEXT,
cn_unique_key TEXT,
software_in_use TEXT,
software_vendor TEXT,
software_category TEXT
);


# Query to create tech history table:
  
CREATE TABLE public.tech_main_history (
snapshot_at TIMESTAMP,
uuid TEXT,
data JSONB
);



# Query to create tech change log table:
  
CREATE TABLE public.tech_main_change_log (
uuid TEXT,
account_global_legal_name TEXT,
cn_unique_key TEXT,
software_in_use TEXT,
field_name TEXT,
old_value TEXT,
new_value TEXT,
changed_at TIMESTAMP
);
