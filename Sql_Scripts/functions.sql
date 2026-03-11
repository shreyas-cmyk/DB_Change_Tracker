# Query to create functions table:
  
CREATE TABLE public.functions_main (
uuid TEXT,
cn_unique_key TEXT,
function_name TEXT
);


# Query to create functions history table:
  
CREATE TABLE public.functions_main_history (
snapshot_at TIMESTAMP,
uuid TEXT,
data JSONB
);


# Query to create functions change log table:

CREATE TABLE public.function_main_change_log (
uuid TEXT,
cn_unique_key TEXT,
function_name TEXT,
field_name TEXT,
old_value TEXT,
new_value TEXT,
changed_at TIMESTAMP
);
