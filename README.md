# DB Change Tracker

This repository contains an automated pipeline designed to synchronize data from Google Sheets to a PostgreSQL database while meticulously tracking and logging any subsequent changes. It serves as a robust Change Data Capture (CDC) mechanism specifically tailored for tracking updates, insertions, and deletions across various business entities.

## 🎯 Purpose and Objective
The core objective of this project is to maintain a historical timeline of data modifications originating from Google Sheets without losing previous state context. Whenever data is pushed from Google Sheets to the database, the system performs an automated sequence to:
1. Snapshot the current state of the database table into a History table.
2. Refresh the active database table with the newest data from the Google Sheet.
3. Compare the old snapshot and the new active table to identify and record granular cell-level changes (including entire new or deleted rows) within a dedicated Change Log table.

## 🗂️ Data Architecture

For each business entity (Accounts, Centers, Functions, Prospects, Services, Tech), three distinct tables are generated in PostgreSQL. The structural pattern remains consistent across all entities.

*For reference on the exact SQL table definitions, please check the corresponding `.sql` files within the `Sql_Scripts/` directory.*

### 1. Main Table (e.g., `accounts_main`)
- **Purpose**: Stores the most recent, active data pulled directly from the Google Sheet.
- **Function**: Acts as the single source of truth for current states. Upon every script execution, this table is truncated, and the new data dump is appended.

### 2. History Table (e.g., `accounts_main_history`)
- **Purpose**: Acts as an archive / snapshot repository.
- **Function**: Before truncating the Main Table, all current rows are inserted here. The data is converted and stored into a versatile `JSONB` column (`data`), alongside a `snapshot_at` timestamp and an entity `uuid`. This dynamic format prevents schema mismatch errors if new columns are dynamically added over time.

### 3. Change Log Table (e.g., `account_main_change_log`)
- **Purpose**: Granularly records *what* was changed, *when* it was changed, and *what the values were*.
- **Function**: Captures updates on a per-cell basis, logging the exact `field_name`, `old_value`, and `new_value`.

## ⚙️ How It Works (Step-by-Step)

Each entity has a designated Python script (e.g., `accounts_main_history_change_log.py`) which coordinates this flow. *Refer to any of these `.py` files in the root directory to inspect the code.*

### Step 1: Data Extraction and Normalization
- Authenticates into Google Workspace using Service Account Credentials.
- Opens the targeted Worksheet and reads the data into a Pandas DataFrame.
- **Normalization**: Cleans column headers (lowercase, stripping whitespaces, replacing spaces with underscores, removing special characters) to align cleanly with SQL column naming conventions.

### Step 2: Snapshot Current Data
- The script executes an `INSERT` statement to dump everything currently in the Main Table over to the History Table.
- The entire row context is cast into `jsonb` under the `data` column, accompanied by a `NOW()` timestamp indicating when the snapshot was taken.

### Step 3: Refresh Main Table
- The script uses `TRUNCATE TABLE` to wipe the Main Table clean.
- The Pandas DataFrame obtained from the Google Sheet is smoothly ingested back into the Main Table using SQL Alchemy's `to_sql` method.

### Step 4: Change Detection & Logging
The system pulls the most recent `snapshot_at` timestamp from the History table and conducts three separate SQL comparisons:
1. **Column Level Changes**: Uses PostgreSQL's lateral JSON capabilities (`jsonb_object_keys`) to compare the unpacked JSON keys from the History snapshot against the fresh data inside the Main Table. If the values `IS DISTINCT FROM` one another, it inserts a new record into the Change Log table indicating the exact `field_name`, `old_value`, and `new_value`.
2. **New Rows/Accounts**: Performs a `LEFT JOIN` from the newly populated Main Table to the recent History snapshot. Any `uuid` missing from the snapshot but present in the Main Table is logged as a newly inserted row (`NEW_ACCOUNT`).
3. **Deleted Rows/Accounts**: Performs a `LEFT JOIN` from the recent History snapshot to the newly populated Main Table. Any `uuid` present in the snapshot but missing from the Main Table is logged as a deleted row (`DELETED_ACCOUNT`).

---
By organizing data in this manner, the DB Change Tracker retains full auditability over your Google Sheets data—enabling users to easily query the historical timeline of any single specific change.
Note: The same steps and table structure was followed for all six tables in this repository(just their column names are different).
