import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, inspect, MetaData
import logging
from typing import Dict, Any, List, Optional
# import config  <--- REMOVED
from pandas import DataFrame
from datetime import datetime
import re

# NOTE: get_db_schema() and get_all_table_schemas() have been MOVED 
# to the new databricks_tools.py file.

def extract_schema_from_df(df: pd.DataFrame, file_name: str, sheet_name: Optional[str]) -> Dict[str, Any]:
    """
    Extracts schema information directly from a pandas DataFrame.

    Returns a dictionary containing metadata, column details, and sample data.
    """
    try:
        
        df.dropna(how='all', inplace=True)
        if df.empty:
            logging.warning(f"DataFrame for '{file_name}' - sheet '{sheet_name}' is empty or contains only null rows.")
            return {"file_name": file_name, "sheet_name": sheet_name, "total_rows": 0, "columns": {}}

        # Extract schema information
        column_details = {}
        for col in df.columns:
            # Get 5 unique, non-null sample values
            sample_values = df[col].dropna().unique().tolist()
            # Ensure samples are JSON serializable (convert timestamps/dates to strings)
            sample_values = [str(s) if isinstance(s, (pd.Timestamp, datetime)) else s for s in sample_values]
            sample_values = sample_values[:5]

            column_details[str(col)] = {
                'inferred_type': str(df[col].dtype),
                'sample_values': sample_values,
                'null_count': int(df[col].isnull().sum())
            }

        schema_summary = {
            "file_name": file_name, # Keep original file name for context
            "sheet_name": sheet_name, # Record which sheet this schema is for
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "columns": column_details
        }

        # Use more specific logging message
        logging.info(f"Successfully extracted schema from DataFrame for: '{file_name}' sheet: '{sheet_name}'")
        return schema_summary

    except Exception as e:
        logging.error(f"Error extracting schema from DataFrame for sheet '{sheet_name}': {e}")
        # Return a minimal schema structure on error
        return {"file_name": file_name, "sheet_name": sheet_name, "total_rows": 0, "columns": {}, "error": str(e)}


def extract_file_schema(file_path: str, sheet_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Reads a CSV or a specific Excel sheet and extracts its schema using extract_schema_from_df.

    If sheet_name is None for Excel, reads the first sheet.
    Returns a dictionary containing metadata, column details, and sample data.
    """
    df = None
    current_sheet_name_for_extraction = None
    file_type = None

    try:
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
            file_type = 'csv'
            current_sheet_name_for_extraction = None # CSV has no sheet name
            logging.info(f"Reading CSV file: {file_path}")
        elif file_path.endswith(('.xls', '.xlsx')):
            file_type = 'excel'
            try:
                # Determine which sheet to read
                sheet_to_read = sheet_name if sheet_name is not None else 0 # Default to first sheet (index 0)

                # Read the specified sheet
                df = pd.read_excel(file_path, sheet_name=sheet_to_read)

                # Get the actual sheet name (if index was used) for reporting
                if isinstance(sheet_to_read, int):
                    xls = pd.ExcelFile(file_path)
                    if sheet_to_read < len(xls.sheet_names):
                        current_sheet_name_for_extraction = xls.sheet_names[sheet_to_read]
                    else:
                        raise IndexError(f"Sheet index {sheet_to_read} is out of bounds.")
                else:
                    current_sheet_name_for_extraction = sheet_to_read

                logging.info(f"Reading Excel file: {file_path}, Sheet: '{current_sheet_name_for_extraction}'")

            except Exception as e:
                # More specific error for sheet reading failure
                logging.error(f"Could not read sheet '{sheet_name if sheet_name is not None else '0 (first sheet)'}' from Excel file '{file_path}': {e}")
                # Return error info consistent with extract_schema_from_df
                return {"file_name": file_path, "sheet_name": sheet_name, "total_rows": 0, "columns": {}, "error": f"Failed to read sheet: {e}"}
        else:
            logging.error(f"Unsupported file type: {file_path}")
            return None # Or raise ValueError

        # --- [REFINED] Use the new DataFrame-based function ---
        # Pass the loaded DataFrame and context to the new function
        return extract_schema_from_df(df, file_path, current_sheet_name_for_extraction)
        # --- [END REFINED] ---

    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        return None
    except Exception as e:
        # General file reading error
        logging.error(f"Error reading file '{file_path}': {e}")
        # Return error info consistent with extract_schema_from_df
        return {"file_name": file_path, "sheet_name": sheet_name, "total_rows": 0, "columns": {}, "error": f"General read error: {e}"}


def compare_schemas(file_schema: Dict[str, Any], db_schema: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Compares file and database schema columns *by name only*.

    Returns a dictionary of missing and extra columns.
    Semantic mapping is left to the LLM.
    """
    try:
        # Handle potential error structure from schema extraction
        if "error" in file_schema or "columns" not in file_schema:
            logging.warning("Cannot compare schemas, file schema extraction failed.")
            return {"missing_in_file": list(db_schema.keys()), "extra_in_file": []}

        file_columns = set(file_schema.get('columns', {}).keys()) # Safely get keys
        db_columns = set(db_schema.keys())

        missing_in_file = list(db_columns - file_columns)
        extra_in_file = list(file_columns - db_columns)

        logging.info("Schema comparison complete.")
        return {
            "columns_missing_from_file": missing_in_file, # In DB, but not in file
            "columns_extra_in_file": extra_in_file      # In file, but not in DB
        }
    except Exception as e:
        logging.error(f"Error comparing schemas: {e}")
        # Attempt to return a default structure on error
        db_keys = list(db_schema.keys()) if isinstance(db_schema, dict) else []
        return {"columns_missing_from_file": db_keys, "columns_extra_in_file": []}


def validate_data_types(df: DataFrame, db_schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Validates DataFrame dtypes against the database schema.

    Provides a 'raw report' of mismatches for the LLM to analyze.
    """
    type_violations = []

    # --- Consider making this map more robust or configurable ---
    pandas_to_sql_map = {
        'int64': ['INTEGER', 'INT'],
        'float64': ['REAL', 'FLOAT', 'NUMERIC', 'DOUBLE'], # Added DOUBLE
        'object': ['TEXT', 'VARCHAR', 'CHAR', 'DATE', 'DATETIME', 'TIMESTAMP', 'STRING'], # Added STRING
        'datetime64[ns]': ['DATE', 'DATETIME', 'TIMESTAMP'],
        'bool': ['BOOLEAN', 'BOOL']
    }
    # Invert the map for easier lookup (SQL -> Pandas general category)
    sql_to_pandas_map = {}
    for pd_type, sql_types in pandas_to_sql_map.items():
        for sql_type in sql_types:
            # Handle potential multiple mappings, prioritize non-object if possible
            if sql_type not in sql_to_pandas_map or pd_type != 'object':
                sql_to_pandas_map[sql_type.upper()] = pd_type # Use upper case for matching

    file_schema_columns = df.columns

    for db_col_name, db_col_details in db_schema.items():
        if db_col_name not in file_schema_columns:
            continue 
        
        column_data = df[db_col_name]
        if isinstance(column_data, pd.DataFrame):
            logging.warning(f"Duplicate column name found for '{db_col_name}' after mapping. "
                            f"This sheet is likely mismatched with the target DB table. "
                            f"Skipping type validation for this column.")

            continue 
        file_dtype = str(df[db_col_name].dtype)
        db_type_base = str(db_col_details['type']).split('(')[0].upper()
        expected_pd_type_category = sql_to_pandas_map.get(db_type_base)
        
        mismatch = False
        if expected_pd_type_category:
            
            if file_dtype != expected_pd_type_category:
                is_db_date_type = db_type_base in ['DATE', 'DATETIME', 'TIMESTAMP']
                # Allow 'object' for date types, as pandas reads date strings as object
                if not (is_db_date_type and file_dtype == 'object'):
                    mismatch = True
        else:
            
            logging.warning(f"DB type '{db_type_base}' for column '{db_col_name}' not in SQL-to-Pandas map. Skipping strict type check.")

        if mismatch:
            sample_invalid_values = []
            # Improved sample finding for common mismatches
            try:
                if expected_pd_type_category == 'int64' and file_dtype == 'object':
                    # Find non-integer strings
                    for val in df[db_col_name].dropna().unique():
                        try:
                            pd.to_numeric(val, errors='raise') 
                            if float(val) != int(float(val)): 
                                sample_invalid_values.append(str(val))
                        except (ValueError, TypeError): 
                            sample_invalid_values.append(str(val))
                        if len(sample_invalid_values) >= 5: break
                elif expected_pd_type_category == 'float64' and file_dtype == 'object':
                        # Find non-numeric strings
                    for val in df[db_col_name].dropna().unique():
                        try:
                            pd.to_numeric(val, errors='raise')
                        except (ValueError, TypeError):
                            sample_invalid_values.append(str(val))
                        if len(sample_invalid_values) >= 5: break
                
                elif file_dtype == 'object': 
                        sample_invalid_values = [str(v) for v in df[db_col_name].dropna().unique()[:5]]


            except Exception as sample_err:
                logging.warning(f"Error collecting invalid samples for column {db_col_name}: {sample_err}")


            violation = {
                "column": db_col_name,
                "expected_db_type": db_type_base, # Use base type
                "found_file_type": file_dtype,
                "sample_invalid_values": sample_invalid_values[:5]
            }
            type_violations.append(violation)

    logging.info(f"Data type validation complete. Found {len(type_violations)} mismatches.")
    return type_violations


def run_data_quality_checks(df: DataFrame, db_schema: Dict[str, Any], engine: sqlalchemy.engine.Engine, table_name: str) -> List[Dict[str, Any]]:
    """
    Runs basic data quality checks based on DB schema constraints (NULL, UNIQUE/PK, CHECK).
    Adds severity level.
    Requires the database engine and table name to fetch check constraints.
    """
    dq_violations = []
    inspector = inspect(engine)

    try:
        check_constraints = inspector.get_check_constraints(table_name)
        logging.info(f"Fetched {len(check_constraints)} CHECK constraints for table '{table_name}'.")
    except Exception as e:
        logging.warning(f"Could not fetch CHECK constraints for table '{table_name}': {e}. Skipping CHECK constraint validation.")
        check_constraints = []

    for db_col_name, db_col_details in db_schema.items():
        if db_col_name not in df.columns:
            continue # Skip missing columns

        column_data = df[db_col_name]
        if isinstance(column_data, pd.DataFrame):
            logging.warning(f"Duplicate column name found for '{db_col_name}' (in data_quality). "
                            f"This sheet is likely mismatched. "
                            f"Skipping all data quality checks for this column.")
            continue

        # --- 1. Null Check (based on 'nullable' constraint) ---
        if not db_col_details['nullable']:
            null_count = int(column_data.isnull().sum())
            # Add check for empty strings treated as nulls if column type is not object/string
            is_numeric_type = pd.api.types.is_numeric_dtype(column_data.dtype)
            empty_string_count = 0
            
            # Check for empty strings ('') if the column is of object type
            if column_data.dtype == 'object':
                empty_string_count = int((column_data == '').sum())
                
            # Treat empty strings as nulls if the target DB type is NOT text-based
            db_type = str(db_col_details.get('type', '')).upper()
            is_db_string_type = any(t in db_type for t in ['CHAR', 'TEXT', 'STRING'])
            
            if not is_db_string_type and empty_string_count > 0:
                null_count += empty_string_count # Add empty strings to null count

            if null_count > 0:
                affected_rows_sample_indices = df[column_data.isnull() | ((column_data.dtype == 'object') & (column_data == ''))].index.tolist()[:5]
                dq_violations.append({
                    "column": db_col_name,
                    "check": "not_null_violation",
                    "count": null_count,
                    "affected_rows_sample_indices": affected_rows_sample_indices,
                    "severity": "high",
                    "details": f"Column is non-nullable but contains {null_count} nulls (or empty strings treated as nulls for non-string columns)."
                })

        # --- 2. Uniqueness Check (based on 'primary_key' constraint) ---
        if db_col_details['primary_key']:
            # Drop rows where PK is null before checking duplicates
            non_null_pk_df = df.dropna(subset=[db_col_name])
            
            # Also drop empty strings if they are not valid PKs (usually they aren't)
            if non_null_pk_df[db_col_name].dtype == 'object':
                non_null_pk_df = non_null_pk_df[non_null_pk_df[db_col_name] != '']

            duplicates_df = non_null_pk_df[non_null_pk_df.duplicated(subset=[db_col_name], keep=False)]
            distinct_duplicate_values = duplicates_df[db_col_name].unique()
            duplicate_record_count = len(duplicates_df) # Total number of records involved in duplication
            distinct_keys_duplicated = len(distinct_duplicate_values)

            if distinct_keys_duplicated > 0:
                sample_duplicates = [str(v) for v in distinct_duplicate_values[:5]] # Ensure JSON serializable
                dq_violations.append({
                    "column": db_col_name,
                    "check": "primary_key_violation",
                    "distinct_keys_duplicated": distinct_keys_duplicated,
                    "total_duplicate_records": duplicate_record_count,
                    "sample_duplicate_values": sample_duplicates,
                    "severity": "high",
                    "details": f"Primary key column contains duplicates for {distinct_keys_duplicated} unique key(s), affecting {duplicate_record_count} records total."
                })

        # --- 3. [NEW] Check Constraints ---
        col_check_constraints = [
            c for c in check_constraints if db_col_name in c.get('sqltext', '')
        ]

        if col_check_constraints:
            
            numeric_col = pd.to_numeric(column_data, errors='coerce')
            # Check if *any* values were coerced to NaN (meaning non-numeric present)
            non_numeric_present = numeric_col.isna().any() and not column_data.isna().all()

            for constraint in col_check_constraints:
                sqltext = constraint.get('sqltext', '').strip()
                
                # Simple numeric constraint parser: `col` > `val`
                match = re.match(rf'["`]?{re.escape(db_col_name)}["`]?\s*(>=|<=|>|<|!=|=)\s*(-?\d+(\.\d+)?)', sqltext, re.IGNORECASE)

                if match and not non_numeric_present:
                    operator = match.group(1)
                    value = float(match.group(2))
                    constraint_name = constraint.get('name')
                    violated_rows = pd.Series(False, index=df.index) # Initialize

                    try:
                        if operator == '>': violated_rows = numeric_col <= value
                        elif operator == '>=': violated_rows = numeric_col < value
                        elif operator == '<': violated_rows = numeric_col >= value
                        elif operator == '<=': violated_rows = numeric_col > value
                        elif operator == '!=': violated_rows = numeric_col == value
                        elif operator == '=': violated_rows = numeric_col != value

                        # Important: Only consider rows that were originally not null
                        violated_rows = violated_rows & numeric_col.notna()

                        violation_count = int(violated_rows.sum())
                        if violation_count > 0:
                            affected_indices = df[violated_rows].index.tolist()[:5]
                            sample_violating_values = df.loc[affected_indices, db_col_name].tolist()[:5]
                            dq_violations.append({
                                "column": db_col_name,
                                "check": "check_constraint_violation",
                                "constraint_name": constraint_name,
                                "sqltext": sqltext,
                                "count": violation_count,
                                "affected_rows_sample_indices": affected_indices,
                                "sample_violating_values": [str(v) for v in sample_violating_values], # Ensure JSON serializable
                                "severity": "medium", # Default severity, could be adjusted
                                "details": f"{violation_count} values violate CHECK constraint '{sqltext}'."
                            })
                    except Exception as check_err:
                        logging.warning(f"Could not evaluate check constraint '{sqltext}' for column '{db_col_name}': {check_err}")

                else:
                    logging.info(f"Skipping CHECK constraint for column '{db_col_name}' as it was complex, non-numeric, or did not match simple patterns: '{sqltext}'")


    logging.info(f"Data quality checks complete. Found {len(dq_violations)} violations.")
    return dq_violations