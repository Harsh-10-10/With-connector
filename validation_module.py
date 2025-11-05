import logging
import os
import glob
import pandas as pd
import json
import sqlalchemy
import time 
import httpx 
import openai 
import tiktoken 
from dotenv import load_dotenv 
from openai import AzureOpenAI 
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

import tools 
import databricks_tools 
import prompts

# --- 1. Load .env and Set Up Logging ---
load_dotenv() 
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 2. Azure Credentials ---
API_VERSION = os.getenv("API_VERSION", "2024-02-01") 
AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT") 
API_KEY = os.getenv("API_KEY") 
DEPLOYMENT_NAME = os.getenv("DEPLOYMENT_NAME", "gpt-4.1-nano") 

# --- 3. Global Client with SSL verification disabled ---
try:
    if not all([AZURE_ENDPOINT, API_KEY, DEPLOYMENT_NAME]):
        raise ValueError("AZURE_ENDPOINT, API_KEY, or DEPLOYMENT_NAME is not set in .env file.")
    
    http_client = httpx.Client(verify=False)
    
    client = AzureOpenAI(
        api_version=API_VERSION,
        azure_endpoint=AZURE_ENDPOINT,
        api_key=API_KEY,
        http_client=http_client,
    )
    logging.info(f"Successfully initialized AzureOpenAI client for endpoint: {AZURE_ENDPOINT}")
    logging.info(f"Using Deployment: {DEPLOYMENT_NAME}")
except Exception as e:
    logging.critical(f"Failed to initialize AzureOpenAI client: {e}. Check your .env file.")
    exit(1) 


# --- 4. System Prompts ---
SYSTEM_PROMPT_INSIGHT = """
You are the **Principal Data Steward**, a senior expert in data governance, quality, and pipeline architecture.
Your job is to provide authoritative, context-aware analysis to protect business operations.
You do **NOT** execute any Python functions. You only analyze.
Your tasks are to:
1.  **Connect Disparate Issues:** Find the *pattern* between problems.
2.  **Assess Business Risk:** Explain the *specific, real-world business impact*.
3.  **Form a Root Cause Hypothesis:** Provide the *most likely real-world source* of the errors.
4.  **Provide Actionable, Prioritized Plans:** Give a 3-5 step plan.
You will format your analysis *only* in the specific JSON structure requested.
"""

SYSTEM_PROMPT_INTERACTIVE = """
You are a helpful database expert. Your job is to analyze a file schema, compare it to database tables, and ask the user to select the correct one.
"""

# --- 5. Token Counter ---
def count_tokens(system_prompt, user_prompt, full_response):
    try:
        encoding = tiktoken.get_encoding("cl100k_base")
        
        system_tokens = len(encoding.encode(system_prompt))
        user_tokens = len(encoding.encode(user_prompt))
        input_tokens = system_tokens + user_tokens
        
        output_tokens = len(encoding.encode(full_response))
        total_tokens = input_tokens + output_tokens
        
        print("\n" + "-"*30 + " TOKEN COUNT " + "-"*30)
        print(f"[AI-CALL] Input Tokens:  {input_tokens} (System: {system_tokens}, User: {user_tokens})")
        print(f"[AI-CALL] Output Tokens: {output_tokens}")
        print(f"[AI-CALL] Total Tokens:  {total_tokens}")
        print("-"*73 + "\n")
        
        return input_tokens, output_tokens, total_tokens
        
    except Exception as e:
        print(f"An error occurred during token counting: {e}")
        return 0, 0, 0

# --- 6. API Calling Function ---
def get_llm_streaming_response(system_prompt: str, user_prompt: str, max_retries: int = 3) -> Optional[str]:
    for attempt in range(max_retries):
        try:
            logging.info(f"Sending prompt to LLM (Attempt {attempt + 1}/{max_retries})...")
            response = client.chat.completions.create(
                stream=True,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.0,
                top_p=1.0,
                frequency_penalty=0.0,
                presence_penalty=0.0,
                model=DEPLOYMENT_NAME,
            )

            full_response = ""
            for chunk in response:
                if chunk.choices and chunk.choices[0].delta.content:
                    full_response += chunk.choices[0].delta.content
            
            count_tokens(system_prompt, user_prompt, full_response)
            return full_response
        
        except openai.RateLimitError as e:
            sleep_time = 60 * (attempt + 1)
            logging.warning(f"Rate limit hit. Retrying in 60s... ({attempt + 1}/{max_retries})")
            time.sleep(60)
            
        except Exception as e:
            logging.error(f"An error occurred during the AI call: {e}", exc_info=True)
            return None 

    logging.error("Max retries exceeded for RateLimitError. Giving up.")
    return None

# --- 7. Schema History Functions (Unchanged) ---
SCHEMA_HISTORY_DIR = "schema_history"
NUM_HISTORICAL_SCHEMAS_TO_LOAD = 3

def save_schema_to_history(table_name: str, file_schema: Dict[str, Any]):
    try:
        safe_table_name = "".join(c if c.isalnum() else "_" for c in table_name)
        os.makedirs(SCHEMA_HISTORY_DIR, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%dT%H%M%SZ')
        filename = f"{safe_table_name}_schema_{timestamp}.json"
        filepath = os.path.join(SCHEMA_HISTORY_DIR, filename)
        schema_to_save = {"columns": file_schema.get("columns", {})}
        with open(filepath, 'w') as f:
            json.dump(schema_to_save, f, indent=2)
        logging.info(f"Saved current schema to history: {filepath}")
    except Exception as e:
        logging.error(f"Error saving schema to history for table '{table_name}': {e}")


def load_historical_schemas(table_name: str, num_history: int) -> List[Dict[str, Any]]:
    historical_schemas = []
    try:
        safe_table_name = "".join(c if c.isalnum() else "_" for c in table_name)
        search_pattern = os.path.join(SCHEMA_HISTORY_DIR, f"{safe_table_name}_schema_*.json")
        history_files = sorted(glob.glob(search_pattern), reverse=True)
        files_to_load = history_files[:num_history]
        logging.info(f"Found {len(history_files)} historical schemas for '{table_name}'. Loading the latest {len(files_to_load)}.")
        for file in files_to_load:
            try:
                with open(file, 'r') as f:
                    schema_data = json.load(f)
                    historical_schemas.append(schema_data)
            except Exception as e:
                logging.warning(f"Error loading historical schema file '{file}': {e}")
    except Exception as e:
        logging.error(f"Error searching for historical schemas for table '{table_name}': {e}")
    return historical_schemas

# --- 8. (NEW) AGENT TOOL 1: GET SHEET NAMES ---

def get_sheet_names(file_path: str) -> List[str]:
    """
    Reads a file and returns a list of its sheet names.
    For CSVs, it returns a list with a single placeholder.
    """
    try:
        if file_path.endswith(('.xls', '.xlsx')):
            xls = pd.ExcelFile(file_path)
            sheet_names = xls.sheet_names
            logging.info(f"Detected Excel file with sheets: {sheet_names}")
            if not sheet_names:
                logging.warning(f"Excel file '{file_path}' contains no sheets.")
                return []
            return sheet_names
        
        elif file_path.endswith('.csv'):
            logging.info(f"Detected CSV file: {file_path}")
            return ["csv_data"] # Placeholder for CSV
        
        else:
            logging.warning(f"Unsupported file type: {file_path}. Only .csv, .xls, and .xlsx are supported.")
            # Return an empty list so the test script loop finds nothing to process
            return []
            
    except Exception as e:
        logging.error(f"Error reading file {file_path} to get sheet names: {e}")
        return []

# --- 9. (NEW) AGENT TOOL 2: GET RECOMMENDATIONS FOR A SHEET ---
def get_recommendations_for_sheet(file_path: str, sheet_name: str) -> Optional[Dict[str, Any]]:
    """
    Analyzes a *single sheet* from a file and compares its schema against
    all Databricks tables to provide intelligent recommendations.
    """
    logging.info(f"--- Starting Table Recommendation for: {file_path} (Sheet: {sheet_name}) ---")
    engine = None
    try:
        # --- Step 1: Create Databricks Engine ---
        engine = databricks_tools.get_databricks_engine()
        if engine is None:
            raise Exception("Failed to create Databricks engine. Check credentials.")

        # --- Step 2: Read *Specific* Sheet and Extract Schema ---
        df = None
        # Use sheet_name unless it's the CSV placeholder
        read_sheet_name = sheet_name if sheet_name != "csv_data" else None
        
        if file_path.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(file_path, sheet_name=read_sheet_name)
            logging.info(f"Reading sheet ('{read_sheet_name}') from Excel file.")
        elif file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
            logging.info("Reading CSV file.")
        else:
            raise ValueError(f"Unsupported file type: {file_path}. Only .csv, .xls, and .xlsx are supported.")

        file_schema = tools.extract_schema_from_df(df, file_path, read_sheet_name)
        if "error" in file_schema or not file_schema.get("columns"):
            raise ValueError("Schema extraction failed for the file.")
        
        file_schema_cols = list(file_schema.get("columns", {}).keys())

        # --- Step 3: Fetch All Database Schemas (Lite) ---
        logging.info("Fetching all table schemas from Databricks...")
        all_db_schemas = databricks_tools.get_all_table_schemas(engine)
        if not all_db_schemas:
            raise ValueError("No tables found in the Databricks schema.")

        # --- Step 4: Call LLM for Analysis ---
        logging.info("Calling LLM for smart table matching analysis...")
        prompt = prompts.get_table_matching_prompt(
            file_schema=file_schema_cols,
            all_db_schemas=all_db_schemas
        )
        
        response_str = get_llm_streaming_response(SYSTEM_PROMPT_INSIGHT, prompt)
        
        if response_str is None:
            raise ValueError("Failed to get a response from LLM for table matching.")

        # --- Step 5: Parse and Return Recommendations (Robust) ---
        try:
            start_index = response_str.find('{')
            end_index = response_str.rfind('}')
            
            if start_index != -1 and end_index != -1 and end_index > start_index:
                json_str = response_str[start_index : end_index + 1]
                recommendations_json = json.loads(json_str)
                logging.info("Successfully received and parsed table recommendations from LLM.")
            else:
                raise ValueError("No JSON object found in the LLM response.")

        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON. Error: {e}")
            logging.error(f"--- LLM Raw Response ---:\n{response_str}\n--------------------------")
            raise ValueError("LLM returned malformed JSON. Check logs for the raw response.")

        recommendations_json["source_file_schema"] = file_schema_cols
        return recommendations_json

    except Exception as e:
        logging.error(f"Error in get_recommendations_for_sheet: {e}", exc_info=True)
        return {"error": str(e)}
    
    finally:
        # --- Step 6: Dispose Engine ---
        if engine:
            engine.dispose()
            logging.info("Databricks engine connection pool disposed.")


# --- 10. (INTERNAL) Core Validation Logic for one sheet ---
def _run_validation_for_sheet_internal(
    df: pd.DataFrame,
    file_path: str,
    sheet_name: Optional[str],
    engine: sqlalchemy.engine.Engine, 
    target_table_name: str # CHANGED: No longer optional
) -> (Dict[str, Any], Dict[str, Any]):
    """
    Runs the validation process for a *single DataFrame* against a *single table*.
    This is the internal logic.
    """
    sheet_report = {}
    schema_analysis_json = {}
    
    # This function *assumes* target_table_name is provided.
    if target_table_name is None:
         raise ValueError("Internal Error: _run_validation_for_sheet_internal called without a target_table_name.")

    try:
        sheet_display_name = sheet_name if sheet_name is not None else "CSV Data"
        logging.info(f"---  Starting Validation for Sheet: '{sheet_display_name}' ---")

        # --- Step 1 (Sheet): Extract Schema ---
        file_schema = tools.extract_schema_from_df(df, file_path, sheet_name)
        if "error" in file_schema or not file_schema.get("columns"):
            raise ValueError(f"Schema extraction failed for sheet '{sheet_display_name}'")

        # --- Step 2 (Sheet): LLM Schema Analysis ---
        logging.info(f"--- [Sheet '{sheet_display_name}'] Step 2: LLM Schema Analysis ---")
        
        db_schema = databricks_tools.get_db_schema(engine, target_table_name)
        if db_schema is None:
            raise ValueError(f"Database table '{target_table_name}' does not exist.")

        raw_comparison = tools.compare_schemas(file_schema, db_schema)
        schema_prompt = prompts.get_schema_analysis_prompt(
            db_schema=db_schema, file_schema=file_schema, raw_comparison=raw_comparison,
            target_table_name=target_table_name, source_file_name=os.path.basename(file_path)
        )
        
        schema_response_str = get_llm_streaming_response(SYSTEM_PROMPT_INSIGHT, schema_prompt)
        if schema_response_str is None:
            raise ValueError("Failed to get schema analysis from LLM.")
        
        try:
            schema_analysis_json = json.loads(schema_response_str)
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON from schema analysis: {e}\nRaw response: {schema_response_str}")
            raise ValueError("LLM did not return valid JSON for schema analysis.")
            
        logging.info(f"LLM Schema Analysis: Complete")

        # --- Step 3 (Sheet): Deep Validation ---
        logging.info(f"--- [Sheet '{sheet_display_name}'] Step 3: Deep Validation ---")
        naming_mismatches = schema_analysis_json.get("naming_mismatches", {})
        mapped_df = df.rename(columns=naming_mismatches)
        
        type_violations = tools.validate_data_types(mapped_df, db_schema)
        dq_violations = tools.run_data_quality_checks(mapped_df, db_schema, engine, target_table_name)
        logging.info(f"Deep validation: Complete")

        # --- Step 4 (Sheet): Infer Dynamic Rules ---
        logging.info(f"--- [Sheet '{sheet_display_name}'] Step 4.5: Inferring Dynamic Rules ---")
        dynamic_rules = []
        try:
            dynamic_rules_prompt = prompts.get_dynamic_rules_prompt(file_schema)
            dynamic_rules_str = get_llm_streaming_response(SYSTEM_PROMPT_INSIGHT, dynamic_rules_prompt)
            if dynamic_rules_str:
                dynamic_rules = json.loads(dynamic_rules_str)
            logging.info(f"LLM Dynamic Rules: Complete")
        except Exception as e:
            logging.warning(f"Could not generate dynamic rules: {e}")
            dynamic_rules = [{"error": "Failed to generate dynamic rules"}]

        # --- Step 5: Assembling Violation Summary ---
        logging.info(f"--- [Sheet '{sheet_display_name}'] Step 5: Assembling Violation Summary ---")
        def _create_violation_summary(types, dq):
            summary = {
                "type_mismatch_summary": [
                    {"column": v["column"], "expected": v["expected_db_type"], "found": v["found_file_type"]}
                        for v in types
                    ],
                "data_quality_issue_summary": [
                    {"column": v["column"], "check": v["check"], "count": v["count"], "severity": v.get("severity", "medium")}
                        for v in dq
                    ]
            }
            return summary

        violations_summary = _create_violation_summary(type_violations, dq_violations)

        # --- Step 6: Build Base Report ---
        logging.info(f"--- [Sheet '{sheet_display_name}'] Step 6: Building Base Report ---")
        file_metadata = {"file_name": os.path.basename(file_path), "sheet_name": sheet_name, "total_rows": file_schema.get("total_rows")}

        base_report = {
            "file_name": file_metadata.get("file_name"),
            "sheet_name": file_metadata.get("sheet_name"),
            "total_rows_checked": file_metadata.get("total_rows"),
            "validated_at": datetime.now(timezone.utc).isoformat(),
            "data_type_mismatch": type_violations,
            "data_quality_issues": dq_violations,
            "dynamic_validation_rules": dynamic_rules,
            "validation_summary": {},
            "data_quality_score": {},
            "triage_plan": [],
            "append_upsert_suggestion": {},
            "schema_drift": {},
            "root_cause_analysis": {},
            "overall_analysis": {}
        }

        # --- Step 7: LLM Final Analysis ---
        logging.info(f"--- [Sheet '{sheet_display_name}'] Step 7: Calling LLM for Final Analysis ---")
        historical_schemas = load_historical_schemas(target_table_name, NUM_HISTORICAL_SCHEMAS_TO_LOAD)
        
        analysis_prompt = prompts.get_analysis_prompt(
            schema_analysis=schema_analysis_json,
            violations_summary=violations_summary,
            historical_schemas=historical_schemas
        )

        analysis_response_str = get_llm_streaming_response(SYSTEM_PROMPT_INSIGHT, analysis_prompt)
        if analysis_response_str is None:
            raise ValueError("Failed to get final analysis from LLM.")

        try:
            llm_analysis_json = json.loads(analysis_response_str)
            base_report.update(llm_analysis_json)

        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON from final analysis: {e}\nRaw response: {analysis_response_str}")
            base_report["validation_summary"] = {"status": "Error", "details": "LLM analysis parsing failed."}

        # Save schema history
        if target_table_name:
            save_schema_to_history(target_table_name, file_schema)

        logging.info(f"--- Sheet '{sheet_display_name}' Validation Complete ---")

        sheet_report = base_report
        
    except Exception as e:
        logging.error(f"---  ERROR during validation for Sheet '{sheet_display_name}': {e} ---", exc_info=True)
        sheet_report = {
            "file_name": file_path, "sheet_name": sheet_name,
            "validated_at": datetime.now(timezone.utc).isoformat(),
            "validation_summary": { "status": "Error", "details": str(e) },
            "error": str(e)
        }
    
    # Return the full report and the schema analysis (for agent)
    return sheet_report, schema_analysis_json


# --- 11. (NEW) AGENT TOOL 3: RUN VALIDATION FOR A SINGLE SHEET ---
def run_validation_for_single_sheet(file_path: str, sheet_name: str, table_name: str) -> Dict[str, Any]:
    """
    Agent-facing tool to run the full validation pipeline for one sheet.
    This creates the engine, reads the data, and calls the internal logic.
    """
    logging.info(f"---  STARTING SINGLE SHEET VALIDATION: {file_path} (Sheet: {sheet_name}) -> (Table: {table_name}) ---")
    
    engine = databricks_tools.get_databricks_engine()
    if engine is None:
        logging.critical("Failed to create Databricks engine. Aborting validation.")
        return {"error": "Failed to create Databricks engine. Check .env credentials."}
    
    try:
        # --- Read the specific sheet data ---
        read_sheet_name = sheet_name if sheet_name != "csv_data" else None
        current_df = None
        
        if file_path.endswith(('.xls', '.xlsx')):
            current_df = pd.read_excel(file_path, sheet_name=read_sheet_name)
        elif file_path.endswith('.csv'):
            current_df = pd.read_csv(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_path}. Only .csv, .xls, and .xlsx are supported.")
        
        logging.info(f"--- Loaded data for sheet: '{sheet_name}' ---")
        
        # --- Call the internal core logic ---
        sheet_report, schema_analysis_json = _run_validation_for_sheet_internal(
            df=current_df, 
            file_path=file_path, 
            sheet_name=read_sheet_name,
            engine=engine,
            target_table_name=table_name
        )
        
        # We need to add the schema_mismatch to the top level for the agent
        # The agent's markdown tool expects it
        final_report = {}
        if "file_name" in sheet_report:
            final_report["file_name"] = sheet_report.pop("file_name")
        if "sheet_name" in sheet_report:
            final_report["sheet_name"] = sheet_report.pop("sheet_name")
        
        final_report["schema_mismatch"] = schema_analysis_json
        final_report.update(sheet_report)

        return final_report

    except Exception as e:
        logging.error(f"A critical error occurred: {e}", exc_info=True)
        return {"error": f"A critical error occurred: {e}"}
    finally:
        if engine:
            engine.dispose()
            logging.info("Databricks engine connection pool disposed.")
