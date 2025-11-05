import json
import logging
from typing import Dict, Any, List
from datetime import datetime, timezone, timedelta


# ==============================================================
# 1️⃣ SCHEMA ANALYSIS PROMPT
# ==============================================================

SCHEMA_ANALYSIS_PROMPT = """
You are an expert Data Validation Agent. Your task is to analyze a schema mismatch report and provide intelligent, context-aware recommendations.

You will be given:
1.  **Target Table**: The database table we are validating against ({target_table_name}).
2.  **Source File**: The file being validated ({source_file_name}).
3.  **DB Schema**: The target database table schema (columns, types, constraints).
4.  **File Schema**: The schema extracted from the user's file (columns, inferred types, sample values).
5.  **Raw Comparison**: A simple list of columns that are 'missing' or 'extra' based on an exact name match.

Your Job:
1.  **Semantic Mapping**: Go beyond exact matches. Identify columns in the File Schema that are semantically similar to columns in the DB Schema (e.g., 'cust' -> 'CustomerID', 'qty' -> 'Quantity').
2.  **Analyze Mismatches**: Re-evaluate the 'missing' and 'extra' columns after accounting for your semantic mapping.
3.  **Generate Insights**: For each mismatch, provide clear reasoning for the problem and actionable recommendations.
4.  **Format Output**: Return *ONLY* a single JSON object (no extra text or markdown) matching the structure below.

---
[INPUT DATA]

**Target Table**: {target_table_name}
**Source File**: {source_file_name}

**Database Schema (Target):**
{db_schema_json}

**File Schema (Source):**
{file_schema_json}

**Raw Comparison (Exact Match):**
{raw_comparison_json}

---
[YOUR ANALYSIS]

Produce a single JSON object in this *exact* format.
**CRITICAL:** For 'naming_mismatches', the key MUST be the column from the File Schema and the value MUST be the column from the Database Schema.

Format: {{"file_column_name": "db_column_name"}}
Example: {{"cust": "CustomerID", "qty": "Quantity"}}

{{
  "target_table": "{target_table_name}",
  "source_file": "{source_file_name}",
  "columns_missing_from_file": [
    "List_of_DB_columns_TRULY_missing_after_mapping"
  ],
  "columns_extra_in_file": [
    "List_of_File_columns_TRULY_extra_after_mapping"
  ],
  "naming_mismatches": {{
    "file_col_1_name": "db_col_1_name",
    "file_col_2_name": "db_col_2_name"
  }},
  "analysis": {{
    "context": "Brief summary of the findings (e.g., 'File is missing X, has extra Y, and 2 columns were semantically mapped.')",
    "reasoning": "Explain the *impact* of these mismatches (e.g., 'Missing 'DiscountCode' may cause incomplete data. Extra 'ShippingMethod' is not in the DB.')",
    "recommendation": [
      "Actionable step 1 (e.g., 'Map 'cust' to 'CustomerID' for loading.')",
      "Actionable step 2 (e.g., 'Add 'DiscountCode' to the source file or set a default value.')",
      "Actionable step 3 (e.g., 'Verify if 'ShippingMethod' should be added to the database table.')"
    ]
  }}
}}
"""

def get_schema_analysis_prompt(
    db_schema: Dict[str, Any],
    file_schema: Dict[str, Any],
    raw_comparison: Dict[str, List[str]],
    target_table_name: str,
    source_file_name: str
) -> str:
    """Helper function to format the schema analysis prompt."""

    file_schema_columns = file_schema.get('columns', {})
    try:
        return SCHEMA_ANALYSIS_PROMPT.format(
            target_table_name=target_table_name,
            source_file_name=source_file_name,
            db_schema_json=json.dumps(db_schema, indent=2, default=str),
            file_schema_json=json.dumps(file_schema_columns, indent=2, default=str),
            raw_comparison_json=json.dumps(raw_comparison, indent=2, default=str)
        )
    except KeyError as e:
        logging.error(f"Missing key in SCHEMA_ANALYSIS_PROMPT format string: {e}")
        return "ERROR: Prompt formatting failed. Check schema analysis prompt template keys."
    except Exception as e:
        logging.error(f"Error formatting SCHEMA_ANALYSIS_PROMPT: {e}")
        return "ERROR: Could not format schema analysis prompt."


# ==============================================================
# 2️⃣ DYNAMIC RULES PROMPT
# ==============================================================

DYNAMIC_RULES_PROMPT = """
You are a Data Analyst. Your only task is to infer potential validation rules by analyzing sample data from a file.

You will be given:
1.  **Current File Schema**: A JSON object showing columns, inferred types, and sample data.

Your Job:
1.  Analyze the `sample_values` for each column.
2.  Infer potential new validation rules (format checks, enum lists, range checks).
3.  Return *ONLY* a single JSON list of rule objects. Do not add any other text, markdown, or explanations.

---
[INPUT DATA]

**Current File Schema (Source):**
{current_file_schema_json}

---
[YOUR ANALYSIS]

Produce a single JSON list in this *exact* format:

[
  {{
    "column": "ColumnName",
    "rule_type": "[format_check | enum_check | range_check]",
    "inferred_from_samples": ["sample1", "sample2"],
    "rule_details": "Explain the inferred rule. E.g., 'Based on X/Y samples, this column appears to follow a regex format: ^[A-Z]{{3}}\\d{{4}}$' OR 'Column appears to be categorical. All samples were from the list: [\"ValueA\", \"ValueB\"]'"
  }}
]
"""

def get_dynamic_rules_prompt(
    current_file_schema: Dict[str, Any]
) -> str:
    """Helper function to format the dynamic rules prompt."""

    current_file_schema_cols = current_file_schema.get('columns', {})
    try:
        return DYNAMIC_RULES_PROMPT.format(
            current_file_schema_json=json.dumps(current_file_schema_cols, indent=2, default=str)
        )
    except KeyError as e:
        logging.error(f"Missing key in DYNAMIC_RULES_PROMPT format string: {e}")
        return "ERROR: Prompt formatting failed."
    except Exception as e:
        logging.error(f"Error formatting DYNAMIC_RULES_PROMPT: {e}")
        return "ERROR: Could not format dynamic rules prompt."

# ==============================================================
# 3️⃣ FINAL ANALYSIS PROMPT (NEW & CHEAP)
# ==============================================================

ANALYSIS_PROMPT = """
You are the Principal Data Steward. You will be given a *summary* of data validation findings and the initial schema analysis.
Your job is to generate ONLY the high-level analysis, scoring, and planning sections, following your core instructions.

[INPUT DATA]

**1. Schema Analysis (What vs. What):**
{schema_analysis_json}

**2. Violation Summaries (The Problems):**
{violations_summary_json}

**3. Historical Schemas (For Drift Analysis):**
{historical_schemas_json}

---
[YOUR ANALYSIS]

Based *only* on the input data, generate a single JSON object with the following keys.
Be specific, authoritative, and link your analysis directly to the data.

{{
  "validation_summary": {{
    "status": "[Passed | Passed with Warnings | Failed]",
    "high_severity_issues": <count of high severity issues>,
    "medium_severity_issues": <count of medium severity issues>,
    "low_severity_issues": <count of low severity issues>
  }},
  "data_quality_score": {{
    "score": <0-100>,
    "grade": "[A | B | C | D | F]",
    "reasoning": "Provide a data-driven explanation for the score. **Link the specific high-severity violations (e.g., 'null OrderIDs') to their business impact and the resulting score.**"
  }},
  "triage_plan": [
     {{ "priority": 1, "action": "First, most critical action.", "reasoning": "Why this is P1 (e.g., 'Blocks all data loading')." }},
     {{ "priority": 2, "action": "Second, most critical action.", "reasoning": "Why this is P2 (e.g., 'Corrupts financial data')." }},
     {{ "priority": 3, "action": "Third, most critical action.", "reasoning": "Why this is P3 (e.g., 'Causes user-facing errors')." }}
  ],
  "append_upsert_suggestion": {{
    "strategy": "[Append | Upsert | Do Not Load]",
    "key_column": "[ColumnName | null]",
    "reasoning": "Explain the strategy. **If 'Upsert', state the key. If 'Do Not Load', explain why it's unsafe.**"
  }},
  "schema_drift": {{
    "detected": <true | false>,
    "analysis": "Analyze the historical schemas. **If drift is detected, describe the *specific change* (e.g., 'Column 'Email' was added', 'Column 'Price' changed from INT to STRING').**"
  }},
  "root_cause_analysis": {{
    "hypothesis": "Provide a *specific, data-driven hypothesis* for the root cause. **Connect the error patterns (e.g., 'null OrderIDs' + 'string-based 'qty'') to a likely real-world source** (e.g., 'This pattern suggests a manual data entry error from a spreadsheet, not an API bug')."
  }},
  "overall_analysis": {{
    "narrative_summary": "Write a 2-sentence summary for a non-technical manager. **State the data's *fitness for use* (e.g., 'Data is NOT safe for production') and the **single biggest business risk** (e.g., 'Risk of data corruption in the Orders table')."
  }}
}}
"""
def get_analysis_prompt(
    schema_analysis: Dict[str, Any],
    violations_summary: Dict[str, Any],
    historical_schemas: List[Dict[str, Any]] # We still need this for drift
) -> str:
    """Helper function to format the new, cheaper analysis prompt."""
    print("\n\n\nHELLO! I AM THE NEW, CHEAP PROMPT.PY FUNCTION!\n\n\n")
    try:
        return ANALYSIS_PROMPT.format(
            schema_analysis_json=json.dumps(schema_analysis, indent=2, default=str),
            violations_summary_json=json.dumps(violations_summary, indent=2, default=str),
            # Note: We pass historical schemas in the prompt, but it's small.
            # The LLM's job is to analyze it, not just see it.
            historical_schemas_json=json.dumps(historical_schemas, indent=2, default=str)
        )
    except Exception as e:
        logging.error(f"Error formatting ANALYSIS_PROMPT: {e}")
        return "ERROR: Could not format analysis prompt."

# ==============================================================
# 4️⃣ SMART TABLE MATCHING PROMPT (TOKEN OPTIMIZED)
# ==============================================================

TABLE_MATCHING_PROMPT = """
You are an expert Database Administrator (DBA).
Your task is to find the best table in a database that matches a user's source file by comparing column lists.

You will be given:
1.  **File Column List**: A simple list of column names from the user's file.
2.  **All Table Columns**: A JSON object where keys are table names and values are lists of column names from the database.

Your Job:
1.  **Analyze Semantically**: Compare the `File Column List` to the column list for every table. Look for semantic matches (e.g., 'CustID' -> 'CustomerID', 'qty' -> 'Quantity').
2.  **Score Confidence**: For each table, calculate a confidence score (0-100) based on how well the column names match.
3.  **Provide Reasoning**: For your top matches, briefly explain *why* it's a good match (e.g., "Matched 5/6 columns including 'OrderID' and 'Quantity'").
4.  **Format Output**: Return *ONLY* a single JSON object. Do not add any other text or markdown.

---
[INPUT DATA]

**1. User's File Column List:**
{file_schema_json}

**2. All Database Table Columns:**
{all_db_schemas_json}

---
[YOUR ANALYSIS]

Produce a single JSON object in this *exact* format.
Rank the tables from highest confidence to lowest. Include a maximum of 3 recommendations.

{{
  "recommendations": [
    {{
      "table_name": "best_match_table",
      "confidence_score": 95,
      "reasoning": "Strong match. Matched 6/6 columns including 'OrderID' and 'Quantity'."
    }},
    {{
      "table_name": "second_best_table",
      "confidence_score": 40,
      "reasoning": "Weak match. Only matched 2/6 columns ('Price')."
    }}
  ]
}}
"""

def get_table_matching_prompt(
    file_schema: List[str],  # <-- This has changed from Dict to List
    all_db_schemas: Dict[str, Any]
) -> str:
    """Helper function to format the smart table matching prompt."""
    try:
        return TABLE_MATCHING_PROMPT.format(
            file_schema_json=json.dumps(file_schema, indent=2), # Pass the simple list
            all_db_schemas_json=json.dumps(all_db_schemas, indent=2, default=str)
        )
    except Exception as e:
        logging.error(f"Error formatting TABLE_MATCHING_PROMPT: {e}")
        return "ERROR: Could not format table matching prompt."