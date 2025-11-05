import json
import logging
from typing import Dict, Any

# Configure logging for the script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def _render_single_report_md(data: dict) -> str:
    """
    Internal helper function to render the markdown for a single report.
    (This can be a CSV or a single Excel sheet).
    
    This function contains all the updated logic for our new JSON keys.
    """
    md_parts = []
    
    # Helper to safely format lists of items
    def format_list(items_list, empty_msg="None"):
        if not items_list:
            return f"- {empty_msg}"
        return "\n".join(f"- `{item}`" for item in items_list)

    # --- 1. Overall Analysis ---
    # This is the new "executive summary"
    overall_analysis = data.get('overall_analysis', {})
    md_parts.append(f"### 🎯 Overall Analysis\n")
    md_parts.append(f"> **{overall_analysis.get('narrative_summary', 'No analysis summary provided.')}**")

    # --- 2. Data Quality Score ---
    score_data = data.get('data_quality_score', {})
    score = score_data.get('score', 'N/A')
    grade = score_data.get('grade', 'N/A')
    reasoning = score_data.get('reasoning', 'No reasoning provided.')
    
    md_parts.append(f"\n### 📊 Data Quality Score: {score} / 100 (Grade: {grade})")
    md_parts.append(f"**Reasoning:** {reasoning}")

    # --- 3. Triage Plan (NEW) ---
    md_parts.append(f"\n###  triage_plan")
    triage_plan = data.get('triage_plan', [])
    if not triage_plan:
        md_parts.append("No triage plan provided.")
    else:
        md_parts.append("| Priority | Action | Reasoning |")
        md_parts.append("| :--- | :--- | :--- |")
        for item in triage_plan:
            md_parts.append(f"| **{item.get('priority')}** | {item.get('action')} | {item.get('reasoning')} |")

    # --- 4. Schema Mismatch ---
    md_parts.append("\n--- \n## 1. Schema Mismatch Analysis")
    # For Excel, the key is 'schema_mismatch'. For CSV, it's at the top level.
    # The new main function passes the correct part, so we just get it.
    schema = data.get('schema_mismatch', {})
    if not schema:
        # This handles the case where the CSV report is the root object
        if 'columns_missing_from_file' in data:
            schema = data
        else:
             md_parts.append("No schema mismatch data found.")
             
    if schema:
        analysis = schema.get('analysis', {})
        md_parts.append(f"**Analysis:** {analysis.get('context', 'N/A')}")
        
        md_parts.append("\n#### Columns Missing from File (Required by Table):")
        md_parts.append(format_list(schema.get('columns_missing_from_file'), "None"))
        
        md_parts.append("\n#### Extra Columns Found in File (Not in Table):")
        md_parts.append(format_list(schema.get('columns_extra_in_file'), "None"))

        md_parts.append("\n#### Suggested Naming Mappings:")
        mappings = schema.get('naming_mismatches', {})
        if not mappings:
            md_parts.append("- None")
        else:
            for file_col, db_col in mappings.items():
                md_parts.append(f"- Map `{file_col}` (file) to `{db_col}` (table)")
        
        md_parts.append("\n#### Recommendations:")
        md_parts.append(format_list(analysis.get('recommendation', []), "No recommendations."))

    # --- 5. Data Quality Violations (Key updated) ---
    md_parts.append("\n--- \n## 2. Data Quality Violations")
    # Key changed from 'data_quality_violations' to 'data_quality_issues'
    dq_violations = data.get('data_quality_issues', [])
    if not dq_violations:
        md_parts.append("No data quality violations found.")
    else:
        for issue in dq_violations:
            md_parts.append(f"\n- **Column: `{issue.get('column')}`**")
            md_parts.append(f"  - **Check:** `{issue.get('check')}`")
            md_parts.append(f"  - **Severity:** {issue.get('severity', 'N/A').title()}")
            md_parts.append(f"  - **Count:** {issue.get('count', 'N/A')}")
            md_parts.append(f"  - **Details:** {issue.get('details', 'N/A')}")

    # --- 6. Data Type Mismatch (Logic simplified) ---
    md_parts.append("\n--- \n## 3. Data Type Violations")
    type_mismatches = data.get('data_type_mismatch', [])
    if not type_mismatches:
        md_parts.append("No data type mismatches found.")
    else:
        for issue in type_mismatches:
            md_parts.append(f"\n- **Column: `{issue.get('column')}`**")
            md_parts.append(f"  - **Expected Type (DB):** `{issue.get('expected_db_type')}`")
            md_parts.append(f"  - **Found Type (File):** `{issue.get('found_file_type')}`")
            md_parts.append(f"  - **Invalid Samples:** `{issue.get('sample_invalid_values', [])}`")

    # --- 7. Root Cause Analysis (Keys updated) ---
    md_parts.append("\n--- \n## 4. Root Cause Analysis")
    # Keys changed to be simpler
    rca = data.get('root_cause_analysis', {})
    if not rca or not rca.get('hypothesis'):
        md_parts.append("No root cause analysis provided.")
    else:
        md_parts.append(f"**Hypothesis:** {rca.get('hypothesis', 'N/A')}")

    # --- 8. Load Strategy (Keys updated) ---
    md_parts.append("\n--- \n## 5. Suggested Load Strategy")
    strategy = data.get('append_upsert_suggestion', {})
    if not strategy:
        md_parts.append("No load strategy analysis found.")
    else:
        md_parts.append(f"- **Strategy:** `{strategy.get('strategy', 'N/A').upper()}`")
        md_parts.append(f"- **Key Column:** `{strategy.get('key_column', 'N/A')}`")
        md_parts.append(f"- **Reasoning:** {strategy.get('reasoning', 'N/A')}")

    # --- 9. Schema Drift (Keys updated) ---
    md_parts.append("\n--- \n## 6. Schema Drift")
    drift = data.get('schema_drift', {})
    if not drift:
        md_parts.append("No schema drift analysis found.")
    else:
        md_parts.append(f"**Drift Detected:** `{drift.get('detected', 'false')}`")
        md_parts.append(f"**Analysis:** {drift.get('analysis', 'N/A')}")

    # --- 10. Dynamic Validation Rules (Table format) ---
    md_parts.append("\n--- \n## 7. Inferred Validation Rules")
    rules = data.get('dynamic_validation_rules', [])
    if not rules:
        md_parts.append("No dynamic validation rules were inferred.")
    else:
        md_parts.append("| Column | Rule Type | Details | Inferred From |")
        md_parts.append("| :--- | :--- | :--- | :--- |")
        for rule in rules:
            col = f"`{rule.get('column', 'N/A')}`"
            rule_type = f"`{rule.get('rule_type', 'N/A')}`"
            details = rule.get('rule_details', 'N/A')
            samples = f"`{rule.get('inferred_from_samples', [])}`"
            md_parts.append(f"| {col} | {rule_type} | {details} | {samples} |")

    return "\n".join(md_parts)


def create_validation_markdown(data: dict) -> str:
    """
    Converts a data validation JSON (as a dictionary) into a formatted Markdown string.
    
    This function now intelligently handles both CSV (flat) and Excel (nested)
    JSON report formats.
    """
    md_parts = []
    
    # Check if this is an Excel report (nested)
    if 'sheet_validation_results' in data:
        file_name = data.get('User_file_name', 'Excel Report')
        md_parts.append(f"# 🗂️ Multi-Sheet Validation Report: '{file_name}'")
        md_parts.append(f"**Processed At:** {data.get('Processed_at', 'N/A')}")
        
        sheet_results = data.get('sheet_validation_results', {})
        if not sheet_results:
            md_parts.append("\n\n---\n\n## No Sheets Processed")
            md_parts.append("The Excel file was processed, but no individual sheet reports were found.")
            return "\n".join(md_parts)
            
        for sheet_name, sheet_data in sheet_results.items():
            md_parts.append(f"\n\n---\n\n## 📈 Report for Sheet: `{sheet_name}`")
            
            # --- Summary Table for this Sheet ---
            summary = sheet_data.get('validation_summary', {})
            score = sheet_data.get('data_quality_score', {}).get('score', 'N/A')
            grade = sheet_data.get('data_quality_score', {}).get('grade', 'N/A')
            
            md_parts.append("\n### Sheet at a Glance")
            md_parts.append("| Metric | Value |")
            md_parts.append("| :--- | :--- |")
            md_parts.append(f"| Validation Status | **{summary.get('status', 'N/A')}** |")
            md_parts.append(f"| Data Quality Score | **{score} (Grade: {grade})** |")
            md_parts.append(f"| Target Table (Inferred) | `{sheet_data.get('schema_mismatch', {}).get('target_table', 'N/A')}` |")
            md_parts.append(f"| High Severity Issues | {summary.get('high_severity_issues', 0)} |")
            md_parts.append(f"| Medium Severity Issues | {summary.get('medium_severity_issues', 0)} |")
            md_parts.append(f"| Total Rows Checked | {sheet_data.get('total_rows_checked', 'N/A')} |")
            
            # Use the helper to render the full report for this sheet
            md_parts.append(_render_single_report_md(sheet_data))
            
    # Check if this is a CSV report (flat)
    elif 'schema_mismatch' in data:
        file_name = data.get('User_file_name', 'CSV Report')
        md_parts.append(f"# 📄 Single File Validation Report: '{file_name}'")
        md_parts.append(f"**Processed At:** {data.get('Processed_at', 'N/A')}")
        
        # --- Summary Table for this File ---
        summary = data.get('validation_summary', {})
        score = data.get('data_quality_score', {}).get('score', 'N/A')
        grade = data.get('data_quality_score', {}).get('grade', 'N/A')

        md_parts.append("\n### File at a Glance")
        md_parts.append("| Metric | Value |")
        md_parts.append("| :--- | :--- |")
        md_parts.append(f"| Validation Status | **{summary.get('status', 'N/A')}** |")
        md_parts.append(f"| Data Quality Score | **{score} (Grade: {grade})** |")
        md_parts.append(f"| Target Table (Inferred) | `{data.get('inferred_target_table', 'N/A')}` |")
        md_parts.append(f"| High Severity Issues | {summary.get('high_severity_issues', 0)} |")
        md_parts.append(f"| Medium Severity Issues | {summary.get('medium_severity_issues', 0)} |")
        md_parts.append(f"| Total Rows Checked | {data.get('total_rows_checked', 'N/A')} |")
        
        # Use the helper to render the full report
        md_parts.append(_render_single_report_md(data))
        
    else:
        md_parts.append("# ❌ Unknown Report Format")
        md_parts.append("The input JSON does not match the expected CSV or Excel report format.")

    return "\n".join(md_parts)


# --- Example of how to use the function ---
if __name__ == "__main__":
    
    # Change this to the name of your JSON file
    json_file_path = 'validation_report_converted.json' 
    output_markdown_file = 'data_validation_report.md'

    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            validation_data = json.load(f)
            
        logging.info(f"Generating markdown from '{json_file_path}'...")
        markdown_output = create_validation_markdown(validation_data)
        
        # print("\n--- MARKDOWN PREVIEW ---")
        # print(markdown_output)
        # print("------------------------\n")

        with open(output_markdown_file, 'w', encoding='utf-8') as md_file:
            md_file.write(markdown_output)
            
        logging.info(f"Successfully generated and saved report to '{output_markdown_file}'")

    except FileNotFoundError:
        logging.error(f"Error: The file '{json_file_path}' was not found.")
    except json.JSONDecodeError:
        logging.error(f"Error: Could not decode JSON from '{json_file_path}'.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)