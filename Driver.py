import validation_module
import json
import logging
from pprint import pprint

# --- 1. CONFIGURE LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


FILE_TO_TEST = "new_orders.csv" 


def run_test():
    print(f"--- 🚀 STARTING TEST FOR: {FILE_TO_TEST} ---")

    # --- 1. Get Sheet Names ---
    # This is the first new function we call
    sheet_names = validation_module.get_sheet_names(FILE_TO_TEST)
    if not sheet_names:
        print("--- ❌ ERROR: Could not find any sheets in the file. ---")
        return

    print(f"Found {len(sheet_names)} sheet(s): {sheet_names}")
    
    final_report_collection = {}
    
    # --- 2. Loop Through Each Sheet ---
    for sheet_name in sheet_names:
        print("\n" + "="*50)
        print(f"--- 🚀 PROCESSING SHEET: [{sheet_name}] ---")
        print("="*50)

        # --- 3. Get Recommendations for this sheet ---
        print(f"Getting table recommendations for '{sheet_name}'...")
        recommendations = validation_module.get_recommendations_for_sheet(
            file_path=FILE_TO_TEST, 
            sheet_name=sheet_name
        )
        
        if "error" in recommendations:
            print(f"--- ❌ ERROR (Recommendations): {recommendations['error']} ---")
            continue # Skip to the next sheet

        print("\n--- ✅ Recommendations Found ---")
        pprint(recommendations.get("recommendations"))

        # --- 4. Get User Input for this sheet ---
        table_name = input(f"\nPlease type the table name for sheet '{sheet_name}': ")
        
        if not table_name:
            print(f"--- ⚠️ WARNING: No table name provided for '{sheet_name}'. Skipping this sheet. ---")
            continue

        # --- 5. Run Validation for this sheet ---
        print(f"\nValidating '{sheet_name}' against table '{table_name}'...")
        single_sheet_report = validation_module.run_validation_for_single_sheet(
            file_path=FILE_TO_TEST,
            sheet_name=sheet_name,
            table_name=table_name
        )
        
        if "error" in single_sheet_report:
            print(f"--- ❌ ERROR (Validation): {single_sheet_report['error']} ---")
            continue
            
        final_report_collection[sheet_name] = single_sheet_report
        print(f"--- ✅ SUCCESS: Validation complete for sheet '{sheet_name}' ---")

    # --- 6. Assemble Final Report ---
    print("\n" + "="*80)
    print("--- 🏁 ALL SHEETS PROCESSED. FINAL COMBINED REPORT: ---")
    
    final_output = {
        "User_file_name": FILE_TO_TEST,
        "sheet_validation_results": final_report_collection
    }
    # --- THIS IS THE NEW CODE TO ADD ---
    
    # 1. Convert the final dictionary to a pretty-printed string
    final_report_str = json.dumps(final_output, indent=2)
    
    # 2. Save the string to a file
    with open("validation_report_final.json", "w") as f:
        f.write(final_report_str)
        
    # 3. Print the report to the console (as before)
    print(final_report_str)
    
    print(json.dumps(final_output, indent=2))
    print("="*80)


if __name__ == "__main__":
    run_test()