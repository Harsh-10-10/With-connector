import os
import autogen
import json
import logging
import sqlalchemy
from typing import Annotated, Optional, List

# Import our new, specific tools
import validation_module
import build_md  # Assuming you have this file for markdown conversion
from dotenv import load_dotenv

# --- 2. CONFIGURE LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 3. LOAD CONFIG ---
load_dotenv()
API_VERSION = os.getenv("API_VERSION", "2024-02-01") 
AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT") 
API_KEY = os.getenv("API_KEY") 
DEPLOYMENT_NAME = os.getenv("DEPLOYMENT_NAME", "gpt-4.1-nano") 

config_list = [
    {
        "model": DEPLOYMENT_NAME,
        "api_key": API_KEY,
        "base_url": AZURE_ENDPOINT,
        "api_type": "azure",
        "api_version": API_VERSION,
    }
]

llm_config = {
    "config_list": config_list,
    "cache_seed": 42,
    "timeout": 600, # Increased timeout for potentially long validation
}

# --- 4. TOOL DEFINITIONS (NEW "PER-SHEET" TOOLS) ---

# TOOL 1: Get Sheet Names
def get_sheet_names(
    file_path: Annotated[str, "The file path to the CSV or Excel file"]
) -> Annotated[str, "A JSON list of sheet names."]:
    """
    Inspects a file and returns a JSON list of its sheet names.
    For CSVs, it will return '[\"csv_data\"]'.
    """
    logging.info(f"... EXECUTING: get_sheet_names('{file_path}')...")
    try:
        sheet_list = validation_module.get_sheet_names(file_path)
        return json.dumps(sheet_list)
    except Exception as e:
        logging.error(f"... ERROR in get_sheet_names: {e}")
        return json.dumps({"error": str(e)})

# TOOL 2: Get Recommendations for a Sheet
def get_recommendations_for_sheet(
    file_path: Annotated[str, "The file path to the CSV or Excel file"],
    sheet_name: Annotated[str, "The specific sheet name to analyze"]
) -> Annotated[str, "A JSON string of recommended tables."]:
    """
    Analyzes a *single sheet* and calls the LLM to find the best matching tables 
    in the Databricks database.
    """
    logging.info(f"... EXECUTING: get_recommendations_for_sheet('{file_path}', '{sheet_name}')...")
    try:
        recommendations_dict = validation_module.get_recommendations_for_sheet(file_path, sheet_name)
        return json.dumps(recommendations_dict)
    except Exception as e:
        logging.error(f"... ERROR in get_recommendations_for_sheet: {e}")
        return json.dumps({"error": str(e)})

# TOOL 3: Run Validation for a Single Sheet
def run_validation_for_single_sheet(
    file_path: Annotated[str, "The file path to the CSV or Excel file"],
    sheet_name: Annotated[str, "The specific sheet name to validate"],
    table_name: Annotated[str, "The target database table name (e.g., 'customer_orders')"]
) -> Annotated[str, "The FULL JSON string result of the schema validation for *that sheet*."]:
    """
    Runs the full schema validation process for one specific sheet against one table.
    """
    logging.info(f"... EXECUTING: run_validation_for_single_sheet('{file_path}', '{sheet_name}', '{table_name}')...")
    try:
        final_report_dict = validation_module.run_validation_for_single_sheet(
            file_path=file_path,
            sheet_name=sheet_name,
            table_name=table_name
        )
        return json.dumps(final_report_dict)
    except Exception as e:
        logging.error(f"... ERROR in run_validation_for_single_sheet: {e}")
        return json.dumps({"error": str(e)})

# TOOL 4: (UNCHANGED) Markdown Converter
def convert_json_to_markdown(
    json_report_str: Annotated[str, "The JSON report string from a previous tool call"]
) -> Annotated[str, "A formatted Markdown report"]:
    """Converts a JSON report into a human-readable Markdown format."""
    logging.info(f"... EXECUTING: convert_json_to_markdown(...) ...")
    try:
        data = json.loads(json_report_str)
        # Note: We must update build_md.py if the JSON structure changed
        # For now, we assume it takes the single-sheet report format
        markdown_report = build_md.create_validation_markdown(data)
        return markdown_report
    except Exception as e:
        logging.error(f"... ERROR in convert_json_to_markdown: {e}")
        return json.dumps({"error": str(e)})

# --- 5. AGENT DEFINITIONS ---

# AGENT 1: The User
user_proxy = autogen.UserProxyAgent(
    name="User",
    human_input_mode="TERMINATE", 
    max_consecutive_auto_reply=10,
    is_termination_msg=lambda x: x.get("content", "").rstrip().endswith("TERMINATE"),
    system_message="""You are the user and code executor.
    You provide the file path and answer questions.
    When a tool is called, you execute it.
    You ONLY stop for input when a message ends with the word TERMINATE.""",
    code_execution_config={"work_dir": "autogen_work_dir", "use_docker": False}, 
)

# AGENT 2: The Conductor (NEW "SHEET-AWARE" BRAIN)
conductor_agent = autogen.AssistantAgent(
    name="ConductorAgent",
    llm_config=llm_config,
    system_message="""You are the **Conductor**, the primary Data Steward assistant.
    Your job is to manage a complex, multi-step, "per-sheet" validation workflow.
    
    **YOUR STATE:**
    You must internally keep track of:
    - `file_path`: The user's file.
    - `sheet_list`: The list of sheet names to process.
    - `processed_sheets`: A list of sheet names you have finished.
    - `final_reports`: A list of all the JSON reports you have gathered.
    
    **YOUR GOAL (Follow this *exact* workflow):**
    
    1.  **Greet & Get Sheets**: Acknowledge the user's `file_path`. Call `get_sheet_names` to get the `sheet_list`.
    2.  **Start Loop**: Pick the *first* sheet from `sheet_list` that is not in `processed_sheets`.
    3.  **Announce Sheet**: Tell the user, "Okay, let's start with sheet: [sheet_name]".
    4.  **Get Recommendations**: Call `get_recommendations_for_sheet` for the current `sheet_name`.
    5.  **Present & Ask**:
        a.  Show the user the top recommendations (table name, score, reasoning).
        b.  Ask: "Here are the top matches for [sheet_name]. **Please type the name of the table** you want me to use for this sheet. TERMINATE"
    6.  **Get User's Choice**:
        a.  The user will reply with a `table_name`.
    7.  **Call Validator**:
        a.  Announce: "Great. Validating [sheet_name] against [table_name]..."
        b.  Call the `@SchemaValidatorAgent` to execute `run_validation_for_single_sheet` using the `file_path`, `sheet_name`, and `table_name`.
    8.  **Store Report**:
        a.  Get the JSON report back. Add it to your `final_reports` list, keyed by `sheet_name`.
        b.  Add the `sheet_name` to your `processed_sheets` list.
    9.  **Check Loop**:
        a.  If `processed_sheets` length == `sheet_list` length, all sheets are done. Go to Step 10.
        b.  If not, go back to Step 2 to process the next sheet.
    10. **Final Report**:
        a.  Tell the user, "All sheets have been validated."
        b.  Combine all reports from `final_reports` into a *single* final JSON object. (e.g., `{"User_file_name": "file.xlsx", "sheet_validation_results": {"Sheet1": {...}, "Sheet2": {...}}}`).
        c.  Call the `@MarkdownAgent` to format this *final combined JSON*.
        d.  Present the final Markdown report.
    
    **CRITICAL RULES:**
    - ASK ONE QUESTION AT A TIME.
    - Always end user questions with `TERMINATE`.
    - You must manage the loop and state (sheet list, processed sheets) yourself.
    """
)

# AGENT 3: The Validator (Specialist)
schema_validator_agent = autogen.AssistantAgent(
    name="SchemaValidatorAgent",
    llm_config=llm_config,
    system_message="""You are a silent specialist. Your only job is to call the `run_validation_for_single_sheet` tool.
    Report the JSON result back to the `ConductorAgent`."""
)

# AGENT 4: The Markdown Formatter (Specialist)
markdown_agent = autogen.AssistantAgent(
    name="MarkdownAgent",
    llm_config=llm_config,
    system_message="""You are a silent specialist. Your only job is to call the `convert_json_to_markdown` tool.
    Report the Markdown string result back to the `ConductorAgent`."""
)

# --- 6. TOOL REGISTRATION (MODIFIED) ---

# We register all tools with the Conductor, who will delegate to specialists.
# The user_proxy is the executor for all.

autogen.register_function(
    get_sheet_names,
    caller=conductor_agent,
    executor=user_proxy,
    name="get_sheet_names",
    description="Inspects a file and returns a JSON list of its sheet names."
)

autogen.register_function(
    get_recommendations_for_sheet,
    caller=conductor_agent,
    executor=user_proxy,
    name="get_recommendations_for_sheet",
    description="Analyzes a single sheet to find and recommend the best matching DB tables."
)

# Register the validator tool with its specialist agent
autogen.register_function(
    run_validation_for_single_sheet,
    caller=schema_validator_agent,
    executor=user_proxy, 
    name="run_validation_for_single_sheet",
    description="Runs the full validation for one specific sheet against one table."
)

autogen.register_function(
    convert_json_to_markdown,
    caller=markdown_agent,
    executor=user_proxy, 
    name="convert_json_to_markdown",
    description="Convert a JSON report to Markdown."
)
# --- 7. GROUP CHAT SETUP ---
agents = [user_proxy, conductor_agent, schema_validator_agent, markdown_agent]
group_chat = autogen.GroupChat(
    agents=agents,
    messages=[],
    max_round=100, # Increased max rounds for multi-sheet conversation
    speaker_selection_method="auto",
    allow_repeat_speaker=True
)

manager = autogen.GroupChatManager(
    name="Orchestrator",
    groupchat=group_chat,
    llm_config=llm_config,
    system_message="""You are the Orchestrator. Your job is to select the next agent to speak.
    
    **THE FLOW (Follow this precisely):**
    
    1.  **After the `User` (human) speaks:** YOU MUST ALWAYS select the `ConductorAgent`.
    
    2.  **After a `Specialist` (`SchemaValidatorAgent`, `MarkdownAgent`) speaks:** YOU MUST ALWAYS select the `ConductorAgent`.
    
    3.  **After the `User` (as executor) posts a tool result:** YOU MUST ALWAYS select the `ConductorAgent`.
    
    4.  **After the `ConductorAgent` speaks:**
        a) If it called a specialist (e.g., "@SchemaValidatorAgent"), select that specialist.
        b) If it called its *own* tool (e.g., `get_sheet_names` or `get_recommendations_for_sheet`), select the `User` (who is the executor).
        c) If it asked the user a question (ending in `TERMINATE`), select the `User` (who is the human).

    This flow ensures the `ConductorAgent` is the central brain.
    """
)
# --- 8. RUN THE CHAT ---
print("="*50)
print("🚀 STARTING CHAT (Per-Sheet Workflow)")
print("Type 'exit' or 'terminate' to end the conversation.")
print("="*50)

# We provide the file path. The Conductor's new brain will handle the loop.
user_proxy.initiate_chat(
    manager,
    message="Hi, I have a file called 'new_orders.csv'. Can you help me with it?"
    # Note: Make sure 'ironclad.xlsx' is a multi-sheet file for this to work!
    # If you use 'new_orders.csv', it will just run for the one "csv_data" sheet.
)