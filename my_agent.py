import os
import autogen
import json
import logging
import sqlalchemy
from typing import Annotated, Optional

# --- MODIFIED IMPORTS ---
import validation_module
import build_md
# 'tools' is no longer needed here, as the agent calls 'validation_module'
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
    "timeout": 300, 
}

# --- 4. TOOL DEFINITIONS ---

# REMOVED: DB_URL = "sqlite:///database/sample_data.db"
# The database connection is now handled by databricks_tools.py

# TOOL 1: (NEW) Smart Table Recommender
def get_table_recommendations(
    file_path: Annotated[str, "The file path to the CSV or Excel file"]
) -> Annotated[str, "A JSON string of recommended tables and their match scores."]:
    """
    Analyzes the file and calls the LLM to find the best matching tables 
    in the Databricks database.
    """
    logging.info(f"... EXECUTING: get_table_recommendations('{file_path}')...")
    try:
        # This function now does all the work!
        recommendations_dict = validation_module.get_smart_table_recommendations(file_path)
        return json.dumps(recommendations_dict)
    except Exception as e:
        logging.error(f"... ERROR in get_table_recommendations: {e}")
        return json.dumps({"error": str(e)})

# TOOL 2: (UNCHANGED) Placeholder Profiler
def run_data_profiling(
    file_path: Annotated[str, "The file path to the CSV or Excel file"]
) -> Annotated[str, "The JSON string result of the data profiling"]:
    """Runs a data profiling process on the available data."""
    logging.info(f"... EXECUTING: profiler('{file_path}')...")
    result = {
        "file_name": file_path, "total_rows": 5000, "total_columns": 10,
        "column_stats": {"OrderID": {"nulls": 50}, "Email": {"nulls": 120}}
    }
    return json.dumps(result)

# TOOL 3: (MODIFIED) Schema Validator
def run_schema_validation(
    file_path: Annotated[str, "The file path to the CSV or Excel file"],
    table_name: Annotated[str, "The target database table name (e.g., 'customer_orders')"]
) -> Annotated[str, "The FULL JSON string result of the schema validation"]:
    """Runs the full schema validation process on a file against a specific table."""
    logging.info(f"... EXECUTING: run_schema_validation('{file_path}', '{table_name}')...")
    try:
        # --- KEY CHANGE ---
        # Removed the 'db_url' argument. 
        # The validation_module now creates its own Databricks engine.
        final_report_dict = validation_module.run_multi_sheet_validation(
            file_path=file_path,
            user_provided_table_name=table_name
        )
        return json.dumps(final_report_dict)
    except Exception as e:
        logging.error(f"... ERROR in run_schema_validation: {e}")
        return json.dumps({"error": str(e)})

# TOOL 4: (UNCHANGED) Markdown Converter
def convert_json_to_markdown(
    json_report_str: Annotated[str, "The JSON report string from a previous tool call"]
) -> Annotated[str, "A formatted Markdown report"]:
    """Converts a JSON report into a human-readable Markdown format."""
    logging.info(f"... EXECUTING: convert_json_to_markdown(...) ...")
    try:
        data = json.loads(json_report_str)
        markdown_report = build_md.create_validation_markdown(data)
        return markdown_report
    except Exception as e:
        logging.error(f"... ERROR in convert_json_to_markdown: {e}")
        return json.dumps({"error": str(e)})

# REMOVED: def get_available_tables()
# This tool is now obsolete and replaced by get_table_recommendations.

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

# AGENT 2: The Conductor (UPDATED BRAIN)
# AGENT 2: The Conductor (UPDATED BRAIN)
conductor_agent = autogen.AssistantAgent(
    name="ConductorAgent",
    llm_config=llm_config,
    # --- REPLACE THE system_message WITH THIS ---
    system_message="""You are the **Conductor**, the primary Data Steward assistant.
    Your job is to have a fluid, step-by-step conversation with the User.
    
    **YOUR GOAL:**
    Your goal is to validate a user's file. You MUST follow this *exact* workflow:
    
    1.  **Greet the User**: Acknowledge their file.
    2.  **State Your Plan**: Tell the user you will first analyze their file to find the best table match in Databricks.
    3.  **Call Recommender Tool**: Immediately call the `get_table_recommendations` tool with the user's `file_path`.
    4.  **Analyze Recommendations**:
        a.  If the tool returns an error, report it to the user.
        b.  If it returns recommendations, get the list (e.g., `recommendations_json["recommendations"]`).
    5.  **Present Recommendations**:
        a.  Format the list of recommendations for the user. Show the `table_name`, `confidence_score`, and `reasoning` for each match.
        b.  Ask the user an open-ended question: "Here are the top matches I found. **Please type the name of the table** you want me to validate against. TERMINATE"
    6.  **Get User's Choice**:
        a.  The user will reply with a table name (e.g., "customer_orders").
        b.  You now have the `table_name` from the user's message.
    7.  **Call Validator**:
        a.  Call the `@SchemaValidatorAgent` to run the validation using the `file_path` and the `table_name` the user just provided.
    8.  **Final Report**:
        a.  Get the JSON report back from the `@SchemaValidatorAgent`.
        b.  Call the `@MarkdownAgent` to format the JSON report.
        c.  Present the final, human-readable Markdown report to the user.
    
    **CRITICAL RULES:**
    - **ASK ONE QUESTION AT A TIME.**
    - When you need to ask the user for input (like typing the table name), end your *entire* message with the single word `TERMINATE`.
    """
)
# AGENT 3: The Profiler (Specialist)
data_profiler_agent = autogen.AssistantAgent(
    name="DataProfilerAgent",
    llm_config=llm_config,
    system_message="""You are a silent specialist. Your only job is to call the `run_data_profiling` tool.
    Report the JSON result back to the `ConductorAgent`."""
)

# AGENT 4: The Validator (Specialist)
schema_validator_agent = autogen.AssistantAgent(
    name="SchemaValidatorAgent",
    llm_config=llm_config,
    system_message="""You are a silent specialist. Your only job is to call the `run_schema_validation` tool.
    Report the JSON result back to the `ConductorAgent`."""
)

# AGENT 5: The Markdown Formatter (Specialist)
markdown_agent = autogen.AssistantAgent(
    name="MarkdownAgent",
    llm_config=llm_config,
    system_message="""You are a silent specialist. Your only job is to call the `convert_json_to_markdown` tool.
    Report the Markdown string result back to the `ConductorAgent`."""
)
# --- 6. TOOL REGISTRATION (MODIFIED) ---

# REMOVED: autogen.register_function(get_available_tables, ...)

# ADDED: Register the new recommender tool
autogen.register_function(
    get_table_recommendations,
    caller=conductor_agent,
    executor=user_proxy,
    name="get_table_recommendations",
    description="Analyzes a file to find and recommend the best matching DB tables."
)

autogen.register_function(
    run_data_profiling,
    caller=data_profiler_agent,
    executor=user_proxy, 
    name="run_data_profiling",
    description="Run the data profiler tool."
)

autogen.register_function(
    run_schema_validation,
    caller=schema_validator_agent,
    executor=user_proxy, 
    name="run_schema_validation",
    description="Run the schema validator tool."
)

autogen.register_function(
    convert_json_to_markdown,
    caller=markdown_agent,
    executor=user_proxy, 
    name="convert_json_to_markdown",
    description="Convert a JSON report to Markdown."
)
# --- 7. GROUP CHAT SETUP ---
agents = [user_proxy, conductor_agent, data_profiler_agent, schema_validator_agent, markdown_agent]
group_chat = autogen.GroupChat(
    agents=agents,
    messages=[],
    max_round=50, 
    speaker_selection_method="auto",
    allow_repeat_speaker=True
)

manager = autogen.GroupChatManager(
    name="Orchestrator",
    groupchat=group_chat,
    llm_config=llm_config,
    # MODIFIED: Updated the system message to reflect the new tool
    system_message="""You are the Orchestrator. Your job is to select the next agent to speak.
    
    **THE FLOW (Follow this precisely):**
    
    1.  **After the `User` (human) speaks:** YOU MUST ALWAYS select the `ConductorAgent`.
    
    2.  **After a `Specialist` (`DataProfilerAgent`, `SchemaValidatorAgent`, `MarkdownAgent`) speaks:** YOU MUST ALWAYS select the `ConductorAgent`.
    
    3.  **After the `User` (as executor) posts a tool result (e.g., "***** Response from calling tool *****"):** YOU MUST ALWAYS select the `ConductorAgent`.
    
    4.  **After the `ConductorAgent` speaks:**
        a) If it called a specialist (e.g., "@DataProfilerAgent"), select that specialist.
        b) If it called its own tool (e.g., `get_table_recommendations`), select the `User` (who is the executor).
        c) If it asked the user a question (ending in `TERMINATE`), select the `User` (who is the human).

    This flow ensures the `ConductorAgent` is the central brain.
    """
)
# --- 8. RUN THE CHAT ---
print("="*50)
print("🚀 STARTING CHAT")
print("Type 'exit' or 'terminate' to end the conversation.")
print("="*50)

# We provide the file path in the first message.
# The Conductor's new brain will handle the rest.
user_proxy.initiate_chat(
    manager,
    message="Hi, I have a file called 'new_orders.csv'. Can you help me with it?"
)