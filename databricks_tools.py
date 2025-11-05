import os
import sqlalchemy as sa
import logging
from dotenv import load_dotenv
from typing import Dict, Any, Optional

# --- 1. DATABRICKS ENGINE ---

def get_databricks_engine():
    """
    Creates and returns a SQLAlchemy engine for Databricks.
    Pulls all credentials from your .env file.
    """
    load_dotenv()
    
    hostname = os.getenv("DB_HOST")
    http_path = os.getenv("DB_PATH")
    token = os.getenv("DB_TOKEN")

    if not all([hostname, http_path, token]):
        logging.error("Databricks credentials (DB_HOST, DB_PATH, DB_TOKEN) not found in .env file.")
        return None

    try:
        connection_string = (
            f"databricks://token:{token}@{hostname}?"
            f"http_path={http_path}&"
            f"catalog=workspace&" 
            f"schema=default"
        )
        
        engine = sa.create_engine(connection_string)
        
        # Test the connection
        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
        
        logging.info("SQLAlchemy engine for Databricks (workspace.default) created successfully.")
        return engine
        
    except Exception as e:
        logging.error(f"Error creating Databricks SQLAlchemy engine: {e}")
        return None

# --- 2. (UPGRADED) GET SINGLE TABLE SCHEMA ---

def get_db_schema(engine: sa.engine.Engine, table_name: str) -> Optional[Dict[str, Any]]:
    """
    Fetches the DETAILED schema for a specific table from Databricks.
    
    This is the "robust" version that replaces the old tools.py function.
    It fetches column type, nullable status, and primary key constraints.
    """
    try:
        inspector = sa.inspect(engine)

        if not inspector.has_table(table_name):
            logging.warning(f"Table '{table_name}' does not exist in the database.")
            return None

        columns = inspector.get_columns(table_name)
        pk_constraint = inspector.get_pk_constraint(table_name)
        primary_keys = pk_constraint.get('constrained_columns', [])

        schema_info = {}
        for col in columns:
            schema_info[col['name']] = {
                'type': str(col['type']),
                'nullable': col['nullable'],
                'primary_key': col['name'] in primary_keys
            }

        logging.info(f"Successfully fetched detailed schema for table: {table_name}")
        return schema_info

    except Exception as e:
        logging.error(f"Error fetching DB schema for table '{table_name}': {e}")
        raise

# --- 3. (UPGRADED) GET ALL TABLE SCHEMAS ---

def get_all_table_schemas(engine: sa.engine.Engine) -> Dict[str, Any]:
    """
    Fetches the "lite" schema for ALL tables in the database.
    
    Returns:
        A dict of {table_name: [col1, col2, col3]}
    """
    logging.info("Fetching all table schemas (lite) from Databricks...")
    all_schemas = {}
    try:
        inspector = sa.inspect(engine)
        table_names = inspector.get_table_names()

        if not table_names:
            logging.warning("No tables found in the database.")
            return {}

        for table_name in table_names:
            # Get columns, but only store the names
            columns = inspector.get_columns(table_name)
            all_schemas[table_name] = [col['name'] for col in columns]

        logging.info(f"Successfully fetched column lists for {len(all_schemas)} tables.")
        return all_schemas

    except Exception as e:
        logging.error(f"Error fetching all DB schemas: {e}")
        return {}