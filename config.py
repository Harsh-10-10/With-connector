import os
import logging
from dotenv import load_dotenv
load_dotenv()

AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_KEY")
AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION")

DATABASE_URL = os.getenv("DATABASE_URL")

config_list = [
    {
        "model": AZURE_OPENAI_DEPLOYMENT,
        "api_key": AZURE_OPENAI_KEY,
        "base_url": AZURE_OPENAI_ENDPOINT,
        "api_type": "azure",
        "api_version": AZURE_OPENAI_API_VERSION,
    }
]
if not all([AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_KEY, AZURE_OPENAI_DEPLOYMENT, AZURE_OPENAI_API_VERSION]):
    logging.warning("One or more Azure OpenAI environment variables are not set. Please check your .env file.")

if not DATABASE_URL:
    logging.warning("DATABASE_URL is not set. Please check your .env file.")
else:
    logging.info(f"Database URL loaded: {DATABASE_URL}")