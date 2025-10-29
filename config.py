import os
from dotenv import load_dotenv

load_dotenv(encoding='utf-8')

CLIMA_API = os.environ.get("CLIMA_API")
DB_CONNECTION_STRING = os.environ.get("DB_CONNECTION_STRING")
TABELA_CLIMA = os.environ.get("TABELA_CLIMA")