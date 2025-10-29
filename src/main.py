import logging
import config
from sqlalchemy import create_engine
from .clima_pipeline import run_pipeline



logging.basicConfig(
    level=logging.INFO,  
    format='%(asctime)s - %(levelname)s - %(message)s', 
    datefmt='%Y-%m-%d %H:%M:%S', 
    handlers=[
        logging.FileHandler("pipeline.log"), 
        logging.StreamHandler()            
    ]
)
credencial_bd = config.DB_CONNECTION_STRING
engine = create_engine(credencial_bd)
table_name = config.TABELA_CLIMA 


if __name__ == '__main__':
    logging.info("======================================================")
    logging.info("==  Iniciando o pipeline de ETL de dados climaticos ==")
    logging.info("== Credencial sendo usada ==")
    logging.info("======================================================")

    try:
        run_pipeline(engine,table_name)
    except Exception as e:
        logging.error(f"ERRO CRÍTICO NO PIPELINE: Ocorreu um erro inesperado: {e}")

    logging.info("\n======================================================")
    logging.info("==  Pipeline de ETL concluído!              ==")
    logging.info("== Verifique seu banco de dados.              ==")
    logging.info("======================================================")