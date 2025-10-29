import config
import logging
import pandas as pd
import requests
import datetime
from sqlalchemy import create_engine, text

# Extract

def extract_clima_data():
    logging.info("Iniciando extração de dados!")

    # URL da API climatica
    api_url = config.CLIMA_API

    # Parametros da API
    parametros_api = {
        "latitude": -21.75,
        "longitude": -45.33,
        "daily":"temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone":"America/Sao_Paulo"
    }

    try:
        response = requests.get(api_url,params=parametros_api,timeout=10)

        response.raise_for_status()

        raw_data = response.json()
        logging.info(f"Extração de dados concluida. {len(raw_data['daily']['time'])} dias de previsão recebidos")
        return raw_data

    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao extrair dados da API {e}")
        return None
    


# Transform

def transform_data(raw_data):
    # Verificando erro na extração de dados
    if raw_data is None:
        logging.warning("Nenhum dado foi encontrado para tratamento")
        return None
    
    logging.info("Iniciando transformação de dados")

    # Convertendo dados brutos em DataFrame
    try:
        daily_data = raw_data['daily']
        df = pd.DataFrame(data=daily_data)
    except KeyError as e:
        logging.error("Erro ao transformar dados: chave 'daily' não encontrada.",exc_info=True)
        return None
    
    # Renomeando colunas
    df = df.rename(columns={
        'time':'data_previsao',
        'temperature_2m_min':'temp_min_c',
        'temperature_2m_max':'temp_max_c',
        'precipitation_sum':'precipitacao_mm'
    })

    # Criando coluna com data da carga
    df['data_coleta'] = datetime.date.today()

    # Tratamento de colunas do tipo data 
    colunas_para_converter = ['data_previsao','data_coleta']
    df[colunas_para_converter] = df[colunas_para_converter].apply(pd.to_datetime)

    logging.info("Transformação de dados bem sucedida")

    return df
    

# Load

def load_clima_data(df,engine,table_name):

    if df is None:
        logging.warning("Nenhum dado encontrando para carga")
        return None

    try:
        SQL_DELETE = text(f"DELETE FROM {table_name} WHERE DATE(data_coleta) = '{datetime.date.today()}'")
        with engine.connect() as conn:
            conn.execute(SQL_DELETE)
        logging.info("Limpeza diaria de partição concluída ")
    except Exception as e:
        logging.warning(f"Não foi possivel limpar a partição: {e}")

    try:
        logging.info("Iniciando carga no banco de dados")
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False
        )
        logging.info("Carga de dados concluída com sucesso")
    except Exception as e:
        logging.error(f"Falha ao carregar os dados no banco.",exc_info=True)


# Start Pipeline
def run_pipeline(engine,table_name):
    dados_brutos = extract_clima_data()
    dados_transformados = transform_data(dados_brutos)
    load_clima_data(dados_transformados,engine,table_name)