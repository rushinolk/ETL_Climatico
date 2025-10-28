import config
import logging
import pandas as pd
import requests


# Extract

def extract_clima_data():
    logging.info("Iniciando extração de dados")

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