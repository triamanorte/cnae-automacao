import requests
import pandas as pd
from io import BytesIO

URL_EXCEL = "https://triamanortetratores-my.sharepoint.com/:x:/g/personal/rodrigo_triamanorte_com_br/IQB56sRGwzMMT4nJJU3c9EFhAVho8l6XxGMJ921sHh7Xc54?download=1"

def baixar_excel():
    print("⬇ Baixando Excel do OneDrive...")

    response = requests.get(URL_EXCEL)

    if response.status_code != 200:
        raise Exception(f"Erro ao baixar Excel: {response.status_code}")

    return pd.read_excel(BytesIO(response.content), dtype=str)

# usar assim:
df = baixar_excel()
