import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import time
from io import BytesIO

# ==============================
# CONFIGURAÇÕES
# ==============================
URL_EXCEL = "https://triamanortetratores-my.sharepoint.com/:x:/g/personal/rodrigo_triamanorte_com_br/IQB56sRGwzMMT4nJJU3c9EFhAVho8l6XxGMJ921sHh7Xc54?download=1"

ARQUIVO_CACHE = "cache_cnae.csv"
ARQUIVO_ERROS = "erros_cnae.csv"
MAX_WORKERS = 5

# ==============================
# FUNÇÕES
# ==============================

def baixar_excel():
    print("⬇ Baixando Excel do OneDrive...")
    response = requests.get(URL_EXCEL)

    if response.status_code != 200:
        raise Exception(f"Erro ao baixar Excel: {response.status_code}")

    df = pd.read_excel(BytesIO(response.content), dtype=str)

    print(f"📊 Total de linhas no Excel: {len(df)}")
    print("🔍 Primeiros CNPJs lidos:")
    print(df.head())

    return df


def limpa_cnpj(cnpj):
    cnpj = ''.join(filter(str.isdigit, str(cnpj)))
    return cnpj.zfill(14)


def consulta_cnpj(cnpj):
    url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}"
    try:
        r = requests.get(url, timeout=10)

        if r.status_code == 200:
            data = r.json()

            secundarios = data.get("cnaes_secundarios", [])

            return {
                "CNPJ": cnpj,
                "CNAE_PRINCIPAL": data.get("cnae_fiscal"),
                "DESC_PRINCIPAL": data.get("cnae_fiscal_descricao"),
                "CNAE_SEC_1": secundarios[0]["codigo"] if len(secundarios) > 0 else None,
                "DESC_SEC_1": secundarios[0]["descricao"] if len(secundarios) > 0 else None,
                "CNAE_SEC_2": secundarios[1]["codigo"] if len(secundarios) > 1 else None,
                "DESC_SEC_2": secundarios[1]["descricao"] if len(secundarios) > 1 else None,
                "STATUS": "OK"
            }
        else:
            return {"CNPJ": cnpj, "STATUS": f"HTTP_{r.status_code}"}

    except:
        return {"CNPJ": cnpj, "STATUS": "ERRO"}


# ==============================
# LEITURA DO EXCEL
# ==============================

df = baixar_excel()

df.columns = df.columns.str.strip().str.upper()

if "CNPJ" not in df.columns:
    raise Exception("❌ Coluna 'CNPJ' não encontrada no Excel")

df["CNPJ_LIMPO"] = df["CNPJ"].apply(limpa_cnpj)

# ==============================
# CACHE
# ==============================

if os.path.exists(ARQUIVO_CACHE):
    cache = pd.read_csv(ARQUIVO_CACHE, dtype=str)
else:
    cache = pd.DataFrame(columns=["CNPJ"])

cnpjs_existentes = set(cache["CNPJ"])

novos = df[~df["CNPJ_LIMPO"].isin(cnpjs_existentes)]

total = len(novos)

print(f"🔎 Total de novos CNPJs: {total}")

# ==============================
# PROCESSAMENTO
# ==============================

inicio = time.time()
processados = 0
sucesso = 0
erro = 0

resultados = []

if total > 0:
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(consulta_cnpj, cnpj): cnpj for cnpj in novos["CNPJ_LIMPO"]}

        for future in as_completed(futures):
            resultado = future.result()
            resultados.append(resultado)

            processados += 1

            if resultado.get("STATUS") == "OK":
                sucesso += 1
            else:
                erro += 1

            tempo_decorrido = time.time() - inicio
            tempo_medio = tempo_decorrido / processados
            restante = total - processados
            tempo_restante = restante * tempo_medio

            percentual = (processados / total) * 100

            print(
                f"📊 {percentual:.2f}% | "
                f"{processados}/{total} | "
                f"✔ {sucesso} ❌ {erro} | "
                f"⏱ Restante: {int(tempo_restante)}s"
            )

# ==============================
# SALVAR RESULTADOS
# ==============================

if len(resultados) > 0:
    df_novos = pd.DataFrame(resultados)

    # salvar erros
    df_erros = df_novos[df_novos["STATUS"] != "OK"]
    if not df_erros.empty:
        df_erros.to_csv(ARQUIVO_ERROS, index=False, encoding="utf-8-sig")

    # juntar com cache
    cache_final = pd.concat([cache, df_novos], ignore_index=True)
    cache_final.drop_duplicates(subset=["CNPJ"], inplace=True)

    cache_final.to_csv(ARQUIVO_CACHE, index=False, encoding="utf-8-sig")

    print("✅ Cache atualizado com sucesso!")
else:
    print("⚠ Nenhum CNPJ novo para processar")

print("🏁 Finalizado!")
