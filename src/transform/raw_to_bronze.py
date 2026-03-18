import os
import duckdb
import boto3
from dotenv import load_dotenv

load_dotenv()

PROFILE = os.getenv("AWS_PROFILE")
REGION = os.getenv("AWS_REGION")
PREFIX = os.getenv("PROJECT_BUCKET_PREFIX")
RAW_BUCKET = f"{PREFIX}-raw"
BRONZE_BUCKET = f"{PREFIX}-bronze" 

def process_raw_to_bronze(file_path_suffix):
    raw_path = f"s3://{RAW_BUCKET}/{file_path_suffix}.csv"
    bronze_path = f"s3://{BRONZE_BUCKET}/{file_path_suffix}.parquet"

    print(f"Iniciando conversão:\nOrigem: {raw_path}\nDestino: {bronze_path}")

    # Pegando Credenciais 
    session = boto3.Session(profile_name=PROFILE, region_name=REGION)
    creds = session.get_credentials().get_frozen_credentials()
    
    # Iniciando o DuckDB em Memória
    con = duckdb.connect(':memory:')

    # Carrega ExtensÕes Necessárias para Falar com AWS S3
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute("INSTALL aws;")
    con.execute("LOAD aws;")
    
    # Configurando a sessão do DuckDB
    con.execute(f"SET s3_region='{REGION}';")
    con.execute(f"SET s3_access_key_id='{creds.access_key}';")
    con.execute(f"SET s3_secret_access_key='{creds.secret_key}';")

    # DECISÃO DE ENGENHARIA 1: encoding='latin-1' e delim=';'. O TSE não usa UTF-8 nem vírgula como padrão.
    # DECISÃO DE ENGENHARIA 2: all_varchar=True. Na camada Bronze, não tipamos dados. 
    # Forçamos tudo como texto para evitar que o pipeline quebre por inferência errada de tipos numa coluna de 20 milhões de linhas.
    # DECISÃO DE ENGENHARIA 3: COMPRESSION 'ZSTD'. Padrão moderno, melhor taxa de compressão/velocidade.

    query = f"""
    COPY (SELECT * FROM read_csv_auto('{raw_path}', header=true, delim=';', encoding='latin-1', all_varchar=true)) 
    TO '{bronze_path}' (FORMAT PARQUET, COMPRESSION 'ZSTD')
    """

    print("Executando leitura, conversão e escrita via DuckDB...")
    con.execute(query)
    print("[OK] Arquivo Parquet gerado com sucesso na camada Bronze.")

    con.close()

def main():
    file_suffix = "perfil_comparecimento_abstencao/2022/perfil_comparecimento_abstencao_2022_BRASIL"
    process_raw_to_bronze(file_suffix)

if __name__ == "__main__":
    main()
