import os
import duckdb
import boto3
from dotenv import load_dotenv

load_dotenv()

PROFILE = os.getenv("AWS_PROFILE")
REGION = os.getenv("AWS_REGION")
PREFIX = os.getenv("PROJECT_BUCKET_PREFIX")
BRONZE_BUCKET = f"{PREFIX}-bronze"

def explore_data():
    session = boto3.Session(profile_name=PROFILE, region_name=REGION)
    creds = session.get_credentials().get_frozen_credentials()

    con = duckdb.connect(':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws;")
    con.execute(f"SET s3_region='{REGION}';")
    con.execute(f"SET s3_access_key_id='{creds.access_key}';")
    con.execute(f"SET s3_secret_access_key='{creds.secret_key}';")

    parquet_path = f"s3://{BRONZE_BUCKET}/perfil_comparecimento_abstencao/2022/perfil_comparecimento_abstencao_2022_BRASIL.parquet"

    print("--- SCHEMA DO ARQUIVO (COLUNAS) ---")
    schema = con.execute(f"DESCRIBE SELECT * FROM '{parquet_path}'").df()
    print(schema['column_name'].tolist())

    print("\n--- AMOSTRA DOS DADOS (5 LINHAS) ---")
    sample = con.execute(f"SELECT * FROM '{parquet_path}' LIMIT 5").df()
    import pandas as pd
    pd.set_option('display.max_columns', None)
    print(sample.head())

if __name__ == "__main__":
    explore_data()