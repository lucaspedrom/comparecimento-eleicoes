import boto3
import os
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# Carrega variáveis do arquivo .env
load_dotenv()

PROFILE = os.getenv("AWS_PROFILE")
REGION = os.getenv("AWS_REGION")
PREFIX = os.getenv("PROJECT_BUCKET_PREFIX")

def get_boto_session():
    """Cria uma sessão com o profile configurado."""
    try:
        return boto3.Session(profile_name=PROFILE, region_name=REGION)
    except Exception as e:
        print(f"Erro ao carregar perfil AWS: {e}")
        exit(1)

def create_bucket(s3_client, bucket_name):
    """Cria um bucket S3 se ele não existir."""
    try:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"[OK] Bucket criado/verificado: {bucket_name}")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'BucketAlreadyExists':
            print(f"[ERRO] O nome '{bucket_name}' já está em uso globalmente. Mude o PREFIX no .env.")
        elif error_code == 'BucketAlreadyOwnedByYou':
            print(f"[INFO] Bucket '{bucket_name}' já é seu.")
        else:
            print(f"[ERRO] Falha ao criar {bucket_name}: {e}")

def main():
    session = get_boto_session()
    s3 = session.client('s3')

    # Definindo as camadas do Data Lake
    buckets = [
        f"{PREFIX}-raw",    # Chegada de dados (Landzone)
        f"{PREFIX}-bronze", # Dados convertidos (Parquet)
        f"{PREFIX}-gold"    # Dados agregados (Business)
    ]

    print(f"--- Iniciando Setup de Infraestrutura na AWS ({REGION}) ---")
    for bucket in buckets:
        create_bucket(s3, bucket)
    print("--- Fim do Setup ---")

if __name__ == "__main__":
    main()