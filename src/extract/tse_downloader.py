import os
import zipfile
import requests
import boto3
from dotenv import load_dotenv

# Carregando as credenciais necessárias
load_dotenv()
PROFILE = os.getenv("AWS_PROFILE")
REGION = os.getenv("AWS_REGION")
PREFIX = os.getenv("PROJECT_BUCKET_PREFIX")
RAW_BUCKET = f"{PREFIX}-raw"

# Função responsável por criar a sessão de conexão ao S3
def get_s3_client():
    session = boto3.Session(profile_name=PROFILE, region_name=REGION)
    return session.client('s3')

def download_and_upload(url, s3_folder):
    """Faz o download do ZIP do TSE, extrai e envia o CSVs para a camada Raw no S3."""
    print(f"Iniciando download: {url}")

    # Download em chunks para o disco.
    local_zip_path = "temp_data.zip"
    with requests.get(url,  stream=True) as r:
        r.raise_for_status()
        with open(local_zip_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    print(f"Download concluído. Iniciando extração e upload para S3.")
    s3_client = get_s3_client()

    # Extração e Upload Multipart via Stream.
    with zipfile.ZipFile(local_zip_path, 'r') as z:
        for file_info in z.infolist():
            if file_info.filename.endswith('BRASIL.csv'):
                print(f"Extraindo e enviando: {file_info.filename}")               
                # Define o caminho no S3 (Ex: eleitorado/2022/arquivo.csv)
                s3_key = f"{s3_folder}/{file_info.filename}"
                print(f"Enviando {file_info.filename} (Tamanho original: ~{file_info.file_size / (1024**2):.2f} MB)...")
                
                # Abrir o arquivo CSV de dentro do zip como um fluxo de leitura
                with z.open(file_info.filename) as file_stream:
                    s3_client.upload_fileobj(file_stream, RAW_BUCKET, s3_key)
                print(f"[OK] {file_info.filename} salvo em s3://{RAW_BUCKET}/{s3_key}")

    #Limpeza do ambiente.
    os.remove(local_zip_path)
    print("Arquivo temporário removido. Pipeline de Extração Finalizado.")

def main():
    # URL de exemplo (Você precisará substituir pela URL real do TSE para 2022)
    url_eleitorado_2022 = "https://cdn.tse.jus.br/estatistica/sead/odsele/perfil_comparecimento_abstencao/perfil_comparecimento_abstencao_2022.zip"
    download_and_upload(url_eleitorado_2022, "perfil_comparecimento_abstencao/2022")

if __name__ == "__main__":
    main()