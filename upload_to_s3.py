import boto3
import os
from dotenv import load_dotenv

 # Laster hemmeligheter fra den lokale .env-filen
load_dotenv()

 # Leser konfigurasjonen fra miljøet
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_S3_BUCKET_NAME = os.environ.get("AWS_S3_BUCKET_NAME")
AWS_S3_REGION = os.environ.get("AWS_S3_REGION")

FILES_TO_UPLOAD = ['database_biler.parquet', 'metadata.json']

def upload_files():
     if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_S3_BUCKET_NAME, AWS_S3_REGION]):
         print("!!! ADVARSEL !!!")
         print("Kunne ikke finne alle AWS-variablene i .env-filen.")
         print("Sørg for at .env-filen er korrekt utfylt. Avbryter.")
         return

     s3_client = boto3.client(
         's3',
         aws_access_key_id=AWS_ACCESS_KEY_ID,
         aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
         region_name=AWS_S3_REGION
     )
     print(f"Starter opplasting til S3-bøtte: '{AWS_S3_BUCKET_NAME}'...")
     for file_name in FILES_TO_UPLOAD:
         if os.path.exists(file_name):
             try:
                 s3_client.upload_file(file_name, AWS_S3_BUCKET_NAME, file_name)
                 print(f"  ✓ Vellykket opplasting av '{file_name}'.")
             except Exception as e:
                 print(f"  ✗ FEIL under opplasting av '{file_name}': {e}")
         else:
             print(f"  ! ADVARSEL: Fant ikke filen '{file_name}' lokalt. Hopper over.")
     print("\nOpplasting ferdig.")

if __name__ == "__main__":
     upload_files()