from flask import Flask, jsonify
import gspread
from google.oauth2.service_account import Credentials
import psycopg2
from psycopg2.extras import execute_values
import os
import datetime
from threading import Thread
from time import sleep
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

app = Flask(__name__)

# Configuração do Google Sheets
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")

# Reconstruindo o JSON das credenciais do Google a partir das variáveis de ambiente
credentials_dict = {
    "type": os.getenv("GOOGLE_TYPE"),
    "project_id": os.getenv("GOOGLE_PROJECT_ID"),
    "private_key_id": os.getenv("GOOGLE_PRIVATE_KEY_ID"),
    "private_key": os.getenv("GOOGLE_PRIVATE_KEY").replace("\\n", "\n"),
    "client_email": os.getenv("GOOGLE_CLIENT_EMAIL"),
    "client_id": os.getenv("GOOGLE_CLIENT_ID"),
    "auth_uri": os.getenv("GOOGLE_AUTH_URI"),
    "token_uri": os.getenv("GOOGLE_TOKEN_URI"),
    "auth_provider_x509_cert_url": os.getenv("GOOGLE_AUTH_PROVIDER_CERT_URL"),
    "client_x509_cert_url": os.getenv("GOOGLE_CLIENT_CERT_URL"),
}

credentials = Credentials.from_service_account_info(credentials_dict)
client = gspread.authorize(credentials)
sheet = client.open_by_key(SPREADSHEET_ID).sheet1

# Conexão ao banco de dados PostgreSQL
db = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME"),
    port=os.getenv("DB_PORT", 5432)
)
cursor = db.cursor()

# Função para obter o último ID sincronizado
def get_last_synced_id():
    rows = sheet.get_all_values()
    if len(rows) > 1:
        last_row = rows[-1]
        try:
            return int(last_row[0])
        except ValueError:
            return 0
    return 0

# Função para sincronizar com o Google Sheets
def sync_db_to_sheet():
    try:
        last_synced_id = get_last_synced_id()
        cursor.execute("SELECT * FROM users WHERE id > %s ORDER BY id ASC", (last_synced_id,))
        rows = cursor.fetchall()

        for row in rows:
            processed_row = [
                col.isoformat() if isinstance(col, datetime.datetime) else
                str(col) if isinstance(col, datetime.timedelta) else
                col
                for col in row
            ]
            sheet.append_row(processed_row)
        print(f"[INFO] {len(rows)} linhas sincronizadas com sucesso.")
        return {"message": "Sincronização concluída", "rows_synced": len(rows)}
    except Exception as e:
        print(f"[ERROR] Falha na sincronização: {e}")
        return {"error": str(e)}

# Loop contínuo para sincronização automática
def auto_sync(interval=300):  # Intervalo padrão: 300 segundos (5 minutos)
    while True:
        print("[INFO] Iniciando sincronização automática...")
        sync_db_to_sheet()
        print(f"[INFO] Próxima sincronização em {interval} segundos.")
        sleep(interval)

# Endpoint para verificar o status da API
@app.route('/status', methods=['GET'])
def status():
    return jsonify({"message": "API está funcionando!", "next_sync": "Automatizada a cada 5 minutos"})

# Inicializa o servidor Flask e a sincronização automática
if __name__ == '__main__':
    # Inicia o loop de sincronização automática em uma thread separada
    thread = Thread(target=auto_sync, args=(300,))  # Define o intervalo de sincronização como 5 minutos
    thread.daemon = True  # Permite encerrar o loop quando o programa for finalizado
    thread.start()

    # Inicia o servidor Flask
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000)))
