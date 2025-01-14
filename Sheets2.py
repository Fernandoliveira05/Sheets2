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

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

app = Flask(__name__)

# Configuração do Google Sheets
SCOPES = os.getenv("GOOGLE_SCOPES", "https://www.googleapis.com/auth/spreadsheets").split(",")

SERVICE_ACCOUNT_INFO = {
    "type": os.getenv("TYPE"),
    "project_id": os.getenv("PROJECT_ID"),
    "private_key_id": os.getenv("PRIVATE_KEY_ID"),
    "private_key": os.getenv("GOOGLE_PRIVATE_KEY", "").replace("\\n", "\n"),
    "client_email": os.getenv("CLIENT_EMAIL"),
    "client_id": os.getenv("CLIENT_ID"),
    "auth_uri": os.getenv("AUTH_URI"),
    "token_uri": os.getenv("TOKEN_URI"),
    "auth_provider_x509_cert_url": os.getenv("AUTH_PROVIDER_X509_CERT_URL"),
    "client_x509_cert_url": os.getenv("CLIENT_X509_CERT_URL"),
}

print(SERVICE_ACCOUNT_INFO)  # Depuração: verifique se todas as chaves foram carregadas corretamente

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")

credentials = Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=SCOPES)
client = gspread.authorize(credentials)
sheet = client.open_by_key(SPREADSHEET_ID).sheet1

# Conexão ao banco de dados PostgreSQL
db = psycopg2.connect(
    host=os.getenv("DB_HOST", "dpg-cu2km11u0jms73dai7f0-a.oregon-postgres.render.com"),
    user=os.getenv("DB_USER", "originei_contatos_y7vy_user"),
    password=os.getenv("DB_PASSWORD", "jJ1sDPZlv9ptrp6vwJ0Rk3jGFG2jgmY7"),
    database=os.getenv("DB_NAME", "originei_contatos_y7vy"),
    port=os.getenv("DB_PORT", "5432")
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
        return {"message": "Sincronização concluída", "rows_synced": len(rows)}
    except Exception as e:
        return {"error": str(e)}

# Endpoint manual de sincronização
@app.route('/sync', methods=['POST'])
def manual_sync():
    result = sync_db_to_sheet()
    return jsonify(result)

# Endpoint para verificar o status da API
@app.route('/status', methods=['GET'])
def status():
    return jsonify({"message": "API está funcionando!"})

# Loop contínuo para sincronização automática
def auto_sync(interval=5):
    while True:
        print("[INFO] Iniciando sincronização automática...")
        result = sync_db_to_sheet()
        print("[INFO] Resultado da sincronização automática:", result)
        sleep(interval)

# Inicializa o servidor Flask e a sincronização automática
if __name__ == '__main__':
    # Inicia o loop de sincronização automática em uma thread separada
    thread = Thread(target=auto_sync, args=(5,))
    thread.daemon = True  # Permite encerrar o loop quando o programa for finalizado
    thread.start()

    # Inicia o servidor Flask
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000)))
