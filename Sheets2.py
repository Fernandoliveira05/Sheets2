from flask import Flask, jsonify
import gspread
from google.oauth2.service_account import Credentials
import os
from dotenv import load_dotenv
from psycopg2.extras import execute_values
import psycopg2
import datetime
from time import sleep
from threading import Thread

# Carregar variáveis de ambiente
load_dotenv()

app = Flask(__name__)

# Conexão ao banco de dados PostgreSQL
try:
    db = psycopg2.connect(
        host=os.getenv("DB_HOST", "dpg-cu2km11u0jms73dai7f0-a.oregon-postgres.render.com"),
        user=os.getenv("DB_USER", "originei_contatos_y7vy_user"),
        password=os.getenv("DB_PASSWORD", "jJ1sDPZlv9ptrp6vwJ0Rk3jGFG2jgmY7"),
        database=os.getenv("DB_NAME", "originei_contatos_y7vy"),
        port=os.getenv("DB_PORT", "5432")
    )
    cursor = db.cursor()
    print("[INFO] Conexão com o banco de dados estabelecida com sucesso.")
except Exception as e:
    print(f"[ERRO] Falha ao conectar ao banco de dados: {e}")
    exit(1)

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

credentials = Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=SCOPES)
client = gspread.authorize(credentials)

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1gGsG63QDT_7p7V0SLvyRM78U_fE0VqfmWggltiWKSKU")
sheet = client.open_by_key(SPREADSHEET_ID).sheet1

# Funções auxiliares
def get_last_synced_id():
    try:
        rows = sheet.get_all_values()
        if len(rows) > 1:
            last_row = rows[-1]
            print(f"[INFO] Última linha na planilha: {last_row}")
            return int(last_row[0])
    except (ValueError, IndexError):
        print("[INFO] Nenhum ID válido encontrado. Retornando 0.")
    return 0

def add_headers():
    try:
        if not sheet.acell('A1').value:
            print("[INFO] Adicionando cabeçalhos à planilha.")
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'users'")
            headers = [col[0] for col in cursor.fetchall()]
            if headers:
                sheet.update('A1', [headers])
                print("[INFO] Cabeçalhos adicionados.")
            else:
                print("[ERRO] Nenhum cabeçalho encontrado.")
    except Exception as e:
        print(f"[ERRO] Falha ao adicionar cabeçalhos: {e}")

def get_unsynced_rows(last_synced_id):
    try:
        query = "SELECT * FROM users WHERE id > %s ORDER BY id ASC"
        cursor.execute(query, (last_synced_id,))
        rows = cursor.fetchall()
        return [
            [
                col.isoformat() if isinstance(col, datetime.datetime) else
                str(col) if isinstance(col, datetime.timedelta) else
                col
                for col in row
            ]
            for row in rows
        ]
    except Exception as e:
        print(f"[ERRO] Falha ao buscar linhas não sincronizadas: {e}")
        return []

def sync_db_to_sheet():
    try:
        add_headers()
        last_synced_id = get_last_synced_id()
        unsynced_rows = get_unsynced_rows(last_synced_id)

        if unsynced_rows:
            for row in unsynced_rows:
                sheet.append_row(row)
                print(f"[INFO] Linha sincronizada: {row}")
            print("[INFO] Sincronização concluída.")
        else:
            print("[INFO] Nenhuma nova linha para sincronizar.")
    except Exception as e:
        print(f"[ERRO] Falha ao sincronizar: {e}")

# Sincronização automática
def auto_sync(interval=5):
    while True:
        print("[INFO] Iniciando sincronização automática...")
        sync_db_to_sheet()
        print("[INFO] Aguardando próximo ciclo...")
        sleep(interval)

# Rotas do Flask
@app.route('/', methods=['GET'])
def home():
    return jsonify({"message": "Bem-vindo! Serviço ativo."})

@app.route('/sync', methods=['POST'])
def manual_sync():
    sync_db_to_sheet()
    return jsonify({"message": "Sincronização manual concluída."})

@app.route('/status', methods=['GET'])
def status():
    return jsonify({"message": "API está funcionando!"})

# Inicialização
if __name__ == '__main__':
    # Inicia sincronização automática em thread separada
    sync_thread = Thread(target=auto_sync, args=(5,))
    sync_thread.daemon = True
    sync_thread.start()

    # Inicia o servidor Flask
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000)))
