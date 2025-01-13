import os
import gspread
from google.oauth2.service_account import Credentials
from flask import Flask, jsonify

app = Flask(__name__)

# Reconstruindo o JSON das variáveis de ambiente
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

# Autenticando com o Google Sheets
credentials = Credentials.from_service_account_info(credentials_dict)
client = gspread.authorize(credentials)

# Conectando à planilha
SPREADSHEET_ID = "1gGsG63QDT_7p7V0SLvyRM78U_fE0VqfmWggltiWKSKU"
sheet = client.open_by_key(SPREADSHEET_ID).sheet1


@app.route('/status', methods=['GET'])
def status():
    """
    Endpoint para verificar o status da API.
    """
    return jsonify({"message": "API funcionando corretamente!"})


@app.route('/sync', methods=['POST'])
def sync():
    """
    Endpoint para sincronizar dados do banco de dados com o Google Sheets.
    """
    try:
        # Simula dados para sincronização
        data_to_sync = [
            ["ID", "Nome", "Email"],
            [1, "John Doe", "john.doe@example.com"],
            [2, "Jane Smith", "jane.smith@example.com"]
        ]

        # Adiciona os dados ao Google Sheets
        for row in data_to_sync:
            sheet.append_row(row)

        return jsonify({"message": "Sincronização concluída com sucesso!"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000)))
