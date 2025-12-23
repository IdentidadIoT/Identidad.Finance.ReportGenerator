import os
import json
from datetime import datetime, timedelta
import argparse
import requests
from core.config import init_config
import core.config as cfg

# ===========================
# CONFIGURACIÓN BASE
# ===========================
config = init_config()


def invoke_replicator(url, start_date=None, end_date=None):
    """
    Invoca el replicator usando un token obtenido dinámicamente.
    """

    # 1️⃣ Obtener token desde el servicio OAUTH
    token_info = get_token()
    token = token_info["access_token"]

    today = datetime.now()

    # 2️⃣ Determinar fechas
    if start_date and end_date:
        try:
            sd = datetime.fromisoformat(start_date)
            ed = datetime.fromisoformat(end_date)
        except:
            sd = datetime.strptime(start_date, "%m-%d-%Y %H:%M:%S")
            ed = datetime.strptime(end_date, "%m-%d-%Y %H:%M:%S")
    else:
        first_day_last_month = today - timedelta(days=1)
        last_day_last_month = today
        sd = datetime(first_day_last_month.year, first_day_last_month.month, first_day_last_month.day, 0, 0, 0)
        ed = datetime(last_day_last_month.year, last_day_last_month.month, last_day_last_month.day, 0, 0, 0)

    # 3️⃣ Payload
    payload = {
        "StartDate": sd.isoformat(),
        "EndDate": ed.isoformat()
    }

    print(f"\nInvoking API: {url}")
    print(f"Payload: {json.dumps(payload)}")

    # 4️⃣ Headers con token real
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=60)

        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}\n")

    except requests.exceptions.RequestException as e:
        print(f"Error invoking replicator: {e}\n")


def get_token():
    """
    Obtiene un token desde un servicio OAuth2 (grant_type=password)
    usando un archivo de configuración JSON.
    """

    url = cfg.get_parameter("NegativeMarginMigrator", "token_url")
    username = cfg.get_parameter("NegativeMarginMigrator", "username")
    password = cfg.get_parameter("NegativeMarginMigrator", "password")

    payload = {
        "grant_type": "password",
        "username": username,
        "password": password
    }

    headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
    }


    try:
        
        response = requests.post(url, headers=headers, data=payload, timeout=60)
        response.raise_for_status()

        json_response = response.json()

        token_data = {
            "access_token": json_response.get("access_token", ""),
            "token_type": json_response.get("token_type", "Bearer"),
            "expires_in": json_response.get("expires_in", 0),
            "refresh_token": json_response.get("refresh_token", "")
        }

        # Validación mínima
        if not token_data["access_token"]:
            raise Exception(f"Invalid token response: {json_response}")

        return token_data

    except Exception as ex:
        raise Exception(f"Error getting token: {ex}")
    
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="Invoke NegativeMargin replicator")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DDTHH:MM:SS)", default=None)
    parser.add_argument("--end-date", help="End date (YYYY-MM-DDTHH:MM:SS)", default=None)
    args = parser.parse_args()

    # Load configuration file
    config_path = os.path.join(os.path.dirname(__file__), "config.json")
    config = {}
    if os.path.exists(config_path):
        print(f"Reading configuration from {config_path}...")
        with open(config_path, "r") as f:
            try:
                config = json.load(f)
            except Exception as e:
                print(f"Warning: failed to read configuration file: {e}")

    negative_margin_url = cfg.get_parameter("NegativeMarginMigrator", "negative_margin_url")
    swaps_client_url = cfg.get_parameter("NegativeMarginMigrator", "swaps_client_url")
    swaps_vendor_url = cfg.get_parameter("NegativeMarginMigrator", "swaps_vendor_url")
    swaps_Generate_ClientTraffic_url = cfg.get_parameter("NegativeMarginMigrator", "swaps_Generate_ClientTraffic")

    print(f"\nStarting replicators...\n {datetime.now()}".format(today=datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    
    print("\nGenerating Total Client Traffic...\n")
    invoke_replicator(swaps_Generate_ClientTraffic_url, start_date=args.start_date, end_date=args.end_date)

    print("\nReplicating Negative Margin...\n")
    invoke_replicator(negative_margin_url, start_date=args.start_date, end_date=args.end_date)

    print("\nReplicating Swaps Client...\n")
    invoke_replicator(swaps_client_url, start_date=args.start_date, end_date=args.end_date)

    print("\nReplicating Swaps Vendor...\n")
    invoke_replicator(swaps_vendor_url, start_date=args.start_date, end_date=args.end_date)




    print("Process finished.\n")

# 0 1 * * * /usr/bin/python3 /opt/pythonapps/Identidad.Finance.ReportGenerator/NegativeMarginMigrator.py >> 
# /opt/pythonapps/Identidad.Finance.ReportGenerator/NegativeMarginMigratorlog.log 2>&1


