import requests
import datetime

# ===========================
# CONFIGURACIÓN BASE
# ===========================
BASE_URL = "http://172.16.111.67:8001/api/"
HEADERS = {"Content-Type": "application/json"}
TIMEOUT = 120  # segundos

# ===========================
# FUNCIONES AUXILIARES
# ===========================
def iso_now():
    return datetime.datetime.now().isoformat()

def post_endpoint(endpoint, payload, desc=""):
    url = f"{BASE_URL}{endpoint}"
    try:
        response = requests.post(url, json=payload, headers=HEADERS, timeout=TIMEOUT)
        if 200 <= response.status_code < 300:
            print(f"{desc} [{endpoint}] -> OK")
        else:
            print(f"{desc} [{endpoint}] -> Error {response.status_code}: {response.text}")
    except Exception as e:
        print(f"{desc} [{endpoint}] -> Excepción: {e}")

def date_range(start_day, end_day):
    now = datetime.datetime.now()
    start = now.replace(day=start_day, hour=0, minute=0, second=0, microsecond=0)
    end = now.replace(day=end_day, hour=23, minute=59, second=59, microsecond=0)
    return start.isoformat(), end.isoformat()

def get_month_range():
    now = datetime.datetime.now()
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    next_month = (start + datetime.timedelta(days=32)).replace(day=1)
    end = next_month - datetime.timedelta(seconds=1)
    return start.isoformat(), end.isoformat()

def get_fortnight_range():
    now = datetime.datetime.now()
    if now.day < 16:
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end = start.replace(day=16)
        return start.isoformat(), end.isoformat()
    else:
        start = now.replace(day=16, hour=0, minute=0, second=0, microsecond=0)
        next_month = (start + datetime.timedelta(days=32)).replace(day=1)
        end = next_month - datetime.timedelta(seconds=1)
        return start.isoformat(), end.isoformat()

def get_week_range():
    today = datetime.datetime.now()
    monday = today - datetime.timedelta(days=today.weekday())
    sunday = monday + datetime.timedelta(days=7)
    return monday.isoformat(), sunday.isoformat()

# ========== # 
# ENDPOINTS  #
# ========== #
def post_raw_answer_sms(start, end, billing_cycle, invoice_number):
    url = f"sms/RawAnswerSms?billing_cycle={billing_cycle}&InvoiceNumber={invoice_number}"
    data = {"StartDate": start, "EndDate": end}
    post_endpoint(url, data, "RawAnswerSms")

def post_monthly_edrs(start, end, billing_cycle):
    url = f"sms/RawAnswerSm/MonthlyEdrs?billing_cycle={billing_cycle}"
    data = {"StartDate": start, "EndDate": end}
    post_endpoint(url, data, "RawAnswerSm/MonthlyEdrs")

def post_answer_sms_gmt_carriers(start, end, billing_cycle, invoice_number):
    url = f"sms/RawAnswerSm/GMTCarriers?billing_cycle={billing_cycle}&InvoiceNumber={invoice_number}"
    data = {"StartDate": start, "EndDate": end}
    post_endpoint(url, data, "RawAnswerSm/GMTCarriers")

def post_originate_sms(start, end, billing_cycle):
    url = f"sms/RawOriginateSms?billing_cycle={billing_cycle}"
    data = {"StartDate": start, "EndDate": end}
    post_endpoint(url, data, "RawOriginateSms")

def post_originate_sms_gmt(start, end, billing_cycle):
    url = f"sms/RawOriginateSms/gmt?billing_cycle={billing_cycle}"
    data = {"StartDate": start, "EndDate": end}
    post_endpoint(url, data, "RawOriginateSms/gmt")

def post_originate_sms_custom_gmt(start, end, billing_cycle):
    url = f"sms/RawAnswerSm/MonthlyEdrs?billing_cycle={billing_cycle}"
    data = {"StartDate": start, "EndDate": end}
    post_endpoint(url, data, "RawOriginateSmsCustomGmt (alias de MonthlyEdrs)")

def post_provisionals_sms(start, end, billing_cycle, currency_id):
    url = f"sms/Provisionals?billing_cycle={billing_cycle}&currency_ID={currency_id}"
    data = {"StartDate": start, "EndDate": end}
    post_endpoint(url, data, f"Provisionals (currency {currency_id})")

def post_provisionals_gmt_sms(start, end, billing_cycle, currency_id):
    url = f"sms/Provisionals/GMT?billing_cycle={billing_cycle}&currency_ID={currency_id}"
    data = {"StartDate": start, "EndDate": end}
    post_endpoint(url, data, f"Provisionals/GMT (currency {currency_id})")

# ===============================
# LÓGICA DE EJECUCIÓN AUTOMÁTICA
# ===============================
def main():
    today = datetime.datetime.now()
    weekday = today.weekday()
    day = today.day

    # --------------------------
    # LUNES → SEMANALES
    # --------------------------
    if weekday == 2:
        start, end = get_week_range()
        billing_cycle = "2"
        invoice_number = "0"

        print("Ejecutando ciclo semanal...")

        # Ejecuta endpoints semanales
        post_originate_sms_gmt(start, end, billing_cycle)
        post_originate_sms(start, end, billing_cycle)
        post_raw_answer_sms(start, end, billing_cycle, invoice_number)
        post_answer_sms_gmt_carriers(start, end, billing_cycle, invoice_number)
        post_originate_sms_custom_gmt(start, end, billing_cycle)

        for currency_id in ["0", "1"]:  # USD, EUR
            post_provisionals_sms(start, end, billing_cycle, currency_id)
            post_provisionals_gmt_sms(start, end, billing_cycle, currency_id)

    # --------------------------
    # 1° DEL MES → MENSUALES
    # --------------------------
    elif day == 3:
        start, end = get_month_range()
        billing_cycle = "5"
        invoice_number = "0"

        print("Ejecutando ciclo mensual...")

        post_originate_sms_gmt(start, end, billing_cycle)
        post_originate_sms(start, end, billing_cycle)
        post_monthly_edrs(start, end, billing_cycle)
        post_originate_sms_custom_gmt(start, end, billing_cycle)
        post_raw_answer_sms(start, end, billing_cycle, invoice_number)
        post_answer_sms_gmt_carriers(start, end, billing_cycle, invoice_number)

        for currency_id in ["0", "1"]:
            post_provisionals_sms(start, end, billing_cycle, currency_id)
            post_provisionals_gmt_sms(start, end, billing_cycle, currency_id)
        
        day = 16  # Para ejecutar también el ciclo quincenal

    # --------------------------
    # 16 → QUINCENALES
    # --------------------------
    elif day == 18:
        start, end = get_fortnight_range()
        billing_cycle = "4"
        invoice_number = "0"

        print("Ejecutando ciclo quincenal...")

        post_answer_sms_gmt_carriers(start, end, billing_cycle, invoice_number)
        post_raw_answer_sms(start, end, billing_cycle, invoice_number)
        post_originate_sms_gmt(start, end, billing_cycle)

    else:
        print("No hay endpoints configurados para ejecutarse hoy.")


if __name__ == "__main__":
    main()

print("Proceso finalizado.")