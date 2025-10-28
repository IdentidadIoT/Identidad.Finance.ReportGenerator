from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dataclasses import dataclass
from typing import List, Dict, Optional
import pandas as pd
from core.db import get_engine
from core.logger import init_log
from core.oracle_connector import create_oracle_connection, output_type_handler
from core.sharepoint import init_sharepoint, upload_sharepoint
from core.email import send_email, init_email
import core.config as cfg
from core.config import init_config
import os
import re
import time
import pytz
import asyncio
import uvicorn
from enum import IntEnum
import uuid
import concurrent.futures


global config, logger, interval_time, to_emails, to_emails_filtered_report, semaphore

app = FastAPI()
logger = init_log()

process_pool = concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count())
jobs = {}
semaphore = asyncio.Semaphore(5)


def init():
    global config, logger, interval_time, to_emails, to_emails_filtered_report

    config = init_config()
    init_email()
    init_sharepoint()
    try:
        interval_time = int(cfg.get_parameter('General', 'interval_time_minutes'))
    except Exception:
        interval_time = 60
    to_emails = cfg.get_parameter('Smtp_Server', "to_emails")
    to_emails_filtered_report = cfg.get_parameter('Smtp_Server', 'to_emails_filtered_report')


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    logger.info(f"{request.method} {request.url.path} - START")
    try:
        response = await call_next(request)
    except Exception as ex:
        try:
            send_email(to_emails, f"Error {request.method}", f"Exception in {request.method} {request.url.path}: {str(ex)}")
        except Exception:
            logger.exception("Failed to send error email")
        logger.exception(f"Exception in {request.method} {request.url.path}: {str(ex)}")
        raise
    duration = time.time() - start_time
    logger.info(f"{request.method} {request.url.path} - END in {duration:.2f}s")
    return response

def register_job(func, *args, **kwargs):
    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "status": "pending/queued",
        "result": None,
        "error": None,
        "request": {"function": func.__name__, "args": args},
    }

    async def wrapper():
        async with semaphore:
            try:
                jobs[job_id]["status"] = "running"

                # Ejecutar la función en un proceso separado (usa otro núcleo)
                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(process_pool, func, *args, **kwargs)

                jobs[job_id]["status"] = "done"
                jobs[job_id]["result"] = result

            except Exception as e:
                jobs[job_id]["status"] = "error"
                jobs[job_id]["error"] = str(e)
                logger.exception(f"Error ejecutando job {job_id} ({func.__name__})")

    loop = asyncio.get_event_loop()
    loop.create_task(wrapper())
    return job_id

def sanitize_filename(filename: str) -> str:
    return re.sub(r'[<>:"/\\|?*]', '_', filename)

def safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


class BillingCycleDateDto(BaseModel):
    StartDate: datetime
    EndDate: datetime

    class Config:
        json_encoders = {
            datetime: lambda v: v.strftime("%Y-%m-%d %H:%M:%S")
        }

class BillingCycle(IntEnum):
    PREPAY = 1
    WEEKLY = 2
    BIWEEKLY = 3
    FORTNIGHTLY = 4
    MONTHLY = 5
    ByDateInterval = 6
    
class CurrencyID(IntEnum):
    USD = 0
    EUR = 1

class FinancialAreaEquivalenceDto(BaseModel):
    Id: int
    Class: Optional[str] = None
    Description: Optional[str] = None
    Item: Optional[str] = None
    Name: Optional[str] = None


@dataclass
class ExcelImporterSmsDto:
    Customer: str
    InvoiceNumber: str
    ItemCode: str
    Destination: str
    Class: str
    Period: str
    CreationDate: str
    Terms: str
    DueDate: str
    EmailSent: str
    Note: str
    Rate: float
    Messages: int
    Amount: float
    
@dataclass
class AnswerOriginateSmsDto:
    ClientId: Optional[int]
    Client: Optional[str]
    ClientProduct: Optional[str]
    ClientCountry: Optional[str]
    ClientNet: Optional[str]
    ClientMccMnc: Optional[str]
    ClientCurrencyCode: Optional[str]
    ClientRate: Optional[float]
    QuantityC: int
    ClientAmount: Optional[float]
    ClientAmountUSD: Optional[float]

@dataclass
class CarrierCurrencyDto:
    ClientId: int
    Result: bool  # True = single currency, False = multi-currency


def set_dates_from_input(start: datetime, end: datetime) -> BillingCycleDateDto:
    return BillingCycleDateDto(StartDate=start, EndDate=end)

def calculate_dates_weekly(_: BillingCycleDateDto) -> BillingCycleDateDto:
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    end = today - timedelta(days=today.weekday())  # Monday
    start = end - timedelta(days=7)
    return BillingCycleDateDto(StartDate=start, EndDate=end)

def calculate_dates_biweekly(_: BillingCycleDateDto) -> BillingCycleDateDto:
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    end = today - timedelta(days=today.weekday())  # Monday
    start = end - timedelta(days=14)
    return BillingCycleDateDto(StartDate=start, EndDate=end)

def calculate_dates_fortnightly() -> BillingCycleDateDto:
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    if today.day > 15:
        # Estamos en la segunda quincena → devolver la PRIMERA del mes actual
        start = today.replace(day=1)
        end = today.replace(day=16)
    else:
        # Estamos en la primera quincena → devolver la SEGUNDA del mes anterior
        last_month = today - relativedelta(months=1)
        start = last_month.replace(day=16)
        end = today.replace(day=1)
    return BillingCycleDateDto(StartDate=start, EndDate=end)

def calculate_dates_monthly() -> BillingCycleDateDto:
    today = (datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - relativedelta(months=1))
    start = today.replace(day=1)
    end = start + relativedelta(months=1)
    return BillingCycleDateDto(StartDate=start, EndDate=end)

def calculate_query_dates_by_billing_cycle(answer_dto: BillingCycleDateDto, billing_cycle_id: int) -> BillingCycleDateDto:
    if billing_cycle_id == BillingCycle.PREPAY:
        return set_dates_from_input(answer_dto.StartDate, answer_dto.EndDate)
    if billing_cycle_id == BillingCycle.WEEKLY:
        return calculate_dates_weekly(answer_dto)
    if billing_cycle_id == BillingCycle.BIWEEKLY:
        return calculate_dates_biweekly(answer_dto)
    if billing_cycle_id == BillingCycle.FORTNIGHTLY:
        return calculate_dates_fortnightly()
    if billing_cycle_id == BillingCycle.MONTHLY:
        return calculate_dates_monthly()
    # default / BIWEEKLY
    return set_dates_from_input(answer_dto.StartDate, answer_dto.EndDate)


def fetch_carriers() -> pd.DataFrame:
    try:
        engine = get_engine()
        query = "SELECT * FROM Carriers"
        df = pd.read_sql(query, engine)
        logger.info("Fetched carriers: %d rows", len(df))
        return df
    except Exception as ex:
        logger.exception("Error fetching Carriers data: %s", str(ex))
        return pd.DataFrame()


def get_originate_reconciliation_by_period_sms(start_date: datetime, end_date: datetime, period: int) -> pd.DataFrame:
    try:
        engine = get_engine()
        query = f'''select * from originateReconciliationSms
                    where StartDateIdentidad >= '{start_date.strftime("%Y-%m-%d %H:%M:%S")}'
                    and EndDateIdentidad <= '{end_date.strftime("%Y-%m-%d %H:%M:%S")}'
                    and ({period} = -1 or BillingCycleId = {period} )'''
        df = pd.read_sql(query, engine)
        logger.info("Fetched carriers: %d rows", len(df))
        return df
    except Exception as ex:
        logger.exception("Error fetching Carriers data: %s", str(ex))
        return pd.DataFrame()



def fetch_financial_area_equivalence() -> pd.DataFrame:
    try:
        engine = get_engine()
        query = """
            SELECT Id, Class, Description, Item, Name
            FROM FinancialAreaEquivalenceSms
        """
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        logger.info("Fetched financial area equivalence: %d rows", len(df))
        return df
    except Exception as ex:
        logger.exception("Error fetching FinancialAreaEquivalence data: %s", str(ex))
        return pd.DataFrame()


def raw_originate_sms_customGmt_fun(originateSmsDto):
    try:
        df_carriers = fetch_carriers()
        billingCycleDate = calculate_query_dates_by_billing_cycle(originateSmsDto["billingCycleDate"], originateSmsDto["ClientBillingCycleId"][0])
        frames = []

        for custom_gmt, group in df_carriers.groupby("CustomGMT"):
            local_offset = datetime.now().astimezone().utcoffset()
            current_offset_hours = int(local_offset.total_seconds() / 3600)

            custom_time_span = current_offset_hours if custom_gmt == 0 else current_offset_hours - custom_gmt

            start_date = billingCycleDate.StartDate + timedelta(hours=custom_time_span)
            end_date = billingCycleDate.EndDate + timedelta(hours=custom_time_span)

            # pasar el sub-dataframe con todos los CarrierId del grupo
            df_result = fetch_AnswerOriginateSms_By_date_carrier(group, start_date, end_date, isAnswer=False)
            frames.append(df_result)

        frames_non_empty = [f for f in frames if not f.empty]
        if frames_non_empty:
            df = pd.concat(frames_non_empty, ignore_index=True)
        else:
            df = pd.DataFrame()

        logger.info("Data fetched, number of rows: %d", len(df))

        if df.empty:
            return {"content":{"message": "data not found for the given date range", "rows": 0}, "status_code":200}

        grouped = df.groupby([
            "VendorId", "Vendor", "VendorProduct", "VendorCountry", "VendorNet",
            "VendorMccMnc", "VendorCurrencyCode", "VendorRate"
        ], dropna=False)

        rows = []
        for keys, group in grouped:
            sum_quantity = group["QuantityV"].sum()
            sum_amount = group["VendorAmount"].sum()
            sum_amount_usd = group["VendorAmountUSD"].sum()
            carrier_info = df_carriers[df_carriers["CarrierId"].astype(str) == str(keys[0])]
            quickbox_name = carrier_info["VendorQuickBoxName"].values[0] if not carrier_info.empty and "VendorQuickBoxName" in carrier_info.columns else None

            rows.append({
                "VendorId": str(keys[0]),
                "Vendor": keys[1],
                "VendorProduct": keys[2],
                "VendorCountry": keys[3],
                "Network": keys[4],
                "MccMnc": keys[5],
                "VendorCurrencyCode": keys[6],
                "VendorRate": keys[7],
                "Messages": int(sum_quantity),
                "VendorAmount": sum_amount,
                "VendorAmountUSD": sum_amount_usd,
                "VendorQuickBoxName": quickbox_name
            })

        df_output = pd.DataFrame(rows)
        filename = sanitize_filename(f"RawOriginateSMS_CustomGMT_{BillingCycle(originateSmsDto['VendorBillingCycleId'][0]).name}_{billingCycleDate.StartDate.strftime('%Y%m%d')}_{billingCycleDate.EndDate.strftime('%Y%m%d')}.CSV")
        output_folder = "output"
        os.makedirs(output_folder, exist_ok=True)
        output_path = os.path.join(output_folder, filename)
        df_output.to_csv(output_path, index=False)
        logger.info("CSV file generated at: %s", output_path)
        try:
            upload_sharepoint(output_path, filename)
        except Exception:
            logger.exception("Error uploading raw originate CSV to sharepoint")
        return {"content":{"message": "Reporte generado exitosamente.", "file_path": output_path, "rows": len(df_output)}, "status_code":200}
    except Exception as ex:
        logger.exception("Error procesando reporte raw originate")
        send_email(to_emails, "Error raw originate", f"Exception: {str(ex)}")
        return {"status_code":500, "content":{"error": f"Error procesando reporte: {str(ex)}"}}

def fetch_AnswerOriginateSms_By_date_carrier(df_carriers, start_date: datetime, end_date: datetime, isAnswer: bool, currency: str = None) -> pd.DataFrame:
    try:
        engine = get_engine()
        tuple_ids = ()
        if isinstance(df_carriers, pd.DataFrame):
            contractor_ids = df_carriers["CarrierId"].astype(str).tolist()
            ids_str = ",".join(f"'{x}'" for x in contractor_ids)
            tuple_ids = f"({ids_str})"
        else:
            contractor_ids = [str(df_carriers.CarrierId)]
            tuple_ids = tuple(contractor_ids)
            tuple_ids = f"('{tuple_ids[0]}')"

        if not contractor_ids:
            logger.warning("No contractor ids found in carriers")
            return pd.DataFrame()
        id_column = "ClientId" if isAnswer else "VendorId"
        # build SQL tuple safely
        
        query = f"""
            SELECT *
            FROM AnswerOriginateSms
            WHERE Date >= '{start_date.strftime("%Y-%m-%d %H:%M:%S")}' AND Date < '{end_date.strftime("%Y-%m-%d %H:%M:%S")}'
            AND {id_column} IN {tuple_ids}
        """
        if currency:
            query += f" AND {'ClientCurrencyCode' if isAnswer else 'VendorCurrencyCode'} = '{currency}'"

        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        logger.info("Fetched Answer/Originate SMS rows: %d", len(df))
        return df
    except Exception as ex:
        logger.exception("Error fetching AnswerOriginateSms data: %s", str(ex))
        return pd.DataFrame()

def set_gmt_scheduled(billingCycleDateDto: BillingCycleDateDto) -> BillingCycleDateDto:
    local_tz = datetime.now().astimezone().tzinfo
    current_offset = local_tz.utcoffset(datetime.now())

    return BillingCycleDateDto(
        StartDate=billingCycleDateDto.StartDate + current_offset,
        EndDate=billingCycleDateDto.EndDate + current_offset
    )

def create_answer_importer_excel_dto(
    answer_or_dto,
    billing_cycle_date_dto: BillingCycleDateDto,
    answer_data: AnswerOriginateSmsDto,
    carrier_list: pd.DataFrame,
    financial_area_equivalence_dto_list: pd.DataFrame,
    answer_importer_sms_excel_dtos: List[ExcelImporterSmsDto],
    list_clients: List[CarrierCurrencyDto]
):

    client_id_str = str(answer_data.ClientId) if answer_data.ClientId else None

    carrier_row = carrier_list[carrier_list["CarrierId"].astype(str) == client_id_str]
    carrier = carrier_row.iloc[0] if not carrier_row.empty else None

    financial_row = financial_area_equivalence_dto_list[
        financial_area_equivalence_dto_list["Name"].str.upper().str.strip() ==
        answer_data.ClientCountry.upper().strip()
    ]
    financial = financial_row.iloc[0] if not financial_row.empty else None

    currency_info = next((c for c in list_clients if c.ClientId == answer_data.ClientId), None)

    if carrier is None:
        customer_value = f"carrier no existe {answer_data.Client}"
    else:
        quickbox = carrier.get("ClientQuickBoxName") or f"Nombre Quickbox no existe {carrier.get('Name')}"
        multi_currency = False if currency_info is None else not currency_info.Result
        if multi_currency:
            cur_code = answer_data.ClientCurrencyCode or ""
            customer_value = f"{quickbox}_{cur_code.upper()}"

    if answer_importer_sms_excel_dtos:
        last_customer = answer_importer_sms_excel_dtos[-1].Customer
        if last_customer != customer_value:
            answer_or_dto["InvoiceNumber"] += 1

    period = f"{billing_cycle_date_dto.StartDate:%m/%d/%Y} to {(billing_cycle_date_dto.EndDate - timedelta(days=1)):%m/%d/%Y}"
    creation_date = (datetime.now() - timedelta(days=1)).strftime("%m/%d/%Y")

    if carrier is not None:
        terms_days = int(carrier.get("ClientPaymentTerms", 0) or 0)
        terms = f"{terms_days} DAYS"
        due_date = (-timedelta(days=1) + billing_cycle_date_dto.EndDate + timedelta(days=terms_days - (1 if terms_days > 0 else 0))).strftime("%m/%d/%Y")
    else:
        terms = f"carrier no existe {answer_data.Client}"
        due_date = terms

    email_sent = cfg.get_parameter("Answer", "AnswerEmailSend")
    note = cfg.get_parameter("Answer", "AnswerFinancialNote")

    dto = ExcelImporterSmsDto(
        Customer=f"{customer_value}_",
        InvoiceNumber="Insert Bill Number" if len(answer_importer_sms_excel_dtos) == 0 else f"=IF(A{len(answer_importer_sms_excel_dtos)+2} = A{len(answer_importer_sms_excel_dtos)+1}, B{len(answer_importer_sms_excel_dtos)+1},B{len(answer_importer_sms_excel_dtos)+1}+1)",
        ItemCode=financial["Item"] if financial is not None else answer_data.ClientCountry,
        Destination=financial["Name"] if financial is not None else answer_data.ClientCountry,
        Class=financial["Class"] if financial is not None else "DefaultFinancialClass",
        Period=period,
        CreationDate=creation_date,
        Terms=terms,
        DueDate=due_date,
        EmailSent=email_sent,
        Note=note,
        Rate=safe_float(answer_data.ClientRate),
        Messages=safe_float(answer_data.QuantityC),
        Amount=safe_float(answer_data.ClientRate) * safe_float(answer_data.QuantityC)
    )

    answer_importer_sms_excel_dtos.append(dto)

def generate_excel_answer_importer_file(billingCycleId: int, answerOrDto, billingCycleDateDto: BillingCycleDateDto,
    carrier_list: pd.DataFrame, financial_area_equivalence_dto_list: pd.DataFrame, data: List[AnswerOriginateSmsDto],
    gmtCarriers: bool, gmt: int, list_clients: List[CarrierCurrencyDto]) -> str:

    if billingCycleId != 6:
        invalid_carriers = carrier_list[
            ~carrier_list["ClientBillingCycleId"].fillna(0).astype(int).isin(answerOrDto["ClientBillingCycleId"])
        ]

        print(invalid_carriers[["CarrierId", "Name", "ClientBillingCycleId"]].head(20))
        
        for carrier_id in invalid_carriers["CarrierId"].astype(str).tolist():
            data = [d for d in data if d.ClientId not in (None, 0, int(carrier_id))]
    else:
        data = [d for d in data if d.ClientId not in (None, 0)]

    AnswerSmsGMTContractors = [int(x.strip()) for x in cfg.get_parameter("Answer", "AnswerSmsGMTContractors").split(",")]
    gmt_carriers = carrier_list[
        (carrier_list["IsGMT"] == True) & (~carrier_list["CarrierId"].astype(int).isin(AnswerSmsGMTContractors))
    ]
    for carrier_id in gmt_carriers["CarrierId"].astype(str).tolist():
        data = [d for d in data if d.ClientId != int(carrier_id)]

    data = sorted(data, key=lambda x: (x.Client, x.ClientCountry,))

    answerImporterExcelDtos: List[ExcelImporterSmsDto] = []
    for d in data:
        create_answer_importer_excel_dto(answerOrDto, billingCycleDateDto, d, carrier_list, 
                                         financial_area_equivalence_dto_list, answerImporterExcelDtos, list_clients)

    if answerImporterExcelDtos:
        headers = [
            "Customer",
            "Invoice Number",
            "Item Code",
            "Destination",
            "Class",
            "Period From/To",
            "Creation date",
            "TERM",
            "Due Date",
            "Email Sent",
            "Note",
            "Rate",
            "Messages"
        ]

        df_importer = pd.DataFrame([dto.__dict__ for dto in answerImporterExcelDtos])
        df_importer.rename(columns={
            "Customer": "Customer",
            "InvoiceNumber": "Invoice Number",
            "ItemCode": "Item Code",
            "Destination": "Destination",
            "Class": "Class",
            "Period": "Period From/To",
            "CreationDate": "Creation date",
            "Terms": "TERM",
            "DueDate": "Due Date",
            "EmailSent": "Email Sent",
            "Note": "Note",
            "Rate": "Rate",
            "Messages": "Messages"
        }, inplace=True)

        df_importer = df_importer[headers]

        if gmtCarriers:
            report_name = f"AnswerSms__ForGMT{gmt}_{BillingCycle(billingCycleId).name}_{billingCycleDateDto.StartDate:%m%d%Y%H%M}_{billingCycleDateDto.EndDate:%m%d%Y%H%M}.xlsx"
        else:
            report_name = f"AnswerSms_{BillingCycle(billingCycleId).name}_{billingCycleDateDto.StartDate:%m%d%Y%H%M}_{billingCycleDateDto.EndDate:%m%d%Y%H%M}.xlsx"
        
        output_path = os.path.join("output", report_name)
        os.makedirs("output", exist_ok=True)
        df_importer.to_excel(output_path, index=False)

        logger.info("Raw Excel file generated at: %s", output_path) 
                
        try: 
            print("----")
            #upload_sharepoint(output_path, report_name) 
        except Exception: 
            logger.exception("Error uploading raw answer CSV to sharepoint")

        return output_path

def generate_answer_files(answerSmsDto):
    try:
        carrier_list = fetch_carriers()  # devuelve pd.DataFrame con CarrierDto
        if carrier_list.empty:
            raise Exception("Not carriers found")

        for billing_cycle_id in answerSmsDto["ClientBillingCycleId"]:
            billingCycleDateDto = calculate_query_dates_by_billing_cycle(answerSmsDto["billingCycleDate"], billing_cycle_id)
            financialAreaEquivalenceDtoList = fetch_financial_area_equivalence()

            data = fetch_AnswerOriginateSms_By_date_carrier(
                carrier_list, billingCycleDateDto.StartDate, billingCycleDateDto.EndDate, isAnswer=True
            )

            grouped = (
                data.groupby(
                    ["ClientId", "Client", "ClientProduct", "ClientCountry",
                     "ClientNet", "ClientMccMnc", "ClientCurrencyCode", "ClientRate"],
                    dropna=False
                ).agg(
                    QuantityC=("QuantityC", "sum"),
                    ClientAmount=("ClientAmount", "sum"),
                    ClientAmountUSD=("ClientAmountUSD", "sum"),
                ).reset_index()
            )
            
            grouped_ = grouped.copy()

            carrier_map = dict(
                zip(carrier_list["CarrierId"].astype(str), carrier_list["ClientQuickBoxName"])
            )

            # Aplicamos la lógica C# con pandas apply
            grouped["Client"] = grouped.apply(
                lambda row: (
                    f"{carrier_map.get(str(row['ClientId']))}_{row['ClientCurrencyCode']}"
                    if str(row["ClientId"]) in carrier_map
                    else row["Client"]
                ),
                axis=1,
            )

            grouped.rename(columns={
                    "ClientNet": "Network",
                    "ClientMccMnc": "MccMnc",
                    "QuantityC": "Messages"
            }, inplace=True)

            columns_order = [
                "Client",
                "ClientProduct",
                "ClientCountry",
                "Network",
                "MccMnc",
                "ClientRate",
                "Messages",
                "ClientAmount",
                "ClientCurrencyCode",
                "ClientAmountUSD"
            ]

            grouped = grouped[columns_order]


            grouped_data = [
                AnswerOriginateSmsDto(
                    ClientId=int(row["ClientId"]) if row["ClientId"] else None,
                    Client=row["Client"],
                    ClientProduct=row["ClientProduct"],
                    ClientCountry=row["ClientCountry"],
                    ClientNet=row["ClientNet"],
                    ClientMccMnc=row["ClientMccMnc"],
                    ClientCurrencyCode=row["ClientCurrencyCode"],
                    ClientRate=row["ClientRate"],
                    QuantityC=row["QuantityC"],
                    ClientAmount=row["ClientAmount"],
                    ClientAmountUSD=row["ClientAmountUSD"],
                )
                for _, row in grouped_.iterrows()
                if row["QuantityC"] > 0
            ]

            if not grouped_data:
                raise Exception("There is no data for the selected dates")

            # Generar CSV Raw
            raw_file = f"RawAnswerSMS_{BillingCycle(billing_cycle_id).name}_{billingCycleDateDto.StartDate:%Y%m%d}_{billingCycleDateDto.EndDate:%Y%m%d}.csv"
            output_path = os.path.join("output", raw_file)
            os.makedirs("output", exist_ok=True)
            pd.DataFrame(grouped).to_csv(output_path, index=False)

            logger.info("Raw Excel file generated at: %s", output_path) 

            try: 
                upload_sharepoint(output_path, raw_file) 
            except Exception: 
                logger.exception("Error uploading raw answer CSV to sharepoint")

            list_clients = (
                pd.DataFrame(grouped_)
                .groupby("ClientId")["ClientCurrencyCode"]
                .nunique()
                .reset_index()
            )
            list_clients["Result"] = list_clients["ClientCurrencyCode"] == 1
            list_clients = [
                CarrierCurrencyDto(ClientId=int(row.ClientId), Result=row.Result)
                for _, row in list_clients.iterrows()
            ]

            # Generar archivo Importer
            generate_excel_answer_importer_file(
                billing_cycle_id, answerSmsDto, billingCycleDateDto,
                carrier_list, financialAreaEquivalenceDtoList,
                grouped_data, False, 0, list_clients
            )

    except Exception as ex:
        logger.exception("Error in generate_answer_files")
        return {"status_code":500, "content":{"error": str(ex)}}

def generate_answer_files_gmt_carriers(answerSmsDto):
    try:
        carrier_list = fetch_carriers() 
        frames_gmt = {}
        frames = []
        if carrier_list.empty:
            raise Exception("Not carriers found")

        for billing_cycle_id in answerSmsDto["ClientBillingCycleId"]:
            billingCycleDateDto = calculate_query_dates_by_billing_cycle(answerSmsDto, billing_cycle_id)
            financialAreaEquivalenceDtoList = fetch_financial_area_equivalence()

            for custom_gmt, group in carrier_list.groupby("CustomGMT"):
                
                local_offset = datetime.now(pytz.timezone('America/New_York'))
                current_offset_hours = (int(local_offset.strftime('%z'))/100)

                custom_time_span = current_offset_hours if custom_gmt == 0 else current_offset_hours - custom_gmt

                start_date = billingCycleDateDto.StartDate + timedelta(hours=custom_time_span)
                end_date = billingCycleDateDto.EndDate + timedelta(hours=custom_time_span)

                data = fetch_AnswerOriginateSms_By_date_carrier(
                    group, start_date, end_date, isAnswer=True
                )
                #frames.append(data)
                frames_gmt[custom_gmt] = group

                if not data.empty:
                    grouped = (
                        data.groupby(
                            ["ClientId", "Client", "ClientProduct", "ClientCountry",
                            "ClientNet", "ClientMccMnc", "ClientCurrencyCode", "ClientRate"],
                            dropna=False
                        ).agg(
                            QuantityC=("QuantityC", "sum"),
                            ClientAmount=("ClientAmount", "sum"),
                            ClientAmountUSD=("ClientAmountUSD", "sum"),
                        ).reset_index()
                    )

                    # Convertir a DTOs
                    grouped_data = [
                        AnswerOriginateSmsDto(
                            ClientId=int(row["ClientId"]) if row["ClientId"] else None,
                            Client=row["Client"],
                            ClientProduct=row["ClientProduct"],
                            ClientCountry=row["ClientCountry"],
                            ClientNet=row["ClientNet"],
                            ClientMccMnc=row["ClientMccMnc"],
                            ClientCurrencyCode=row["ClientCurrencyCode"],
                            ClientRate=row["ClientRate"],
                            QuantityC=row["QuantityC"],
                            ClientAmount=row["ClientAmount"],
                            ClientAmountUSD=row["ClientAmountUSD"],
                        )
                        for _, row in grouped.iterrows()
                        if row["QuantityC"] > 0
                    ]

                    if not grouped_data:
                        raise Exception("There is no data for the selected dates")

                    # Generar CSV Raw
                    raw_file = f"RawAnswerSMS_forGMT{custom_gmt}_{BillingCycle(billing_cycle_id).name}_{billingCycleDateDto.StartDate:%Y%m%d}_{billingCycleDateDto.EndDate:%Y%m%d}.csv"
                    output_path = os.path.join("output", raw_file)
                    os.makedirs("output", exist_ok=True)
                    pd.DataFrame(grouped).to_csv(output_path, index=False)

                    logger.info("Raw Excel file generated at: %s", output_path) 

                    try: 
                        upload_sharepoint(output_path, raw_file) 
                    except Exception: 
                        logger.exception("Error uploading raw answer CSV to sharepoint")

                    list_clients = (
                        pd.DataFrame(grouped)
                        .groupby("ClientId")["ClientCurrencyCode"]
                        .nunique()
                        .reset_index()
                    )
                    list_clients["Result"] = list_clients["ClientCurrencyCode"] == 1
                    list_clients = [
                        CarrierCurrencyDto(ClientId=int(row.ClientId), Result=row.Result)
                        for _, row in list_clients.iterrows()
                    ]

                    # Generar archivo Importer
                    generate_excel_answer_importer_file(
                        billing_cycle_id, answerSmsDto, billingCycleDateDto,
                        group, financialAreaEquivalenceDtoList,
                        grouped_data, True, custom_gmt, list_clients
                    )
                else:
                    msg = f"No data for billing cycle {BillingCycle(billing_cycle_id).name} GMT{custom_gmt} _{start_date}_{end_date}"
                    logger.warning(msg)
                    send_email(to_emails, "Error GMT Answer", msg)

    except Exception as ex:
        logger.exception("Error in generate_answer_files")

def get_answer_sms_get_monthly_fun(answerSmsDto):
    id_carrier = ""
    message = ""

    try:
        gmt_carriers = set(cfg.get_parameter("Answer", "AnswerSmsMonthlyEdrsContractors").split(",")) \
                        .intersection(set(cfg.get_parameter("Answer", "AnswerSmsGMTContractors").split(",")))

        carrier_list = fetch_carriers()
        monthly_carriers = carrier_list[
            carrier_list["CarrierId"].astype(str).isin(cfg.get_parameter("Answer", "AnswerSmsMonthlyEdrsContractors").split(","))
        ]

        if monthly_carriers.empty:
            message = "No carriers found for Answer Sms Monthly EDR special carriers."
            logger.warning(message)
            send_email(to_emails, message, message)
            return

        for billing_cycle_id in answerSmsDto["ClientBillingCycleId"]:
            answer_sms_excel_dtos = []
            data = []

            billingCycleDateDto = calculate_query_dates_by_billing_cycle(answerSmsDto, billing_cycle_id)

            for _, carrier in monthly_carriers.iterrows():
                customBillingCycleDateDto = BillingCycleDateDto(
                    StartDate=billingCycleDateDto.StartDate,
                    EndDate=billingCycleDateDto.EndDate
                )

                if str(carrier["CarrierId"]) in gmt_carriers:
                    local_offset = datetime.now().astimezone().utcoffset()
                    current_offset_hours = int(local_offset.total_seconds() / 3600)
                    custom_time_span = current_offset_hours if carrier["CustomGMT"] == 0 else current_offset_hours - carrier["CustomGMT"]

                    customBillingCycleDateDto = BillingCycleDateDto(
                        StartDate=billingCycleDateDto.StartDate + timedelta(hours=custom_time_span),
                        EndDate=billingCycleDateDto.EndDate + timedelta(hours=custom_time_span),
                    )

                df = fetch_AnswerOriginateSms_By_date_carrier(
                    carrier, 
                    customBillingCycleDateDto.StartDate, 
                    customBillingCycleDateDto.EndDate, 
                    isAnswer=True
                )

                if df.empty:
                    continue

                grouped = (
                    df.groupby(
                        ["ClientId", "Client", "ClientProduct", "ClientCountry",
                         "ClientNet", "ClientMccMnc", "ClientCurrencyCode", "ClientRate"],
                        dropna=False
                    ).agg(
                        QuantityC=("QuantityC", "sum"),
                        ClientAmount=("ClientAmount", "sum"),
                    ).reset_index()
                )

                for _, row in grouped.iterrows():
                    dto = AnswerOriginateSmsDto(
                        ClientId=row["ClientId"],
                        Client=row["Client"],
                        ClientProduct=row["ClientProduct"],
                        ClientCountry=row["ClientCountry"],
                        ClientNet=row["ClientNet"],
                        ClientMccMnc=row["ClientMccMnc"],
                        ClientCurrencyCode=row["ClientCurrencyCode"],
                        ClientRate=row["ClientRate"],
                        QuantityC=row["QuantityC"],
                        ClientAmount=row["ClientAmount"],
                        ClientAmountUSD=None, 
                    )
                    answer_sms_excel_dtos.append(dto)

            for carrier_id in cfg.get_parameter("Answer", "AnswerSmsMonthlyEdrsContractors").split(","):
                id_carrier = carrier_id
                carrier_row = monthly_carriers[monthly_carriers["CarrierId"].astype(str) == carrier_id]
                carrier_name = carrier_row["Name"].values[0] if not carrier_row.empty else "UnknownCarrier"

                data_by_carrier = [d for d in answer_sms_excel_dtos if str(d.ClientId) == carrier_id]

                if data_by_carrier:
                    df_export = pd.DataFrame([d.__dict__ for d in data_by_carrier])
                    file_name = f"Monthly_AnswerSms_EDR_{carrier_name}_{(billingCycleDateDto.StartDate):%m%d%Y}-{(billingCycleDateDto.EndDate):%m%d%Y}.csv"
                    output_path = os.path.join("output", file_name)
                    os.makedirs("output", exist_ok=True)

                    try:
                        df_export.to_csv(output_path, index=False)
                        upload_sharepoint(output_path, file_name)
                    except Exception as ex:
                        message = f"There was an error while uploading the CSV file for {carrier_name}, Error: {str(ex)}"
                        logger.error(message)
                        send_email(to_emails, "Error uploading Answer SMS Monthly EDR", message)
                else:
                    message = f"There was no EDR data for the Answer Sms Monthly carrier: {carrier_name}."
                    logger.warning(message)
                    send_email(to_emails, "No Data for Answer SMS Monthly EDR", message)

    except Exception as ex:
        message = f"There was an unexpected error while exporting Answer Sms Monthly EDR for carrier id: {id_carrier}. Error: {str(ex)}"
        logger.error(message)
        send_email(to_emails, "Error Answer SMS Monthly EDR", message)

def raw_originate_sms_gmt_fun(originateSmsDto):
    try:

        df_carriers = fetch_carriers()

        if originateSmsDto["VendorBillingCycleId"][0] != 6:
            carriers = df_carriers[
                (df_carriers["VendorBillingCycleId"] == originateSmsDto["VendorBillingCycleId"][0]) &
                (df_carriers["IsGMT"] == True) &
                (df_carriers["IsEnabled"] == True)
            ]
        else:
            carriers = df_carriers[
                (df_carriers["VendorBillingCycleId"] == originateSmsDto.CarrierBillingPeriod) &
                (df_carriers["IsGMT"] == True) &
                (df_carriers["IsEnabled"] == True)
            ]

        if carriers.empty:
            msg = "No carriers found for GMT Originate report"
            logger.warning(msg)
            send_email(to_emails, "Error GMT Originate", msg)
            return {"content":{"message": msg}, "status_code":200}

        total_rows = 0
        generated_files = []

        for billingCycleId in originateSmsDto["VendorBillingCycleId"]:
            billingCycleDate = calculate_query_dates_by_billing_cycle(originateSmsDto['billingCycleDate'], billingCycleId)
            gmtDates = set_gmt_scheduled(billingCycleDate)

            df = fetch_AnswerOriginateSms_By_date_carrier(
                carriers, gmtDates.StartDate, gmtDates.EndDate, isAnswer=False
            )

            if df.empty:
                msg = f"No data for billing cycle {BillingCycle(billingCycleId).name} GMT _{gmtDates.StartDate}_{gmtDates.EndDate}"
                logger.warning(msg)
                send_email(to_emails, "Error GMT Originate", msg)
                continue

            grouped = df.groupby([
                "VendorId", "Vendor", "VendorProduct", "VendorCountry", "VendorNet",
                "VendorMccMnc", "VendorCurrencyCode", "VendorRate"
            ], dropna=False)

            rows = []
            for keys, group in grouped:
                rows.append({
                    "Vendor": keys[1],
                    "VendorProduct": keys[2],
                    "VendorCountry": keys[3],
                    "Network": keys[4],
                    "MccMnc": keys[5],
                    "VendorRate": keys[7],
                    "VendorCurrencyCode": keys[6],
                    "Messages": int(group["QuantityV"].sum()),
                    "VendorAmount": group["VendorAmount"].sum()
                })

            df_output = pd.DataFrame(rows)

            filename = sanitize_filename(
                f"GMT_RawOriginateSMS_{BillingCycle(billingCycleId).name}_{gmtDates.StartDate.strftime('%Y%m%d')}_{gmtDates.EndDate.strftime('%Y%m%d')}.csv"
            )
            output_folder = "output"
            os.makedirs(output_folder, exist_ok=True)
            output_path = os.path.join(output_folder, filename)
            df_output.to_csv(output_path, index=False)

            logger.info("CSV file generated at: %s", output_path)
            total_rows += len(df_output)
            generated_files.append(output_path)

            try:
                upload_sharepoint(output_path, filename)
            except Exception:
                logger.exception("Error uploading GMT originate Excel to SharePoint")

        if total_rows == 0:
            msg = "No data found for any of the requested billing cycles"
            logger.warning(msg)
            send_email(to_emails, "Error GMT Originate", msg)
            return {"content":{"message": msg, "rows": 0}, "status_code":200}

        return {
            "content":{
                "message": "GMT Originate report(s) generated successfully",
                "files": generated_files,
                "rows": total_rows
            },
            "status_code":200
        }

    except Exception as ex:
        logger.exception("Error generating GMT Originate SMS report")
        send_email(to_emails, "Error GMT Originate", f"Exception: {str(ex)}")
        return {"status_code":500, "message":{"error": f"Error generating report: {str(ex)}"}}

def raw_originate_sms_fun(originateSmsDto):
    try:
        df_carriers = fetch_carriers()
        billingCycleDate = calculate_query_dates_by_billing_cycle(originateSmsDto['billingCycleDate'], originateSmsDto['CarrierBillingCycleId'][0])
        df = fetch_AnswerOriginateSms_By_date_carrier(df_carriers, billingCycleDate.StartDate, billingCycleDate.EndDate, isAnswer=False)
        logger.info("Data fetched, number of rows: %d", len(df))
        if df.empty:
            return JSONResponse(content={"message": "data not found for the given date range", "rows": 0}, status_code=200)

        grouped = df.groupby([
            "VendorId", "Vendor", "VendorProduct", "VendorCountry", "VendorNet",
            "VendorMccMnc", "VendorCurrencyCode", "VendorRate"
        ], dropna=False)

        rows = []
        for keys, group in grouped:
            sum_quantity = group["QuantityV"].sum()
            sum_amount = group["VendorAmount"].sum()
            sum_amount_usd = group["VendorAmountUSD"].sum()

            carrier_info = df_carriers[df_carriers["CarrierId"].astype(str) == str(keys[0])]
            quickbox_name = carrier_info["VendorQuickBoxName"].values[0] if not carrier_info.empty and "VendorQuickBoxName" in carrier_info.columns else None

            rows.append({
                "VendorId": str(keys[0]),
                "Vendor": keys[1],
                "VendorProduct": keys[2],
                "VendorCountry": keys[3],
                "Network": keys[4],
                "MccMnc": keys[5],
                "VendorCurrencyCode": keys[6],
                "VendorRate": keys[7],
                "Messages": int(sum_quantity),
                "VendorAmount": sum_amount,
                "VendorAmountUSD": sum_amount_usd,
                "VendorQuickBoxName": quickbox_name
            })

        df_output = pd.DataFrame(rows)
        filename = sanitize_filename(f"RawOriginateSMS_{BillingCycle(originateSmsDto['CarrierBillingCycleId'][0]).name}_{billingCycleDate.StartDate.strftime('%Y%m%d')}_{billingCycleDate.EndDate.strftime('%Y%m%d')}.CSV")
        output_folder = "output"
        os.makedirs(output_folder, exist_ok=True)
        output_path = os.path.join(output_folder, filename)
        df_output.to_csv(output_path, index=False)
        logger.info("CSV file generated at: %s", output_path)
        try:
            upload_sharepoint(output_path, filename)
        except Exception:
            logger.exception("Error uploading raw originate CSV to sharepoint")
        return JSONResponse(content={"message": "Reporte generado exitosamente.", "file_path": output_path, "rows": len(df_output)}, status_code=200)
    except Exception as ex:
        logger.exception("Error procesando reporte raw originate")
        send_email(to_emails, "Error raw originate", f"Exception: {str(ex)}")
        return JSONResponse(status_code=500, content={"error": f"Error procesando reporte: {str(ex)}"})


@app.get("/api")
async def email_test():
    try:
        send_email(to_emails, "test email to_emails", "test email to_emails")
        send_email(to_emails_filtered_report, "test email to_emails_filtered_report", "test email to_emails_filtered_report")
    except Exception:
        logger.exception("Error sending test emails")
    return {"message": "emails sends succesfull"}

@app.get("/api/sms/jobs")
async def get_jobs():
    return {"Status:": "pending/queued → created but not started yet.  running → currently in execution.  done → finished successfully.  error → finished with an exception.",
            "List of jobs": jobs            
            }


@app.post("/api/sms/RawOriginateSms")
async def raw_originate_sms(billingCycleDate: BillingCycleDateDto, billing_cycle: BillingCycle):
    originateSmsDto = {"ClientBillingCycleId": [int(billing_cycle)], 
                        "VendorBillingCycleId": [int(billing_cycle)], 
                        "CarrierBillingCycleId": [int(billing_cycle)],
                        "billingCycleDate": billingCycleDate}
    
    job_id = register_job(raw_originate_sms_fun, originateSmsDto)
    return JSONResponse(content={"message": "The request was created successfully.", "job_id": job_id})

@app.post("/api/sms/RawOriginateSms/gmt")
async def raw_originate_sms_gmt(billingCycleDate: BillingCycleDateDto, billing_cycle: BillingCycle):
    originateSmsDto = {"ClientBillingCycleId": [int(billing_cycle)], 
                        "VendorBillingCycleId": [int(billing_cycle)], 
                        "CarrierBillingCycleId": [int(billing_cycle)],
                        "billingCycleDate": billingCycleDate}
    
    job_id = register_job(raw_originate_sms_gmt_fun, originateSmsDto)
    return JSONResponse(content={"message": "The request was created successfully.", "job_id": job_id})

@app.post("/api/sms/RawOriginateSms/CustomGmt")
async def raw_originate_sms_customGmt(billingCycleDate: BillingCycleDateDto, billing_cycle: BillingCycle):
    originateSmsDto = {"ClientBillingCycleId": [int(billing_cycle)], 
                  "VendorBillingCycleId": [int(billing_cycle)], 
                  "CarrierBillingCycleId": [int(billing_cycle)],
                  "billingCycleDate": billingCycleDate}

    job_id = register_job(raw_originate_sms_customGmt_fun, originateSmsDto)
    return JSONResponse(content={"message": "The request was created successfully.", "job_id": job_id})


@app.post("/api/sms/RawAnswerSms")
async def get_answer_sms(billingCycleDate: BillingCycleDateDto, billing_cycle: BillingCycle, InvoiceNumber: int):
    
    answer_dto = {"ClientBillingCycleId": [int(billing_cycle)], 
                  "InvoiceNumber": InvoiceNumber, 
                  "VendorBillingCycleId": [int(billing_cycle)], 
                  "CarrierBillingCycleId": [int(billing_cycle)],
                  "billingCycleDate": billingCycleDate}

    job_id = register_job(generate_answer_files, answer_dto)
    return JSONResponse(content={"message": "The request was created successfully.", "job_id": job_id})

@app.post("/api/sms/RawAnswerSm/GMTCarriers")
async def get_answer_sms_gmt_carriers(billingCycleDate: BillingCycleDateDto, billing_cycle: BillingCycle, InvoiceNumber: int):
    
    answer_dto = {"ClientBillingCycleId": [int(billing_cycle)], 
                  "InvoiceNumber": InvoiceNumber, 
                  "VendorBillingCycleId": [int(billing_cycle)], 
                  "CarrierBillingCycleId": [int(billing_cycle)],
                  "billingCycleDate": billingCycleDate}

    job_id = register_job(generate_answer_files_gmt_carriers, answer_dto)
    return JSONResponse(content={"message": "The request was created successfully.", "job_id": job_id})

@app.post("/api/sms/RawAnswerSm/MonthlyEdrs")
async def get_answer_sms_monthly(billingCycleDate: BillingCycleDateDto, billing_cycle: BillingCycle):
    
    answer_dto = {"ClientBillingCycleId": [int(billing_cycle)], 
                  "VendorBillingCycleId": [int(billing_cycle)], 
                  "CarrierBillingCycleId": [int(billing_cycle)],
                  "billingCycleDate": billingCycleDate}
    
    job_id = register_job(get_answer_sms_get_monthly_fun, answer_dto)
    return JSONResponse(content={"message": "The request was created successfully.", "job_id": job_id})

def get_alaris_active_carrier():
    try:
        oracle_connection = create_oracle_connection()
        with oracle_connection as conn:
            cursor = conn.cursor()

            query = """select      
            bc.CAR_ID, bc.CAR_NAME, ba.ACC_CURRENCY_CODE
            from        invoice.BAS_CARRIER bc
            inner join  invoice.bas_account ba
            ON ba.ACC_CAR_ID = bc.CAR_ID
            where bc.CAR_IS_ACTIVE = 1 order by bc.CAR_ID asc"""

            cursor.execute(query)

            cursor.rowfactory = lambda *args: dict(
                zip([d[0] for d in cursor.description], args)
            )
            data = cursor.fetchall()
        return data
    except Exception as ex:
        logger.warning("Failed try to get carriers from Alaris: %s", str(ex))
        return pd.DataFrame(columns=["CAR_ID"])

def cross_data(data_answer_sum, data_orig_sum, carrier_list, billing_cycle_id, 
               billing_cycle_dates, is_gmt, currency, list_originates_reconciliation=None):

    # Convertir listas de dict a DataFrames si no lo son
    df_answer = pd.DataFrame(data_answer_sum)
    df_orig = pd.DataFrame(data_orig_sum)
    df_carriers = pd.DataFrame(carrier_list)

    cross_weekly, cross_fornightly, cross_monthly = [], [], []

    # --- Aplicar QuickBoxName a cada registro de cross_* ---
    def apply_quickbox_name(cross_list):
        for rec in cross_list:
            carrier_id = rec.get("CarrierId")
            match = df_carriers[df_carriers["CarrierId"] == carrier_id]
            if not match.empty:
                rec["CarrierName"] = match.iloc[0]["QuickBoxName"]
        return cross_list

    # --- Unir Answer con Originate por CarrierId ---
    merged = pd.merge(
        df_answer, df_orig, on="CarrierId", how="outer", suffixes=("_Answer", "_Originate")
    ).fillna(0)

    # --- Combinar precios de ambas fuentes ---
    for _, row in merged.iterrows():
        carrier_id = row["CarrierId"]
        carrier_name = row.get("Client", "") or row.get("Vendor", "")
        client_price = row.get("TotalPrice_Answer", 0)
        vendor_price = row.get("TotalPrice_Originate", 0)

        carrier = df_carriers[df_carriers["CarrierId"] == carrier_id]
        
        if carrier.empty:
            continue

        cycle = int(carrier["ClientBillingCycleId"].iloc[0]) if not pd.isna(carrier["ClientBillingCycleId"].iloc[0]) else 0
        record = {
            "CarrierId": carrier_id,
            "CarrierName": carrier_name,
            "ClientPrice": client_price,
            "VendorPrice": vendor_price,
        }

        if cycle == 2:
            cross_weekly.append(record)
        elif cycle == 4:
            cross_fornightly.append(record)
        elif cycle == 5:
            cross_monthly.append(record)

    for _, row in df_carriers.iterrows():
        cid, name, cb, vb = row["CarrierId"], row.get("Name", ""), row.get("ClientBillingCycleId"), row.get("VendorBillingCycleId")
        empty = {"CarrierId": cid, "CarrierName": name, "ClientPrice": 0, "VendorPrice": 0}
        if cb == 2 or vb == 2:
            if cid not in [x["CarrierId"] for x in cross_weekly]:
                cross_weekly.append(empty)
        if cb == 4 or vb == 4:
            if cid not in [x["CarrierId"] for x in cross_fornightly]:
                cross_fornightly.append(empty)
        if cb == 5 or vb == 5:
            if cid not in [x["CarrierId"] for x in cross_monthly]:
                cross_monthly.append(empty)

    def group_report(data, head_first=""):
        df = pd.DataFrame(data)

        if df.empty:
            return df

        # 🔹 Asegurar que todas las columnas relevantes existan
        for col in ["CarrierName", "ClientPrice", "VendorPrice"]:
            if col not in df.columns:
                df[col] = 0

        # 🔹 Convertir tipos para evitar errores de comparación y suma
        df["CarrierName"] = df["CarrierName"].astype(str).fillna("")
        df["ClientPrice"] = pd.to_numeric(df["ClientPrice"], errors="coerce").fillna(0)
        df["VendorPrice"] = pd.to_numeric(df["VendorPrice"], errors="coerce").fillna(0)

        # 🔹 Agrupar y ordenar de forma segura (ignorando mayúsculas/minúsculas)
        df = (
            df.groupby(["CarrierName"], as_index=False)
            .agg(ClientPrice=("ClientPrice", "sum"), VendorPrice=("VendorPrice", "sum"))
            .sort_values("CarrierName", key=lambda s: s.str.lower())
        )

        return df

    # cross_weekly = apply_quickbox_name(cross_weekly)
    # cross_fornightly = apply_quickbox_name(cross_fornightly)
    # cross_monthly = apply_quickbox_name(cross_monthly)

    cross_weekly = group_report(cross_weekly, head_first="Weekly")
    cross_fornightly = group_report(cross_fornightly, head_first="Fortnightly")
    cross_monthly = group_report(cross_monthly, head_first="Monthly")

    # --- Integrar conciliación (si aplica) ---
    if list_originates_reconciliation is not None:
        # Convertir a DataFrame si viene como lista
        if isinstance(list_originates_reconciliation, list):
            df_recon = pd.DataFrame(list_originates_reconciliation)
        elif isinstance(list_originates_reconciliation, pd.DataFrame):
            df_recon = list_originates_reconciliation.copy()
        else:
            df_recon = pd.DataFrame()

        if not df_recon.empty:
            for df_cross in [cross_weekly, cross_fornightly, cross_monthly]:
                for idx, row in df_cross.iterrows():
                    name = row["CarrierName"]
                    matches = df_recon[df_recon["Vendor"] == name]
                    if not matches.empty:
                        cost_usd = pd.to_numeric(matches["CostUSD"], errors="coerce").sum()
                        if cost_usd != 0:
                            df_cross.loc[idx, "VendorPrice"] = cost_usd

    
    # --- Calcular diferencia ---
    def calculate_difference(df):
        if df.empty:
            return df
        df["Difference"] = df["ClientPrice"] - df["VendorPrice"]
        return df

    cross_weekly = calculate_difference(cross_weekly)
    cross_fornightly = calculate_difference(cross_fornightly)
    cross_monthly = calculate_difference(cross_monthly)

    def generate_data(client_list, vendor_list, cross_data):
        for crossed in cross_data:
            carrier_name = crossed.get("CarrierName", "")
            diff = crossed.get("Difference", 0) or 0
            if diff < 0:
                vendor_list.append({"Carrier": carrier_name, "Difference": abs(diff)})
                client_list.append({"Carrier": carrier_name, "Difference": 0})
            elif diff > 0:
                client_list.append({"Carrier": carrier_name, "Difference": abs(diff)})
                vendor_list.append({"Carrier": carrier_name, "Difference": 0})
            else:
                empty = {"Carrier": carrier_name, "Difference": 0}
                client_list.append(empty)
                vendor_list.append(empty)

    # --- Generar listas finales ---
    client_final_weekly, vendor_final_weekly = [], []
    generate_data(client_final_weekly, vendor_final_weekly, cross_weekly.to_dict(orient="records"))

    client_final_fornightly, vendor_final_fornightly = [], []
    generate_data(client_final_fornightly, vendor_final_fornightly, cross_fornightly.to_dict(orient="records"))

    client_final_monthly, vendor_final_monthly = [], []
    generate_data(client_final_monthly, vendor_final_monthly, cross_monthly.to_dict(orient="records"))

    # --- Crear archivo Excel final ---
    filename = (
        f"{'GMT_' if is_gmt else ''}Provisionales_sms_{currency}_{billing_cycle_dates.StartDate:%Y%m%d}_{billing_cycle_dates.EndDate:%Y%m%d}.xlsx"
    )
    date_cicle=f"_{billing_cycle_dates.StartDate:%Y%m%d}_{billing_cycle_dates.EndDate:%Y%m%d}"
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, filename)

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        cross_weekly.to_excel(writer, sheet_name=f"Weekly"[:31], index=False)
        pd.DataFrame(client_final_weekly).to_excel(writer, sheet_name=f"ClientWeekly{date_cicle}"[:31], index=False)
        pd.DataFrame(vendor_final_weekly).to_excel(writer, sheet_name=f"VendorWeekly{date_cicle}"[:31], index=False)
        cross_fornightly.to_excel(writer, sheet_name=f"Fornightly"[:31], index=False)
        pd.DataFrame(client_final_fornightly).to_excel(writer, sheet_name=f"ClientFornightly{date_cicle}"[:31], index=False)
        pd.DataFrame(vendor_final_fornightly).to_excel(writer, sheet_name=f"VendorFornightly{date_cicle}"[:31], index=False)
        cross_monthly.to_excel(writer, sheet_name=f"Monthly"[:31], index=False)
        pd.DataFrame(client_final_monthly).to_excel(writer, sheet_name=f"ClientMonthly{date_cicle}"[:31], index=False)
        pd.DataFrame(vendor_final_monthly).to_excel(writer, sheet_name=f"VendorMonthly{date_cicle}"[:31], index=False)

    logger.info(filename, " Excel generated at: %s", output_path)
    try:
        upload_sharepoint(output_path, filename)
    except Exception as ex:
        logger.warning("SharePoint upload failed: %s", str(ex))

    return output_path

def get_provisionals_sms_fun(model: dict, is_gmt: bool):
    message = ""

    try:
        df_carriers = fetch_carriers()
        if df_carriers.empty:
            raise Exception("Not Found Carriers in Apollo")

        for billing_cycle_id in model["ClientBillingCycleId"]:
            if not is_gmt:
                carrier_list = df_carriers[
                    df_carriers["Currency"].str.contains(CurrencyID(model["currency_ID"]).name, na=False)]
            else:
                if billing_cycle_id != 6:
                    carrier_list = df_carriers[
                        (df_carriers["ClientBillingCycleId"] == billing_cycle_id)
                        & (df_carriers["IsGMT"] == True)
                    ]
                else:
                    carrier_list = df_carriers[
                        (df_carriers["ClientBillingCycleId"] == model["CarrierBillingPeriod"])
                        & (df_carriers["IsGMT"] == True)
                    ]


            if carrier_list.empty:
                logger.warning(f"No carriers found for billingCycleId {billing_cycle_id}")
                continue

            # --- Calcular fechas ---
            billing_cycle_dates = calculate_query_dates_by_billing_cycle(model["billingCycleDate"], billing_cycle_id)

            # --- Obtener Answer y Originate ---
            df_answer = fetch_AnswerOriginateSms_By_date_carrier(
                carrier_list, billing_cycle_dates.StartDate, billing_cycle_dates.EndDate, isAnswer=True
            )
            df_orig = fetch_AnswerOriginateSms_By_date_carrier(
                carrier_list, billing_cycle_dates.StartDate, billing_cycle_dates.EndDate, isAnswer=False
            )

            if df_answer.empty and df_orig.empty:
                logger.warning("No data found for the selected period")
                continue

            currency_code = CurrencyID(model["currency_ID"]).name 

            if not df_answer.empty: 
                df_answer = df_answer[ (df_answer["QuantityC"] > 0) 
                                      & (df_answer["ClientCurrencyCode"].fillna("") == currency_code) ] 
                df_answer["CarrierId"] = df_answer["ClientId"].astype(str) 
                df_answer["Client"] = df_answer["Client"].fillna("")
                df_answer_sum = ( df_answer.groupby(["CarrierId", "Client"], 
                                                    dropna=False).agg(TotalMessages=("QuantityC", "sum"), 
                                                                      TotalPrice=("ClientAmount", "sum")) .reset_index() ) 
            else: 
                df_answer_sum = pd.DataFrame(columns=["CarrierId", "Client", "TotalMessages", "TotalPrice"])
 
            if not df_orig.empty:
                df_orig = df_orig[
                    (df_orig["QuantityV"] > 0)
                    & (df_orig["VendorCurrencyCode"].fillna("") == currency_code)
                ]
                df_orig["CarrierId"] = df_orig["VendorId"].astype(str)
                df_orig["Vendor"] = df_orig["Vendor"].fillna("")
                df_orig_sum = (
                    df_orig.groupby(["CarrierId", "Vendor"], dropna=False)
                    .agg(TotalMessages=("QuantityV", "sum"), TotalPrice=("VendorAmount", "sum"))
                    .reset_index()
                )
            else:
                df_orig_sum = pd.DataFrame(columns=["CarrierId", "Vendor", "TotalMessages", "TotalPrice"])

            
            output_path = cross_data(
                df_answer_sum.to_dict(orient="records"),
                df_orig_sum.to_dict(orient="records"),
                carrier_list.to_dict(orient="records"),
                billing_cycle_id,
                billing_cycle_dates,
                is_gmt,
                currency_code,
            )

            logger.info("CrossReport generated and saved at: %s", output_path)

        return {"content": {"message": "CrossReport completed successfully"}, "status_code": 200}

    except Exception as ex:
        message = f"Error generating CrossReport: {str(ex)}"
        logger.exception(message)
        send_email(to_emails, "Error CrossReport", message)
        return {"status_code": 500, "content": {"error": message}}
    
@app.post("/api/sms/Provisionals")
async def get_provisionals_sms(billingCycleDate: BillingCycleDateDto, billing_cycle: BillingCycle, currency_ID: CurrencyID):
    provisionals_dto = {"ClientBillingCycleId": [int(billing_cycle)], 
                  "VendorBillingCycleId": [int(billing_cycle)], 
                  "CarrierBillingCycleId": [int(billing_cycle)],
                  "billingCycleDate": billingCycleDate,
                  "currency_ID": currency_ID}
    
    job_id = register_job(get_provisionals_sms_fun, provisionals_dto, False)
    return JSONResponse(content={"message": "The request was created successfully.", "job_id": job_id})


def get_provisionals_sms_GMT_fun(model: dict, is_gmt: bool):
    try:
        message = ""

        # Calcular fechas del ciclo de facturación
        billing_cycle_dates = calculate_query_dates_by_billing_cycle(
            model["billingCycleDate"],
            model["ClientBillingCycleId"][0]
        )

        for billing_cycle_id in model["ClientBillingCycleId"]:
            data_answer_sum = []
            data_originate_sum = []

            # Obtener carriers activos de Alaris
            alaris_active = get_alaris_active_carrier()
            if isinstance(alaris_active, pd.DataFrame):
                active_ids = alaris_active["CAR_ID"].astype(str).tolist()
            else:
                active_ids = [str(x.get("CAR_ID")) for x in alaris_active] if alaris_active else []
            if not active_ids:
                raise Exception("Not Found Active Carriers in Alaris")

            df_carriers = fetch_carriers()
            if isinstance(df_carriers, pd.DataFrame):
                df_carriers_ap_ac = df_carriers["CarrierId"].astype(str).tolist()
            else:
                df_carriers_ap_ac = [str(x.get("CarrierId")) for x in df_carriers] if df_carriers else []
            if not df_carriers_ap_ac:
                raise Exception("Not Found Active Carriers in Alaris")

            currency = CurrencyID(model["currency_ID"]).name

            if "DIDs" not in currency:
                carrier_list_no_gmt = df_carriers[
                    (~df_carriers["IsGMT"]) &
                    (df_carriers["Currency"].fillna("").str.contains(currency.replace("DIDs", ""), case=False)) &
                    (~df_carriers["Name"].str.lower().str.endswith("did"))
                ]
                carrier_list_gmt = df_carriers[
                    (df_carriers["IsGMT"]) &
                    (df_carriers["Currency"].fillna("").str.contains(currency.replace("DIDs", ""), case=False)) &
                    (~df_carriers["Name"].str.lower().str.endswith("did"))
                ]
            else:
                carrier_list_no_gmt = df_carriers[
                    (~df_carriers["IsGMT"]) &
                    (df_carriers["Currency"].fillna("").str.contains(currency.replace("DIDs", ""), case=False)) &
                    (df_carriers["Name"].str.lower().str.endswith("did"))
                ]
                carrier_list_gmt = df_carriers[
                    (df_carriers["IsGMT"]) &
                    (df_carriers["Currency"].fillna("").str.contains(currency.replace("DIDs", ""), case=False)) &
                    (df_carriers["Name"].str.lower().str.endswith("did"))
                ]

            carrier_list = pd.concat([carrier_list_no_gmt, carrier_list_gmt])
            if carrier_list.empty:
                raise Exception("Not Found Carriers in Apollo (no match for filters)")

            def apply_quickbox_name(cross_list, df_carriers):
                # Crear un diccionario CarrierId -> QuickBoxName
                carrier_map = df_carriers.set_index("CarrierId")["VendorQuickBoxName"].to_dict()

                for rec in cross_list:
                    carrier_id = rec.get("CarrierId")
                    # Si existe QuickBoxName para ese carrier, úsalo
                    if carrier_id in carrier_map:
                        rec["Client"] = carrier_map[carrier_id] if carrier_map[carrier_id] != "" else rec["Client"]
                    else:
                        # Valor por defecto si no hay coincidencia
                        rec["Client"] = rec.get("Client", rec["Client"])
                return cross_list

            def process_answer():
                df_answer_no_gmt = fetch_AnswerOriginateSms_By_date_carrier(
                    carrier_list_no_gmt, billing_cycle_dates.StartDate, billing_cycle_dates.EndDate, isAnswer=True, currency=currency)
                
                all_answer = [df_answer_no_gmt]

                for gmt_value, carr in carrier_list_gmt.groupby("CustomGMT"):
                    miami_tz = pytz.timezone("America/New_York")
                    tz_offset = miami_tz.utcoffset(datetime.now()).total_seconds() / 3600

                    # Calcular la diferencia de horas según el GMT del carrier
                    custom_span = tz_offset if gmt_value == 0 else tz_offset - gmt_value

                    start = billing_cycle_dates.StartDate + timedelta(hours=custom_span)
                    end = billing_cycle_dates.EndDate + timedelta(hours=custom_span)

                    df_custom = fetch_AnswerOriginateSms_By_date_carrier(carr, start, end, isAnswer=True, currency=currency)
                    all_answer.append(df_custom)

                df_answer = pd.concat(all_answer, ignore_index=True) 
                if df_answer.empty:
                    return pd.DataFrame()

                df_answer = df_answer[
                    (df_answer["ClientId"].astype(str).isin(active_ids)) | ((df_answer["QuantityC"] > 0) )]

                df_answer = df_answer[
                    (df_answer["ClientCurrencyCode"] == currency) &
                    (df_answer["ClientCurrencyCode"].notna())
                ]

                df_answer["CarrierId"] = df_answer["ClientId"].astype(str)
                df_answer["Client"] = df_answer["Client"].fillna("")

                df_answer = pd.DataFrame(apply_quickbox_name(df_answer.to_dict(orient="records"), carrier_list))

                df_answer_sum = (
                    df_answer.groupby(["CarrierId", "Client"], dropna=False)
                    .agg(TotalMessages=("QuantityC", "sum"), TotalPrice=("ClientAmount", "sum"))
                    .reset_index()
                )
                return df_answer_sum

            def process_originate():
                all_orig = []

                for gmt_value, carr in carrier_list.groupby("CustomGMT"):

                    miami_tz = pytz.timezone("America/New_York")
                    tz_offset = miami_tz.utcoffset(datetime.now()).total_seconds() / 3600
                    custom_span = tz_offset if gmt_value == 0 else tz_offset - gmt_value

                    
                    start = billing_cycle_dates.StartDate + timedelta(hours=custom_span)
                    end = billing_cycle_dates.EndDate + timedelta(hours=custom_span)
                    
                    df_custom = fetch_AnswerOriginateSms_By_date_carrier(carr, start, end, isAnswer=False, currency=currency)
                    all_orig.append(df_custom)

                df_orig = pd.concat(all_orig, ignore_index=True) 
                if df_orig.empty:
                    return pd.DataFrame()

                df_orig = df_orig[
                    (df_orig["VendorId"].astype(str).isin(active_ids)) | ((df_orig["QuantityV"] > 0) )]

                df_orig = df_orig[
                    (df_orig["VendorCurrencyCode"] == currency) &
                    (df_orig["VendorCurrencyCode"].notna())
                ]
                df_orig["CarrierId"] = df_orig["VendorId"].astype(str)
                df_orig["Vendor"] = df_orig["Vendor"].fillna("")

                df_orig = pd.DataFrame(apply_quickbox_name(df_orig.to_dict(orient="records"), carrier_list))

                df_orig_sum = (
                    df_orig.groupby(["CarrierId", "Vendor"], dropna=False)
                    .agg(TotalMessages=("QuantityV", "sum"), TotalPrice=("VendorAmount", "sum"))
                    .reset_index()
                )
                return df_orig_sum

            data_answer_sum = process_answer()
            data_originate_sum = process_originate()

            if data_answer_sum.empty and data_originate_sum.empty:
                logger.warning("No data found for the given cycle")
                continue

            data_ans_carrier = data_answer_sum["CarrierId"].astype(str).tolist()
            data_orig_carrier = data_originate_sum["CarrierId"].astype(str).tolist()

            filtered_carriers = [
                w for w in carrier_list.to_dict(orient="records")
                if (
                    ((currency and currency in str(w.get("Currency", ""))) or not currency)
                    # Validar que esté activo o en alguna lista
                    and (
                        w.get("CarrierId") in active_ids
                        or w.get("CarrierId") in data_ans_carrier
                        or w.get("CarrierId") in data_orig_carrier
                    )
                )
            ]

            null_carriers_answer = []
            null_carrier_originate = []

            # carrier_list puede ser DataFrame o lista de dicts — normalizamos a DataFrame para la validación
            if not isinstance(carrier_list, pd.DataFrame):
                try:
                    carrier_list_df = pd.DataFrame(carrier_list)
                except Exception:
                    carrier_list_df = pd.DataFrame()
            else:
                carrier_list_df = carrier_list.copy()

            # Solo validar si existen las columnas esperadas
            if not carrier_list_df.empty:
                if "ClientBillingCycleId" in carrier_list_df.columns:
                    null_carriers_answer = carrier_list_df[
                        carrier_list_df["ClientBillingCycleId"].isna() &
                        carrier_list_df["CarrierId"].astype(str).isin(data_ans_carrier)
                    ]["CarrierId"].astype(str).tolist()
                if "VendorBillingCycleId" in carrier_list_df.columns:
                    null_carrier_originate = carrier_list_df[
                        carrier_list_df["VendorBillingCycleId"].isna() &
                        carrier_list_df["CarrierId"].astype(str).isin(data_orig_carrier)
                    ]["CarrierId"].astype(str).tolist()

            # Si hay carriers sin ciclo, levantar excepción (mismo mensaje que el C#)
            if null_carriers_answer or null_carrier_originate:
                message = (
                    f"Couldn't find billing cycles for the next contractor id's for answer: "
                    f"{','.join(null_carriers_answer)}, "
                    f"and the next contractor id's for originate: {','.join(null_carrier_originate)}."
                )
                raise Exception(message)

            period = -1
            listOriginatesReconciliation = get_originate_reconciliation_by_period_sms(start_date=billing_cycle_dates.StartDate, 
                                                                                      end_date=billing_cycle_dates.EndDate, period=period)

            if not isinstance(listOriginatesReconciliation, pd.DataFrame):
                listOriginatesReconciliation = pd.DataFrame(listOriginatesReconciliation)

            filtered_reconciliation = listOriginatesReconciliation[
                listOriginatesReconciliation["VendorCurrencyCode"] == currency
        ]

            # Ejecutar conciliación cruzada
            output_path = cross_data(
                data_answer_sum.to_dict(orient="records"),
                data_originate_sum.to_dict(orient="records"),
                filtered_carriers,
                billing_cycle_id,
                billing_cycle_dates,
                is_gmt,
                currency,
                filtered_reconciliation
            )

            logger.info(f"CrossReport Excel generated at: {output_path}")

        logger.info("---- CrossReport_Reconciliation completed ----")
        return {"content": {"message": "CrossReport Reconciliation completed successfully"}, "status_code": 200}

    except Exception as ex:
        message = f"Error generating CrossReport_Reconciliation: {str(ex)}"
        logger.exception(message)
        send_email(to_emails, "Error CrossReport_Reconciliation", message)
        return {"status_code": 500, "content": {"error": message}}

@app.post("/api/sms/Provisionals/GMT")
async def get_provisionals_sms_GMT(billingCycleDate: BillingCycleDateDto, billing_cycle: BillingCycle, currency_ID: CurrencyID):
    provisionals_dto = {"ClientBillingCycleId": [int(billing_cycle)], 
                  "VendorBillingCycleId": [int(billing_cycle)], 
                  "CarrierBillingCycleId": [int(billing_cycle)],
                  "billingCycleDate": billingCycleDate,
                  "currency_ID": currency_ID}
    
    job_id = register_job(get_provisionals_sms_GMT_fun, provisionals_dto, True)
    return JSONResponse(content={"message": "The request was created successfully.", "job_id": job_id})


init()

if __name__ == "__main__":

    logger.debug('-----------------Init Application------------------------')

    uvicorn.run(
         "main:app",
         host="0.0.0.0",
         port=int(cfg.get_parameter("General", "port"))
    )

# Para ejecutar: uvicorn main:app --port 8001 --reload

'''
calcula el biling cycle date
recorre el billing cycle id
determina el periodo con base a las fechas, si es mes fortnight o week
trae la lista de carriers de alaris
valida que el currency sea diferente de DIDs, si lo es valida si GMT es false/true con lo que divide los que tienen GMT desde apollo
crea una lista de carriers con ambos resultados -> carrierList
genera la consulta de answer/originate por carrier y ajusta las fechas si es carrier GMT
valida que la lista answer/originate no este vacia y que los carrierID existan en alaris o que la cantidad sea mayor a 0
!!!!!!! recorre la la lista de answer/originate para obtener el VendorQuickBoxName de la lista carrierList y lo asigna al campo Client del answer/originate
genera la agrupacion por cliente/venedor y carrierid sumando cantidad y precio
'''