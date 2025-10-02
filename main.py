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


global config, logger, interval_time, to_emails, to_emails_filtered_report, semaphore

jobs: Dict[str, dict] = {}
app = FastAPI()
logger = init_log()


def init():
    global config, logger, interval_time, to_emails, to_emails_filtered_report, semaphore

    semaphore = asyncio.Semaphore(5)
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
    jobs[job_id] = {"status": "pending/queued", 
                    "result": None, 
                    "error": None,
                    "request": {
                            "function": func.__name__,
                            "args": args,
                        }
                }

    async def wrapper():
        async with semaphore:
            try:
                jobs[job_id]["status"] = "running"
                result = await asyncio.to_thread(func, *args, **kwargs)

                if isinstance(result, dict) and "status_code" in result:
                    jobs[job_id]["status"] = "done" if result["status_code"] == 200 else "error"
                    jobs[job_id]["result"] = str(result)
                else:
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
    FORTNIGHTLY = 3
    MONTHLY = 4
    BIWEEKLY = 5

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
    Class_: str
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

def calculate_dates_fortnightly() -> BillingCycleDateDto:
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    day = today.day
    if day > 15:
        start = today.replace(day=1)
        end = today.replace(day=16)
    else:
        start = today.replace(day=16, month=today.month - 1)
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

def fetch_AnswerOriginateSms_By_date_carrier(df_carriers, start_date: datetime, end_date: datetime, isAnswer: bool) -> pd.DataFrame:
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
            customer_value = f"{quickbox}_{cur_code.lower()}"
        else:
            customer_value = quickbox

    # InvoiceNumber: si cambia Customer
    if answer_importer_sms_excel_dtos:
        last_customer = answer_importer_sms_excel_dtos[-1].Customer
        if last_customer != customer_value:
            answer_or_dto["InvoiceNumber"] += 1

    period = f"{billing_cycle_date_dto.StartDate:%Y-%m-%d} to {(billing_cycle_date_dto.EndDate - timedelta(days=1)):%Y-%m-%d}"
    creation_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    if carrier is not None:
        terms_days = int(carrier.get("ClientPaymentTerms", 0) or 0)
        terms = f"{terms_days} DAYS"
        due_date = (datetime.now() + timedelta(days=terms_days - 1 if terms_days > 0 else 0)).strftime("%Y-%m-%d")
    else:
        terms = f"carrier no existe {answer_data.Client}"
        due_date = terms

    email_sent = cfg.get_parameter("Answer", "AnswerEmailSend")
    note = cfg.get_parameter("Answer", "AnswerFinancialNote")

    dto = ExcelImporterSmsDto(
        Customer=customer_value,
        InvoiceNumber="Insert Bill Number" if len(answer_importer_sms_excel_dtos) == 0 else f"=IF(A{len(answer_importer_sms_excel_dtos)+2} = A{len(answer_importer_sms_excel_dtos)+1}, B{len(answer_importer_sms_excel_dtos)+1},B{len(answer_importer_sms_excel_dtos)+1}+1)",
        ItemCode=financial["Item"] if financial is not None else answer_data.ClientCountry,
        Destination=financial["Name"] if financial is not None else answer_data.ClientCountry,
        Class_=financial["Class"] if financial is not None else "DefaultFinancialClass",
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
        df_importer = pd.DataFrame([dto.__dict__ for dto in answerImporterExcelDtos])
        
        if gmtCarriers:
            report_name = f"AnswerImporter_forGMT{gmt}_{BillingCycle(billingCycleId).name}_{billingCycleDateDto.StartDate:%Y%m%d}_{billingCycleDateDto.EndDate:%Y%m%d}.xlsx"
        else:
            report_name = f"AnswerImporter_{BillingCycle(billingCycleId).name}_{billingCycleDateDto.StartDate:%Y%m%d}_{billingCycleDateDto.EndDate:%Y%m%d}.xlsx"
        output_path = os.path.join("output", report_name)
        os.makedirs("output", exist_ok=True)
        df_importer.to_excel(output_path, index=False)

        logger.info("Raw Excel file generated at: %s", output_path) 
                
        try: 
            upload_sharepoint(output_path, report_name) 
        except Exception: 
            logger.exception("Error uploading raw answer CSV to sharepoint")

        return output_path

def generate_answer_files(answerSmsDto):
    try:
        carrier_list = fetch_carriers()  # devuelve pd.DataFrame con CarrierDto
        if carrier_list.empty:
            raise Exception("Not carriers found")

        for billing_cycle_id in answerSmsDto["ClientBillingCycleId"]:
            billingCycleDateDto = calculate_query_dates_by_billing_cycle(answerSmsDto, billing_cycle_id)
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
            raw_file = f"RawAnswerSMS_{BillingCycle(billing_cycle_id).name}_{billingCycleDateDto.StartDate:%Y%m%d}_{billingCycleDateDto.EndDate:%Y%m%d}.csv"
            output_path = os.path.join("output", raw_file)
            os.makedirs("output", exist_ok=True)
            pd.DataFrame(grouped).to_csv(output_path, index=False)

            logger.info("Raw Excel file generated at: %s", output_path) 

            try: 
                upload_sharepoint(output_path, raw_file) 
            except Exception: 
                logger.exception("Error uploading raw answer CSV to sharepoint")

            # ListClients (moneda única/múltiple)
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
                    "VendorId": str(keys[0]),
                    "Vendor": keys[1],
                    "VendorProduct": keys[2],
                    "VendorCountry": keys[3],
                    "Network": keys[4],
                    "MccMnc": keys[5],
                    "VendorCurrencyCode": keys[6],
                    "VendorRate": keys[7],
                    "Messages": int(group["QuantityV"].sum()),
                    "VendorAmount": group["VendorAmount"].sum()
                })

            df_output = pd.DataFrame(rows)

            filename = sanitize_filename(
                f"GMT_RawOriginateSMS_{billingCycleId.name}_{gmtDates.StartDate.strftime('%Y%m%d')}_{gmtDates.EndDate.strftime('%Y%m%d')}.csv"
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


@app.get("/")
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
async def get_answer_sms_get_monthly(billingCycleDate: BillingCycleDateDto, billing_cycle: BillingCycle):
    
    answer_dto = {"ClientBillingCycleId": [int(billing_cycle)], 
                  "VendorBillingCycleId": [int(billing_cycle)], 
                  "CarrierBillingCycleId": [int(billing_cycle)],
                  "billingCycleDate": billingCycleDate}
    
    job_id = register_job(get_answer_sms_get_monthly_fun, answer_dto)
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