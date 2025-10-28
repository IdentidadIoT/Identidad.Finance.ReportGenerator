import os
import time
import requests
from msal import ConfidentialClientApplication

import core.config as cfg
from core.logger import init_log
from core.email import send_email, init_email

__all__ = ['init_sharepoint', 'upload_sharepoint']


def init_sharepoint():
    global tenant_id, client_id, client_secret, site_domain, site_path, folder_path
    global logger, token, graph_base_url, to_emails, to_emails_filtered_report

    # Inicializar email (si necesitas enviar notificaciones)
    init_email()

    # Log
    logger = init_log()

    # Configuración de SharePoint
    tenant_id = cfg.get_parameter("SharePoint", "tenant_id")
    client_id = cfg.get_parameter("SharePoint", "client_id")
    client_secret = cfg.get_parameter("SharePoint", "client_secret")
    site_domain = cfg.get_parameter("SharePoint", "site_domain")
    site_path = cfg.get_parameter("SharePoint", "site_path")
    folder_path = cfg.get_parameter("SharePoint", "target_folder_path")
    graph_base_url = cfg.get_parameter("SharePoint", "graph_base_url")

    # Emails
    to_emails = cfg.get_parameter("Smtp_Server", "to_emails")
    to_emails_filtered_report = cfg.get_parameter("Smtp_Server", "to_emails_filtered_report")

    # Autenticación
    token = _get_graph_token()
    logger.info("[*] SharePoint connection initialized")

def _get_graph_token():
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=authority
    )
    result = app.acquire_token_for_client(scopes=[f"{graph_base_url}/.default"] )
    if "access_token" not in result:
        raise Exception(result.get("error_description"))
    return result["access_token"]

def _refresh_token():
    """
    Refresca el token de acceso utilizando MSAL.
    """
    global token
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=authority
    )
    result = app.acquire_token_for_client(scopes=[f"{graph_base_url}/.default"] )
    if "access_token" not in result:
        raise Exception(result.get("error_description"))
    token = result["access_token"]


def upload_sharepoint(file_path: str, file_name: str):
    """
    Sube un archivo a SharePoint usando Graph API (usando path completo en lugar de folder ID).
    Envía un correo de notificación de éxito o error.
    """
    try:
        upload_start = time.perf_counter()

        # Validar archivo local
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        # Obtener site y drive
        site_id = _get_site_id()
        drive_id = _get_drive_id(site_id)

        # Leer archivo
        with open(file_path, 'rb') as f:
            file_data = f.read()

        # Construir URL de subida directa usando path
        upload_url = f"{graph_base_url}/v1.0/sites/{site_id}/drives/{drive_id}/root:/{folder_path}/{file_name}:/content"
        upload_headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/octet-stream'
        }

        # Subida
        response = requests.put(upload_url, headers=upload_headers, data=file_data)

        # Si el token ha expirado (error 401), refrescar el token y reintentar
        if response.status_code == 401:
            logger.warning("[!] Token expired, refreshing token...")
            _refresh_token()  # Actualiza el token
            upload_headers['Authorization'] = f'Bearer {token}'  # Actualizar el header de autorización
            response = requests.put(upload_url, headers=upload_headers, data=file_data)

        if response.status_code not in [200, 201]:
            raise Exception(f"Error uploading file: {response.status_code} - {response.text}")

        info = response.json()
        web_url = info.get('webUrl', 'N/A')
        logger.info(f"File uploaded to SharePoint: {web_url}")
        logger.info(f"Upload duration: {time.perf_counter() - upload_start:.2f} seconds")

        # Notificación de éxito
        message = f" Informative Message! ** The {file_name}** report was generated in SharePoint.\n\n URL: {web_url}"
        send_email(to_emails_filtered_report, "Success: OriginateSMS Report - Apollo", message)

    except Exception as err:
        logger.error(f"Error uploading file to SharePoint: {str(err)}")
        message = f"There was an error uploading file **{file_name}** to SharePoint:\n\n{str(err)}"
        send_email(to_emails, "Error: OriginateSMS Report - Apollo", message)


def _get_site_id():
    url = f"{graph_base_url}/v1.0/sites/{site_domain}:{site_path}"
    headers = {'Authorization': f'Bearer {token}'}
    resp = requests.get(url, headers=headers)
    
    if resp.status_code == 401:
        logger.warning("[!] Token expired while getting site info, refreshing token...")
        _refresh_token()  # Refrescar token y reintentar la solicitud
        headers['Authorization'] = f'Bearer {token}'
        resp = requests.get(url, headers=headers)
    
    if resp.status_code != 200:
        raise Exception(f"Error getting site: {resp.status_code} - {resp.text}")
    return resp.json()["id"]


def _get_drive_id(site_id):
    url = f"{graph_base_url}/v1.0/sites/{site_id}/drives"
    headers = {'Authorization': f'Bearer {token}'}
    resp = requests.get(url, headers=headers)
    
    if resp.status_code == 401:
        logger.warning("[!] Token expired while getting drive info, refreshing token...")
        _refresh_token()  # Refrescar token y reintentar la solicitud
        headers['Authorization'] = f'Bearer {token}'
        resp = requests.get(url, headers=headers)
    
    if resp.status_code != 200:
        raise Exception("Error getting drives")
    
    for drive in resp.json().get("value", []):
        if drive.get("name") == "Documents":
            return drive.get("id")
    
    raise Exception("Drive 'Documents' not found")
