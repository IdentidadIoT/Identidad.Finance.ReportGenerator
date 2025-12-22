"""
/*************************************************************************
 * 
 *  [2022] Identidad Technologies. 
 *  All Rights Reserved.
 * 
 * NOTICE:  All information contained herein is, and remains
 * the property of Identidad Technologies,
 * The intellectual and technical concepts contained
 * herein are proprietary to Identidad Technologies
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Identidad Technologies.
 */
"""


from sqlalchemy import create_engine
import urllib
import core.config as cfg  # Usar el mismo config.py

def get_engine():
    # Obtener credenciales desde config.cfg
    server = cfg.get_parameter("Database_SQLServer", "DB_SERVER")
    database = cfg.get_parameter("Database_SQLServer", "DB_NAME")
    username = cfg.get_parameter("Database_SQLServer", "DB_USER")
    password = cfg.get_parameter("Database_SQLServer", "DB_PASSWORD")

    # Codificar la cadena de conexión para SQLAlchemy
    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"MultipleActiveResultSets=True;"
    )

    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    return engine
