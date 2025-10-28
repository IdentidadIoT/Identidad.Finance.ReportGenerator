from core.config import init_config, get_parameter
import oracledb
print("Modo de conexión Oracle:", "THIN" if oracledb.is_thin_mode() else "THICK")


# Forzar modo THIN desde el inicio
# (Si hay variables que apuntan a libclntsh.so, se ignorarán)
if not oracledb.is_thin_mode():
    oracledb.init_oracle_client(lib_dir=None)

_connection = None

__all__ = ['create_oracle_connection', 'close_oracle_connection', 'output_type_handler']


def create_oracle_connection():
    try:
        host = get_parameter('Oracle', 'host')
        port = get_parameter('Oracle', 'port')
        service_name = get_parameter('Oracle', 'sid')  # usa SERVICE_NAME, no SID
        username = get_parameter('Oracle', 'username')
        password = get_parameter('Oracle', 'password')

        # En modo thin, el formato correcto es host:port/service_name
        dsn = oracledb.makedsn(host, port, sid=service_name)

        global _connection
        _connection = oracledb.connect(
            user=username,
            password=password,
            dsn=dsn
        )

        print("Conexión establecida (modo THIN)")
        return _connection

    except oracledb.DatabaseError as err:
        print(f"Error al conectar a Oracle: {err}")
        print("El modo thin no requiere Instant Client ni libclntsh.so")
        raise err


def close_oracle_connection():
    global _connection
    if _connection:
        _connection.close()
        print("Conexión Oracle cerrada.")


def output_type_handler(cursor, name, defaultType, size, precision, scale):
    if defaultType == oracledb.STRING:
        return cursor.var(defaultType, size, arraysize=cursor.arraysize, encodingErrors="replace")


if __name__ == '__main__':
    init_config()
