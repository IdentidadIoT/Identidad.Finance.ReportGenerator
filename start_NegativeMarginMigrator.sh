#!/bin/bash

# =========================
# CONFIGURACIÓN
# =========================
APP_DIR="/opt/pythonapps/Identidad.Finance.ReportGenerator"
ENTRYPOINT="NegativeMarginMigrator.py"

PYTHON_BIN="/usr/bin/python3.10"
VENV_PATH="$APP_DIR/venv"
PY_SCRIPT="$APP_DIR/$ENTRYPOINT"

# =========================
# VALIDACIONES
# =========================

# ¿Existe Python 3.10?
if [ ! -x "$PYTHON_BIN" ]; then
    echo "[ERROR] Python 3.10 no encontrado en $PYTHON_BIN"
    exit 1
fi

# ¿Existe el venv?
if [ ! -d "$VENV_PATH" ]; then
    echo "[ERROR] Venv no encontrado en $VENV_PATH"
    exit 1
fi

# ¿Existe el script Python?
if [ ! -f "$PY_SCRIPT" ]; then
    echo "[ERROR] Script Python no encontrado en $PY_SCRIPT"
    exit 1
fi

# =========================
# EJECUCIÓN AISLADA
# =========================

cd "$APP_DIR" || {
    echo "[ERROR] No se pudo acceder al directorio $APP_DIR"
    exit 1
}

# Activar venv
source "$VENV_PATH/bin/activate"

# Ejecutar explícitamente con el python del venv
"$VENV_PATH/bin/python" "$PY_SCRIPT"
STATUS=$?

# Desactivar venv
deactivate

# =========================
# RESULTADO
# =========================
if [ $STATUS -ne 0 ]; then
    echo "[ERROR] El script Python falló con código $STATUS"
    exit $STATUS
fi

echo "[OK] Ejecución finalizada correctamente"
exit 0


#0 1 * * * /opt/pythonapps/Identidad.Finance.ReportGenerator/start_NegativeMarginMigrator.sh \
#>> /opt/pythonapps/Identidad.Finance.ReportGenerator/NegativeMarginMigrator.log 2>&1