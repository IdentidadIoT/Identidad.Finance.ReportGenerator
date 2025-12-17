#!/bin/bash

# ================================
# Configuración
# ================================
APP_NAME="report-sms"
APP_DIR="/home/daridel/Identidad.Finance.ReportGenerator"  # Ruta a la aplicación
VENV_DIR="$APP_DIR/venv"
ENTRYPOINT="worker_sms.py"        # o app.py, gunicorn, uvicorn, etc.
PYTHON_BIN="$VENV_DIR/bin/python3"

# ================================
# Validaciones
# ================================


if [ ! -f "$APP_DIR/$ENTRYPOINT" ]; then
  echo "No existe el archivo $ENTRYPOINT"
  exit 1
fi

if [ ! -d "$VENV_DIR" ]; then
  echo "El venv no existe en $VENV_DIR"
  exit 1
fi


# ================================
# PM2 Start
# ================================
cd "$APP_DIR" || exit 1

pm2 start "$PYTHON_BIN" \
  --name "$APP_NAME" \
  -- "$ENTRYPOINT"

# ================================
# Guardar estado
# ================================
pm2 save

echo "Aplicación '$APP_NAME' iniciada con PM2 usando venv"
