#!/bin/bash

# ================================
# Configuración
# ================================
APP_NAME="MicroService-report-sms"
APP_DIR="/opt/pythonapps/Identidad.Finance.ReportGenerator"
VENV_DIR="$APP_DIR/venv"
ENTRYPOINT="worker_sms.py"

PYTHON_CMD="python3.10"
PYTHON_BIN="$VENV_DIR/bin/python"
PIP_BIN="$VENV_DIR/bin/pip"

REQUIRED_MAJOR=3
REQUIRED_MINOR=10

# ================================
# Validar Python versión
# ================================

if ! command -v $PYTHON_CMD &> /dev/null; then
  echo "python3 no está instalado"
  exit 1
fi

PY_VERSION=$($PYTHON_CMD - <<EOF
import sys
print(f"{sys.version_info.major}.{sys.version_info.minor}")
EOF
)

if [[ "$PY_VERSION" != "$REQUIRED_MAJOR.$REQUIRED_MINOR" ]]; then
  echo "Python $REQUIRED_MAJOR.$REQUIRED_MINOR requerido. Detectado: $PY_VERSION"
  exit 1
fi

echo "Python $PY_VERSION detectado"

# ================================
# Validar entrypoint
# ================================

if [ ! -f "$APP_DIR/$ENTRYPOINT" ]; then
  echo "No existe el archivo $APP_DIR/$ENTRYPOINT"
  exit 1
fi

# ================================
# Crear venv si no existe
# ================================

if [ ! -d "$VENV_DIR" ]; then
  echo "venv no existe. Creando con python3 ($PY_VERSION)..."
  cd "$APP_DIR" || exit 1

  $PYTHON_CMD -m venv venv || {
    echo "Error creando el venv"
    exit 1
  }

  echo "venv creado correctamente"
fi

# ================================
# Instalar dependencias
# ================================

if [ -f "$APP_DIR/requirements.txt" ]; then
  echo "Instalando dependencias..."
  $PIP_BIN install --upgrade pip
  $PIP_BIN install -r "$APP_DIR/requirements.txt"
else
  echo "No existe requirements.txt, se omite instalación"
fi

# ================================
# PM2 Start
# ================================

cd "$APP_DIR" || exit 1

pm2 start "$PYTHON_BIN" \
  --name "$APP_NAME" \
  -- "$ENTRYPOINT"

pm2 save

echo "Aplicación '$APP_NAME' iniciada con PM2 usando Python $PY_VERSION"

# ================================
# Cerrar venv si está activo
# ================================

if [[ -n "$VIRTUAL_ENV" ]]; then
  echo "Cerrando virtualenv activo: $VIRTUAL_ENV"
  deactivate
else
  echo "No hay virtualenv activo para cerrar"
fi
