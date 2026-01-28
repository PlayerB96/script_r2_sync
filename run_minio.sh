#!/bin/bash

# Ir a la carpeta donde está el script
cd "$(dirname "$0")"

# Activar virtualenv
source ./venv/bin/activate

# Ejecutar script
python app.py

# Desactivar virtualenv
deactivate
