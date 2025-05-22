# Script para Eliminar Carpetas __pycache__
#Este script de Bash (`.sh`) está diseñado para encontrar y eliminar todas las carpetas `__pycache__` dentro del directorio actual y sus subdirectorios, excluyendo específicamente la carpeta `venv` (comúnmente usada para entornos virtuales de Python).
## Desglose del Script

#!/bin/bash
echo "Searching and deleting _pychache_ (not including venv folder)"

find . -type d -name "__pycache__" ! -path "./venv/*" -exec rm -rf {} +

echo "Delete success."

# chmod +x delete_pycache.sh
# ./delete_pycache.sh