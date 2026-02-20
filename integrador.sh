#!/bin/bash
# Integrador Linx - API Humanus -> ifPonto/Hevi
# Executado pelo cron a cada 30 minutos

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

LOCK_FILE="$SCRIPT_DIR/.integrador.lock"
exec 9>"$LOCK_FILE"
flock -n 9 || { echo "Integração já em execução. Saindo."; exit 1; }

# Usar venv se existir
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Executar integração
python3 main.py
