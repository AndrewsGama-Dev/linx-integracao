#!/bin/bash
# Obtém token da API Humanus e grava no .config
# Execute antes do integrador quando precisar atualizar o token

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

if [ -d "venv" ]; then
    source venv/bin/activate
fi

python3 atualizar_token_config.py
