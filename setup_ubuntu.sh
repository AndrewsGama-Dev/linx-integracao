#!/bin/bash
# Setup inicial da integração Linx no Ubuntu/Hostinger

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

echo "=== Setup Integração Linx ==="

# Verificar Python 3
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 não encontrado. Instale com: apt install python3 python3-venv python3-pip"
    exit 1
fi

# Criar venv
if [ ! -d "venv" ]; then
    echo "📦 Criando ambiente virtual..."
    python3 -m venv venv
fi

# Ativar e instalar dependências
echo "📦 Instalando dependências..."
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Permissão de execução no integrador.sh
chmod +x integrador.sh
chmod +x setup_ubuntu.sh

echo "✅ Setup concluído!"
echo ""
echo "Próximos passos:"
echo "1. Configure o arquivo .config com token e URLs"
echo "2. Teste: ./integrador.sh"
echo "3. Configure o cron: crontab -e"
echo "   Adicione: */30 * * * * cd $SCRIPT_DIR && ./integrador.sh >> $SCRIPT_DIR/integrador.log 2>&1"
