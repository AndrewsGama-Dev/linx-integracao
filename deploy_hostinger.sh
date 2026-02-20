#!/bin/bash
# Deploy via Git - executar no servidor
# Uso: ./deploy_hostinger.sh

DIR="/home/gogotech/integracao/linx"
REPO_URL=""  # Preencha com URL do repositório Git (ex: git@github.com:usuario/linx-integracao.git)

cd "$DIR" || exit 1

if [ -z "$REPO_URL" ]; then
    echo "⚠️  Configure REPO_URL no script ou use git pull manualmente"
    echo "   git pull origin main"
    git pull 2>/dev/null || echo "Execute: git pull"
else
    if [ ! -d ".git" ]; then
        echo "📥 Clonando repositório..."
        cd /home/gogotech/integracao
        git clone "$REPO_URL" linx
        cd linx
    else
        echo "📥 Atualizando do Git..."
        git pull
    fi
fi

# Setup se necessário
if [ ! -d "venv" ]; then
    ./setup_ubuntu.sh
fi

# Permissões
chmod +x integrador.sh 2>/dev/null

echo "✅ Deploy concluído!"
