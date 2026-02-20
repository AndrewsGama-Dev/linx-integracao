# Git e Deploy - Integração Linx

## 1. Configurar Git no projeto (no seu PC)

```bash
cd "caminho/para/linx-integracao"

# Inicializar repositório
git init

# Adicionar arquivos (o .config NÃO será versionado - está no .gitignore)
git add .
git status   # Verifique o que será commitado

# Primeiro commit
git commit -m "Integração Linx - API Humanus"
```

## 2. Criar repositório remoto

- **GitHub:** Crie um repositório em github.com (pode ser privado)
- **GitLab / Bitbucket:** Alternativas

Depois, vincule e envie:

```bash
git remote add origin https://github.com/SEU_USUARIO/linx-integracao.git
git branch -M main
git push -u origin main
```

## 3. Deploy no servidor Hostinger

### Opção A: Primeira vez (sem Git no servidor)

1. **Enviar arquivos manualmente** (FTP, FileZilla, etc.):
   - Pasta destino: `/home/gogotech/integracao/linx/`
   - Arquivos: todos os `.py`, `integrador.sh`, `setup_ubuntu.sh`, `requirements.txt`, `.config`

2. **No servidor:**
```bash
cd /home/gogotech/integracao/linx
chmod +x setup_ubuntu.sh integrador.sh
./setup_ubuntu.sh
# Configure o .config com os dados do servidor
./integrador.sh   # Testar
```

### Opção B: Com Git no servidor (para atualizações)

1. **Primeira vez - clonar:**
```bash
cd /home/gogotech/integracao
git clone https://github.com/SEU_USUARIO/linx-integracao.git linx
cd linx
```

2. **Criar .config no servidor** (não vem do Git):
```bash
cp .config.example .config
nano .config   # Editar com token, URLs, etc.
```

3. **Setup:**
```bash
chmod +x setup_ubuntu.sh integrador.sh
./setup_ubuntu.sh
```

4. **Cron:**
```bash
crontab -e
# Adicionar:
*/30 * * * * cd /home/gogotech/integracao/linx && ./integrador.sh >> /home/gogotech/integracao/linx/integrador.log 2>&1
```

### Opção C: Atualizar após mudanças (quando já tem Git)

```bash
cd /home/gogotech/integracao/linx
git pull
# O .config local é preservado (não é versionado)
```

## 4. Fluxo de atualizações

1. **No seu PC:** altere o código e faça commit/push
2. **No servidor:** execute `git pull` na pasta da integração
3. O `.config` do servidor não é sobrescrito (está no .gitignore)

## 5. Arquivos NÃO versionados (.gitignore)

- `.config` - contém tokens e senhas
- `venv/` - ambiente virtual
- `integracao_cache.db` - cache local
- `integrador.log` - logs
- `__pycache__/` - cache Python

Use `.config.example` como modelo para criar o `.config` no servidor.
