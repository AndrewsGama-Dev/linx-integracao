# Deploy - Integração Linx no Hostinger

## Estrutura no servidor

```
/home/gogotech/integracao/linx/
├── integrador.sh          # Script executado pelo cron
├── main.py
├── .config                # Configurações (token, URLs, empresas)
├── requirements.txt
├── integrador.log         # Log da execução (gerado automaticamente)
├── integracao_cache.db    # Cache SQLite (gerado automaticamente)
├── api_humanus.py
├── cache_db.py
├── config_reader.py
├── cargos.py
├── departamentos.py
├── funcionarios.py
├── afastamentos.py
├── ferias.py
├── demissoes.py
├── venv/                  # Ambiente virtual Python
└── relatorio_integracao_*.json  # Relatórios (padrão para histórico)
```

## Comandos para deploy

### 1. Criar pasta e enviar arquivos

```bash
# No servidor (SSH)
mkdir -p /home/gogotech/integracao/linx
```

Envie os arquivos do projeto para `/home/gogotech/integracao/linx/` (via FTP, SCP ou Git).

### 2. Executar setup inicial

```bash
cd /home/gogotech/integracao/linx
chmod +x setup_ubuntu.sh integrador.sh
./setup_ubuntu.sh
```

### 3. Configurar .config

Certifique-se de que o arquivo `.config` está preenchido com:
- Token da API Humanus
- URLs do APITARGET
- Configurações SOAP (para demissões)
- empresas_permitidas (ex: 004)

### 4. Testar execução

```bash
cd /home/gogotech/integracao/linx
./integrador.sh
```

### 5. Configurar Cron (30 em 30 minutos)

```bash
crontab -e
```

Adicione a linha:

```
*/30 * * * * cd /home/gogotech/integracao/linx && ./integrador.sh >> /home/gogotech/integracao/linx/integrador.log 2>&1
```

### 6. Permissões (se necessário)

```bash
# Se o cron rodar como outro usuário (ex: gogotech)
chown -R gogotech:gogotech /home/gogotech/integracao/linx
chmod +x /home/gogotech/integracao/linx/integrador.sh
```

## Configuração no painel (Editar Integrador)

- **Diretório Base:** `/home/gogotech/integracao/linx`
- **Arquivo Executável:** `integrador.sh`
- **Arquivo de Log:** `integrador.log`
- **Arquivo de Configuração:** `.config`
- **Padrão dos Relatórios:** `relatorio_integracao_*.json`
- **Intervalo:** 30 minutos
