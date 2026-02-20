# Comandos de Atualização - Integração Linx

## Atualizar token no servidor (quando necessário)

Se o token expirar ou der erro de autenticação, execute:

```bash
cd /home/gogotech/integracao/linx
chmod +x atualizar_token.sh   # só na primeira vez
./atualizar_token.sh
```

O script lê as credenciais (url_token, alias_name, user_name, password) do `.config`, obtém o token na API Humanus e grava no próprio `.config`. Depois execute `./integrador.sh` normalmente.

---

## No seu PC (após alterar o código)

```powershell
cd "c:\Users\gamaa\OneDrive\Documents\Andrews\Projetos\Integrações\linx-integracao"

git add .
git status
git commit -m "descrição da alteração"
git push origin main
```

---

## No servidor (para baixar as atualizações)

```bash
cd /home/gogotech/integracao/linx
git pull
```

O arquivo `.config` do servidor **não é sobrescrito** (está no .gitignore).

---

## Configuração do Cron (com lock)

Edite o cron: `crontab -e`

Adicione a linha (lock evita execução simultânea):

```bash
*/30 * * * * flock -n /home/gogotech/integracao/linx/.integrador.lock -c "cd /home/gogotech/integracao/linx && ./integrador.sh >> /home/gogotech/integracao/linx/integrador.log 2>&1"
```

- `*/30 * * * *` = a cada 30 minutos
- `flock -n` = se já estiver rodando, não inicia outra instância
- `.integrador.lock` = arquivo de lock (criado automaticamente)
