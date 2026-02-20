# Comandos de Atualização - Integração Linx

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
