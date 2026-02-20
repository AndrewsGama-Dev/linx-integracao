# Cria pasta com arquivos para deploy (exclui venv, cache, logs)
# Execute no PowerShell: .\criar_pacote_deploy.ps1

$pastaDeploy = "linx_deploy"
$arquivos = @(
    "main.py", "api_humanus.py", "cache_db.py", "config_reader.py",
    "cargos.py", "departamentos.py", "funcionarios.py",
    "afastamentos.py", "ferias.py", "demissoes.py",
    "integrador.sh", "setup_ubuntu.sh", "deploy_hostinger.sh",
    "requirements.txt", ".config.example",
    "README_DEPLOY.md", "README_GIT_DEPLOY.md"
)

if (Test-Path $pastaDeploy) { Remove-Item $pastaDeploy -Recurse -Force }
New-Item -ItemType Directory -Path $pastaDeploy | Out-Null

foreach ($arq in $arquivos) {
    if (Test-Path $arq) {
        Copy-Item $arq -Destination $pastaDeploy
        Write-Host "  Copiado: $arq"
    }
}

# Copiar .config separadamente (usuario deve incluir manualmente no servidor)
if (Test-Path ".config") {
    Copy-Item ".config" -Destination "$pastaDeploy\.config"
    Write-Host "  Copiado: .config (com suas configuracoes)"
} else {
    Copy-Item ".config.example" -Destination "$pastaDeploy\.config"
    Write-Host "  Criado: .config (a partir do exemplo - EDITE no servidor!)"
}

Write-Host ""
Write-Host "Pacote criado em: $pastaDeploy"
Write-Host "Envie a pasta para o servidor em: /home/gogotech/integracao/linx/"
Write-Host "Ou use: scp -r $pastaDeploy/* usuario@servidor:/home/gogotech/integracao/linx/"
