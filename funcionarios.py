import requests
import json
import pandas as pd
from datetime import datetime
import time
import hashlib
import pytz
import configparser
from config_reader import obter_headers_api, obter_campo_chave_funcionarios
from api_humanus import buscar_colaboradores_paginado, formatar_data_iso_para_br

def carregar_configuracoes_target():
    """
    Carrega configurações da seção [APITARGET] do arquivo .config
    """
    try:
        config = configparser.ConfigParser()
        config.read('.config', encoding='utf-8')
        
        if 'APITARGET' not in config:
            print("❌ Seção [APITARGET] não encontrada no arquivo .config")
            return None
        
        return {
            'url': config['APITARGET'].get('url', '').strip(),
            'integracao': config['APITARGET'].get('integracao', '').strip(),
            'token_base': config['APITARGET'].get('token_base', '').strip()
        }
    except Exception as e:
        print(f"❌ Erro ao carregar configurações [APITARGET]: {e}")
        return None

def gerar_token_target():
    """
    Gera o token para a API de destino usando a data atual
    """
    config_target = carregar_configuracoes_target()
    if not config_target:
        return None, None
    
    # Configurar timezone para São Paulo
    tz_sao_paulo = pytz.timezone('America/Sao_Paulo')
    data_atual = datetime.now(tz_sao_paulo).strftime('%d/%m/%Y')
    
    # Gerar token final
    token_concatenado = config_target['token_base'] + data_atual
    token_final = hashlib.sha256(token_concatenado.encode('utf-8')).hexdigest()
    
    print(f"🔑 Data atual: {data_atual}")
    print(f"🔗 Token base: {config_target['token_base']}")
    print(f"🔐 Token final gerado: {token_final[:32]}...")
    
    return config_target, token_final

def formatar_cpf_11_digitos(cpf):
    """
    Formata CPF para garantir 11 dígitos com zeros à esquerda
    """
    if not cpf:
        return ""
    
    # Converter para string e remover caracteres não numéricos
    cpf_str = str(cpf).replace('.', '').replace('-', '').replace('/', '').strip()
    
    # Se não for numérico ou estiver vazio, retornar vazio
    if not cpf_str.isdigit():
        return ""
    
    # Completar com zeros à esquerda para 11 dígitos
    cpf_formatado = cpf_str.zfill(11)
    
    # Validar se tem exatamente 11 dígitos
    if len(cpf_formatado) == 11:
        return cpf_formatado
    
    return ""

def enviar_csv_para_api_target(nome_arquivo_csv):
    """
    Envia o CSV de funcionários para a API da Hevi
    """
    import os
    
    if not os.path.exists(nome_arquivo_csv):
        print(f"❌ Arquivo {nome_arquivo_csv} não encontrado!")
        return False
    
    print(f"✅ Arquivo {nome_arquivo_csv} encontrado")
    
    # Obter configurações e token
    config_target, token_final = gerar_token_target()
    if not config_target or not token_final:
        print("❌ Falha ao gerar token para API de destino")
        return False
    
    usuario_integracao = config_target['integracao']
    
    headers = {
        "user": usuario_integracao,
        "token": token_final
    }
    
    data = {
        "pag": "funcionario_cadastrar",
        "cmd": "importar_cad",
        "separador": ";"
    }
    
    try:
        print(f"📤 Enviando POST para API da Hevi...")
        print(f"🌐 URL: {config_target['url']}")
        print(f"👤 Usuário: {usuario_integracao}")
        print(f"📄 Endpoint: funcionario_cadastrar")
        print(f"🔑 Token: {token_final[:32]}...")
        
        with open(nome_arquivo_csv, 'rb') as arquivo:
            files = {
                'arquivo': (nome_arquivo_csv, arquivo, 'text/csv')
            }
            
            response = requests.post(
                config_target['url'], 
                data=data, 
                files=files,
                headers=headers,
                timeout=30
            )
        
        print(f"📊 Status da resposta: {response.status_code}")
        
        if response.status_code == 200:
            try:
                resultado = response.json()
                
                if resultado.get('success') == False:
                    print(f"❌ API retornou erro:")
                    print(f"📝 Resposta: {json.dumps(resultado, indent=2, ensure_ascii=False)}")
                    return False
                else:
                    print(f"✅ POST de funcionários realizado com sucesso!")
                    print(f"📋 Resposta da API:")
                    print(json.dumps(resultado, indent=2, ensure_ascii=False))
                    
                    cadastrados = resultado.get('ok', 0)
                    if cadastrados > 0:
                        print(f"🎉 {cadastrados} funcionário(s) cadastrado(s) com sucesso!")
                    
                    return True
                
            except json.JSONDecodeError:
                print(f"⚠️ Resposta não é JSON válido:")
                print(f"📝 Resposta: {response.text[:500]}...")
                return False
                
        else:
            print(f"❌ ERRO no POST - Status: {response.status_code}")
            print(f"📝 Resposta: {response.text[:500]}...")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ ERRO na requisição para API da Hevi: {e}")
        return False

def _eh_funcionario_demitido(col):
    """Verifica se o colaborador tem situacaoPessoa com sitCodSituacao '3' (demitido)"""
    situacoes = col.get('situacaoPessoa') or []
    for sit in situacoes:
        if str(sit.get('sitCodSituacao', '')) == '3':
            return True
    return False

def consultar_funcionarios_ativos_api_humanus():
    """
    Coleta funcionários da API Humanus, excluindo os demitidos (sitCodSituacao=3)
    """
    print("🔍 INICIANDO COLETA DE FUNCIONÁRIOS ATIVOS - API Humanus...")
    
    colaboradores = buscar_colaboradores_paginado()
    # Filtrar demitidos
    funcionarios_ativos = [c for c in colaboradores if not _eh_funcionario_demitido(c)]
    
    print(f"\n✅ Funcionários ativos (excluindo demitidos): {len(funcionarios_ativos)}")
    return funcionarios_ativos

def formatar_data_brasileira(data_iso):
    """
    Converte data ISO para formato brasileiro DD/MM/AAAA
    """
    if not data_iso:
        return ""
    
    try:
        data_str = data_iso.replace('Z', '').split('T')[0]
        data_obj = datetime.strptime(data_str, '%Y-%m-%d')
        return data_obj.strftime('%d/%m/%Y')
    except Exception as e:
        return ""

def mapear_colaborador_para_csv(col):
    """
    Mapeia um colaborador da API Humanus para o formato CSV.
    Campos conforme Consultas.txt
    """
    pfi = col.get('pessoaFisica') or {}
    pff = col.get('pessoaFisFunc') or {}
    pfu = col.get('pessoaFunc') or {}
    lotacao = pfu.get('lotacao') or {}
    
    cpf = formatar_cpf_11_digitos(pfi.get('pfiCpfnumeroDigito', ''))
    matricula = str(col.get('nroMatrExterno', '')).zfill(6)
    
    # estado_civil: S=Solteiro, C=Casado
    ec = pfi.get('pfiEstadoCivil', '')
    estado_civil = 'Solteiro' if ec == 'S' else ('Casado' if ec == 'C' else '')
    
    # pes* - podem estar no objeto raiz ou em pessoa
    pessoa = col.get('pessoa') or {}
    pes_email = col.get('pesEmail') or pessoa.get('pesEmail', '')
    pes_end_rua = col.get('pesEndRua') or pessoa.get('pesEndRua', '')
    pes_end_bairro = col.get('pesEndBairro') or pessoa.get('pesEndBairro', '')
    pes_end_cidade = col.get('pesEndCidade') or pessoa.get('pesEndCidade', '')
    pes_end_estado = col.get('pesEndEstado') or pessoa.get('pesEndEstado', '')
    pes_end_cep = col.get('pesEndCep') or pessoa.get('pesEndCep', '')
    
    campo_chave = obter_campo_chave_funcionarios()
    
    funcionario_csv = {
        'campo_chave': campo_chave,
        'nome': col.get('nomeExtenso', ''),
        'cpf': cpf,
        'matricula': matricula,
        'pis': pfi.get('pfiPisnumeroDigito', '') or cpf,
        'dtadmissao': formatar_data_iso_para_br(pfu.get('pfuDtInicioContrato', '')),
        'email': pes_email,
        'endereco': pes_end_rua,
        'bairro': pes_end_bairro,
        'cidade': pes_end_cidade,
        'uf': pes_end_estado,
        'cep': pes_end_cep,
        'login': cpf,
        'cod_empresa': col.get('codEmpresa', ''),
        'codigo_legado_empresa': col.get('codEmpresa', ''),
        'salario': str(pff.get('pffValorSalario', '')),
        'dtnascimento': formatar_data_iso_para_br(pfi.get('pfiDataNascim', '')),
        'nome_mae': pfi.get('pfiNomeMae', ''),
        'nome_pai': pfi.get('pfiNomePai', ''),
        'estado_civil': estado_civil,
        'codigo_unidade': lotacao.get('lotCodlotacao', ''),
        'codigo_cargo': pff.get('pffCodCargo', ''),
        'nome_cargo': pff.get('pffDescricaoCargo', ''),
    }
    
    # Garantir campo_chave como primeira coluna - reordenar
    return funcionario_csv

def gerar_csv_funcionarios():
    """
    Função principal para gerar o CSV dos funcionários - API Humanus
    """
    print("=" * 80)
    print("         🚀 GERAÇÃO DE CSV DE FUNCIONÁRIOS - API Humanus")
    print("=" * 80)
    
    headers = obter_headers_api()
    if not headers:
        print("❌ Falha ao carregar token (configure token ou credenciais em [APISOURCE])")
        return None
    
    colaboradores = consultar_funcionarios_ativos_api_humanus()
    
    if not colaboradores:
        print("❌ Nenhum funcionário ativo foi coletado")
        return None
    
    print(f"\n🔄 Convertendo {len(colaboradores)} funcionários para formato CSV...")
    
    funcionarios_csv = []
    erros = []
    campo_chave = obter_campo_chave_funcionarios()
    
    for i, col in enumerate(colaboradores, 1):
        try:
            func_csv = mapear_colaborador_para_csv(col)
            # Garantir campo_chave como primeira coluna
            func_ordenado = {'campo_chave': campo_chave}
            func_ordenado.update({k: v for k, v in func_csv.items() if k != 'campo_chave'})
            funcionarios_csv.append(func_ordenado)
            
            if i % 50 == 0:
                print(f"  ✅ Processados {i}/{len(colaboradores)} funcionários...")
        except Exception as e:
            erros.append({'matricula': col.get('nroMatrExterno', 'N/A'), 'erro': str(e)})
            print(f"  ❌ Erro ao processar funcionário {col.get('nroMatrExterno', 'N/A')}: {e}")
    
    if not funcionarios_csv:
        print("❌ Nenhum funcionário foi convertido com sucesso")
        return
    
    print(f"\n📊 Criando DataFrame com {len(funcionarios_csv)} funcionários...")
    df = pd.DataFrame(funcionarios_csv)
    
    nome_arquivo = "funcionarios_api.csv"
    
    try:
        df.to_csv(nome_arquivo, index=False, encoding='utf-8-sig', sep=';')
        print(f"✅ CSV gerado com sucesso: {nome_arquivo}")
        
        print(f"\n📈 ESTATÍSTICAS:")
        print(f"  📊 Total de funcionários processados: {len(funcionarios_csv)}")
        print(f"  ❌ Erros de conversão: {len(erros)}")
        print(f"  📋 Colunas no CSV: {len(df.columns)}")
        
        print(f"\n👁️ PREVIEW DOS DADOS (primeiras 3 linhas):")
        print(df.head(3).to_string())
        
        return nome_arquivo
        
    except Exception as e:
        print(f"❌ Erro ao gerar CSV: {e}")
        return None

def validar_dados_csv(nome_arquivo):
    """
    Valida os dados do CSV gerado
    """
    if not nome_arquivo:
        return
    
    try:
        print(f"\n🔍 VALIDANDO DADOS DO CSV: {nome_arquivo}")
        
        df = pd.read_csv(nome_arquivo, sep=';', encoding='utf-8-sig')
        
        print(f"  📊 Total de registros: {len(df)}")
        print(f"  📋 Total de colunas: {len(df.columns)}")
        
        campos_obrigatorios = ['nome', 'cpf', 'matricula']
        
        for campo in campos_obrigatorios:
            if campo in df.columns:
                vazios = df[campo].isna().sum() + (df[campo] == '').sum()
                if vazios > 0:
                    print(f"  ⚠️ Campo '{campo}': {vazios} registros vazios")
                else:
                    print(f"  ✅ Campo '{campo}': todos preenchidos")
        
        print(f"  ✅ Validação concluída")
        
    except Exception as e:
        print(f"  ❌ Erro na validação: {e}")

def processar_integracao_completa():
    """
    Função principal que executa todo o processo
    """
    print("=" * 80)
    print("    🚀 INTEGRAÇÃO COMPLETA DE FUNCIONÁRIOS ATIVOS - eContador → Hevi")
    print("=" * 80)
    
    print("\n📋 ETAPA 1: Coletando funcionários da API Humanus...")
    arquivo_csv = gerar_csv_funcionarios()
    
    if not arquivo_csv:
        print("❌ Falha na geração do CSV. Processo interrompido.")
        return False
    
    print("\n🔍 ETAPA 2: Validando dados do CSV...")
    validar_dados_csv(arquivo_csv)
    
    print("\n📤 ETAPA 3: Enviando CSV para API da Hevi...")
    sucesso_envio = enviar_csv_para_api_target(arquivo_csv)
    
    if sucesso_envio:
        print("\n🎉 INTEGRAÇÃO COMPLETA FINALIZADA COM SUCESSO!")
        print(f"✅ Funcionários coletados da API Humanus")
        print(f"✅ CSV gerado: {arquivo_csv}")
        print(f"✅ Dados enviados para sistema Hevi")
        return True
    else:
        print("\n💥 FALHA NA INTEGRAÇÃO!")
        print(f"✅ CSV gerado: {arquivo_csv}")
        print(f"❌ Falha no envio para sistema Hevi")
        return False

# Exemplo de uso
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        comando = sys.argv[1].lower()
        
        if comando == "integracao":
            sucesso = processar_integracao_completa()
            if sucesso:
                print(f"\n🚀 INTEGRAÇÃO FINALIZADA COM SUCESSO!")
            else:
                print(f"\n💥 INTEGRAÇÃO FALHOU - Verifique os logs acima")
        else:
            print("Comandos disponíveis:")
            print("  python funcionarios.py integracao       - Executar integração completa (PADRÃO)")
    else:
        # Executar integração completa automaticamente
        sucesso = processar_integracao_completa()
        if sucesso:
            print(f"\n🚀 INTEGRAÇÃO FINALIZADA COM SUCESSO!")
        else:
            print(f"\n💥 INTEGRAÇÃO FALHOU - Verifique os logs acima")