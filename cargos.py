import requests
import json
import pandas as pd
from datetime import datetime
import time
import hashlib
import pytz
import configparser
from config_reader import obter_headers_api, ler_token_config
from api_humanus import buscar_colaboradores_paginado

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

def enviar_csv_para_api_target(nome_arquivo_csv):
    """
    Envia o CSV de cargos para a API de destino via POST
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
    
    # Usar 'gotech' como usuário
    usuario_correto = 'gotech'
    
    # Preparar headers e dados
    headers = {
        "user": usuario_correto,
        "token": token_final
    }
    
    data = {
        "pag": "configuracao_cargo",
        "cmd": "importar_cad",
        "separador": ";"
    }
    
    try:
        print(f"📤 Enviando POST para API de destino...")
        print(f"🌐 URL: {config_target['url']}")
        print(f"👤 Usuário: {usuario_correto}")
        print(f"📄 Endpoint: configuracao_cargo")
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
                    
                    if 'login' in str(resultado.get('info', '')).lower():
                        print(f"\n💡 SUGESTÕES PARA CORRIGIR ERRO DE LOGIN:")
                        print(f"1. ❌ Verificar se token_base está correto: '{config_target['token_base']}'")
                        print(f"2. ❌ Verificar formato da data (atual: {datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%d/%m/%Y')})")
                        print(f"3. ❌ Confirmar usuário correto (usando: '{usuario_correto}')")
                        print(f"4. ❌ Execute debug_token.py para mais detalhes")
                    
                    return False
                else:
                    print(f"✅ POST de cargos realizado com sucesso!")
                    print(f"📋 Resposta da API:")
                    print(json.dumps(resultado, indent=2, ensure_ascii=False))
                    
                    cadastrados = resultado.get('ok', 0)
                    if cadastrados > 0:
                        print(f"🎉 {cadastrados} cargo(s) cadastrado(s) com sucesso!")
                    
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
        print(f"❌ ERRO na requisição para API de destino: {e}")
        return False

def extrair_cargos_da_api_humanus():
    """
    Extrai cargos únicos a partir dos colaboradores da API Humanus.
    codigo_legado = pffCodCargo, nome = pffDescricaoCargo
    """
    print("🔍 INICIANDO COLETA DE CARGOS - API Humanus...")
    
    colaboradores = buscar_colaboradores_paginado()
    if not colaboradores:
        return {}
    
    cargos_unicos = {}
    for col in colaboradores:
        pff = col.get('pessoaFisFunc') or {}
        codigo = pff.get('pffCodCargo', '')
        nome = pff.get('pffDescricaoCargo', '')
        if codigo:
            if codigo not in cargos_unicos:
                cargos_unicos[codigo] = {'codigo': codigo, 'nome': nome or codigo}
    
    print(f"\n✅ Total de cargos únicos encontrados: {len(cargos_unicos)}")
    return cargos_unicos

def mapear_cargo_para_csv(codigo, dados_cargo, id_empresa='1'):
    """
    Mapeia um cargo para o formato esperado no CSV.
    id-empresa = "1" conforme especificação
    """
    cargo_csv = {
        'campo_chave': 'codigo_legado',
        'codigo_legado': codigo,
        'nome': dados_cargo.get('nome', codigo),
        'id-empresa': id_empresa,
        'nome_cbo': '',
        'nro_cbo': ''
    }
    return cargo_csv

def gerar_csv_cargos():
    """
    Função principal para gerar o CSV dos cargos - API Humanus
    """
    print("=" * 80)
    print("         💼 GERAÇÃO DE CSV DE CARGOS - API Humanus")
    print("=" * 80)
    
    token = ler_token_config()
    if not token:
        print("❌ Falha ao carregar token do arquivo .config")
        return None
    
    # Extrair cargos da API Humanus (pffCodCargo, pffDescricaoCargo)
    cargos_dict = extrair_cargos_da_api_humanus()
    
    if not cargos_dict:
        print("❌ Nenhum cargo foi extraído")
        return None
    
    print(f"\n🔄 Convertendo {len(cargos_dict)} cargos para formato CSV...")
    
    cargos_csv = []
    erros = []
    id_empresa = "1"  # Conforme especificação
    
    for codigo, dados_cargo in cargos_dict.items():
        try:
            cargo_csv = mapear_cargo_para_csv(codigo, dados_cargo, id_empresa)
            cargos_csv.append(cargo_csv)
        except Exception as e:
            erros.append({'codigo': codigo, 'erro': str(e)})
            print(f"  ❌ Erro ao processar cargo {codigo}: {e}")
    
    if not cargos_csv:
        print("❌ Nenhum cargo foi convertido com sucesso")
        return None
    
    # Criar DataFrame
    print(f"\n📊 Criando DataFrame com {len(cargos_csv)} cargos...")
    df = pd.DataFrame(cargos_csv)
    
    # Ordenar por código legado
    df = df.sort_values('codigo_legado')
    
    # Gerar arquivo CSV
    nome_arquivo = f"cargos_api.csv"
    
    try:
        df.to_csv(nome_arquivo, index=False, encoding='utf-8-sig', sep=';')
        print(f"✅ CSV gerado com sucesso: {nome_arquivo}")
        
        # Estatísticas
        print(f"\n📈 ESTATÍSTICAS:")
        print(f"  💼 Total de cargos processados: {len(cargos_csv)}")
        print(f"  ❌ Erros de conversão: {len(erros)}")
        print(f"  📋 Colunas no CSV: {len(df.columns)}")
        print(f"  🏢 id-empresa: {id_empresa}")
        print(f"  💾 Arquivo gerado: {nome_arquivo}")
        
        # Mostrar preview dos dados
        print(f"\n👁️  PREVIEW DOS DADOS (primeiras 5 linhas):")
        print(df.head(5).to_string())
        
        # Salvar relatório de erros se houver
        if erros:
            arquivo_erros = f"erros_cargos.json"
            with open(arquivo_erros, 'w', encoding='utf-8') as f:
                json.dump(erros, f, indent=2, ensure_ascii=False)
            print(f"\n⚠️  Relatório de erros salvo em: {arquivo_erros}")
        
        # Salvar dados detalhados das funções
        dados_detalhados = {
            'cargos_extraidos': cargos_dict,
            'id_empresa': id_empresa,
            'total_cargos': len(cargos_csv),
            'timestamp': datetime.now().isoformat()
        }
        
        with open('cargos_dados_detalhados.json', 'w', encoding='utf-8') as f:
            json.dump(dados_detalhados, f, indent=2, ensure_ascii=False)
        print(f"💾 Dados detalhados salvos em 'cargos_dados_detalhados.json'")
        
        # Verificar campos com dados
        print(f"\n🔍 ANÁLISE DE PREENCHIMENTO DOS CAMPOS:")
        for coluna in df.columns:
            valores_nao_vazios = df[coluna].notna().sum() - (df[coluna] == '').sum()
            percentual = (valores_nao_vazios / len(df)) * 100
            status = "✅" if percentual > 0 else "⭕"
            print(f"  {status} {coluna:<20}: {valores_nao_vazios:3d}/{len(df)} ({percentual:5.1f}%)")
        
        return nome_arquivo
        
    except Exception as e:
        print(f"❌ Erro ao gerar CSV: {e}")
        return None

def processar_integracao_completa():
    """
    Função principal que executa todo o processo: coleta da API -> CSV -> POST para destino
    """
    print("=" * 80)
    print("    🚀 INTEGRAÇÃO COMPLETA DE CARGOS - eContador → Sistema Destino")
    print("=" * 80)
    
    # Etapa 1: Gerar CSV dos cargos
    print("\n📋 ETAPA 1: Coletando cargos da API Humanus...")
    arquivo_csv = gerar_csv_cargos()
    
    if not arquivo_csv:
        print("❌ Falha na geração do CSV. Processo interrompido.")
        return False
    
    # Etapa 2: Validar dados
    print("\n🔍 ETAPA 2: Validando dados do CSV...")
    validar_dados_cargos_csv(arquivo_csv)
    
    # Etapa 3: Enviar para API de destino
    print("\n📤 ETAPA 3: Enviando CSV para API de destino...")
    sucesso_envio = enviar_csv_para_api_target(arquivo_csv)
    
    if sucesso_envio:
        print("\n🎉 INTEGRAÇÃO COMPLETA FINALIZADA COM SUCESSO!")
        print(f"✅ Cargos coletados da API Humanus")
        print(f"✅ CSV gerado: {arquivo_csv}")
        print(f"✅ Dados enviados para sistema de destino")
        return True
    else:
        print("\n💥 FALHA NA INTEGRAÇÃO!")
        print(f"✅ CSV gerado: {arquivo_csv}")
        print(f"❌ Falha no envio para sistema de destino")
        return False

def validar_dados_cargos_csv(nome_arquivo):
    """
    Valida os dados do CSV de cargos gerado
    """
    if not nome_arquivo:
        return
    
    try:
        print(f"\n🔍 VALIDANDO DADOS DO CSV: {nome_arquivo}")
        
        # Ler o CSV gerado
        df = pd.read_csv(nome_arquivo, sep=';', encoding='utf-8-sig')
        
        print(f"  📊 Total de registros: {len(df)}")
        print(f"  📋 Total de colunas: {len(df.columns)}")
        
        # Verificar campos obrigatórios
        campos_obrigatorios = ['codigo_legado', 'nome', 'campo_chave']
        
        for campo in campos_obrigatorios:
            if campo in df.columns:
                vazios = df[campo].isna().sum() + (df[campo] == '').sum()
                if vazios > 0:
                    print(f"  ⚠️  Campo '{campo}': {vazios} registros vazios")
                else:
                    print(f"  ✅ Campo '{campo}': todos preenchidos")
            else:
                print(f"  ❌ Campo obrigatório '{campo}' não encontrado")
        
        # Verificar duplicatas por código legado
        if 'codigo_legado' in df.columns:
            codigos_duplicados = df['codigo_legado'].duplicated().sum()
            if codigos_duplicados > 0:
                print(f"  ⚠️  Códigos legados duplicados: {codigos_duplicados}")
            else:
                print(f"  ✅ Nenhum código legado duplicado")
        
        # Estatísticas de preenchimento
        print(f"\n📊 ESTATÍSTICAS DE PREENCHIMENTO:")
        print(f"  💼 Cargos com nome preenchido: {(df['nome'] != '').sum()}")
        print(f"  🏢 Cargos com empresa definida: {(df['id-empresa'] != '').sum()}")
        
        print(f"  ✅ Validação concluída")
        
    except Exception as e:
        print(f"  ❌ Erro na validação: {e}")

# Exemplo de uso
if __name__ == "__main__":
    # Executar integração completa automaticamente
    sucesso = processar_integracao_completa()
    if sucesso:
        print(f"\n🚀 INTEGRAÇÃO FINALIZADA COM SUCESSO!")
    else:
        print(f"\n💥 INTEGRAÇÃO FALHOU - Verifique os logs acima")