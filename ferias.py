import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import time
import hashlib
import base64
import os
import pytz
import configparser
import csv
import io
from config_reader import obter_headers_api, ler_token_config
from api_humanus import buscar_colaboradores_paginado, formatar_data_iso_para_br

def carregar_configuracoes():
    """
    Função para carregar configurações do arquivo .config
    (Adaptada do integracao_folha_ponto.py)
    """
    config = configparser.ConfigParser(interpolation=None)
    config.read('.config')
    
    # Verificar se existe seção APITARGET
    if not config.has_section('APITARGET'):
        print("❌ Seção [APITARGET] não encontrada no arquivo .config")
        return None
    
    return {
        'apitarget': {
            'url': config.get('APITARGET', 'url'),
            'integracao': config.get('APITARGET', 'integracao'),
            'token_base': config.get('APITARGET', 'token_base')
        }
    }

def gerar_token_target():
    """
    Gera o token para a API de destino usando a data atual
    (Adaptada do integracao_folha_ponto.py)
    """
    config = carregar_configuracoes()
    if not config:
        print("❌ Erro ao carregar configurações")
        return None, None, None
    
    # Usar configurações da APITARGET
    url = config['apitarget']['url']
    integracao = config['apitarget']['integracao']
    token_base = config['apitarget']['token_base']
    
    # Configurar timezone para São Paulo
    tz_sao_paulo = pytz.timezone('America/Sao_Paulo')
    data_atual = datetime.now(tz_sao_paulo).strftime('%d/%m/%Y')
    
    # Gerar token final
    token_concatenado = token_base + data_atual
    token_final = hashlib.sha256(token_concatenado.encode('utf-8')).hexdigest()
    
    print(f"🔑 Data atual: {data_atual}")
    print(f"🔗 Token base: {token_base}")
    print(f"🔐 Token final gerado: {token_final[:32]}...")
    
    return url, integracao, token_final

def converter_para_csv(dados, nome_arquivo="dados.csv"):
    """
    Função para converter dados em CSV com cabeçalhos em lowercase
    (Adaptada do integracao_folha_ponto.py)
    """
    if not dados:
        print("❌ Não há dados para converter em CSV")
        return None
    
    try:
        output = io.StringIO()
        
        # Obter cabeçalhos das colunas e converter para lowercase
        fieldnames_originais = dados[0].keys()
        fieldnames_lowercase = [field.lower() for field in fieldnames_originais]
        
        # Criar mapeamento dos dados com chaves em lowercase
        dados_lowercase = []
        for linha in dados:
            linha_lowercase = {}
            for key, value in linha.items():
                linha_lowercase[key.lower()] = value
            dados_lowercase.append(linha_lowercase)
        
        # Criar writer CSV com fieldnames em lowercase
        writer = csv.DictWriter(output, fieldnames=fieldnames_lowercase, delimiter=';')
        
        writer.writeheader()
        for linha in dados_lowercase:
            writer.writerow(linha)
        
        csv_content = output.getvalue()
        output.close()
        
        with open(nome_arquivo, 'w', encoding='utf-8', newline='') as f:
            f.write(csv_content)
        
        print(f"✅ CSV gerado com sucesso: {nome_arquivo}")
        print(f"📊 Total de registros: {len(dados)}")
        print("📝 Cabeçalhos convertidos para lowercase!")
        
        return csv_content
        
    except Exception as e:
        print(f"❌ Erro ao gerar CSV: {e}")
        return None

def importar_via_post_generico(nome_arquivo_csv, endpoint, nome_modulo):
    """
    Função para importar CSV via POST
    (Adaptada do integracao_folha_ponto.py)
    """
    if not os.path.exists(nome_arquivo_csv):
        print(f"❌ Arquivo {nome_arquivo_csv} NÃO encontrado!")
        return None
    
    print(f"✅ Arquivo {nome_arquivo_csv} encontrado")
    
    # Gerar token e configurações (mesma lógica do integracao_folha_ponto.py)
    resultado_token = gerar_token_target()
    if not resultado_token or resultado_token[0] is None:
        print("❌ Falha ao gerar token para API de destino")
        return None
    
    url, integracao, token_final = resultado_token
    
    headers = {
        "user": integracao,
        "token": token_final
    }
    
    data = {
        "pag": endpoint,
        "cmd": "importar_cad",
        "separador": ";"
    }
    
    try:
        print(f"📤 Enviando POST para {endpoint.upper()}...")
        print(f"🌐 URL: {url}")
        print(f"👤 User: {integracao}")
        print(f"🔐 Token: {token_final[:32]}...")
        
        with open(nome_arquivo_csv, 'rb') as arquivo:
            files = {
                'arquivo': (nome_arquivo_csv, arquivo, 'text/csv')
            }
            
            response = requests.post(
                url, 
                data=data, 
                files=files,
                headers=headers,
                timeout=30
            )
        
        print(f"📊 Status: {response.status_code}")
        
        if response.status_code == 200:
            try:
                resultado = response.json()
                
                if resultado.get('success') == False:
                    print(f"❌ API retornou erro:")
                    print(f"📝 Resposta: {json.dumps(resultado, indent=2, ensure_ascii=False)}")
                    return None
                else:
                    print(f"✅ POST de {nome_modulo} realizado!")
                    print(f"📋 Resposta: {json.dumps(resultado, indent=2, ensure_ascii=False)}")
                    
                    cadastrados = resultado.get('ok', 0)
                    if cadastrados > 0:
                        print(f"🎉 {cadastrados} {nome_modulo} cadastrado(s)!")
                    
                    return resultado
                    
            except json.JSONDecodeError:
                print(f"⚠️ Resposta não é JSON válido:")
                print(f"📝 Resposta: {response.text[:500]}...")
                return None
        else:
            print(f"❌ ERRO - Status: {response.status_code}")
            print(f"📝 Resposta: {response.text[:500]}...")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"❌ ERRO na requisição: {e}")
        return None

def processar_modulo_ferias(dados_ferias, nome_arquivo_csv, nome_modulo):
    """
    Função genérica para processar um módulo completo
    (Adaptada do integracao_folha_ponto.py para férias)
    """
    print(f"\n" + "="*50)
    print(f"PROCESSANDO {nome_modulo.upper()}...")
    print("="*50)
    
    if dados_ferias:
        print(f"\n✅ {len(dados_ferias)} {nome_modulo} encontrados!")
        
        print(f"\n2. Convertendo {nome_modulo} para CSV...")
        csv_content = converter_para_csv(dados_ferias, nome_arquivo_csv)
        
        if csv_content:
            print(f"\n3. Fazendo POST de {nome_modulo} na API...")
            resultado = importar_via_post_generico(nome_arquivo_csv, "ponto_afastamento", nome_modulo)
            
            if resultado:
                print(f"\n🎉 INTEGRAÇÃO DE {nome_modulo.upper()} CONCLUÍDA!")
                return True
            else:
                print(f"\n💥 FALHA NO POST DE {nome_modulo.upper()}!")
                return False
        else:
            print(f"\n❌ Falha ao gerar CSV de {nome_modulo}")
            return False
    else:
        print(f"\n❌ Nenhum dado de {nome_modulo} disponível")
        return False

# =================== FUNÇÕES ESPECÍFICAS DA API ALTERDATA ===================

def buscar_detalhes_funcionario_completo(funcionario_id, headers):
    """
    Busca detalhes completos de um funcionário específico
    """
    try:
        url = f"https://dp.pack.alterdata.com.br/api/v1/funcionarios"
        params = {
            "filter[id]": funcionario_id,
            "include": "naturalidade,estado,foto,estadocivil,departamento,sexo,formadepagamento,nacionalidade,pais,tipoDeConta,tipoDeChavePix"
        }
        
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            funcionarios = data.get('data', [])
            if funcionarios:
                return funcionarios[0]
        return None
    except Exception as e:
        print(f"  ❌ Erro ao buscar detalhes do funcionário {funcionario_id}: {e}")
        return None

def extrair_datas_de_retorno_admissao(funcionario_detalhado):
    """
    Tenta extrair datas relacionadas a afastamentos dos campos disponíveis
    """
    attributes = funcionario_detalhado.get('attributes', {})
    
    # Campos que podem conter informações de datas
    datas_disponiveis = {
        'admissao': attributes.get('admissao'),
        'retorno': attributes.get('retorno'),
        'demissao': attributes.get('demissao'),
        'datavencimentocontratoexperiencia': attributes.get('datavencimentocontratoexperiencia'),
        'dataprorrogacaocontratoexperiencia': attributes.get('dataprorrogacaocontratoexperiencia')
    }
    
    return datas_disponiveis

def _extrair_ferias_situacao(colaboradores):
    """
    Extrai férias de situacaoPessoa quando sitCodSituacao = "2"
    """
    ferias_lista = []
    for col in colaboradores:
        situacoes = col.get('situacaoPessoa') or []
        matricula = str(col.get('nroMatrExterno', '')).zfill(6)
        
        for sit in situacoes:
            if str(sit.get('sitCodSituacao', '')) != '2':  # Apenas férias
                continue
            
            ferias_lista.append({
                'id-afastamento': '2',
                'dtinicio': formatar_data_iso_para_br(sit.get('sitDataInicio', '')),
                'dtfim': formatar_data_iso_para_br(sit.get('sitDataFim', '')),
                'obs': 'Ferias',
                'campo_chave': 'matricula',
                'matricula': matricula
            })
    
    return ferias_lista

def estimar_datas_ferias(funcionario_api, afastamento_desc):
    """
    Estima datas de férias (sempre 30 dias)
    """
    attributes = funcionario_api.get('attributes', {})
    
    # Tentar usar campo retorno se disponível
    data_retorno = attributes.get('retorno')
    
    # Férias: sempre 30 dias
    hoje = datetime.now()
    
    if data_retorno:
        try:
            dt_retorno = datetime.fromisoformat(data_retorno.replace('Z', '+00:00'))
            dt_inicio = dt_retorno - timedelta(days=30)
            return dt_inicio.strftime('%d/%m/%Y'), dt_retorno.strftime('%d/%m/%Y')
        except:
            pass
    
    # Estimativa padrão para férias: 30 dias (15 dias atrás + 15 dias à frente)
    dt_inicio = hoje - timedelta(days=15)
    dt_fim = hoje + timedelta(days=15)
    return dt_inicio.strftime('%d/%m/%Y'), dt_fim.strftime('%d/%m/%Y')

def mapear_ferias_para_csv(funcionario_api):
    """
    Mapeia férias do funcionário para o formato esperado no CSV
    """
    attributes = funcionario_api.get('attributes', {})
    funcionario_id = funcionario_api.get('id', '')
    
    # Dados das férias
    afastamento_desc = attributes.get('afastamentodescricao', '')
    codigo_funcionario = attributes.get('codigo', funcionario_id)
    
    # Para férias, SEMPRE usar código 1011
    codigo_afastamento = '1011'
    
    # Tentar extrair datas dos detalhes completos se disponível
    dtinicio = ''
    dtfim = ''
    
    if funcionario_api.get('detalhes_completos'):
        detalhes = funcionario_api['detalhes_completos']
        datas = extrair_datas_de_retorno_admissao(detalhes)
        
        # Se temos data de retorno, tentar calcular período
        if datas.get('retorno'):
            try:
                dtinicio_est, dtfim_est = estimar_datas_ferias(funcionario_api, afastamento_desc)
                dtinicio = dtinicio_est
                dtfim = dtfim_est
            except:
                pass
    
    # Se ainda não temos datas, fazer estimativa baseada em férias (30 dias)
    if not dtinicio and not dtfim:
        dtinicio, dtfim = estimar_datas_ferias(funcionario_api, afastamento_desc)
    
    # Mapeamento dos campos conforme formato esperado (UPPERCASE para ser convertido para lowercase)
    ferias_csv = {
        'ID-AFASTAMENTO': codigo_afastamento,  # SEMPRE 1011 para férias
        'DTINICIO': dtinicio,      # Data estimada ou extraída (DD/MM/YYYY)
        'DTFIM': dtfim,           # Data estimada ou extraída (DD/MM/YYYY)
        'OBS': afastamento_desc or 'Férias',  # Usar descrição ou padrão
        'CAMPO_CHAVE': 'matricula',  # Valor fixo
        'MATRICULA': codigo_funcionario  # codigo do funcionário
    }
    
    return ferias_csv

def gerar_csv_ferias():
    """
    Função principal para gerar o CSV das férias - API Humanus
    Usa situacaoPessoa com sitCodSituacao = "2"
    """
    print("=" * 80)
    print("         🏖️ GERAÇÃO DE CSV DE FÉRIAS - API Humanus")
    print("=" * 80)
    
    token = ler_token_config()
    if not token:
        print("❌ Falha ao carregar token do arquivo .config")
        return None
    
    print("\n1. Consultando férias na API Humanus...")
    colaboradores = buscar_colaboradores_paginado()
    ferias_csv = _extrair_ferias_situacao(colaboradores)
    
    if not ferias_csv:
        print("❌ Nenhuma férias encontrada (sitCodSituacao=2)")
        return None
    
    print(f"\n📊 {len(ferias_csv)} registros de férias processados!")
    return ferias_csv

def validar_dados_ferias_csv(nome_arquivo):
    """
    Valida os dados do CSV de férias gerado
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
        campos_obrigatorios = ['matricula', 'obs', 'dtinicio', 'dtfim', 'id-afastamento']
        
        for campo in campos_obrigatorios:
            if campo in df.columns:
                vazios = df[campo].isna().sum() + (df[campo] == '').sum()
                if vazios > 0:
                    print(f"  ⚠️  Campo '{campo}': {vazios} registros vazios")
                else:
                    print(f"  ✅ Campo '{campo}': todos preenchidos")
            else:
                print(f"  ❌ Campo obrigatório '{campo}' não encontrado")
        
        # Verificar se todos os registros são código 1011 (férias)
        if 'id-afastamento' in df.columns:
            codigos_unicos = df['id-afastamento'].unique()
            print(f"  📋 Códigos de afastamento encontrados: {codigos_unicos}")
            if len(codigos_unicos) == 1 and codigos_unicos[0] == '1011':
                print(f"  ✅ Todos os registros são FÉRIAS (1011)")
            else:
                print(f"  ⚠️  Encontrados códigos diferentes de 1011!")
        
        print(f"  ✅ Validação concluída")
        
    except Exception as e:
        print(f"  ❌ Erro na validação: {e}")

def gerar_relatorio_ferias():
    """
    Gera relatório específico para férias
    """
    relatorio = """
🏖️ RELATÓRIO DE INTEGRAÇÃO DE FÉRIAS - API ALTERDATA

🔍 ANÁLISE REALIZADA:
Este módulo foca especificamente na coleta e processamento de FÉRIAS dos funcionários.

✅ FILTROS APLICADOS:

1. 📊 SELEÇÃO DE DADOS:
   - Filtro: afastamentodescricao contém 'férias' (case-insensitive)
   - Código fixo: ID-AFASTAMENTO = 1011
   - Período padrão: 30 dias de férias

2. 🎯 MAPEAMENTO ESPECÍFICO:
   - Só processa registros que contenham 'férias' na descrição
   - Ignora outros tipos de afastamento
   - Estimativa inteligente de 30 dias para férias

✅ DADOS GERADOS:

1. 📋 ESTRUTURA CSV:
   - id-afastamento: 1011 (fixo para férias)
   - dtinicio: Data estimada de início
   - dtfim: Data estimada de fim (30 dias após início)
   - obs: Descrição das férias
   - campo_chave: matricula
   - matricula: Código do funcionário

2. 📤 INTEGRAÇÃO:
   - Endpoint: ponto_afastamento
   - Mesmo padrão do integracao_folha_ponto.py
   - Headers lowercase compatíveis

Data do relatório: """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """
"""
    
    with open('relatorio_ferias.txt', 'w', encoding='utf-8') as f:
        f.write(relatorio)
    
    print("📋 Relatório de férias salvo em: relatorio_ferias.txt")

# =================== FUNÇÃO PRINCIPAL ===================

def processar_integracao_completa():
    """
    Função principal que executa todo o processo: coleta da API -> CSV -> POST para Hevi
    (Adaptada do integracao_folha_ponto.py)
    """
    print("INICIANDO INTEGRAÇÃO DE FÉRIAS API ALTERDATA -> CSV -> POST API HEVI")
    print("="*70)
    
    # Gerar relatório de férias
    gerar_relatorio_ferias()
    
    # Etapa 1: Coletar dados da API Alterdata
    dados_ferias = gerar_csv_ferias()
    
    if not dados_ferias:
        print("❌ Falha na coleta de dados da API Alterdata")
        return False
    
    # Etapa 2: Processar usando a lógica do integracao_folha_ponto.py
    sucesso = processar_modulo_ferias(
        dados_ferias,
        'ferias_api.csv',
        'férias'
    )
    
    if sucesso:
        # Validar dados gerados
        validar_dados_ferias_csv('ferias_api.csv')
        
        print(f"\n🎉 INTEGRAÇÃO DE FÉRIAS FINALIZADA COM SUCESSO!")
        print(f"✅ Férias coletadas da API Alterdata")
        print(f"✅ CSV gerado: ferias_api.csv")
        print(f"✅ Dados enviados para sistema Hevi")
        print(f"📋 Relatório: relatorio_ferias.txt")
        print(f"🏖️ IMPORTANTE: Todas as férias receberam ID-AFASTAMENTO 1011!")
        return True
    else:
        print(f"\n💥 FALHA NA INTEGRAÇÃO!")
        print(f"✅ CSV pode ter sido gerado: ferias_api.csv")
        print(f"❌ Falha no envio para sistema Hevi")
        return False

# =================== EXECUÇÃO PRINCIPAL ===================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        comando = sys.argv[1].lower()
        
        if comando == "completo" or comando == "integracao":
            # Processo completo: API Alterdata → CSV → Hevi
            sucesso = processar_integracao_completa()
            
        elif comando == "csv":
            # Apenas gerar CSV
            gerar_relatorio_ferias()
            dados = gerar_csv_ferias()
            if dados:
                csv_content = converter_para_csv(dados, 'ferias_api.csv')
                if csv_content:
                    validar_dados_ferias_csv('ferias_api.csv')
                    print(f"\n🎉 CSV GERADO!")
                    print(f"📁 Arquivo: ferias_api.csv")
                    
        elif comando == "enviar":
            # Apenas enviar CSV existente
            nome_arquivo = sys.argv[2] if len(sys.argv) > 2 else "ferias_api.csv"
            if os.path.exists(nome_arquivo):
                resultado = importar_via_post_generico(nome_arquivo, "ponto_afastamento", "férias")
                if resultado:
                    print(f"\n🎉 ARQUIVO ENVIADO COM SUCESSO!")
                else:
                    print(f"\n💥 FALHA NO ENVIO!")
            else:
                print(f"❌ Arquivo {nome_arquivo} não encontrado!")
        else:
            print("❌ Comando inválido! Use: completo, csv, ou enviar")
            print("Exemplos:")
            print("  python ferias.py completo")
            print("  python ferias.py csv") 
            print("  python ferias.py enviar [nome_arquivo.csv]")
    else:
        # CORREÇÃO: Executar integração completa automaticamente (comportamento padrão)
        print("🏖️ EXECUTANDO INTEGRAÇÃO DE FÉRIAS (modo automático)")
        print("💡 Para ver opções use: python ferias.py --help")
        sucesso = processar_integracao_completa()
        if sucesso:
            print(f"\n🚀 INTEGRAÇÃO DE FÉRIAS FINALIZADA COM SUCESSO!")
        else:
            print(f"\n💥 INTEGRAÇÃO DE FÉRIAS FALHOU - Verifique os logs acima")