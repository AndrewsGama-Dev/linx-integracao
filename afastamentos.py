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
from api_humanus import buscar_colaboradores_paginado, buscar_situacoes, formatar_data_iso_para_br

def carregar_configuracoes():
    """Funcao para carregar configuracoes do arquivo .config"""
    config = configparser.ConfigParser(interpolation=None)
    config.read('.config')
    
    if not config.has_section('APITARGET'):
        print("Secao [APITARGET] nao encontrada no arquivo .config")
        return None
    
    return {
        'apitarget': {
            'url': config.get('APITARGET', 'url'),
            'integracao': config.get('APITARGET', 'integracao'),
            'token_base': config.get('APITARGET', 'token_base')
        }
    }

def gerar_token_target():
    """Gera o token para a API de destino usando a data atual"""
    config = carregar_configuracoes()
    if not config:
        print("Erro ao carregar configuracoes")
        return None, None, None
    
    url = config['apitarget']['url']
    integracao = config['apitarget']['integracao']
    token_base = config['apitarget']['token_base']
    
    tz_sao_paulo = pytz.timezone('America/Sao_Paulo')
    data_atual = datetime.now(tz_sao_paulo).strftime('%d/%m/%Y')
    
    token_concatenado = token_base + data_atual
    token_final = hashlib.sha256(token_concatenado.encode('utf-8')).hexdigest()
    
    return url, integracao, token_final

def converter_para_csv(dados, nome_arquivo="dados.csv"):
    """Funcao para converter dados em CSV com cabecalhos em lowercase"""
    if not dados:
        print("Nao ha dados para converter em CSV")
        return None
    
    try:
        output = io.StringIO()
        
        fieldnames_originais = dados[0].keys()
        fieldnames_lowercase = [field.lower() for field in fieldnames_originais]
        
        dados_lowercase = []
        for linha in dados:
            linha_lowercase = {}
            for key, value in linha.items():
                linha_lowercase[key.lower()] = value
            dados_lowercase.append(linha_lowercase)
        
        writer = csv.DictWriter(output, fieldnames=fieldnames_lowercase, delimiter=';')
        writer.writeheader()
        for linha in dados_lowercase:
            writer.writerow(linha)
        
        csv_content = output.getvalue()
        output.close()
        
        with open(nome_arquivo, 'w', encoding='utf-8', newline='') as f:
            f.write(csv_content)
        
        print(f"CSV gerado com sucesso: {nome_arquivo}")
        print(f"Total de registros: {len(dados)}")
        
        return csv_content
        
    except Exception as e:
        print(f"Erro ao gerar CSV: {e}")
        return None

def importar_via_post_generico(nome_arquivo_csv, endpoint, nome_modulo):
    """Funcao para importar CSV via POST"""
    if not os.path.exists(nome_arquivo_csv):
        print(f"Arquivo {nome_arquivo_csv} NAO encontrado!")
        return None
    
    resultado_token = gerar_token_target()
    if not resultado_token or resultado_token[0] is None:
        print("Falha ao gerar token para API de destino")
        return None
    
    url, integracao, token_final = resultado_token
    
    headers = {"user": integracao, "token": token_final}
    data = {"pag": endpoint, "cmd": "importar_cad", "separador": ";"}
    
    try:
        with open(nome_arquivo_csv, 'rb') as arquivo:
            files = {'arquivo': (nome_arquivo_csv, arquivo, 'text/csv')}
            response = requests.post(url, data=data, files=files, headers=headers, timeout=30)
        
        if response.status_code == 200:
            try:
                resultado = response.json()
                if resultado.get('success') == False:
                    print(f"API retornou erro: {json.dumps(resultado, indent=2, ensure_ascii=False)}")
                    return None
                else:
                    print(f"POST de {nome_modulo} realizado!")
                    cadastrados = resultado.get('ok', 0)
                    if cadastrados > 0:
                        print(f"{cadastrados} {nome_modulo} cadastrado(s)!")
                    return resultado
            except json.JSONDecodeError:
                print(f"Resposta nao eh JSON valido: {response.text[:500]}...")
                return None
        else:
            print(f"ERRO - Status: {response.status_code}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"ERRO na requisicao: {e}")
        return None

def processar_modulo_afastamentos(dados_afastamentos, nome_arquivo_csv, nome_modulo):
    """Funcao generica para processar um modulo completo"""
    print(f"\n" + "="*50)
    print(f"PROCESSANDO {nome_modulo.upper()}...")
    print("="*50)
    
    if dados_afastamentos:
        print(f"\n{len(dados_afastamentos)} {nome_modulo} encontrados!")
        
        csv_content = converter_para_csv(dados_afastamentos, nome_arquivo_csv)
        
        if csv_content:
            resultado = importar_via_post_generico(nome_arquivo_csv, "ponto_afastamento", nome_modulo)
            
            if resultado:
                print(f"\nINTEGRACAO DE {nome_modulo.upper()} CONCLUIDA!")
                return True
            else:
                print(f"\nFALHA NO POST DE {nome_modulo.upper()}!")
                return False
        else:
            return False
    else:
        print(f"\nNenhum dado de {nome_modulo} disponivel")
        return False

def _extrair_afastamentos_situacao(colaboradores):
    """
    Extrai afastamentos de situacaoPessoa.
    Ignora: sitCodSituacao 1 (ativo), 2 (férias), 3 (demissão)
    """
    afastamentos = []
    mapa_situacoes = buscar_situacoes()
    
    for col in colaboradores:
        situacoes = col.get('situacaoPessoa') or []
        matricula = str(col.get('nroMatrExterno', '')).zfill(6)
        
        for sit in situacoes:
            cod = str(sit.get('sitCodSituacao', '')).strip()
            cod_normalizado = cod.lstrip('0') or '0'  # "01"->"1", "02"->"2", "03"->"3"
            if cod_normalizado in ('1', '2', '3'):
                continue  # Ignorar ativo, férias, demissão (têm arquivos exclusivos)
            
            obs = mapa_situacoes.get(cod, f'Afastamento {cod}')
            afastamentos.append({
                'id-afastamento': cod,
                'dtinicio': formatar_data_iso_para_br(sit.get('sitDataInicio', '')),
                'dtfim': formatar_data_iso_para_br(sit.get('sitDataFim', '')),
                'obs': obs,
                'campo_chave': 'matricula',
                'matricula': matricula
            })
    
    return afastamentos

def extrair_datas_dos_campos_corretos(attributes):
    """
    FUNCAO CORRIGIDA: Extrai datas dos campos corretos identificados
    
    CAMPOS CORRETOS IDENTIFICADOS:
    - attributes['afastamento'] = Data de INICIO (2025-07-16T03:00:00Z)
    - attributes['retorno'] = Data de FIM (2025-07-18T03:00:00Z)
    """
    print(f"  Extraindo datas dos CAMPOS CORRETOS...")
    
    # BUSCAR CAMPOS CORRETOS
    campo_inicio = attributes.get('afastamento')  # Data de INICIO
    campo_fim = attributes.get('retorno')         # Data de FIM
    
    print(f"    Campo 'afastamento' (INICIO): {campo_inicio}")
    print(f"    Campo 'retorno' (FIM): {campo_fim}")
    
    # Verificar se temos ambos os campos
    if campo_inicio and campo_fim:
        try:
            # Converter datas ISO para formato DD/MM/YYYY
            dt_inicio = datetime.fromisoformat(campo_inicio.replace('Z', '+00:00'))
            dt_fim = datetime.fromisoformat(campo_fim.replace('Z', '+00:00'))
            
            data_inicio_fmt = dt_inicio.strftime('%d/%m/%Y')
            data_fim_fmt = dt_fim.strftime('%d/%m/%Y')
            
            print(f"    DATAS EXTRAIDAS: {data_inicio_fmt} ate {data_fim_fmt}")
            return data_inicio_fmt, data_fim_fmt, "CAMPOS_CORRETOS_API"
            
        except Exception as e:
            print(f"    Erro ao converter datas: {e}")
    
    # Se nao temos ambos, tentar pelo menos o retorno
    elif campo_fim:
        try:
            dt_fim = datetime.fromisoformat(campo_fim.replace('Z', '+00:00'))
            data_fim_fmt = dt_fim.strftime('%d/%m/%Y')
            
            print(f"    Apenas data FIM: {data_fim_fmt}")
            return None, data_fim_fmt, "APENAS_RETORNO"
            
        except Exception as e:
            print(f"    Erro ao converter data de retorno: {e}")
    
    print(f"    Campos de data nao encontrados")
    return None, None, "SEM_DATAS_API"

def gerar_csv_afastamentos_humanus():
    """Gera CSV de afastamentos a partir da API Humanus (situacaoPessoa)"""
    colaboradores = buscar_colaboradores_paginado()
    return _extrair_afastamentos_situacao(colaboradores)

def mapear_afastamento_para_csv(funcionario_api):
    """
    FUNCAO PRINCIPAL CORRIGIDA: Usar os campos corretos
    """
    attributes = funcionario_api.get('attributes', {})
    funcionario_id = funcionario_api.get('id', '')
    
    afastamento_desc = attributes.get('afastamentodescricao', '')
    codigo_funcionario = attributes.get('codigo', funcionario_id)
    
    # DEFINIR ID-AFASTAMENTO BASEADO NO CONTEUDO DO OBS
    obs_normalizada = afastamento_desc.lower().strip() if afastamento_desc else ''
    
    print(f"\nMapeando funcionario {codigo_funcionario}")
    print(f"    Descricao original: '{afastamento_desc}'")
    print(f"    Descricao normalizada: '{obs_normalizada}'")
    
    if 'ferias' in obs_normalizada or 'férias' in obs_normalizada or 'fÃ©rias' in obs_normalizada:
        codigo_afastamento = '1011'  # Ferias
        print(f"    >>> DETECTADO: Ferias -> ID 1011")
    elif 'atestado' in obs_normalizada:
        codigo_afastamento = '1012'  # Atestado
        print(f"    >>> DETECTADO: Atestado -> ID 1012")
    else:
        codigo_afastamento = '1012'  # Default para outros tipos de afastamento
        print(f"    >>> DEFAULT: Outro tipo -> ID 1012")
    
    print(f"    ID-Afastamento FINAL: {codigo_afastamento}")
    
    # USAR CAMPOS CORRETOS DIRETAMENTE
    dtinicio, dtfim, origem_data = extrair_datas_dos_campos_corretos(attributes)
    
    # Se nao conseguimos extrair
    if not dtinicio or not dtfim:
        print(f"    ERRO: Nao foi possivel obter datas dos campos")
        dtinicio = dtinicio or 'SEM_DATA_API'
        dtfim = dtfim or 'SEM_DATA_API'
        origem_data = 'ERRO_API'
    
    # Mapeamento final - OBS agora contem apenas a descricao
    afastamento_csv = {
        'ID-AFASTAMENTO': codigo_afastamento,
        'DTINICIO': dtinicio,
        'DTFIM': dtfim,
        'OBS': afastamento_desc if afastamento_desc else 'Afastamento',
        'CAMPO_CHAVE': 'matricula',
        'MATRICULA': codigo_funcionario
    }
    
    return afastamento_csv

def gerar_csv_afastamentos():
    """Gera CSV de afastamentos - API Humanus"""
    print("=" * 80)
    print("     GERACAO DE CSV DE AFASTAMENTOS - API Humanus")
    print("=" * 80)
    
    token = ler_token_config()
    if not token:
        print("Falha ao carregar token do arquivo .config")
        return None
    
    print("\n1. Consultando afastamentos na API Humanus...")
    afastamentos_csv = gerar_csv_afastamentos_humanus()
    
    if not afastamentos_csv:
        print("Nenhum afastamento foi encontrado (excluindo ativo, ferias, demissao)")
        return None
    
    print(f"\n   Total de registros de afastamento: {len(afastamentos_csv)}")
    return afastamentos_csv

def processar_integracao_completa():
    """FUNCAO PRINCIPAL CORRIGIDA"""
    print("INICIANDO INTEGRACAO FINAL CORRIGIDA")
    print("="*50)
    
    dados_afastamentos = gerar_csv_afastamentos()
    
    if not dados_afastamentos:
        print("Falha na coleta de dados")
        return False
    
    sucesso = processar_modulo_afastamentos(
        dados_afastamentos,
        'afastamentos_api.csv',
        'afastamentos'
    )
    
    if sucesso:
        print(f"\nINTEGRACAO FINAL CONCLUIDA!")
        print(f"CSV gerado: afastamentos_api.csv")
        
        # Mostrar todos os registros gerados
        try:
            df = pd.read_csv('afastamentos_api.csv', sep=';')
            print(f"\nREGISTROS GERADOS ({len(df)} total):")
            for i, row in df.iterrows():
                print(f"   {row['matricula']}: {row['dtinicio']} a {row['dtfim']} | {row['obs']}")
                
        except Exception as e:
            print(f"Erro ao ler CSV: {e}")
        
        return True
    else:
        print(f"\nFALHA NA INTEGRACAO!")
        return False

# =================== EXECUCAO PRINCIPAL ===================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        comando = sys.argv[1].lower()
        
        if comando == "completo" or comando == "integracao":
            sucesso = processar_integracao_completa()
            
        elif comando == "csv":
            dados = gerar_csv_afastamentos()
            if dados:
                csv_content = converter_para_csv(dados, 'afastamentos_api.csv')
                if csv_content:
                    print(f"\nCSV FINAL GERADO!")
                    print(f"Arquivo: afastamentos_api.csv")
                    
                    # Mostrar todos os registros
                    try:
                        df = pd.read_csv('afastamentos_api.csv', sep=';')
                        print(f"\nTODOS OS REGISTROS ({len(df)}):")
                        for i, row in df.iterrows():
                            print(f"   {row['matricula']}: {row['dtinicio']} a {row['dtfim']} | {row['obs']}")
                            
                    except Exception as e:
                        print(f"Erro ao analisar CSV: {e}")
                    
        elif comando == "enviar":
            nome_arquivo = sys.argv[2] if len(sys.argv) > 2 else "afastamentos_api.csv"
            if os.path.exists(nome_arquivo):
                resultado = importar_via_post_generico(nome_arquivo, "ponto_afastamento", "afastamentos")
                if resultado:
                    print(f"\nARQUIVO ENVIADO COM SUCESSO!")
                else:
                    print(f"\nFALHA NO ENVIO!")
            else:
                print(f"Arquivo {nome_arquivo} nao encontrado!")
                
        else:
            print("Comando invalido! Use:")
            print("  python afastamentos.py completo   # Integracao completa")
            print("  python afastamentos.py csv        # Apenas gerar CSV")
            print("  python afastamentos.py enviar [arquivo.csv]")
    else:
        print("EXECUTANDO INTEGRACAO FINAL CORRIGIDA")
        sucesso = processar_integracao_completa()
        if sucesso:
            print(f"\nPROBLEMA RESOLVIDO!")
        else:
            print(f"\nINTEGRACAO FALHOU")