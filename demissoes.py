import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import time
import configparser
import xml.etree.ElementTree as ET
import os
from config_reader import obter_headers_api
from api_humanus import buscar_colaboradores_paginado, formatar_data_iso_para_br

try:
    from cache_db import get_demissoes_ja_enviadas, registrar_demissao_enviada
except ImportError:
    get_demissoes_ja_enviadas = lambda: set()
    registrar_demissao_enviada = lambda m, d, n='': None

def carregar_configuracoes_soap():
    """
    Função para carregar configurações SOAP do arquivo .config
    """
    config = configparser.ConfigParser(interpolation=None)
    config.read('.config')
    
    if not config.has_section('SOAP'):
        print("❌ Seção [SOAP] não encontrada no arquivo .config")
        return None
    
    return {
        'url': config.get('SOAP', 'url'),
        'client_id': config.get('SOAP', 'client_id'),
        'usuario': config.get('SOAP', 'usuario'),
        'senha': config.get('SOAP', 'senha')
    }

def _extrair_demissoes_situacao(colaboradores):
    """
    Extrai demissões de situacaoPessoa quando sitCodSituacao = "3"
    """
    demissoes_lista = []
    for col in colaboradores:
        situacoes = col.get('situacaoPessoa') or []
        matricula = str(col.get('nroMatrExterno', '')).zfill(6)
        nome = col.get('nomeExtenso', '')
        
        for sit in situacoes:
            if str(sit.get('sitCodSituacao', '')) != '3':  # Apenas demissão
                continue
            
            sit_data = sit.get('sitDataInicio', '')
            demissoes_lista.append({
                'matricula': matricula,
                'data_demissao_iso': sit_data,
                'data_demissao': formatar_data_iso_para_br(sit_data),
                'obs': 'Demissao',
                'nome': nome
            })
    
    return demissoes_lista

def buscar_funcionario_matricula(funcionario_id, headers):
    """
    Busca a matrícula (código) do funcionário através do ID
    ATUALIZADO: Agora os dados já vêm completos da consulta principal
    """
    # Não precisa mais buscar, os dados já vêm na consulta principal
    return str(funcionario_id).zfill(6)

def formatar_data_brasileira(data_iso):
    """
    Converte data ISO para formato brasileiro DD/MM/AAAA
    """
    if not data_iso:
        return ""
    
    try:
        # Remove timezone e converte
        data_str = data_iso.replace('Z', '').split('T')[0]
        data_obj = datetime.strptime(data_str, '%Y-%m-%d')
        return data_obj.strftime('%d/%m/%Y')
    except:
        return ""

def calcular_datas_demissao(data_demissao_iso):
    """
    Calcula datas estimadas baseadas na data real de demissão da API
    """
    if not data_demissao_iso:
        # Se não tem data, usar data atual como base
        hoje = datetime.now()
        data_demissao = hoje.strftime('%d/%m/%Y')
        data_aviso = (hoje - timedelta(days=30)).strftime('%d/%m/%Y')
        data_ultimo_dia = hoje.strftime('%d/%m/%Y')
        data_acerto = (hoje + timedelta(days=10)).strftime('%d/%m/%Y')
        return data_demissao, data_aviso, data_ultimo_dia, data_acerto
    
    try:
        # Converter data ISO para datetime
        data_obj = datetime.fromisoformat(data_demissao_iso.replace('Z', '+00:00'))
        
        # Usar a data real de demissão
        data_demissao = data_obj.strftime('%d/%m/%Y')
        data_aviso = (data_obj - timedelta(days=30)).strftime('%d/%m/%Y')  # 30 dias antes
        data_ultimo_dia = data_obj.strftime('%d/%m/%Y')  # Mesmo dia da demissão
        data_acerto = (data_obj + timedelta(days=10)).strftime('%d/%m/%Y')  # 10 dias após
        
        return data_demissao, data_aviso, data_ultimo_dia, data_acerto
    except:
        # Fallback se der erro na conversão
        hoje = datetime.now()
        data_demissao = hoje.strftime('%d/%m/%Y')
        data_aviso = (hoje - timedelta(days=30)).strftime('%d/%m/%Y')
        data_ultimo_dia = hoje.strftime('%d/%m/%Y')
        data_acerto = (hoje + timedelta(days=10)).strftime('%d/%m/%Y')
        return data_demissao, data_aviso, data_ultimo_dia, data_acerto

def mapear_demissao_humanus_para_csv(demissao_dict):
    """
    Mapeia demissão da API Humanus para formato CSV (compatível com SOAP)
    """
    data_demissao, data_aviso, data_ultimo_dia, data_acerto = calcular_datas_demissao(
        demissao_dict.get('data_demissao_iso', '')
    )
    
    return {
        'matricula': demissao_dict.get('matricula', ''),
        'DATA_DEMISSAO': data_demissao,
        'obs': demissao_dict.get('obs', 'Demissao'),
        'nome': demissao_dict.get('nome', ''),
        'data_aviso': data_aviso,
        'data_ultimo_dia_trabalhado': data_ultimo_dia,
        'data_acerto': data_acerto,
        'motivo': 'Demissão',
        'local_exame': '',
        'opcao_empregado': '',
        'tipo_aviso': 'Indenizado',
        'devolveu_cracha': 'Sim',
        'dias_indenizados': 0,
        'data_exame': ''
    }

def filtrar_demissoes_recentes(funcionarios_demitidos, data_limite='2025-01-01'):
    """
    Filtra demissões a partir de uma data específica
    ATUALIZADO: Agora trabalha com funcionários demitidos diretamente
    """
    demissoes_filtradas = []
    data_limite_obj = datetime.strptime(data_limite, '%Y-%m-%d')
    
    for funcionario in funcionarios_demitidos:
        attributes = funcionario.get('attributes', {})
        data_demissao = attributes.get('demissao', '')
        
        if data_demissao:
            try:
                data_demissao_obj = datetime.fromisoformat(data_demissao.replace('Z', '+00:00'))
                data_demissao_sem_tz = data_demissao_obj.replace(tzinfo=None)
                
                if data_demissao_sem_tz >= data_limite_obj:
                    demissoes_filtradas.append(funcionario)
            except:
                # Se der erro na conversão, incluir mesmo assim
                demissoes_filtradas.append(funcionario)
    
    return demissoes_filtradas

# =================== FUNÇÕES SOAP ===================

def construir_xml_demissao(matricula, data_demissao, soap_config):
    """Constrói o XML de demissão no formato SOAP para um único funcionário"""
    soap_xml = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:urn="urn:ifPonto">
    <soapenv:Header/>
    <soapenv:Body>
        <urn:demissao>
            <urn:pack>
                <urn:clientId>{soap_config['client_id']}</urn:clientId>
                <urn:user>{soap_config['usuario']}</urn:user>
                <urn:pass>{soap_config['senha']}</urn:pass>
                <urn:funcionario>
                    <urn:matricula>{matricula}</urn:matricula>
                    <urn:dtdemissao>{data_demissao}</urn:dtdemissao>
                </urn:funcionario>
            </urn:pack>
        </urn:demissao>
    </soapenv:Body>
</soapenv:Envelope>"""
    return soap_xml

def enviar_demissao_soap(xml_data, soap_url):
    """Envia o XML para o webservice SOAP"""
    headers = {'Content-Type': 'text/xml; charset=utf-8'}
    try:
        response = requests.post(
            soap_url,
            data=xml_data,
            headers=headers,
            timeout=10
        )
        return response
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro na comunicação com o webservice SOAP: {str(e)}")
        return None

def salvar_xml_demissao(xml_data, matricula, tipo="request"):
    """Salva o XML de demissão localmente para registro"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"demissao_{tipo}_{matricula}_{timestamp}.xml"
    
    # Criar diretório se não existir
    os.makedirs('logs_demissao', exist_ok=True)
    filepath = os.path.join('logs_demissao', filename)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(xml_data)
    
    print(f"📄 XML de demissão ({tipo}) salvo em: {filepath}")
    return filepath

def analisar_resposta_soap(resposta_xml):
    """
    Analisa a resposta XML do SOAP para determinar se foi bem-sucedida
    """
    try:
        # Parse do XML
        root = ET.fromstring(resposta_xml)
        
        # Namespaces baseados na resposta real
        namespaces = {
            'soap-env': 'http://schemas.xmlsoap.org/soap/envelope/',
            'ns1': 'urn:ifPonto'
        }
        
        # Procurar por SOAP Fault primeiro
        soap_fault = root.find('.//soap-env:Fault', namespaces) or root.find('.//Fault')
        if soap_fault is not None:
            fault_string = soap_fault.find('faultstring')
            fault_msg = fault_string.text if fault_string is not None else "Erro SOAP desconhecido"
            return False, f"SOAP Fault: {fault_msg}"
        
        # Procurar por ResultArray e result
        result_array = root.find('.//ns1:ResultArray', namespaces)
        if result_array is not None:
            results = result_array.findall('ns1:result', namespaces)
            
            if results:
                for result in results:
                    # Procurar por descrição
                    descricao_elem = result.find('ns1:descricao', namespaces)
                    if descricao_elem is not None:
                        descricao = descricao_elem.text
                        
                        if descricao:
                            descricao_lower = descricao.lower()
                            
                            # Indicadores de sucesso
                            sucessos = ['sucesso', 'ok', 'processado', 'realizado', 'concluido', 'gravado', 'salvo', 'demitido']
                            if any(palavra in descricao_lower for palavra in sucessos):
                                return True, descricao
                            
                            # Indicadores de erro
                            erros = ['erro', 'falha', 'inválido', 'negado', 'não encontrado', 'já existe']
                            if any(palavra in descricao_lower for palavra in erros):
                                return False, descricao
                    
                    # Procurar por outros campos
                    for campo in ['ns1:status', 'ns1:codigo', 'ns1:retorno']:
                        elem = result.find(campo, namespaces)
                        if elem is not None:
                            valor = elem.text
                            
                            if valor and valor.lower() in ['ok', 'sucesso', '1', 'true', 'sim']:
                                return True, valor
                            elif valor and valor.lower() in ['erro', 'falha', '0', 'false', 'nao', 'não']:
                                return False, valor
                
                return True, "Resposta processada sem erros aparentes"
        
        # Procurar qualquer elemento que possa indicar resultado
        for elem in root.iter():
            tag_name = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
            if elem.text and any(campo in tag_name.lower() for campo in ['result', 'response', 'return']):
                
                if elem.text:
                    texto_lower = elem.text.lower()
                    if any(palavra in texto_lower for palavra in ['sucesso', 'ok', 'processado']):
                        return True, elem.text
                    elif any(palavra in texto_lower for palavra in ['erro', 'falha', 'inválido']):
                        return False, elem.text
        
        return True, "Status indeterminado - XML válido sem SOAP Fault"
            
    except ET.ParseError as e:
        return False, f"Erro de parse XML: {e}"
    except Exception as e:
        return False, f"Erro na análise: {e}"

def enviar_demissoes_via_soap(demissoes_csv):
    """
    Envia as demissões via SOAP
    """
    print("\n" + "="*60)
    print("📤 ENVIANDO DEMISSÕES VIA SOAP")
    print("="*60)
    
    # Carregar configurações SOAP
    soap_config = carregar_configuracoes_soap()
    if not soap_config:
        print("❌ Falha ao carregar configurações SOAP")
        return False
    
    print(f"🔧 Configurações SOAP:")
    print(f"   URL: {soap_config['url']}")
    print(f"   Client ID: {soap_config['client_id']}")
    print(f"   Usuário: {soap_config['usuario']}")
    
    sucessos = 0
    erros = 0
    
    print(f"\n📤 Processando {len(demissoes_csv)} demissões via SOAP...")
    print("-" * 50)
    
    for i, demissao in enumerate(demissoes_csv, 1):
        matricula = demissao.get('matricula')
        data_demissao = demissao.get('DATA_DEMISSAO')
        
        if not matricula or not data_demissao:
            print(f"❌ Demissão {i}: Dados incompletos - Matrícula: {matricula}, Data: {data_demissao}")
            erros += 1
            continue
        
        print(f"\n📤 Processando demissão {i}/{len(demissoes_csv)}:")
        print(f"   Matrícula: {matricula}")
        print(f"   Data: {data_demissao}")
        
        # Construir XML de requisição
        xml_demissao = construir_xml_demissao(matricula, data_demissao, soap_config)
        
        # Salvar XML da requisição
        salvar_xml_demissao(xml_demissao, matricula, "request")
        
        # Enviar via SOAP
        resposta = enviar_demissao_soap(xml_demissao, soap_config['url'])
        
        if resposta and resposta.status_code == 200:
            print(f"✅ Requisição enviada com sucesso!")
            print(f"📊 Status HTTP: {resposta.status_code}")
            
            # Salvar XML da resposta
            salvar_xml_demissao(resposta.text, matricula, "response")
            
            # Analisar a resposta XML
            sucesso, mensagem = analisar_resposta_soap(resposta.text)
            
            if sucesso:
                sucessos += 1
                registrar_demissao_enviada(matricula, data_demissao, demissao.get('nome', ''))
                print(f"🎉 Demissão da matrícula {matricula} processada com sucesso!")
                print(f"✅ Mensagem: {mensagem}")
            else:
                print(f"❌ Erro no processamento da matrícula {matricula}")
                print(f"❌ Mensagem: {mensagem}")
                erros += 1
                
        else:
            print(f"❌ Erro ao enviar demissão {i}")
            if resposta:
                print(f"Status HTTP: {resposta.status_code}")
                print(f"Resposta: {resposta.text[:200]}...")
            erros += 1
        
        print("-" * 30)
        time.sleep(1)  # Pausa entre requisições
    
    # Resumo final
    print(f"\n📊 RESUMO DO ENVIO SOAP:")
    print(f"✅ Sucessos: {sucessos}")
    print(f"❌ Erros: {erros}")
    print(f"📊 Total processadas: {len(demissoes_csv)}")
    
    return sucessos > 0

# =================== FUNÇÃO PRINCIPAL ===================

def gerar_csv_demissoes():
    """
    Função principal para gerar o CSV das demissões - API Humanus
    Usa situacaoPessoa com sitCodSituacao = "3"
    """
    print("=" * 80)
    print("         📋 GERAÇÃO DE CSV DE DEMISSÕES - API Humanus")
    print("=" * 80)
    
    headers = obter_headers_api()
    if not headers:
        print("❌ Falha ao carregar token (configure token ou credenciais em [APISOURCE])")
        return None
    
    colaboradores = buscar_colaboradores_paginado()
    demissoes_raw = _extrair_demissoes_situacao(colaboradores)
    
    if not demissoes_raw:
        print("❌ Nenhuma demissão encontrada (sitCodSituacao=3)")
        return None
    
    # Filtrar demissões já enviadas (histórico)
    ja_enviadas = get_demissoes_ja_enviadas()
    demissoes_novas = []
    for d in demissoes_raw:
        chave = (d.get('matricula', ''), d.get('data_demissao', ''))
        if chave not in ja_enviadas:
            demissoes_novas.append(d)
    
    if ja_enviadas:
        print(f"📋 Demissões já enviadas (histórico): {len(ja_enviadas)}")
    print(f"📋 Demissões novas a processar: {len(demissoes_novas)}")
    
    if not demissoes_novas:
        print("✅ Nenhuma demissão nova para processar - todas já foram enviadas")
        return []  # Sucesso: não há nada a fazer
    
    demissoes_raw = demissoes_novas
    
    print(f"\n🔄 Convertendo {len(demissoes_raw)} demissões para formato CSV...")
    
    demissoes_csv = []
    erros = []
    
    for i, demissao_dict in enumerate(demissoes_raw, 1):
        try:
            demissao_csv = mapear_demissao_humanus_para_csv(demissao_dict)
            if demissao_csv['matricula']:
                demissoes_csv.append(demissao_csv)
            if i % 20 == 0:
                print(f"  ✅ Processadas {i}/{len(demissoes_raw)} demissões...")
        except Exception as e:
            erros.append({'matricula': demissao_dict.get('matricula', 'N/A'), 'erro': str(e)})
    
    if not demissoes_csv:
        print("❌ Nenhuma demissão foi convertida com sucesso")
        return None
    
    # Criar DataFrame
    print(f"\n📊 Criando DataFrame com {len(demissoes_csv)} demissões...")
    
    df = pd.DataFrame(demissoes_csv)
    
    # Gerar arquivo CSV
    nome_arquivo = "demissoes_api.csv"
    
    try:
        df.to_csv(nome_arquivo, index=False, encoding='utf-8-sig', sep=';')
        print(f"✅ CSV gerado com sucesso: {nome_arquivo}")
        
        # Estatísticas
        print(f"\n📈 ESTATÍSTICAS:")
        print(f"  📋 Total de demissões: {len(demissoes_csv)}")
        print(f"  👥 Total de registros: {len(demissoes_csv)}")
        print(f"  ❌ Erros de conversão: {len(erros)}")
        print(f"  📋 Colunas no CSV: {len(df.columns)}")
        print(f"  💾 Arquivo gerado: {nome_arquivo}")
        
        # Aviso sobre datas estimadas
        print(f"\n⚠️  ATENÇÃO:")
        print(f"  📅 As datas foram ESTIMADAS baseadas na data de solicitação")
        print(f"  ✏️  Recomenda-se verificar e ajustar as datas conforme necessário")
        print(f"  📋 Dados baseados apenas nas notificações de rescisão da API")
        
        # Mostrar preview dos dados
        print(f"\n👁️  PREVIEW DOS DADOS (primeiras 3 linhas):")
        print(df.head(3).to_string())
        
        # Salvar relatório de erros se houver
        if erros:
            arquivo_erros = "erros_demissoes.json"
            with open(arquivo_erros, 'w', encoding='utf-8') as f:
                json.dump(erros, f, indent=2, ensure_ascii=False)
            print(f"\n⚠️  Relatório de erros salvo em: {arquivo_erros}")
        
        return demissoes_csv  # Retornar dados para uso no SOAP
        
    except Exception as e:
        print(f"❌ Erro ao gerar CSV: {e}")
        return None

def validar_dados_demissoes_csv(nome_arquivo):
    """
    Valida os dados do CSV de demissões gerado
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
        campos_obrigatorios = ['matricula', 'DATA_DEMISSAO']
        
        for campo in campos_obrigatorios:
            if campo in df.columns:
                vazios = df[campo].isna().sum() + (df[campo] == '').sum()
                if vazios > 0:
                    print(f"  ⚠️  Campo '{campo}': {vazios} registros vazios")
                else:
                    print(f"  ✅ Campo '{campo}': todos preenchidos")
            else:
                print(f"  ❌ Campo obrigatório '{campo}' não encontrado")
        
        # Verificar consistência de datas
        campos_data = ['DATA_DEMISSAO', 'data_aviso', 'data_ultimo_dia_trabalhado', 'data_acerto']
        for campo in campos_data:
            if campo in df.columns:
                registros_com_data = (df[campo] != '').sum()
                print(f"  📅 {campo}: {registros_com_data} registros com data")
        
        # Verificar funcionários únicos
        if 'matricula' in df.columns:
            funcionarios_unicos = df['matricula'].nunique()
            print(f"  👥 Funcionários únicos demitidos: {funcionarios_unicos}")
        
        print(f"  ✅ Validação concluída")
        
    except Exception as e:
        print(f"  ❌ Erro na validação: {e}")

def processar_integracao_completa():
    """
    Função principal que executa todo o processo: API → CSV → SOAP
    """
    print("=" * 80)
    print("    🚀 INTEGRAÇÃO COMPLETA DE DEMISSÕES - eContador → CSV → SOAP")
    print("=" * 80)
    
    # Etapa 1: Gerar CSV das demissões
    print("\n📋 ETAPA 1: Coletando demissões da API Humanus...")
    demissoes_csv = gerar_csv_demissoes()
    
    if demissoes_csv is None:
        print("❌ Falha na geração dos dados. Processo interrompido.")
        return False
    
    if demissoes_csv == []:
        print("\n🎉 PROCESSO CONCLUÍDO COM SUCESSO!")
        print("✅ Todas as demissões já foram processadas anteriormente - nada a fazer.")
        return True
    
    # Etapa 2: Validar dados do CSV
    print("\n🔍 ETAPA 2: Validando dados...")
    validar_dados_demissoes_csv("demissoes_api.csv")
    
    # Etapa 3: Enviar via SOAP
    print("\n📤 ETAPA 3: Enviando demissões via SOAP...")
    sucesso_soap = enviar_demissoes_via_soap(demissoes_csv)
    
    if sucesso_soap:
        print("\n🎉 INTEGRAÇÃO COMPLETA FINALIZADA COM SUCESSO!")
        print(f"✅ Demissões coletadas da API Humanus")
        print(f"✅ CSV gerado: demissoes_api.csv")
        print(f"✅ Demissões enviadas via SOAP")
        print(f"📁 XMLs salvos em: logs_demissao/")
        return True
    else:
        print("\n💥 FALHA NA INTEGRAÇÃO!")
        print(f"✅ CSV gerado: demissoes_api.csv")
        print(f"❌ Falha no envio via SOAP")
        return False

# Exemplo de uso
if __name__ == "__main__":
    # EXECUTAR AUTOMATICAMENTE O PROCESSO COMPLETO
    print("🚀 Executando integração completa de demissões...")
    sucesso = processar_integracao_completa()
    
    if sucesso:
        print("\n✅ Integração finalizada com sucesso!")
    else:
        print("\n❌ Integração finalizada com erros!")