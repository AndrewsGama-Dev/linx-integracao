import configparser
import os

def ler_config():
    """
    Lê o arquivo .config e retorna um dicionário com todas as seções
    """
    try:
        if not os.path.exists('.config'):
            print("❌ Arquivo .config não encontrado")
            return None
        
        config = configparser.ConfigParser()
        config.read('.config', encoding='utf-8')
        
        # Converter para dicionário para facilitar o uso
        config_dict = {}
        for secao in config.sections():
            config_dict[secao] = dict(config[secao])
        
        return config_dict
        
    except Exception as e:
        print(f"❌ Erro ao ler arquivo .config: {e}")
        return None

def ler_token_config():
    """
    Lê especificamente o token da seção APISOURCE (API Humanus)
    """
    try:
        config = ler_config()
        if config and 'APISOURCE' in config:
            token = config['APISOURCE'].get('token')
            if token:
                print("✅ Token carregado do arquivo .config")
                return token.strip('"').strip()  # Remove aspas e espaços
        
        print("❌ Token não encontrado na seção [APISOURCE]")
        return None
        
    except Exception as e:
        print(f"❌ Erro ao ler token: {e}")
        return None

def obter_config_api_humanus():
    """
    Obtém configurações da API Humanus (APISOURCE)
    """
    try:
        config = ler_config()
        if config and 'APISOURCE' in config:
            apisource = config['APISOURCE']
            return {
                'url_base': apisource.get('url_base', 'https://humanus.crsistemas.net.br/api/MALHECIDADES/COLABORADOR/colaborador/v2/exportar').strip(),
                'token': apisource.get('token', '').strip('"').strip(),
                'tamanho_pagina': int(apisource.get('tamanho_pagina', 50)),
                'url_situacao': apisource.get('url_situacao', 'https://humanus.crsistemas.net.br/api/MALHECIDADES/COLABORADOR/situacao/tudo').strip()
            }
        return None
    except Exception as e:
        print(f"❌ Erro ao carregar config API Humanus: {e}")
        return None

def obter_empresas_permitidas():
    """
    Retorna set com os códigos de empresas permitidas na integração.
    Aceita "004" ou "4" - normaliza para comparação com codEmpresa da API.
    Se não configurado ou vazio, retorna None (inclui todas as empresas).
    """
    try:
        config = ler_config()
        if config and 'EMPRESAS' in config:
            valor = config['EMPRESAS'].get('empresas_permitidas', '').strip()
            if not valor:
                return None
            # Suporta "004", "4" ou "004,005" - normaliza (004->4, 001->1)
            codigos = {c.strip().lstrip('0') or '0' for c in valor.split(',') if c.strip()}
            return codigos if codigos else None
    except Exception:
        pass
    return None

def obter_campo_chave_funcionarios():
    """
    Obtém o campo_chave configurado para funcionários (cpf ou matricula)
    """
    try:
        config = ler_config()
        if config and 'FUNCIONARIOS' in config:
            return config['FUNCIONARIOS'].get('campo_chave', 'cpf').strip().lower()
        return 'cpf'
    except Exception:
        return 'cpf'

def obter_headers_api():
    """
    Obtém os headers necessários para chamadas à API Humanus
    """
    config = obter_config_api_humanus()
    if not config or not config.get('token'):
        return None
    
    headers = {
        'accept': 'text/plain',
        'Authorization': f'Bearer {config["token"]}'
    }
    
    return headers