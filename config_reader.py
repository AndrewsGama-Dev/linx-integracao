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
    Obtém configurações da API Humanus (APISOURCE).
    Suporta token fixo ou geração via credenciais (alias_name, user_name, password).
    """
    try:
        config = ler_config()
        if config and 'APISOURCE' in config:
            apisource = config['APISOURCE']
            token = apisource.get('token', '').strip('"').strip()
            return {
                'url_base': apisource.get('url_base', 'https://humanus.crsistemas.net.br/api/MALHECIDADES/COLABORADOR/colaborador/v2/exportar').strip(),
                'token': token if token and token.lower() not in ('', 'seu_token_aqui') else None,
                'url_token': apisource.get('url_token', 'https://humanus.crsistemas.net.br/api/Autenticacao/Autenticacao/Token').strip(),
                'alias_name': apisource.get('alias_name', '').strip(),
                'user_name': apisource.get('user_name', '').strip(),
                'password': apisource.get('password', '').strip(),
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
    Obtém os headers necessários para chamadas à API Humanus.
    Usa token fixo do .config ou gera token via credenciais (alias_name, user_name, password).
    """
    config = obter_config_api_humanus()
    if not config:
        return None
    
    token = config.get('token')
    
    # Se não tem token fixo, tenta gerar pelas credenciais
    if not token:
        alias = config.get('alias_name')
        user = config.get('user_name')
        pwd = config.get('password')
        url_token = config.get('url_token')
        if url_token and alias and user and pwd:
            try:
                from auth_humanus import gerar_token
                token = gerar_token(url_token, alias, user, pwd, usar_cache=True)
            except ImportError:
                print("❌ Módulo auth_humanus não encontrado")
    
    if not token:
        print("❌ Configure token ou credenciais (alias_name, user_name, password) em [APISOURCE]")
        return None
    
    headers = {
        'accept': 'text/plain',
        'Authorization': f'Bearer {token}'
    }
    
    return headers