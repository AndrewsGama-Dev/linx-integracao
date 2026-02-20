#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para obter o token da API Humanus e gravar no arquivo .config.
Execute antes do integrador quando precisar atualizar o token.

Uso:
    python atualizar_token_config.py
    # ou
    ./atualizar_token_config.py
"""

import os
import sys
import configparser
import requests

def obter_credenciais():
    """Lê url_token, alias_name, user_name, password do .config"""
    if not os.path.exists('.config'):
        print("❌ Arquivo .config não encontrado")
        return None
    
    config = configparser.ConfigParser()
    config.read('.config', encoding='utf-8')
    
    if 'APISOURCE' not in config:
        print("❌ Seção [APISOURCE] não encontrada no .config")
        return None
    
    apisource = config['APISOURCE']
    url_token = apisource.get('url_token', '').strip()
    alias_name = apisource.get('alias_name', '').strip()
    user_name = apisource.get('user_name', '').strip()
    password = apisource.get('password', '').strip()
    
    if not all([url_token, alias_name, user_name, password]):
        print("❌ Preencha url_token, alias_name, user_name e password em [APISOURCE]")
        return None
    
    return {
        'url_token': url_token,
        'alias_name': alias_name,
        'user_name': user_name,
        'password': password
    }


def gerar_token(credenciais):
    """Faz POST na API Humanus e retorna o token"""
    headers = {'Content-Type': 'application/json', 'Accept': '*/*'}
    payload = {
        'aliasName': credenciais['alias_name'],
        'userName': credenciais['user_name'],
        'password': credenciais['password']
    }
    
    try:
        print("🔑 Obtendo token da API Humanus...")
        response = requests.post(
            credenciais['url_token'],
            json=payload,
            headers=headers,
            timeout=30
        )
        
        if response.status_code != 200:
            print(f"❌ Erro: HTTP {response.status_code}")
            return None
        
        dados = response.json()
        token = None
        if isinstance(dados, dict):
            token = dados.get('token') or dados.get('access_token') or dados.get('Token')
        elif isinstance(dados, str):
            token = dados
        
        if not token:
            print("❌ Resposta da API não contém token")
            return None
        
        return token
    except Exception as e:
        print(f"❌ Erro na requisição: {e}")
        return None


def gravar_token_no_config(token):
    """Atualiza o token na seção [APISOURCE] do .config"""
    config = configparser.ConfigParser()
    config.read('.config', encoding='utf-8')
    
    if 'APISOURCE' not in config:
        print("❌ Seção [APISOURCE] não encontrada")
        return False
    
    config['APISOURCE']['token'] = token
    
    try:
        with open('.config', 'w', encoding='utf-8') as f:
            config.write(f)
        print("✅ Token gravado no arquivo .config")
        return True
    except Exception as e:
        print(f"❌ Erro ao gravar .config: {e}")
        return False


def main():
    print("=" * 60)
    print("  ATUALIZAR TOKEN NO .CONFIG - API Humanus")
    print("=" * 60)
    
    credenciais = obter_credenciais()
    if not credenciais:
        sys.exit(1)
    
    token = gerar_token(credenciais)
    if not token:
        sys.exit(1)
    
    if gravar_token_no_config(token):
        print("\n✅ Pronto! Execute ./integrador.sh para rodar a integração.")
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == '__main__':
    main()
