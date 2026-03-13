# -*- coding: utf-8 -*-
"""
Geração de token para a API Humanus.
Envia POST para o endpoint de autenticação com aliasName, userName e password.
O token gerado é usado no header Authorization: Bearer <token>.
"""

import requests
import json
import os

# Cache em arquivo para evitar requisições repetidas (token costuma ser estável)
_TOKEN_CACHE_FILE = '.token_humanus'


def _ler_token_cache():
    """Lê token do cache em disco, se existir."""
    try:
        if os.path.exists(_TOKEN_CACHE_FILE):
            with open(_TOKEN_CACHE_FILE, 'r', encoding='utf-8') as f:
                return f.read().strip()
    except Exception:
        pass
    return None


def _salvar_token_cache(token):
    """Salva token no cache em disco."""
    try:
        with open(_TOKEN_CACHE_FILE, 'w', encoding='utf-8') as f:
            f.write(token)
    except Exception:
        pass


def gerar_token(url_token, alias_name, user_name, password, usar_cache=True):
    """
    Gera token da API Humanus via POST.
    
    Args:
        url_token: URL do endpoint (ex: .../api/Autenticacao/Autenticacao/Token)
        alias_name: Alias do cliente (ex: POSTOS_MAHLE-PRD)
        user_name: Usuário (ex: API)
        password: Senha
        usar_cache: Se True, usa token em cache quando disponível
    
    Returns:
        str: Token JWT ou None em caso de erro
    """
    if not all([url_token, alias_name, user_name, password]):
        print("❌ Credenciais incompletas para gerar token (url_token, alias_name, user_name, password)")
        return None
    
    if usar_cache:
        cached = _ler_token_cache()
        if cached:
            return cached
    
    headers = {
        'accept': '*/*',
        'Content-Type': 'application/json'
    }
    payload = {
        'aliasName': alias_name,
        'userName': user_name,
        'password': password
    }
    
    try:
        print("🔑 Gerando token da API Humanus...")
        response = requests.post(url_token, json=payload, headers=headers, timeout=30)
        
        texto = response.text.strip()
        
        if response.status_code != 200:
            print(f"❌ Erro ao gerar token: HTTP {response.status_code}")
            if texto:
                print(f"   Resposta: {texto[:300]}...")
            return None
        
        if not texto:
            print("❌ Resposta vazia da API")
            return None
        
        # Tenta JSON ou token em texto puro
        token = None
        try:
            dados = json.loads(texto)
            if isinstance(dados, dict):
                token = dados.get('token') or dados.get('access_token') or dados.get('Token')
            elif isinstance(dados, str):
                token = dados
        except json.JSONDecodeError:
            if texto.startswith('eyJ') or (len(texto) > 50 and '"' not in texto[:10]):
                token = texto
            else:
                print(f"❌ Resposta inesperada: {repr(texto[:200])}...")
                return None
        
        if token:
            _salvar_token_cache(token)
            print("✅ Token gerado com sucesso")
            return token
        
        print("❌ Resposta da API não contém token")
        return None
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro na requisição de token: {e}")
        return None
