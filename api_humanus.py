# -*- coding: utf-8 -*-
"""
Módulo central para buscar dados da API Humanus.
Todas as informações (cargos, departamentos, funcionários, afastamentos, férias, demissões)
são obtidas da mesma URL, alterando apenas o número da página.
Usa cache (memória + SQLite) para evitar consultas repetidas.
"""

import requests
import json
import time
import os
from config_reader import obter_config_api_humanus, obter_headers_api, obter_empresas_permitidas


def _buscar_colaboradores_da_api():
    """
    Busca todos os colaboradores da API Humanus com paginação.
    Incrementa NumeroPagina até receber 404 (sem mais resultados).
    
    Returns:
        list: Lista de todos os colaboradores (objetos JSON)
    """
    config = obter_config_api_humanus()
    if not config:
        print("❌ Configuração da API Humanus não encontrada")
        return []
    
    headers = obter_headers_api()
    if not headers:
        print("❌ Não foi possível obter headers da API")
        return []
    
    url_base = config['url_base']
    tamanho_pagina = config.get('tamanho_pagina', 50)
    
    todos_colaboradores = []
    numero_pagina = 1
    
    print("🔍 Buscando colaboradores na API Humanus...")
    
    while True:
        try:
            url = f"{url_base}?NumeroPagina={numero_pagina}&TamanhoPagina={tamanho_pagina}"
            print(f"  📄 Página {numero_pagina}... ", end="")
            
            response = requests.get(url, headers=headers, timeout=60)
            
            if response.status_code == 404:
                print("✅ Fim dos dados (404)")
                break
            
            if response.status_code != 200:
                print(f"❌ Erro {response.status_code}")
                break
            
            # A API pode retornar JSON ou array JSON
            try:
                dados = response.json()
            except json.JSONDecodeError:
                # Pode ser que retorne texto com múltiplos objetos JSON
                texto = response.text.strip()
                if not texto:
                    print("✅ Página vazia - fim")
                    break
                try:
                    dados = json.loads(texto)
                except json.JSONDecodeError:
                    print("❌ Resposta não é JSON válido")
                    break
            
            # Normalizar: pode vir como lista ou objeto com lista
            if isinstance(dados, list):
                colaboradores_pagina = dados
            elif isinstance(dados, dict):
                colaboradores_pagina = dados.get('data', dados.get('colaboradores', [dados]))
                if not isinstance(colaboradores_pagina, list):
                    colaboradores_pagina = [colaboradores_pagina] if colaboradores_pagina else []
            else:
                colaboradores_pagina = []
            
            if not colaboradores_pagina:
                print("✅ Sem mais dados")
                break
            
            todos_colaboradores.extend(colaboradores_pagina)
            print(f"✅ {len(colaboradores_pagina)} colaboradores (Total: {len(todos_colaboradores)})")
            
            if len(colaboradores_pagina) < tamanho_pagina:
                break
            
            numero_pagina += 1
            time.sleep(0.3)  # Evitar sobrecarga
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro na requisição: {e}")
            break
    
    print(f"\n✅ Total de colaboradores coletados: {len(todos_colaboradores)}")
    return todos_colaboradores


def _filtrar_por_empresas(colaboradores):
    """
    Filtra colaboradores pelas empresas permitidas no .config.
    codEmpresa da API pode vir como "001", "004" etc.
    """
    empresas_ok = obter_empresas_permitidas()
    if not empresas_ok:
        return colaboradores  # Sem filtro = inclui todos
    
    filtrados = []
    for col in colaboradores:
        cod = str(col.get('codEmpresa', '')).strip()
        cod_norm = cod.lstrip('0') or '0'  # "004" -> "4", "001" -> "1"
        if cod_norm in empresas_ok:
            filtrados.append(col)
    
    if filtrados != colaboradores:
        print(f"🏢 Filtro de empresas: {len(colaboradores)} -> {len(filtrados)} (permitidas: {empresas_ok})")
    return filtrados


def buscar_colaboradores_paginado(force_api=False):
    """
    Busca colaboradores com cache. Ordem: memória -> disco -> API.
    Filtra por empresas_permitidas do .config.
    """
    if not force_api:
        try:
            from cache_db import get_colaboradores, set_colaboradores_memoria
            cached = get_colaboradores()
            if cached is not None:
                return _filtrar_por_empresas(cached)
        except ImportError:
            pass
    
    # Buscar da API
    colaboradores = _buscar_colaboradores_da_api()
    
    # Salvar no cache (dados brutos, filtro na leitura)
    if colaboradores:
        try:
            from cache_db import set_colaboradores_memoria
            set_colaboradores_memoria(colaboradores)
        except ImportError:
            pass
    
    return _filtrar_por_empresas(colaboradores)


def buscar_situacoes():
    """
    Busca o mapeamento de situações (códigos para descrições).
    Usa arquivo consulta_situacao.txt se existir, senão consulta a API.
    """
    arquivo_situacao = 'consulta_situacao.txt'
    config = obter_config_api_humanus()
    url_situacao = config.get('url_situacao', 'https://humanus.crsistemas.net.br/api/MALHECIDADES/COLABORADOR/situacao/tudo') if config else None
    
    # Tentar ler do arquivo primeiro
    if os.path.exists(arquivo_situacao):
        try:
            with open(arquivo_situacao, 'r', encoding='utf-8') as f:
                conteudo = f.read().strip()
                if conteudo:
                    dados = json.loads(conteudo)
                    return _mapear_situacoes(dados)
        except (json.JSONDecodeError, Exception) as e:
            print(f"⚠️ Erro ao ler {arquivo_situacao}: {e}")
    
    # Buscar da API
    if url_situacao:
        headers = obter_headers_api()
        if headers:
            try:
                response = requests.get(url_situacao, headers=headers, timeout=30)
                if response.status_code == 200:
                    dados = response.json()
                    # Salvar no arquivo para próxima vez
                    with open(arquivo_situacao, 'w', encoding='utf-8') as f:
                        json.dump(dados, f, indent=2, ensure_ascii=False)
                    return _mapear_situacoes(dados)
            except Exception as e:
                print(f"⚠️ Erro ao buscar situações da API: {e}")
    
    return {}


def _mapear_situacoes(dados):
    """
    Mapeia sitCodSituacao -> cadDenominacao.
    dados pode ser lista de objetos com cadCodDetAssunto, cadDenominacao, cadReserva.
    sitCodSituacao usa "1", "2", "3", "10" etc - cadReserva tem valores correspondentes.
    """
    mapa = {}
    if isinstance(dados, list):
        for item in dados:
            # cadReserva corresponde ao sitCodSituacao (ex: "1"=Ativo, "2"=Férias, "3"=Demitido)
            cod = item.get('cadReserva')
            if cod is not None and cod != '':
                mapa[str(cod)] = item.get('cadDenominacao', '')
            # Também mapear cadCodDetAssunto para códigos como "01", "02"
            cod_det = item.get('cadCodDetAssunto', '')
            if cod_det:
                mapa[str(cod_det)] = item.get('cadDenominacao', '')
                # "01" -> "1" para compatibilidade
                if len(cod_det) > 1 and cod_det.lstrip('0'):
                    mapa[cod_det.lstrip('0')] = item.get('cadDenominacao', '')
    return mapa


def formatar_data_iso_para_br(data_iso):
    """Converte data ISO (2025-02-18T00:00:00) para DD/MM/YYYY"""
    if not data_iso:
        return ""
    try:
        data_str = str(data_iso).replace('Z', '').split('T')[0]
        if len(data_str) >= 10:
            from datetime import datetime
            dt = datetime.strptime(data_str[:10], '%Y-%m-%d')
            return dt.strftime('%d/%m/%Y')
    except Exception:
        pass
    return ""
