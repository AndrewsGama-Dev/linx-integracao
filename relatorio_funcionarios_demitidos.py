#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RELATÓRIO DE FUNCIONÁRIOS DEMITIDOS - API eContador
Gerado para análise de sincronização entre Alterdata e API eContador.
Use este relatório para verificar se a API está retornando dados atualizados.
"""
import os
import sys
import json
import time
import pandas as pd
import requests
import configparser
from datetime import datetime

os.chdir(os.path.dirname(os.path.abspath(__file__)))


def obter_headers():
    """Obtem headers da API sem prints com emojis"""
    config = configparser.ConfigParser()
    config.read(".config", encoding="utf-8")
    if "APISOURCE" not in config:
        return None
    token = config["APISOURCE"].get("token", "").strip('"')
    if not token:
        return None
    return {
        "Content-Type": "application/vnd.api+json",
        "Authorization": f"Bearer {token}"
    }


def formatar_data_br(data_iso):
    """Converte data ISO para DD/MM/AAAA"""
    if not data_iso:
        return ""
    try:
        data_str = str(data_iso).replace("Z", "").split("T")[0]
        return datetime.strptime(data_str, "%Y-%m-%d").strftime("%d/%m/%Y")
    except:
        return str(data_iso)


def consultar_funcionarios_por_status(status, page_limit=100):
    """
    Consulta funcionarios na API por status (ativo, demitido, etc).
    Retorna lista completa com paginacao.
    """
    headers = obter_headers()
    if not headers:
        print("Erro: Falha ao obter token do .config")
        return [], None

    base_url = "https://dp.pack.alterdata.com.br/api/v1/funcionarios"
    params = {
        "filter[status]": status,
        "sort": "codigo",
        "page[limit]": str(page_limit)
    }

    todos = []
    url_atual = base_url
    pagina = 1

    while url_atual:
        try:
            print(f"  Pagina {pagina} ({status})... ", end="")
            if pagina == 1:
                response = requests.get(base_url, headers=headers, params=params, timeout=30)
            else:
                response = requests.get(url_atual, headers=headers, timeout=30)

            if response.status_code == 200:
                data = response.json()
                items = data.get("data", [])
                todos.extend(items)
                meta = data.get("meta", {})
                total_api = meta.get("totalResourceCount", len(todos))

                print(f"OK - {len(items)} retornados (Total API: {total_api})")

                url_atual = data.get("links", {}).get("next")
                pagina += 1
                if url_atual:
                    time.sleep(0.3)
            else:
                print(f"Erro {response.status_code}")
                break
        except Exception as e:
            print(f"Erro: {e}")
            break

    return todos, headers


def gerar_relatorio_demitidos():
    """
    Gera relatorio completo de TODOS os funcionarios demitidos na API eContador.
    Sem filtro de data - inclui todos os registros retornados pela API.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    print("=" * 80)
    print("RELATORIO DE FUNCIONARIOS DEMITIDOS - API eContador")
    print("=" * 80)
    print(f"Data/hora da consulta: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print()

    # 1. Coletar TODOS os funcionarios demitidos (sem filtro de data)
    print("1. Coletando funcionarios demitidos da API...")
    demitidos, headers = consultar_funcionarios_por_status("demitido")

    if not demitidos:
        print("\nNenhum funcionario demitido encontrado na API.")
        print("POSSIVEIS CAUSAS:")
        print("  - A API pode retornar 0 registros se nao houver demitidos")
        print("  - A API pode ter delay de sincronizacao com o Alterdata")
        print("  - Verificar token e configuracoes no .config")
        return None

    # 2. Obter contagem de ativos para comparacao
    print("\n2. Coletando contagem de funcionarios ativos (para comparacao)...")
    ativos, _ = consultar_funcionarios_por_status("ativo", page_limit=10)
    # Se tiver mais de 10, a API tem paginacao - vamos pegar o total do meta
    meta_ativo = None
    if ativos:
        # Fazer uma chamada para pegar o meta
        resp = requests.get(
            "https://dp.pack.alterdata.com.br/api/v1/funcionarios",
            headers=headers,
            params={"filter[status]": "ativo", "page[limit]": "1"},
            timeout=30
        )
        if resp.status_code == 200:
            meta_ativo = resp.json().get("meta", {}).get("totalResourceCount")

    # 3. Montar relatorio detalhado
    print("\n3. Gerando relatorio detalhado...")
    registros = []

    for f in demitidos:
        attrs = f.get("attributes", {})
        cpf = attrs.get("cpf") or ""
        if cpf is not None and not isinstance(cpf, str):
            cpf = str(cpf)

        registros.append({
            "id_api": f.get("id"),
            "codigo": attrs.get("codigo", ""),
            "matricula": str(attrs.get("codigo", "")).zfill(6),
            "nome": attrs.get("nome", ""),
            "cpf": cpf or "",
            "status": attrs.get("status", "demitido"),
            "data_admissao": formatar_data_br(attrs.get("admissao")),
            "data_admissao_iso": attrs.get("admissao", ""),
            "data_demissao": formatar_data_br(attrs.get("demissao")),
            "data_demissao_iso": attrs.get("demissao", ""),
            "email": attrs.get("email", ""),
            "nome_funcao": attrs.get("nomefuncao", ""),
        })

    # 4. Criar DataFrame
    df = pd.DataFrame(registros)

    # 5. Salvar CSV
    nome_csv = f"relatorio_demitidos_api_{timestamp}.csv"
    df.to_csv(nome_csv, index=False, encoding="utf-8-sig", sep=";")
    print(f"\nArquivo CSV gerado: {nome_csv}")

    # 6. Salvar JSON com metadata
    relatorio_completo = {
        "metadata": {
            "data_geracao": datetime.now().isoformat(),
            "data_geracao_br": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            "fonte": "API eContador (eContador/Alterdata)",
            "endpoint": "https://dp.pack.alterdata.com.br/api/v1/funcionarios",
            "filtro_usado": "filter[status]=demitido",
            "total_demitidos": len(demitidos),
            "total_ativos": meta_ativo,
            "observacao": "Este relatorio mostra o que a API retorna ATUALMENTE. "
                         "Se os dados estiverem desatualizados, pode haver delay de "
                         "sincronizacao entre Alterdata e a API eContador."
        },
        "funcionarios_demitidos": registros
    }

    nome_json = f"relatorio_demitidos_api_{timestamp}.json"
    with open(nome_json, "w", encoding="utf-8") as f:
        json.dump(relatorio_completo, f, indent=2, ensure_ascii=False)
    print(f"Arquivo JSON gerado: {nome_json}")

    # 7. Resumo no console
    print("\n" + "=" * 80)
    print("RESUMO DO RELATORIO")
    print("=" * 80)
    print(f"  Total de funcionarios DEMITIDOS na API: {len(demitidos)}")
    if meta_ativo is not None:
        print(f"  Total de funcionarios ATIVOS na API: {meta_ativo}")
    print(f"  Arquivos gerados: {nome_csv}, {nome_json}")
    print()
    print("OBSERVACAO PARA DIAGNOSTICO:")
    print("  - Compare com o total de demitidos no sistema Alterdata.")
    print("  - Se o total for menor na API, pode haver DELAY de sincronizacao.")
    print("  - A API eContador pode nao refletir alteracoes em tempo real.")
    print()
    print("Preview (primeiros 5 registros):")
    print(df.head(5).to_string())
    print()

    return df


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        os.system("chcp 65001 > nul")
    gerar_relatorio_demitidos()
