#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Script para consultar funcionários ativos na API eContador"""
import sys
import os
import json
import requests
import configparser

# Mudar para o diretório do script
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

def consultar_funcionarios_ativos():
    headers = obter_headers()
    if not headers:
        print("Erro ao obter token do .config")
        return None

    base_url = "https://dp.pack.alterdata.com.br/api/v1/funcionarios"
    params = {
        "filter[status]": "ativo",
        "sort": "codigo",
        "page[limit]": "100"
    }

    print("Consultando funcionarios ativos na API eContador...")
    response = requests.get(base_url, headers=headers, params=params, timeout=30)

    if response.status_code != 200:
        print(f"Erro: Status {response.status_code}")
        print(response.text[:500])
        return None

    data = response.json()
    funcionarios = data.get("data", [])
    meta = data.get("meta", {})
    links = data.get("links", {})

    print(f"\nTotal retornado: {len(funcionarios)} funcionarios ativos")
    print(f"   Meta: {json.dumps(meta, indent=2)}")
    print()

    if funcionarios:
        print("LISTA DE FUNCIONÁRIOS ATIVOS:")
        print("-" * 90)
        for i, f in enumerate(funcionarios, 1):
            attrs = f.get("attributes", {})
            cpf = attrs.get("cpf", "") or ""
            cpf = str(cpf) if cpf is not None else ""
            cpf_display = cpf[:14] if len(cpf) > 14 else cpf
            print(f"{i:3}. ID: {f.get('id'):<8} | Código: {attrs.get('codigo', ''):<10} | Nome: {attrs.get('nome', '')[:35]:<35} | CPF: {cpf_display}")
        print("-" * 90)
        print(f"\nTotal: {len(funcionarios)} funcionario(s)")
    else:
        print("Nenhum funcionário ativo encontrado.")

    return funcionarios

if __name__ == "__main__":
    if sys.platform.startswith("win"):
        os.system("chcp 65001 > nul")
    consultar_funcionarios_ativos()
