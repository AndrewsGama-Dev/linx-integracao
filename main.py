#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SISTEMA DE INTEGRAÇÃO COMPLETA
eContador API → CSV → Sistema Hevi

Executa todos os módulos de integração na sequência correta:
1. Empresas
2. Departamentos  
3. Cargos
4. Funcionários
5. Afastamentos e Férias
7. Demissões

Autor: Sistema de Integração Automatizada
Data: 2025
"""

import sys
import os
import time
from datetime import datetime
import json

# Importar todos os módulos de integração
try:
    import departamentos
    import cargos
    import funcionarios
    import afastamentos
    import ferias
    import demissoes
    from config_reader import ler_config
except ImportError as e:
    print(f"❌ ERRO: Não foi possível importar um dos módulos necessários: {e}")
    print("📝 Certifique-se de que todos os arquivos estão no mesmo diretório:")
    print("   • departamentos.py") 
    print("   • cargos.py")
    print("   • funcionarios.py")
    print("   • afastamentos.py")
    print("   • ferias.py")
    print("   • demissoes.py")
    print("   • config_reader.py")
    print("   • .config")
    sys.exit(1)

def imprimir_banner():
    """Imprime o banner inicial do sistema"""
    banner = """
╔══════════════════════════════════════════════════════════════════════════════╗
║                    SISTEMA DE INTEGRAÇÃO COMPLETA                           ║
║                    API Humanus → CSV → ifPonto / Hevi                        ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Sequência de Execução:                                                      ║
║    1. 💼 Cargos                                                             ║
║    2. 🏗️  Departamentos                                                     ║
║    3. 👥 Funcionários                                                       ║
║    4. 🚫 Afastamentos                                                       ║
║    5. 🏖️  Férias                                                             ║
║    6. 📋 Demissões                                                          ║
╚══════════════════════════════════════════════════════════════════════════════╝
    """
    print(banner)

def verificar_prerequisitos():
    """Verifica se todos os pré-requisitos estão atendidos"""
    print("🔍 VERIFICANDO PRÉ-REQUISITOS...")
    
    erros = []
    
    # Verificar arquivo .config
    if not os.path.exists('.config'):
        erros.append("❌ Arquivo .config não encontrado")
    else:
        print("✅ Arquivo .config encontrado")
        
        # Verificar configurações
        config = ler_config()
        if not config:
            erros.append("❌ Erro ao ler arquivo .config")
        else:
            # Verificar seções necessárias
            secoes_necessarias = ['APISOURCE', 'APITARGET', 'SOAP']  # APISOURCE = API Humanus
            for secao in secoes_necessarias:
                if secao not in config:
                    erros.append(f"❌ Seção [{secao}] não encontrada no .config")
                else:
                    print(f"✅ Seção [{secao}] encontrada")
            
            # Verificar token ou credenciais para gerar token
            from config_reader import obter_config_api_humanus
            cfg = obter_config_api_humanus()
            tem_token = cfg and cfg.get('token')
            tem_credenciais = cfg and all([
                cfg.get('url_token'), cfg.get('alias_name'),
                cfg.get('user_name'), cfg.get('password')
            ])
            if not tem_token and not tem_credenciais:
                erros.append("❌ Configure token ou credenciais (url_token, alias_name, user_name, password) em [APISOURCE]")
            else:
                print("✅ Token ou credenciais da API encontrados")
    
    # Verificar módulos Python
    modulos_necessarios = [
        'requests', 'pandas', 'configparser', 'pytz', 'hashlib'
    ]
    
    for modulo in modulos_necessarios:
        try:
            __import__(modulo)
            print(f"✅ Módulo {modulo} disponível")
        except ImportError:
            erros.append(f"❌ Módulo Python '{modulo}' não instalado")
    
    if erros:
        print("\n💥 ERROS ENCONTRADOS:")
        for erro in erros:
            print(f"   {erro}")
        print("\n📝 AÇÕES NECESSÁRIAS:")
        print("   1. Instale os módulos Python faltantes: pip install requests pandas pytz")
        print("   2. Em [APISOURCE]: use token OU credenciais (url_token, alias_name, user_name, password)")
        print("   3. Verifique se todas as seções necessárias estão no .config")
        return False
    
    print("✅ Todos os pré-requisitos atendidos!")
    return True

def executar_modulo(nome_modulo, modulo, descricao):
    """Executa um módulo específico e registra o resultado"""
    print(f"\n{'='*80}")
    print(f"🚀 EXECUTANDO: {nome_modulo.upper()} - {descricao}")
    print(f"{'='*80}")
    
    inicio = time.time()
    
    try:
        # Executar o módulo
        sucesso = modulo.processar_integracao_completa()
        
        fim = time.time()
        duracao = fim - inicio
        
        resultado = {
            'modulo': nome_modulo,
            'descricao': descricao,
            'sucesso': sucesso,
            'duracao_segundos': round(duracao, 2),
            'timestamp': datetime.now().isoformat()
        }
        
        if sucesso:
            print(f"\n✅ {nome_modulo.upper()} CONCLUÍDO COM SUCESSO!")
            print(f"⏱️  Tempo de execução: {duracao:.1f} segundos")
        else:
            print(f"\n❌ {nome_modulo.upper()} FALHOU!")
            print(f"⏱️  Tempo até falha: {duracao:.1f} segundos")
        
        return resultado
        
    except Exception as e:
        fim = time.time()
        duracao = fim - inicio
        
        print(f"\n💥 ERRO CRÍTICO NO MÓDULO {nome_modulo.upper()}:")
        print(f"   Erro: {str(e)}")
        print(f"⏱️  Tempo até erro: {duracao:.1f} segundos")
        
        resultado = {
            'modulo': nome_modulo,
            'descricao': descricao,
            'sucesso': False,
            'erro': str(e),
            'duracao_segundos': round(duracao, 2),
            'timestamp': datetime.now().isoformat()
        }
        
        return resultado

def pausar_entre_modulos(segundos=3):
    """Pausa entre módulos para não sobrecarregar as APIs"""
    print(f"\n⏸️  Aguardando {segundos} segundos antes do próximo módulo...")
    for i in range(segundos, 0, -1):
        print(f"   ⏳ {i}...", end='\r')
        time.sleep(1)
    print("   ✅ Continuando...                    ")

def gerar_relatorio_final(resultados):
    """Gera relatório final da execução"""
    print(f"\n{'='*80}")
    print("📊 RELATÓRIO FINAL DA INTEGRAÇÃO COMPLETA")
    print(f"{'='*80}")
    
    sucessos = sum(1 for r in resultados if r['sucesso'])
    falhas = len(resultados) - sucessos
    tempo_total = sum(r['duracao_segundos'] for r in resultados)
    
    print(f"\n📈 RESUMO GERAL:")
    print(f"   ✅ Módulos executados com sucesso: {sucessos}/{len(resultados)}")
    print(f"   ❌ Módulos com falha: {falhas}/{len(resultados)}")
    print(f"   ⏱️  Tempo total de execução: {tempo_total:.1f} segundos ({tempo_total/60:.1f} minutos)")
    print(f"   📅 Data/hora da execução: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    
    print(f"\n📋 DETALHES POR MÓDULO:")
    for resultado in resultados:
        status = "✅ SUCESSO" if resultado['sucesso'] else "❌ FALHA"
        duracao = resultado['duracao_segundos']
        
        print(f"   {status} {resultado['modulo']:<15} - {resultado['descricao']:<30} ({duracao:5.1f}s)")
        
        if not resultado['sucesso'] and 'erro' in resultado:
            print(f"      💥 Erro: {resultado['erro']}")
    
    # Salvar relatório em arquivo
    relatorio_detalhado = {
        'execucao': {
            'data_hora': datetime.now().isoformat(),
            'sucessos': sucessos,
            'falhas': falhas,
            'tempo_total_segundos': tempo_total,
            'tempo_total_minutos': round(tempo_total / 60, 2)
        },
        'modulos': resultados
    }
    
    nome_arquivo_relatorio = f"relatorio_integracao_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    try:
        with open(nome_arquivo_relatorio, 'w', encoding='utf-8') as f:
            json.dump(relatorio_detalhado, f, indent=2, ensure_ascii=False)
        print(f"\n💾 Relatório detalhado salvo em: {nome_arquivo_relatorio}")
    except Exception as e:
        print(f"\n⚠️  Erro ao salvar relatório: {e}")
    
    # Arquivos gerados
    print(f"\n📁 ARQUIVOS GERADOS:")
    arquivos_esperados = [
        "cargos_api.csv",
        "departamentos_api.csv", 
        "funcionarios_api.csv",
        "afastamentos_api.csv",
        "ferias_api.csv",
        "demissoes_api.csv"
    ]
    
    for arquivo in arquivos_esperados:
        if os.path.exists(arquivo):
            tamanho = os.path.getsize(arquivo)
            print(f"   ✅ {arquivo:<25} ({tamanho:,} bytes)")
        else:
            print(f"   ❌ {arquivo:<25} (não encontrado)")
    
    if sucessos == len(resultados):
        print(f"\n🎉 INTEGRAÇÃO COMPLETA FINALIZADA COM 100% DE SUCESSO!")
        print(f"   Todos os {len(resultados)} módulos foram executados com sucesso.")
        return True
    elif sucessos > 0:
        print(f"\n⚠️  INTEGRAÇÃO PARCIALMENTE CONCLUÍDA!")
        print(f"   {sucessos} módulos executados com sucesso, {falhas} falharam.")
        print(f"   Verifique os logs acima para identificar os problemas.")
        return False
    else:
        print(f"\n💥 INTEGRAÇÃO COMPLETAMENTE FALHOU!")
        print(f"   Nenhum módulo foi executado com sucesso.")
        print(f"   Verifique as configurações e dependências.")
        return False

def main():
    """Função principal do sistema"""
    try:
        # Imprimir banner
        imprimir_banner()
        
        # Verificar pré-requisitos
        if not verificar_prerequisitos():
            input("\n❌ Pressione Enter para sair...")
            return False
        
        # Configurar sequência de execução (cargos, departamentos, funcionarios, afastamentos, ferias, demissão)
        sequencia_modulos = [
            ('cargos', cargos, 'Cadastro de Cargos'),
            ('departamentos', departamentos, 'Cadastro de Departamentos'),
            ('funcionarios', funcionarios, 'Cadastro de Funcionários'),
            ('afastamentos', afastamentos, 'Registro de Afastamentos'),
            ('ferias', ferias, 'Registro de Férias'),
            ('demissoes', demissoes, 'Processamento de Demissões')
        ]
        
        print(f"\n🚀 INICIANDO INTEGRAÇÃO COMPLETA...")
        print(f"📊 Total de módulos a executar: {len(sequencia_modulos)}")
        
        resultados = []
        inicio_geral = time.time()
        
        # Executar cada módulo na sequência
        for i, (nome_modulo, modulo, descricao) in enumerate(sequencia_modulos, 1):
            print(f"\n📍 PROGRESSO: {i}/{len(sequencia_modulos)} módulos")
            
            resultado = executar_modulo(nome_modulo, modulo, descricao)
            resultados.append(resultado)
            
            # Pausa entre módulos (exceto no último)
            if i < len(sequencia_modulos):
                pausar_entre_modulos(3)
        
        fim_geral = time.time()
        tempo_total_geral = fim_geral - inicio_geral
        
        # Gerar relatório final
        sucesso_geral = gerar_relatorio_final(resultados)
        
        print(f"\n⏱️  TEMPO TOTAL DA EXECUÇÃO COMPLETA: {tempo_total_geral:.1f} segundos ({tempo_total_geral/60:.1f} minutos)")
        
        if sucesso_geral:
            print(f"\n🎊 PARABÉNS! INTEGRAÇÃO 100% CONCLUÍDA!")
        else:
            print(f"\n⚠️  INTEGRAÇÃO CONCLUÍDA COM RESSALVAS!")
            
        #input(f"\n📋 Pressione Enter para finalizar...")
        return sucesso_geral
        
    except KeyboardInterrupt:
        print(f"\n\n⏹️  INTEGRAÇÃO INTERROMPIDA PELO USUÁRIO!")
        print(f"   A execução foi cancelada manualmente.")
        input(f"\n📋 Pressione Enter para sair...")
        return False
        
    except Exception as e:
        print(f"\n💥 ERRO CRÍTICO NA EXECUÇÃO PRINCIPAL:")
        print(f"   Erro: {str(e)}")
        print(f"   Tipo: {type(e).__name__}")
        input(f"\n❌ Pressione Enter para sair...")
        return False

if __name__ == "__main__":
    # Configurar encoding para Windows
    if sys.platform.startswith('win'):
        os.system('chcp 65001 > nul')
    
    # Argumentos: --limpar-cache (limpa cache e sai), --force-api (força nova consulta à API)
    args = sys.argv[1:]
    if '--limpar-cache' in args:
        try:
            from cache_db import limpar_cache_completo
            limpar_cache_completo()
            print("💡 Use: python main.py --force-api para forçar nova consulta na próxima execução")
        except ImportError:
            print("❌ Módulo cache_db não encontrado")
        sys.exit(0)
    
    force_api = '--force-api' in args
    if force_api:
        try:
            from cache_db import limpar_cache_memoria
            limpar_cache_memoria()
            from api_humanus import buscar_colaboradores_paginado
            # Pré-carrega da API para popular cache (ignora cache)
            print("🔄 Forçando nova consulta à API...")
            buscar_colaboradores_paginado(force_api=True)
        except Exception as e:
            print(f"⚠️ Erro ao forçar API: {e}")
    
    # Executar sistema
    sucesso = main()
    
    # Código de saída
    sys.exit(0 if sucesso else 1)