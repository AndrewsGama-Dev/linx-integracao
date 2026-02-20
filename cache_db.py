# -*- coding: utf-8 -*-
"""
Módulo de cache e histórico para a integração.
Usa SQLite para persistir dados entre execuções e evitar consultas repetidas à API.
"""

import sqlite3
import json
import os
from datetime import datetime, timedelta

# Arquivo do banco na pasta do projeto
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'integracao_cache.db')

# Cache em memória para a execução atual (evita múltiplas consultas à API no mesmo run)
_cache_colaboradores = None
_cache_timestamp = None


def _get_conn():
    """Retorna conexão com o banco SQLite"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _init_db():
    """Inicializa as tabelas do banco se não existirem"""
    conn = _get_conn()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS cache_colaboradores (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                dados_json TEXT NOT NULL,
                total_registros INTEGER,
                atualizado_em TEXT NOT NULL
            );
            
            CREATE TABLE IF NOT EXISTS demissoes_enviadas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                matricula TEXT NOT NULL,
                data_demissao TEXT NOT NULL,
                nome TEXT,
                enviado_em TEXT NOT NULL,
                UNIQUE(matricula, data_demissao)
            );
            
            CREATE INDEX IF NOT EXISTS idx_demissoes_matricula_data 
            ON demissoes_enviadas(matricula, data_demissao);
        """)
        conn.commit()
    finally:
        conn.close()


def obter_cache_validade_minutos():
    """Lê da config quantos minutos o cache é válido (0 = sempre usar cache até próxima execução)"""
    try:
        import configparser
        config = configparser.ConfigParser()
        config.read('.config', encoding='utf-8')
        if 'CACHE' in config:
            val = config['CACHE'].get('validade_minutos', '60').strip()
            return int(val) if val.isdigit() else 60
    except Exception:
        pass
    return 60  # Default: 1 hora


def get_colaboradores_cache():
    """
    Retorna colaboradores do cache em disco (SQLite).
    Retorna None se cache não existir ou estiver expirado.
    """
    _init_db()
    validade_min = obter_cache_validade_minutos()
    
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT dados_json, total_registros, atualizado_em FROM cache_colaboradores WHERE id = 1"
        ).fetchone()
        
        if not row:
            return None
        
        dados_json, total, atualizado_em = row[0], row[1], row[2]
        
        # Verificar validade
        if validade_min > 0:
            try:
                dt_cache = datetime.fromisoformat(atualizado_em)
                if datetime.now() - dt_cache > timedelta(minutes=validade_min):
                    return None  # Cache expirado
            except Exception:
                pass
        
        colaboradores = json.loads(dados_json)
        print(f"📂 Cache carregado: {total} colaboradores (atualizado em {atualizado_em})")
        return colaboradores
        
    except Exception as e:
        print(f"⚠️ Erro ao ler cache: {e}")
        return None
    finally:
        conn.close()


def set_colaboradores_cache(colaboradores):
    """Salva colaboradores no cache em disco"""
    _init_db()
    conn = _get_conn()
    try:
        dados_json = json.dumps(colaboradores, ensure_ascii=False)
        atualizado_em = datetime.now().isoformat()
        conn.execute("""
            INSERT OR REPLACE INTO cache_colaboradores (id, dados_json, total_registros, atualizado_em)
            VALUES (1, ?, ?, ?)
        """, (dados_json, len(colaboradores), atualizado_em))
        conn.commit()
        print(f"💾 Cache salvo: {len(colaboradores)} colaboradores")
    except Exception as e:
        print(f"⚠️ Erro ao salvar cache: {e}")
    finally:
        conn.close()


def get_colaboradores():
    """
    Retorna colaboradores do cache: primeiro memória, depois disco.
    Retorna None se não houver cache válido (sinal para buscar da API).
    """
    global _cache_colaboradores, _cache_timestamp
    
    # 1. Cache em memória (mesma execução - evita 6 chamadas à API)
    if _cache_colaboradores is not None:
        print(f"📂 Usando cache em memória: {len(_cache_colaboradores)} colaboradores")
        return _cache_colaboradores
    
    # 2. Cache em disco (execução anterior)
    cached = get_colaboradores_cache()
    if cached:
        _cache_colaboradores = cached
        _cache_timestamp = datetime.now()
        return cached
    
    # 3. Sem cache - retorna None para api_humanus buscar da API
    return None


def set_colaboradores_memoria(colaboradores):
    """Armazena colaboradores no cache em memória e disco"""
    global _cache_colaboradores, _cache_timestamp
    _cache_colaboradores = colaboradores
    _cache_timestamp = datetime.now()
    set_colaboradores_cache(colaboradores)


def limpar_cache_memoria():
    """Limpa o cache em memória (útil para testes)"""
    global _cache_colaboradores, _cache_timestamp
    _cache_colaboradores = None
    _cache_timestamp = None


# ==================== DEMISSÕES ENVIADAS ====================

def get_demissoes_ja_enviadas():
    """
    Retorna set de (matricula, data_demissao) já enviadas.
    data_demissao em formato DD/MM/YYYY para comparação.
    """
    _init_db()
    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT matricula, data_demissao FROM demissoes_enviadas"
        ).fetchall()
        return {(r[0], r[1]) for r in rows}
    except Exception as e:
        print(f"⚠️ Erro ao ler histórico de demissões: {e}")
        return set()
    finally:
        conn.close()


def registrar_demissao_enviada(matricula, data_demissao, nome=''):
    """Registra uma demissão como já enviada"""
    _init_db()
    conn = _get_conn()
    try:
        conn.execute("""
            INSERT OR IGNORE INTO demissoes_enviadas (matricula, data_demissao, nome, enviado_em)
            VALUES (?, ?, ?, ?)
        """, (matricula, data_demissao, nome, datetime.now().isoformat()))
        conn.commit()
    except Exception as e:
        print(f"⚠️ Erro ao registrar demissão: {e}")
    finally:
        conn.close()


def get_historico_demissoes():
    """Retorna lista de demissões já enviadas para relatório"""
    _init_db()
    conn = _get_conn()
    try:
        rows = conn.execute("""
            SELECT matricula, data_demissao, nome, enviado_em 
            FROM demissoes_enviadas 
            ORDER BY enviado_em DESC
        """).fetchall()
        return [dict(row) for row in rows]
    except Exception:
        return []
    finally:
        conn.close()


def limpar_cache_completo():
    """Remove cache de colaboradores (força nova consulta à API na próxima execução)"""
    _init_db()
    conn = _get_conn()
    try:
        conn.execute("DELETE FROM cache_colaboradores")
        conn.commit()
        limpar_cache_memoria()
        print("🗑️ Cache de colaboradores limpo")
    except Exception as e:
        print(f"⚠️ Erro ao limpar cache: {e}")
    finally:
        conn.close()
