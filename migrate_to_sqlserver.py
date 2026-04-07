import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text
import pyodbc
from datetime import datetime

# Configurações
SQLITE_DB = "weather.db"

# SQL Server - Autenticação com credenciais
SERVER = "KENMOZ-DB04"
INSTANCE = "PRODUCTION"
DATABASE = "KMR_OPSDATA"
USERNAME = "qvadm"
PASSWORD = "@@Rest1221"
DOMAIN = "KENMAREMOZ"

# Tabelas a migrar (todas as tabelas disponíveis no weather.db)
TABLES = [
    "meteorological",
    "waves",
    "currents",
    "tides",
    "water_quality"
]

def list_odbc_sources():
    """Lista todos os ODBC data sources disponíveis"""
    print("📋 ODBC Data Sources disponíveis no sistema:")
    try:
        sources = pyodbc.dataSources()
        for source in sources:
            print(f"   - {source}")
        return list(sources.keys())
    except Exception as e:
        print(f"   ⚠️  Erro ao listar ODBC sources: {e}")
        return []

def test_pyodbc_connection():
    """Tenta conectar usando pyodbc diretamente"""
    print("\n🔐 Testando conexão com pyodbc diretamente...")
    connection_strings = [
        f"Driver={{ODBC Driver 17 for SQL Server}};Server={SERVER}\\{INSTANCE};Database={DATABASE};UID={DOMAIN}\\{USERNAME};PWD={PASSWORD};",
        f"Driver={{ODBC Driver 17 for SQL Server}};Server={SERVER},{INSTANCE};Database={DATABASE};UID={DOMAIN}\\{USERNAME};PWD={PASSWORD};",
        f"Driver={{SQL Server Native Client 11.0}};Server={SERVER}\\{INSTANCE};Database={DATABASE};UID={DOMAIN}\\{USERNAME};PWD={PASSWORD};",
    ]
    
    for i, conn_str in enumerate(connection_strings, 1):
        try:
            print(f"   Tentativa {i}...")
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute("SELECT @@version")
            version = cursor.fetchone()[0]
            print(f"✅ Conexão bem-sucedida!")
            print(f"   Versão: {version[:60]}...")
            conn.close()
            return conn_str
        except Exception as e:
            print(f"   ❌ Falha: {str(e)[:80]}")
    
    return None

def get_sqlite_table_info(table_name, sqlite_conn):
    """Obtém informações da tabela SQLite"""
    cursor = sqlite_conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    return columns

def migrate_table(table_name, sqlite_conn, sql_engine, mode='replace'):
    """
    Migra uma tabela do SQLite para SQL Server
    mode: 'replace' (elimina dados antigos) ou 'append' (adiciona)
    """
    print(f"\n📋 Migrando tabela: {table_name}")
    
    try:
        # Ler dados do SQLite
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", sqlite_conn)
        
        if df.empty:
            print(f"  ⚠️  Tabela {table_name} está vazia, pulando...")
            return 0
        
        print(f"  📊 Lendo {len(df):,} registros do SQLite...")
        
        # Mostrar informações das colunas
        print(f"  📁 Colunas: {', '.join(df.columns.tolist())}")
        
        # Inserir no SQL Server
        print(f"  ⏳ Inserindo dados no SQL Server ({mode} mode)...")
        start_time = datetime.now()
        
        df.to_sql(table_name, con=sql_engine, if_exists=mode, index=False, chunksize=1000)
        
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"  ✅ {len(df):,} registros inseridos em {elapsed:.2f}s")
        
        return len(df)
    
    except Exception as e:
        print(f"  ❌ Erro ao migrar {table_name}: {str(e)}")
        return 0

def verify_migration(sql_engine, tables):
    """Verifica quantos registros foram inseridos em cada tabela"""
    print("\n" + "="*60)
    print("📊 VERIFICAÇÃO DA MIGRAÇÃO:")
    print("="*60)
    
    try:
        with sql_engine.connect() as conn:
            for table in tables:
                try:
                    result = conn.execute(text(f"SELECT COUNT(*) as count FROM {table}"))
                    count = result.scalar()
                    print(f"  ✅ {table:20} | {count:>12,} registros")
                except Exception as e:
                    print(f"  ⚠️  {table:20} | Erro: {str(e)[:40]}")
    except Exception as e:
        print(f"  ❌ Erro ao verificar: {str(e)}")

def main():
    print("\n" + "="*60)
    print("🌊  MIGRAÇÃO WEATHER.DB → SQL SERVER")
    print("="*60)
    print(f"📍 Origem:  {SQLITE_DB}")
    print(f"📍 Destino: {SERVER}\\{INSTANCE}\\{DATABASE}")
    print(f"👤 User:    {DOMAIN}\\{USERNAME}")
    print("="*60 + "\n")

    # Conectar ao SQLite
    try:
        sqlite_conn = sqlite3.connect(SQLITE_DB)
        cursor = sqlite_conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        sqlite_tables = [row[0] for row in cursor.fetchall()]
        print(f"✅ SQLite conectado | {len(sqlite_tables)} tabelas encontradas: {', '.join(sqlite_tables)}")
    except Exception as e:
        print(f"❌ Erro ao conectar ao SQLite: {str(e)}")
        return

    # Liste ODBC sources
    print()
    list_odbc_sources()
    
    # Teste direct pyodbc connection
    pyodbc_conn_str = test_pyodbc_connection()
    
    if not pyodbc_conn_str:
        print(f"\n❌ Não foi possível conectar ao SQL Server")
        print(f"\n💡 Sugestões de troubleshooting:")
        print(f"   1. Verifique se o servidor {SERVER}\\{INSTANCE} está acessível")
        print(f"   2. Teste a conectividade: ping {SERVER}")
        print(f"   3. Verifique credenciais: {DOMAIN}\\{USERNAME}")
        print(f"   4. Confirme que a base de dados {DATABASE} existe")
        print(f"   5. Verifique firewall rules para SQL Server")
        print(f"   6. Instale ODBC Driver 17 if not already installed")
        sqlite_conn.close()
        return

    # Conectar usando SQLAlchemy
    print(f"\n🔄 Convertendo para conexão SQLAlchemy...")
    try:
        sql_engine = create_engine(f"mssql+pyodbc:///?odbc_connect={pyodbc_conn_str}", fast_executemany=True)
        print("✅ Engine SQLAlchemy criada")
    except Exception as e:
        print(f"❌ Erro ao criar engine: {e}")
        sqlite_conn.close()
        return

    print("\n" + "="*60)
    print("📊 INICIANDO MIGRAÇÃO DAS TABELAS:")
    print("="*60)

    total_migrated = 0
    migration_start = datetime.now()

    # Migrar cada tabela
    for table in TABLES:
        if table in sqlite_tables:
            try:
                count = migrate_table(table, sqlite_conn, sql_engine, mode='replace')
                total_migrated += count
            except Exception as e: 
                print(f"❌ Erro ao migrar {table}: {str(e)}")
        else:
            print(f"\n⚠️  Tabela '{table}' não encontrada no SQLite, pulando...")

    sqlite_conn.close()

    # Verificação final
    verify_migration(sql_engine, TABLES)

    elapsed = (datetime.now() - migration_start).total_seconds()
    
    print("\n" + "="*60)
    print(f"✅ MIGRAÇÃO CONCLUÍDA COM SUCESSO!")
    print("="*60)
    print(f"📊 Total de registros migrados: {total_migrated:,}")
    print(f"⏱️  Tempo total: {elapsed:.2f}s ({elapsed/60:.2f} minutos)")
    print(f"💾 Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()