import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text
import pyodbc
from datetime import datetime
import sys

# Configurações
SQLITE_DB = "weather.db"
SERVER = "KENMOZ-DB04"
DATABASE = "KMR_OPSDATA"

# Tabelas a migrar
TABLES = [
    "meteorological",
    "waves",
    "currents",
    "tides",
    "water_quality"
]

def migrate_table(table_name, sqlite_conn, sql_engine, mode='replace'):
    """Migra uma tabela do SQLite para SQL Server"""
    print(f"\n📋 Migrando tabela: {table_name}")
    
    try:
        # Ler dados do SQLite
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", sqlite_conn)
        
        if df.empty:
            print(f"  ⚠️  Tabela {table_name} está vazia, pulando...")
            return 0
        
        print(f"  📊 Lendo {len(df):,} registros do SQLite...")
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
    print(f"📍 Destino: {SERVER}\\{DATABASE}")
    print(f"🔐 Auth:    Windows Trusted Connection")
    print("="*60 + "\n")

    # Conectar ao SQLite
    try:
        sqlite_conn = sqlite3.connect(SQLITE_DB)
        cursor = sqlite_conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        sqlite_tables = [row[0] for row in cursor.fetchall()]
        print(f"✅ SQLite conectado | {len(sqlite_tables)} tabelas: {', '.join(sqlite_tables)}")
    except Exception as e:
        print(f"❌ Erro ao conectar ao SQLite: {str(e)}")
        return

    # Conectar ao SQL Server usando Windows Authentication
    print(f"\n🔐 Conectando ao SQL Server usando Windows Auth...")
    
    conn_str = f"Driver={{ODBC Driver 17 for SQL Server}};Server={SERVER};Database={DATABASE};Trusted_Connection=yes;"
    
    try:
        # Testar conexão direta
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT @@version")
        version = cursor.fetchone()[0]
        conn.close()
        
        print(f"✅ Conexão com SQL Server estabelecida com sucesso!")
        print(f"   Versão: {version[:70]}...")
        
        # Criar engine SQLAlchemy
        sql_engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={conn_str.replace(';', '%3B')}",
            fast_executemany=True
        )
        
    except Exception as e:
        print(f"❌ Erro ao conectar ao SQL Server:")
        print(f"   {str(e)}")
        print(f"\n💡 Verifique:")
        print(f"   - SQL Server está acessível em {SERVER}")
        print(f"   - Sua conta do Windows tem permissão na base de dados {DATABASE}")
        print(f"   - SQL Server está configurado para aceitar Windows Authentication")
        print(f"   - Firewall não bloqueia acesso a {SERVER}:1433")
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
    print(f"✅ MIGRAÇÃO CONCLUÍDA!")
    print("="*60)
    print(f"📊 Total de registros migrados: {total_migrated:,}")
    print(f"⏱️  Tempo total: {elapsed:.2f}s ({elapsed/60:.2f} minutos)")
    print(f"💾 Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()
