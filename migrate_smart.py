import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text
import pyodbc
from datetime import datetime

SQLITE_DB = "weather.db"
SERVER = "KENMOZ-DB04"
DATABASE = "KMR_OPSDATA"

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
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", sqlite_conn)
        
        if df.empty:
            print(f"  ⚠️  Tabela {table_name} está vazia, pulando...")
            return 0
        
        print(f"  📊 Lendo {len(df):,} registros do SQLite...")
        print(f"  📁 Colunas: {', '.join(df.columns.tolist())}")
        
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
    print("="*60)

    # Conectar ao SQLite
    try:
        sqlite_conn = sqlite3.connect(SQLITE_DB)
        cursor = sqlite_conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        sqlite_tables = [row[0] for row in cursor.fetchall()]
        print(f"\n✅ SQLite conectado | Tabelas: {', '.join(sqlite_tables)}")
    except Exception as e:
        print(f"\n❌ Erro ao conectar ao SQLite: {str(e)}")
        return

    # Tentar múltiplas estratégias de conexão
    sql_engine = None
    used_method = None
    
    methods = [
        {
            "name": "Windows Auth com TrustServerCertificate",
            "conn_str": f"Driver={{ODBC Driver 17 for SQL Server}};Server={SERVER};Database={DATABASE};Trusted_Connection=yes;TrustServerCertificate=yes;Encrypt=no;"
        },
        {
            "name": "Windows Auth padrão",
            "conn_str": f"Driver={{ODBC Driver 17 for SQL Server}};Server={SERVER};Database={DATABASE};Trusted_Connection=yes;"
        },
    ]
    
    print(f"\n🔐 Testando métodos de autenticação...")
    
    for method in methods:
        try:
            print(f"\n   Tentando: {method['name']}...")
            conn = pyodbc.connect(method['conn_str'])
            cursor = conn.cursor()
            cursor.execute("SELECT @@version")
            version = cursor.fetchone()[0]
            conn.close()
            
            print(f"   ✅ SUCESSO!")
            print(f"      Versão: {version[:60]}...")
            
            sql_engine = create_engine(
                f"mssql+pyodbc:///?odbc_connect={method['conn_str'].replace(';', '%3B')}",
                fast_executemany=True
            )
            used_method = method['name']
            break
            
        except Exception as e:
            error_msg = str(e)
            if "18456" in error_msg:
                print(f"   ❌ Erro de autenticação/permissão")
            elif "cannot open database" in error_msg.lower():
                print(f"   ❌ Base de dados não existe ou sem acesso")
            else:
                print(f"   ❌ {str(e)[:80]}")

    if not sql_engine:
        print(f"\n❌ FALHA: Não foi possível conectar com nenhum método")
        print(f"\n💡 Verifique:")
        print(f"   1. Se sua conta (KENMAREMOZ\\tpenicela) foi adicionada à base")
        print(f"   2. Se precisa fazer logoff/login para aplicar permissões")
        print(f"   3. Se pode conectar com SQL Server Management Studio primeira")
        print(f"   4. Se qvadm ou outro utilizador consegue conectar")
        sqlite_conn.close()
        return

    print(f"\n✅ Conexão estabelecida usando: {used_method}")

    print("\n" + "="*60)
    print("📊 INICIANDO MIGRAÇÃO DAS TABELAS:")
    print("="*60)

    total_migrated = 0
    migration_start = datetime.now()

    for table in TABLES:
        if table in sqlite_tables:
            try:
                count = migrate_table(table, sqlite_conn, sql_engine, mode='replace')
                total_migrated += count
            except Exception as e:
                print(f"❌ Erro ao migrar {table}: {str(e)}")
        else:
            print(f"\n⚠️  Tabela '{table}' não encontrada no SQLite")

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
