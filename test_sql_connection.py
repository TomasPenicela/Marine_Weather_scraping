import pyodbc
import socket
import subprocess
from datetime import datetime

print("\n" + "="*70)
print("🔍  DIAGNÓSTICO DE CONEXÃO A SQL SERVER")
print("="*70 + "\n")

# Parâmetros
SERVER = "KENMOZ-DB04"
PORT = 1433
USERNAME = "qvadm"
PASSWORD = "@@Rest1221"
DOMAIN = "KENMAREMOZ"
DATABASE = "KMR_OPSDATA"

# 1. Teste de conectividade de rede
print("1️⃣  TESTE DE CONECTIVIDADE DE REDE")
print("-" * 70)
try:
    ip = socket.gethostbyname(SERVER)
    print(f"✅ Servidor '{SERVER}' resolvido para IP: {ip}")
except socket.gaierror:
    print(f"❌ Servidor '{SERVER}' não encontrado no DNS")

try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(3)
    result = sock.connect_ex((SERVER, PORT))
    sock.close()
    if result == 0:
        print(f"✅ Porta {PORT} (SQL Server) acessível")
    else:
        print(f"❌ Porta {PORT} não acessível (firewall?)")
except Exception as e:
    print(f"❌ Erro no teste de porta: {e}")

# 2. Teste de ODBC Drivers
print("\n2️⃣  DRIVERS ODBC DISPONÍVEIS")
print("-" * 70)
try:
    drivers = pyodbc.drivers()
    print(f"Drivers instalados: {len(drivers)}")
    for driver in drivers:
        print(f"  - {driver}")
    if any('17' in d for d in drivers):
        print("✅ ODBC Driver 17 for SQL Server encontrado")
    else:
        print("⚠️  ODBC Driver 17 for SQL Server NÃO encontrado!")
except Exception as e:
    print(f"❌ Erro: {e}")

# 3. Teste de credenciais de domínio
print("\n3️⃣  CREDENCIAIS DE DOMÍNIO")
print("-" * 70)
print(f"Domain:   {DOMAIN}")
print(f"Username: {USERNAME}")
print(f"Password: {'*' * len(PASSWORD)}")

# 4. Tentativas de conexão
print("\n4️⃣  TENTATIVAS DE CONEXÃO")
print("-" * 70)

connection_attempts = [
    {
        "name": "Windows Auth (Trusted)",
        "conn_str": f"Driver={{ODBC Driver 17 for SQL Server}};Server={SERVER};Database={DATABASE};Trusted_Connection=yes;"
    },
    {
        "name": f"Domain User ({DOMAIN}\\{USERNAME})",
        "conn_str": f"Driver={{ODBC Driver 17 for SQL Server}};Server={SERVER};Database={DATABASE};UID={DOMAIN}\\{USERNAME};PWD={PASSWORD};"
    },
    {
        "name": f"SQL User ({USERNAME})",
        "conn_str": f"Driver={{ODBC Driver 17 for SQL Server}};Server={SERVER};Database={DATABASE};UID={USERNAME};PWD={PASSWORD};"
    },
    {
        "name": "Com port e instância",
        "conn_str": f"Driver={{ODBC Driver 17 for SQL Server}};Server={SERVER},1433;Database={DATABASE};UID={DOMAIN}\\{USERNAME};PWD={PASSWORD};"
    },
]

for attempt in connection_attempts:
    print(f"\n🔹 {attempt['name']}")
    try:
        conn = pyodbc.connect(attempt['conn_str'], timeout=5)
        cursor = conn.cursor()
        cursor.execute("SELECT @@version")
        version = cursor.fetchone()[0]
        print(f"  ✅ SUCESSO!")
        print(f"  Versão: {version[:80]}")
        conn.close()
        break
    except pyodbc.Error as e:
        error_code = e.args[0] if e.args else "Unknown"
        error_msg = str(e.args[1] if len(e.args) > 1 else e.args[0])[:100]
        print(f"  ❌ {error_code}: {error_msg}")
    except Exception as e:
        print(f"  ❌ {str(e)[:100]}")

# 5. Informações do sistema
print("\n5️⃣  INFORMAÇÕES DO SISTEMA")
print("-" * 70)
import platform
import sys
print(f"OS:       {platform.system()} {platform.release()}")
print(f"Python:   {sys.version.split()[0]}")
print(f"Hora:     {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

print("\n" + "="*70)
print("📝 RECOMENDAÇÕES:")
print("="*70)
print("""
1. Se "ODBC Driver 17 for SQL Server" NÃO está listado:
   - Instale: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
   
2. Se conectar com "Windows Auth" mas não com credenciais:
   - Use Trusted Connection instead
   - Ou verifique se qvadm tem permissão na base de dados
   
3. Se nenhuma conexão funcionar:
   - Verifique se SQL Server está em execução
   - Verifique firewall/regras de rede
   - Teste com SQL Server Management Studio
   - Verifique se a instância PRODUCTION existe

4. Para usar com sucesso após fix:
   - Editar migrate_to_sqlserver.py
   - Usar a connection string que funcionou acima
""")
print("="*70 + "\n")
