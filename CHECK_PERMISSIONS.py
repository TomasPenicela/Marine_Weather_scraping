"""
Relatório de Diagnóstico - Permissões SQL Server
"""

print("""
╔═══════════════════════════════════════════════════════════════════════════╗
║                  🔍 DIAGNÓSTICO DE PERMISSÕES SQL SERVER                 ║
╚═══════════════════════════════════════════════════════════════════════════╝

✅ CONFIRMADO:
   • Servidor SQL Server KENMOZ-DB04 está acessível
   • ODBC Driver 17 for SQL Server está instalado
   • Conectividade de rede OK (porta 1433 alcançável)
   
❌ PROBLEMA:
   • Login falhou: KENMAREMOZ\\tpenicela (Erro 18456)
   • Sua conta NÃO tem permissão na base KMR_OPSDATA

═════════════════════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────────────────┐
│ SOLUÇÕES POSSÍVEIS:                                                     │
└─────────────────────────────────────────────────────────────────────────┘

1️⃣  OPÇÃO: SQL Server Management Studio (Recomendado)
   ──────────────────────────────────────────────────────
   Se tem SSMS instalado:
   
   a) Abra SSMS
   b) Conecte com conta que tem permissão (ex: admin/qvadm)
   c) Execute estes comandos para dar permissão à sua conta:

      USE [KMR_OPSDATA];
      CREATE LOGIN [KENMAREMOZ\\tpenicela] FROM WINDOWS;
      ALTER ROLE db_owner ADD MEMBER [KENMAREMOZ\\tpenicela];
      
   d) Teste novamente a migração

2️⃣  OPÇÃO: Pedir ao Admin SQL Server
   ──────────────────────────────────────────────────────
   Contacte seu administrador SQL e peça:
   
   "Preciso que adicione KENMAREMOZ\\tpenicela como db_owner 
    na base de dados KMR_OPSDATA no servidor KENMOZ-DB04"

3️⃣  OPÇÃO: Usar Credenciais com Permissão
   ──────────────────────────────────────────────────────
   Se sabe outro utilizador com permissão (ex: qvadm, admin_user):
   
   python migrate_with_credentials.py admin_user password KENMAREMOZ

4️⃣  OPÇÃO: Criar Nova Base de Dados (Admin)
   ──────────────────────────────────────────────────────
   Se a base KMR_OPSDATA não existe, peça ao admin para:
   
   a) Criar a base de dados
   b) Adicionar sua conta com db_owner
   c) Então executar migração

═════════════════════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────────────────┐
│ PARA CONFIRMAR: Teste se consegue ver as bases de dados:               │
└─────────────────────────────────────────────────────────────────────────┘

Abra PowerShell como admin e execute:

    sqlcmd -S KENMOZ-DB04 -E -Q "SELECT name FROM sys.databases"

   -S = servidor
   -E = usar Windows Authentication
   -Q = query

Se funcionar, significa que Windows Auth funciona e seu utilizador 
tem alguma permissão no SQL Server.

═════════════════════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────────────────┐
│ INFORMAÇÕES DO SEU SISTEMA:                                             │
└─────────────────────────────────────────────────────────────────────────┘
""")

import socket
import getpass
import os

print(f"Utilizador Windows: {getpass.getuser()}")
print(f"Domínio: {os.environ.get('USERDOMAIN', 'N/A')}")
print(f"Computador: {socket.gethostname()}")

print("""
═════════════════════════════════════════════════════════════════════════════

📧 PRÓXIMAS ETAPAS:

[ ] 1. Contactar admin com a solução apropriada
[ ] 2. Após resolver permissões, reexecutar:
       
       cd 'c:\Users\tpenicela\Downloads\weather - 2'
       .\.venv\Scripts\python.exe migrate_to_sqlserver_final.py

═════════════════════════════════════════════════════════════════════════════
""")
