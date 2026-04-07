# 🌊 Guia de Migração Weather.db → SQL Server

## Status Atual

A migração de dados foi **parcialmente configurada**. Os seguintes scripts estão prontos:

### Scripts Disponíveis

1. **`migrate_with_credentials.py`** ← Recomendado
   - Suporta credenciais específicas (domínio + utilizador + password)
   - Uso: `python migrate_with_credentials.py [utilizador] [password] [domínio]`
   - Exemplo: `python migrate_with_credentials.py qvadm @@Rest1221 KENMAREMOZ`

2. **`migrate_to_sqlserver_final.py`**
   - Usa Windows Authentication (conta do utilizador atual)
   - Mais seguro se não quer guardar credenciais em texto

3. **`test_sql_connection.py`**
   - Testa conectividade ao SQL Server
   - Mostra quais métodos de autenticação funcionam

## Dados a Migrar

**Origem:** `weather.db` (SQLite local)
- 576,043 registros de meteorological
- 17,524 registros de waves
- 8,978 registros de currents
- 160,734 registros de tides
- 17,529 registros de water_quality

**Destino:** SQL Server
- Servidor: `KENMOZ-DB04`
- Base: `KMR_OPSDATA`
- Tabelas: meteorological, waves, currents, tides, water_quality

## Problema Atual

A tentativa de migração com credenciais `KENMAREMOZ\qvadm` / `@@Rest1221` resultou em:
```
❌ ERRO DE AUTENTICAÇÃO (erro 18456)
   Credenciais incorretas ou utilizador sem permissão
```

## Soluções a Tentar

### Opção 1: Verificar Credenciais
A password `@@Rest1221` pode estar errada ou expirada. Contacte:
- **Administrador SQL Server** → Validar credenciais de qvadm
- **Administrador de Domínio** → Confirmar que qvadm está ativo

### Opção 2: Usar Utilizador Diferente
Se qvadm não funciona, tente outro utilizador:
```powershell
python migrate_with_credentials.py [outro_user] [password] KENMAREMOZ
```

### Opção 3: Usar Windows Authentication
Se sua conta tem permissão no SQL Server:
```powershell
python migrate_to_sqlserver_final.py
```

Isto usa a conta do Windows atualmente autenticada (tpenicela).

### Opção 4: Criar Conta SQL Server
Se não conseguir autenticar, peça ao admin SQL para criar um login:
```sql
CREATE LOGIN [KENMAREMOZ\novo_user] FROM WINDOWS;
ALTER ROLE db_owner ADD MEMBER [KENMAREMOZ\novo_user];
```

## Passos Finais para Completar

1. **Resolver autenticação** (ver soluções acima)

2. **Executar migração** com o método que funciona:
   ```powershell
   cd 'c:\Users\tpenicela\Downloads\weather - 2'
   .\.venv\Scripts\python.exe migrate_with_credentials.py [user] [pass] [domain]
   ```

3. **Verificar resultado**:
   - Script mostrará: "✅ MIGRAÇÃO CONCLUÍDA!"
   - Será exibido o total de registros migrados
   - Verificação de cada tabela em SQL Server

4. **Validação em SQL Server**:
   ```sql
   -- Conectar ao KENMOZ-DB04\PRODUCTION com KMR_OPSDATA
   SELECT TABLE_NAME, 
          (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = t.TABLE_NAME) as Column_Count
   FROM INFORMATION_SCHEMA.TABLES t
   WHERE TABLE_SCHEMA = 'dbo'
   ORDER BY TABLE_NAME;
   ```

## Arquivos de Referência

- **`test_sql_connection.py`** - Diagnóstico completo (executar se houver dúvidas)
- **`migrate_to_sqlserver.py`** - Versão original (para referência)
- **`migrate_to_sqlserver_windows_auth.py`** - Versão alternativa

## Próximas Etapas

Após migração bem-sucedida:

1. ✅ Dados estão em SQL Server
2. ⏭️  Atualizar código de aplicação para ler de SQL Server em vez de SQLite
3. ⏭️  Configurar replicação/sincronização automática (se necessário)
4. ⏭️  Desativar escrita em SQLite

---

**Última atualização:** 2026-04-06 12:25:00
**Status:** Aguardando resolução de autenticação
