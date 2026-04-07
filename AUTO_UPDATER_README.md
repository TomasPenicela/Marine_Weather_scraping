# 🌊 Weather Data Auto-Updater

Sistema automático para atualizar dados meteorológicos a cada hora.

## 📋 Opções de Atualização

### Opção 1: Auto-Updater Manual (Simples)
Execute em um Terminal PowerShell e deixe rodando:

```powershell
python auto_updater.py
```

**Vantagens:**
- Simples de usar
- Sem permissões de admin necessárias
- Fácil de parar (Ctrl+C)

**Desvantagens:**
- Terminal fica aberto
- Para ao desligar o computador

---   

### Opção 2: Windows Task Scheduler (Automático)
Configure uma tarefa que executa a cada hora automaticamente:

#### Passo 1: Abra Terminal como Administrador
1. Click direita no menu **Iniciar** (Windows + X)
2. Selecione **"Terminal do Windows (Admin)"** ou **"PowerShell (Admin)"**
3. Confirme a permissão

#### Passo 2: Execute o Setup
```powershell
cd "c:\Users\tpenicela\Downloads\weather - 2"
python setup_scheduler.py
```

#### Passo 3: Confirme a Instalação
A tarefa será criada como "WeatherDataAutoUpdater" e vai:
- ✅ Executar automaticamente a cada hora
- ✅ Iniciar ao ligar o computador
- ✅ Executar em background (sem janela)

---

## 🎮 Comandos Úteis

### Listar tarefas agendadas:
```powershell
schtasks /query /tn "WeatherDataAutoUpdater" /v
```

### Executar manualmente agora:
```powershell
schtasks /run /tn "WeatherDataAutoUpdater"
```

### Desabilitar (pausar atualizações):
```powershell
schtasks /change /tn "WeatherDataAutoUpdater" /disable
```

### Reabilitar (retomar atualizações):
```powershell
schtasks /change /tn "WeatherDataAutoUpdater" /enable
```

### Excluir a tarefa:
```powershell
schtasks /delete /tn "WeatherDataAutoUpdater" /f
```

---

## 📊 Log de Atualizações

As atualizações são registradas em:
```
weather_updates.log
```

Comandos para visualizar:
```powershell
# Ver últimas 20 linhas:
Get-Content weather_updates.log -Tail 20

# Ver tudo:
Get-Content weather_updates.log

# Tempo real (tipo 'tail -f'):
Get-Content weather_updates.log -Wait
```

---

## 🔍 Status de Atualizações

Para verificar os registros mais recentes:

```powershell
python check_db.py
```

Mostra:
- Total de registros por dataset
- Últimas datas atualizadas
- Conformidade de dados

---

## ⚙️ Configurações

No arquivo `auto_updater.py`, você pode ajustar:

```python
UPDATE_INTERVAL_HOURS = 1  # Mudar de 1 para outro valor
```

Valores sugeridos:
- `0.5` = 30 minutos
- `1` = 1 hora (padrão)
- `2` = 2 horas
- `4` = 4 horas
- `24` = 1 dia

---

## 🚨 Troubleshooting

### "Acesso negado" ao rodar setup_scheduler.py
**Solução:** Execute o Terminal como **Administrador**

### Tarefa não executa
**Verificar:**
1. Abra "Agendador de Tarefas"
2. Procure por "WeatherDataAutoUpdater"
3. Clique em "Histórico" para ver erros
4. Verifique se está marcado como "Habilitado"

### Ver detalhes do erro
```powershell
Get-ScheduledTask -TaskName "WeatherDataAutoUpdater" | Select-Object *
```

### A tarefa sempre roda, mas não produz dados
**Verificar:**
1. Cookie da sessão pode ter expirado (a cada ~7-14 dias)
2. Verifique em `weather_updates.log`
3. Atualize o `SESSION_COOKIE` em `auto_updater.py`

---

## 📈 Métricas de Atualização

Com `UPDATE_INTERVAL_HOURS = 1`:
- **Por hora:** ~100-200 registros novos (variável)
- **Por dia:** ~2.4K - 4.8K registros
- **Por mês:** ~72K - 144K registros
- **Por ano:** ~876K - 1.75M registros

(Números estimados, variam por dataset)

---

## 🛑 Parar Atualizações

### Temporário (Desabilitar):
```powershell
schtasks /change /tn "WeatherDataAutoUpdater" /disable
```

### Permanente (Remover):
```powershell
schtasks /delete /tn "WeatherDataAutoUpdater" /f
```

Ou simplesmente feche o Terminal se usar `python auto_updater.py`

---

## ✅ Checklist de Configuração

- [ ] Python está instalado
- [ ] Pasta weather está acessível
- [ ] Arquivo `auto_updater.py` existe
- [ ] Arquivo `setup_scheduler.py` existe
- [ ] SESSION_COOKIE está válido
- [ ] Terminal foi aberto como Admin (se usar Task Scheduler)
- [ ] `setup_scheduler.py` foi executado
- [ ] Tarefa aparece no Task Scheduler
- [ ] Primeira execução retornou sucesso
- [ ] `weather_updates.log` foi criado

---

**Versão:** 1.0  
**Atualizado:** 01/04/2026  
**Contato:** Weather Data System
