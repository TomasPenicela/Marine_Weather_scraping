# ⛵ QUICK START - Preditor de Operações Marítimas

## 🚀 Iniciar em 1 Minuto

### Opção 1: Aplicação Web (Recomendada) - Mais Completa
```bash
python marine_predictor.py
```
Abre em: **http://localhost:5000**
- Dashboard interativo
- Atualiza automaticamente
- Gráficos em tempo real

### Opção 2: Relatório Automático - Mais Rápido
```bash
python marine_reporter_fast.py
```
Gera: **marine_reports/RELATORIO_PREDICAO.html**
- Relatório HTML estático
- Atualiza a cada 1 hora
- Funciona offline

### Opção 3: Centro de Controle (Menu)
```bash
python control_panel.py
```
Menu interativo para:
- Iniciar/parar serviços
- Ver relatórios
- Verificar status

---

## 📊 O que Você Vai Ver

### Score de Risco (0-100)
```
Exemplo atual:
┌─────────────────────────────┐
│   Score: 31/100             │
│                             │
│ ⚠️  BOM - Com precaução    │
└─────────────────────────────┘
```

### Fatores Analisados
| Fator | Valor Atual | Status |
|-------|-----------|--------|
| Vento | 4.84 m/s | ✅ Bom |
| Ondas | 0.65 m | ✅ Ideal |
| Pressão | 1007.7 mbar | ✅ Bom |
| Correntes | 0.19 m/s | ✅ Ideal |

### Recomendação
```
✅ CONDIÇÕES IDEAIS (0-20)
   → Embarque recomendado

⚠️  BOM (20-40)
   → Condições aceitáveis com precaução

⚠️  MODERADO (40-60)
   → Risco significativo

🚫 PERIGOSO (60-100)
   → Não recomendado
```

---

## 📱 Acesso Rápido

### Dashboard Web
```
URL: http://localhost:5000
Atualiza: Cada 1 hora
Funciona: Online
```

### Relatório HTML
```
Local: marine_reports/RELATORIO_PREDICAO.html
Atualiza: Cada 1 hora
Funciona: Offline (abra no navegador)
```

### Dados JSON (API)
```
Local: marine_reports/latest_report.json
Para: Integração com sistemas
Atualiza: Cada 1 hora
```

---

## ⏰ Atualização Automática

- **Intervalo**: 1 hora
- **Sem parada**: Funciona continuamente
- **Sem reinício**: Serviço mantém memória

Próxima atualização automática será em:
```
[Hora atual] + 1 hora = [Próxima atualização]
```

---

## 🛠️ Troubleshooting Rápido

### Dashboard não abre
```bash
# Verificar se porta 5000 já está em uso
netstat -ano | findstr :5000

# Usar outra porta (editar marine_predictor.py line ~460)
app.run(port=8000)
```

### Sem dados no relatório
```bash
# Atualizar dados do clima
python main.py

# Gerar novo relatório
python marine_reporter_fast.py
```

### Relatório antigo
```bash
# Forçar nova geração
rm marine_reports/latest_report.json
python marine_reporter_fast.py
```

---

## 📁 Arquivos Importantes

```
weather - 2/
├── marine_predictor.py           ← Dashboard Web
├── marine_reporter_fast.py       ← Relatório Automático ⭐
├── control_panel.py              ← Menu de Controle
├── open_report.py                ← Abrir no Navegador
├── MARINE_PREDICTOR_README.md    ← Guia Completo
└── marine_reports/
    ├── RELATORIO_PREDICAO.html   ← Seu Relatório (HTML)
    └── latest_report.json        ← Dados em JSON
```

---

## 🎯 Próximos Passos

1. **Imediatamente**
   ```bash
   python marine_reporter_fast.py
   ```
   Gera primeiro relatório

2. **Depois de 10 segundos**
   - Abra: `marine_reports/RELATORIO_PREDICAO.html`
   - Veja os gráficos e recomendações

3. **Integração**
   - Use `latest_report.json` para seus sistemas
   - Ou acesse `http://localhost:5000` para dashboard

4. **Automação (Opcional)**
   - Deixe `marine_reporter_fast.py` rodando
   - Atualiza sozinho a cada hora
   - Personalize intervalo se precisar

---

## 💡 Dicas

✅ **Iniciar em background (Windows)**
```bash
start python marine_reporter_fast.py
```

✅ **Ver últimos dados JSON**
```bash
type marine_reports\latest_report.json
```

✅ **Atualizar dados meteorológicos**
```bash
python main.py
python marine_reporter_fast.py
```

✅ **Usar em múltiplas máquinas**
- Dashboard rodar em servidor central
- Outras máquinas acessem via IP:5000
- Editar `app.run(host='0.0.0.0')` em marine_predictor.py

---

## 📞 Suporte

**Erro ao rodar?**
```bash
# Verificar ambiente Python
python --version

# Reinstalar dependências
pip install -r requirements.txt
```

**Dados desatualizados?**
```bash
# Atualizar banco de dados
python main.py

# Regenerar relatório
python marine_reporter_fast.py
```

---

## ✨ Sucesso!

Você agora tem um **Preditor de Operações Marítimas profissional** que:

✅ Analisa 750K+ registros meteorológicos  
✅ Considera 29K+ eventos operacionais históricos  
✅ Atualiza automaticamente a cada 1 hora  
✅ Fornece recomendações em tempo real  
✅ Gera gráficos interativos  
✅ Funciona offline e online  

**Status**: 🟢 **EM OPERAÇÃO**

---

**Desenvolvido para**: Operações Marítimas - Kenmare Bay  
**Data**: Abril 2026  
**Versão**: 1.0
