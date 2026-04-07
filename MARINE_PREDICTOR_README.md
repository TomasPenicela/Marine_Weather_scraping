# 🌊 Preditor de Operações Marítimas - Guia Completo

## 📋 Visão Geral

Sistema inteligente que prevê os **melhores dias para embarcar** com base em dados meteorológicos, oceanográficos e histórico de operações marinhas. O sistema se atualiza **automaticamente a cada 1 hora** com previsões em tempo real.

## 📊 Dados Utilizados

### Fonte 1: `weather.db` (SQLite)
- **Meteorológico**: Vento, pressão, temperatura, umidade
- **Ondas**: Altura, período, direção das ondas
- **Correntes**: Velocidade e direção das correntes marinhas
- **Marés**: Níveis observados e previstos
- **Período**: Últimos 30 dias com atualização contínua

### Fonte 2: `Opsdata_MarineHistory.xlsx`
- **Histórico de Operações**: 29.997 eventos registrados
- **Informações**: Datas, horários, duração, motivos de atrasos
- **Análise**: Padrões de sucesso/falha em diferentes condições climáticas

## 🎯 Score de Risco (0-100)

O modelo calcula um **score de risco** baseado em 4 fatores principais:

| Score | Condição | Recomendação |
|-------|----------|--------------|
| **0-20** | ✅ IDEAL | Embarque recomendado |
| **20-40** | ⚠️ BOM | Condições aceitáveis com precaução |
| **40-60** | ⚠️ MODERADO | Risco significativo - avaliar operação |
| **60-100** | 🚫 PERIGOSO | Não recomendado - aguarde melhoria |

### Fatores Analisados:

1. **Velocidade do Vento** (0-30 pontos)
   - Ideal: < 5 m/s
   - Perigoso: > 15 m/s

2. **Altura de Ondas** (0-30 pontos)
   - Ideal: < 1 m
   - Perigoso: > 3 m

3. **Pressão Atmosférica** (0-20 pontos)
   - Ideal: > 1010 mbar
   - Perigoso: < 990 mbar

4. **Velocidade de Correntes** (0-20 pontos)
   - Ideal: < 0.5 m/s
   - Perigoso: > 1 m/s

## 🚀 Como Usar

### Opção 1: Dashboard em Tempo Real (Recomendado)

**Servidor Flask com atualização automática:**

```bash
python marine_predictor.py
```

Acesso: `http://localhost:5000`

**Recursos:**
- Dashboard interativo em tempo real
- Auto-refresh a cada 1 hora
- Gráficos Plotly interativos com zoom/pan
- Visualização de histórico de 30 dias
- Recomendações personalizadas

### Opção 2: Relatório HTML Automático

**Serviço de background com geração de relatórios:**

```bash
python marine_reporter_fast.py
```

**Recursos:**
- Gera relatório HTML em `marine_reports/RELATORIO_PREDICAO.html`
- Atualiza automaticamente a cada 1 hora
- Funciona offline - apenas abra o HTML no navegador
- JSON com dados estruturados em `marine_reports/latest_report.json`

**Para visualizar o relatório:**
```bash
start marine_reports\RELATORIO_PREDICAO.html
```

### Opção 3: Acesso via API (JSON)

Para integração com sistemas externos:

```bash
# Ler dados mais recentes
cat marine_reports/latest_report.json
```

Exemplo de conteúdo:
```json
{
  "timestamp": "2026-04-06T07:51:30",
  "risk_score": 31,
  "recommendation": "⚠️ BOM - Com precaução",
  "factors": {
    "Vento": {"valor": 4.84, "unidade": "m/s", "score": 12},
    "Altura Ondas": {"valor": 0.65, "unidade": "m", "score": 6},
    "Pressão": {"valor": 1007.7, "unidade": "mbar", "score": 10},
    "Correntes": {"valor": 0.19, "unidade": "m/s", "score": 3}
  },
  "num_weather_records": 719,
  "num_operations": 449
}
```

## 📈 Gráficos e Visualizações

### 1. Histórico de Score de Risco
- Série temporal com 30 dias
- Zonas coloridas: Verde (ideal), Amarelo (bom), Vermelho (perigoso)
- Markers em cada medição
- Ajuda a identificar tendências

### 2. Condições Marítimas
- **Vento**: Variação da velocidade do vento ao longo do tempo
- **Ondas**: Evolução da altura significativa de ondas
- **Pressão**: Tendência de pressão atmosférica
- **Correntes**: Intensidade das correntes marinhas

### 3. Histórico de Eventos Operacionais
- Número de eventos por dia
- Duração total de downtime
- Identificação de padrões problemáticos

## ⏰ Atualização Automática

- **Intervalo**: A cada 1 hora (3600 segundos)
- **Próxima atualização**: Mostrada no dashboard
- **Sem parada de serviço**: Continua rodando em background
- **Sincronização**: Todos os serviços compartilham dados

## 🔧 Configuração Avançada

### Arquivo: `marine_predictor.py`
- Servidor Flask com interface web
- Mais recursos, interface responsiva
- Ideal para monitoramento contínuo

### Arquivo: `marine_reporter_fast.py` ⭐ **PADRÃO**
- Geração rápida de relatórios
- Modo headless (sem servidor)
- Melhor para integração com sistemas
- Economia de recursos

### Personalização

Para modificar o intervalo de atualização:

```python
# Em marine_reporter_fast.py, linha ~430
time.sleep(3600)  # Mudar para 1800 = 30 min, 7200 = 2 horas, etc
```

Para ajustar limites de risco:

```python
# Em marine_reporter_fast.py, funções get_recommendation()
if risk_score <= 20:  # Mudar limite
    return "✅ CONDIÇÕES IDEAIS", "green"
```

## 📁 Estrutura de Arquivos

```
weather - 2/
├── weather.db                          # Banco de dados meteorológico
├── Opsdata_MarineHistory.xlsx          # Histórico de operações
├── marine_predictor.py                 # Servidor Flask (Web)
├── marine_reporter_fast.py             # Reporter com atualização automática
├── marine_reports/
│   ├── RELATORIO_PREDICAO.html        # Relatório principal (atualizado)
│   └── latest_report.json             # Dados em JSON (atualizado)
└── migrate_to_sqlserver.py            # Migração para SQL Server
```

## 🔗 Integração com SQL Server

Para copiar os dados para o SQL Server após a previsão:

```bash
python migrate_to_sqlserver.py
```

## 📊 Exemplo de Uso Prático

### Cenário: Você é Capitão de Navio

1. **Manhã (08:00)**
   - Abre `http://localhost:5000`
   - Score: 25/100 - Ligeiramente elevado
   - Recomendação: "⚠️ BOM - Com precaução"
   - Avalia: Vento em 5 m/s, ondas em 0.8 m
   - Decide: Embarcar com cuidado extra

2. **Tarde (14:00)**
   - Verifica novamente
   - Score: 45/100 - Piorou
   - Recomendação: "⚠️ MODERADO"
   - Observa: Vento aumentou para 9 m/s
   - Decide: Aguardar melhora ou retardar operação

3. **Noite (20:00)**
   - Score: 18/100 - Melhorou!
   - Recomendação: "✅ CONDIÇÕES IDEAIS"
   - Embarca com confiança

## 🛡️ Segurança e Disclaimers

⚠️ **IMPORTANTE**: Este sistema é uma ferramenta de suporte. Sempre:
- Consulte meteorologistas profissionais
- Verifique avisos da Guarda Costeira
- Respeite normas marítimas locais
- Não use como único critério de decisão
- Validar com sensores locais se disponível

## 📞 Suporte e Troubleshooting

### Problema: Dashboard não abre em localhost:5000
```bash
# Verificar se porta está em uso
netstat -ano | findstr :5000

# Mudar porta em marine_predictor.py
app.run(port=8000)  # Mudar para 8000 ou outra
```

### Problema: Relatório não atualiza
```bash
# Verificar se marine_reporter_fast.py está rodando
tasklist | findstr python

# Reiniciar serviço
# Ctrl+C para parar e python marine_reporter_fast.py para iniciar
```

### Problema: Dados faltando
```bash
# Verificar banco de dados
python check_db.py

# Atualizar dados
python main.py
```

## 📚 Referências Técnicas

- **Banco de Dados**: SQLite3 com 750K+ registros
- **Visualização**: Plotly (gráficos interativos)
- **Web**: Flask + HTML5/CSS3
- **Processamento**: Pandas + NumPy
- **Histórico**: 29.997 eventos operacionais
- **Taxa de Atualização**: 1 hora (configurável)

## 🎓 Melhorias Futuras

- [ ] Integração com API de meteorologia em tempo real
- [ ] Machine Learning piara prever ondas futuras
- [ ] Alertas por SMS/Email em caso de piora
- [ ] Integração com chatbot para avisos inteligentes
- [ ] Armazenamento de histórico em SQL Server
- [ ] Análise preditiva de 7 dias

---

**Versão**: 1.0  
**Data**: Abril 2026  
**Desenvolvido para**: Operações Marítimas - Kenmare Bay  
**Status**: ✅ Em Produção
