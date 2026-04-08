# 🌊 Weather Data Manager

Sistema modular para coleta e atualização de dados meteorológicos da API Port-Log Kenmare Bay.

## Estrutura dos Arquivos

- **`main.py`**: Script principal com lógica de execução e argumentos de linha de comando
- **`scraping.py`**: Módulo de scraping com classes `WeatherDownloader` e `DataProcessor`
- **`db_insert.py`**: Módulo de banco de dados com funções de criação de tabelas e inserção
- **`weather.db`**: Banco de dados SQLite com todos os dados coletados

## Funcionalidades

- **Download Inicial**: Coleta completa dos dados históricos (2025 até hoje)
- **Atualização Incremental**: Busca apenas dados mais recentes
- **Modo Automático**: Atualização contínua a cada hora

## Como Usar

### Download Inicial (primeira vez)
```bash
python main.py initial
```

### Atualização de Dados
```bash
python main.py update
```

### Modo Automático (background)
```bash
python main.py auto
```

### Usar Cookie Personalizado
```bash
python main.py update --cookie "seu_cookie_aqui"
```

## Datasets Coletados

1. **Tides (Marés)**: Níveis observados vs previstos
2. **CTD**: Condutividade, temperatura e profundidade
3. **Water Quality**: Qualidade da água
4. **Air Quality**: Qualidade do ar
5. **Meteorological**: Dados meteorológicos completos
6. **Waves**: Características de ondas
7. **Currents**: Dados de correntes marinhas

## Requisitos

- Python 3.8+
- requests
- pandas
- numpy

## Configuração

O script usa um session cookie padrão, mas você pode fornecer o seu próprio via parâmetro `--cookie`.

- Python 3.8+
- requests
- pandas
- numpy

## Configuração

O script usa um session cookie padrão, mas você pode fornecer o seu próprio via parâmetro `--cookie`.