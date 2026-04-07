"""
Testes específicos para identificar parametros corretos para Currents
"""

import requests
from datetime import datetime, timedelta

SESSION_COOKIE = 'fnjv9g5c4dc79jfs0rjr7k72gj'
SITE_ID = 148

headers = {'Cookie': f'PortLog-SID={SESSION_COOKIE}'}
URL = 'https://kenmare.port-log.net/live/GetDownload.php'

print("\n" + "="*80)
print("🔧 TESTES DETALHADOS DATASET 7 - CURRENTS")
print("="*80 + "\n")

# Teste 1: Exatamente como está no downloader.py
print("TESTE 1: Exatamente como no downloader.py\n")

params = {
    "dataset": 7,
    "site": SITE_ID,
    "start": "2025-01-01",
    "period": 7,
    "format": "csv",
    "chart": "Download"
}

print(f"Parâmetros: {params}\n")

try:
    res = requests.get(URL, params=params, headers=headers, timeout=5)
    print(f"Status: {res.status_code}")
    print(f"Content-Type: {res.headers.get('Content-Type')}")
    print(f"Tamanho response: {len(res.text)} bytes")
    print(f"Response: '{res.text[:200]}'\n")
except Exception as e:
    print(f"ERRO: {e}\n")

# Teste 2: Testar com start_date em formato diferente
print("TESTE 2: Com formato de data diferente\n")

for date_format in ["2025-01-01", "01/01/2025", "01-01-2025"]:
    params = {
        "dataset": 7,
        "site": SITE_ID,
        "start": date_format,
        "period": 7,
        "format": "csv",
    }
    
    try:
        res = requests.get(URL, params=params, headers=headers, timeout=3)
        print(f"Data format '{date_format}': {res.status_code} ({len(res.text)} bytes)")
    except Exception as e:
        print(f"Data format '{date_format}': ERROR")

# Teste 3: Testar com diferentes períodos
print("\nTESTE 3: Diferentes períodos\n")

for period in [1, 3, 7, 14, 30, 60]:
    params = {
        "dataset": 7,
        "site": SITE_ID,
        "start": "2025-01-01",
        "period": period,
        "format": "csv",
    }
    
    try:
        res = requests.get(URL, params=params, headers=headers, timeout=3)
        if res.status_code == 200 and len(res.text) > 200:
            lines = len(res.text.split('\n'))
            print(f"✓ Período {period} dias: {res.status_code} ({lines} linhas, {len(res.text)} bytes)")
        elif res.status_code == 200:
            print(f"? Período {period} dias: {res.status_code} ({len(res.text)} bytes - vazio)")
        else:
            print(f"✗ Período {period} dias: {res.status_code}")
    except Exception as e:
        print(f"✗ Período {period} dias: ERROR")

# Teste 4: Testar com diferentes datas de início
print("\nTESTE 4: Diferentes datas iniciais\n")

test_dates = [
    "2024-01-01",
    "2024-06-01",
    "2024-10-01",
    "2025-01-01",
    "2025-06-01",
]

for start_date in test_dates:
    params = {
        "dataset": 7,
        "site": SITE_ID,
        "start": start_date,
        "period": 7,
        "format": "csv",
    }
    
    try:
        res = requests.get(URL, params=params, headers=headers, timeout=3)
        if res.status_code == 200 and len(res.text) > 200:
            lines = len(res.text.split('\n'))
            print(f"✓ Data {start_date}: {res.status_code} ({lines} linhas)")
        else:
            print(f"✗ Data {start_date}: {res.status_code}")
    except:
        print(f"✗ Data {start_date}: ERROR")

# Teste 5: Testar sem o parâmetro 'chart'
print("\nTESTE 5: Sem parâmetro 'chart'\n")

params_no_chart = {
    "dataset": 7,
    "site": SITE_ID,
    "start": "2025-01-01",
    "period": 7,
    "format": "csv",
}

try:
    res = requests.get(URL, params=params_no_chart, headers=headers, timeout=3)
    print(f"Sem 'chart': {res.status_code} ({len(res.text)} bytes)")
except Exception as e:
    print(f"Sem 'chart': ERROR")

# Teste 6: Com chart = 'Download'
params_with_chart = {
    "dataset": 7,
    "site": SITE_ID,
    "start": "2025-01-01",
    "period": 7,
    "format": "csv",
    "chart": "Download",
}

try:
    res = requests.get(URL, params=params_with_chart, headers=headers, timeout=3)
    print(f"Com 'chart=Download': {res.status_code} ({len(res.text)} bytes)")
except Exception as e:
    print(f"Com 'chart=Download': ERROR")

# Teste 7: DEBUG - GET request URL
print("\nTESTE 7: URL construída\n")

res = requests.Request('GET', URL, params=params_with_chart).prepare()
print(f"URL completa: {res.url}\n")

# Teste 8: Verificar cookies
print("TESTE 8: Cookie enviado\n")

print(f"Cookie: PortLog-SID={SESSION_COOKIE}")
print(f"Headers: {headers}")

print("\n" + "="*80)
