"""
Diagnóstico completo para encontrar dados de Correntes
Testando diferentes Dataset IDs e combinações de parâmetros
"""

import requests
import re
from datetime import datetime, timedelta

SESSION_COOKIE = 'fnjv9g5c4dc79jfs0rjr7k72gj'
SITE_ID = 148
START_DATE = '2025-01-01'

headers = {'Cookie': f'PortLog-SID={SESSION_COOKIE}'}
URL = 'https://kenmare.port-log.net/live/GetDownload.php'

print("\n" + "="*80)
print("🔍 DIAGNÓSTICO DE DATASET - PROCURANDO DADOS DE CORRENTES")
print("="*80 + "\n")

# Teste 1: Test todos os IDs de 1 a 25
print("📊 TESTE 1: Testando Dataset IDs 1-25\n")
valid_ids = {}

for ds_id in range(1, 26):
    params = {
        'dataset': ds_id,
        'site': SITE_ID,
        'start': START_DATE,
        'period': 7,
        'format': 'csv',
        'chart': 'Download'
    }
    
    try:
        res = requests.get(URL, params=params, headers=headers, timeout=5)
        
        # Extrair informações da resposta
        has_data = len(res.text) > 100 and res.status_code == 200
        is_valid = 'Download_Complete' in res.text or (res.status_code == 200 and len(res.text) > 200)
        
        if is_valid or has_data:
            # Tentar extrair nome do dataset da resposta
            match = re.search(r'Dataset:\s*([^\n,]+)', res.text, re.IGNORECASE)
            name = match.group(1).strip() if match else "Unknown"
            
            valid_ids[ds_id] = {
                'status': res.status_code,
                'has_data': has_data,
                'name': name,
                'length': len(res.text)
            }
            print(f"✓ Dataset {ds_id:2d}: {res.status_code} | {name:40} | {len(res.text):6} bytes")
        elif res.status_code == 204:
            print(f"⚠️  Dataset {ds_id:2d}: {res.status_code} [No data available]")
        elif res.status_code == 400:
            print(f"❌ Dataset {ds_id:2d}: {res.status_code} [Invalid parameter]")
        else:
            print(f"? Dataset {ds_id:2d}: {res.status_code} [Unknown]")
            
    except Exception as e:
        print(f"⚠️  Dataset {ds_id:2d}: ERROR - {str(e)[:40]}")

print("\n" + "="*80)
print("📋 RESUMO DOS DATASETS VÁLIDOS")
print("="*80 + "\n")

for ds_id, info in sorted(valid_ids.items()):
    print(f"Dataset {ds_id}: {info['name']}")

print("\n" + "="*80)
print("🌊 TESTE 2: Analisando resposta do Dataset 7 (Currents) em detalhes\n")

params = {
    'dataset': 7,
    'site': SITE_ID,
    'start': START_DATE,
    'period': 7,
    'format': 'csv',
    'chart': 'Download'
}

try:
    res = requests.get(URL, params=params, headers=headers, timeout=5)
    print(f"Status Code: {res.status_code}")
    print(f"Response Length: {len(res.text)} bytes")
    print(f"Response Headers: {dict(res.headers)}")
    print(f"\nResponse Content (first 500 chars):\n{res.text[:500]}")
    
    if res.status_code == 400:
        print(f"\n⚠️  API Retorna 400 - Pode ser que o Dataset 7 não exista neste site")
        print("    Possíveis causas:")
        print("    1. Estação não tem sensor de correntes")
        print("    2. Dataset ID 7 não é correntes neste site")
        print("    3. Dados de correntes podem estar em outro dataset/site")
        
except Exception as e:
    print(f"ERROR: {e}")

print("\n" + "="*80)
print("🌊 TESTE 3: Testando diferentes Site IDs com Dataset 7\n")

other_sites = [1, 2, 3, 4, 5, 10, 20, 100, 147, 149, 150]

print("Testando Dataset 7 (Currents) em diferentes Sites:\n")

for site_id in other_sites:
    params = {
        'dataset': 7,
        'site': site_id,
        'start': START_DATE,
        'period': 7,
        'format': 'csv',
        'chart': 'Download'
    }
    
    try:
        res = requests.get(URL, params=params, headers=headers, timeout=3)
        
        if res.status_code == 200 and len(res.text) > 300:
            # Contar linhas de dados
            lines = res.text.count('\n')
            print(f"✓ Site {site_id:3d}: Status {res.status_code} | {lines} linhas | {len(res.text):6} bytes")
        elif res.status_code == 204:
            print(f"⚠️  Site {site_id:3d}: {res.status_code} [No data]")
        elif res.status_code == 400:
            print(f"❌ Site {site_id:3d}: {res.status_code} [Invalid]")
        else:
            print(f"? Site {site_id:3d}: {res.status_code}")
            
    except Exception as e:
        print(f"⚠️  Site {site_id:3d}: ERROR")

print("\n" + "="*80)
print("📝 TESTE 4: Verificar últimos dados conhecidos de Correntes\n")

# Testar com diferentes períodos
for period in [1, 3, 7, 14, 30]:
    params = {
        'dataset': 7,
        'site': SITE_ID,
        'start': '2024-10-01',  # Dados historicos
        'period': period,
        'format': 'csv',
        'chart': 'Download'
    }
    
    try:
        res = requests.get(URL, params=params, headers=headers, timeout=3)
        
        if res.status_code == 200 and len(res.text) > 300:
            lines = [l for l in res.text.split('\n') if l.strip() and not l.startswith('Dataset')]
            print(f"✓ Period {period:2d} days (Oct 2024): {res.status_code} | {len(lines)} linhas de dados")
        elif res.status_code == 204:
            print(f"⚠️  Period {period:2d} days (Oct 2024): 204 [No data]")
        else:
            print(f"? Period {period:2d} days (Oct 2024): {res.status_code}")
    except:
        print(f"⚠️  Period {period:2d} days (Oct 2024): ERROR")

print("\n" + "="*80)
