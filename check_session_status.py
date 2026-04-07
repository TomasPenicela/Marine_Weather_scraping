"""
Verificar status geral da API e se cookie está válido
"""

import requests

SESSION_COOKIE = 'fnjv9g5c4dc79jfs0rjr7k72gj'
SITE_ID = 148
START_DATE = '2025-01-01'

headers = {'Cookie': f'PortLog-SID={SESSION_COOKIE}'}
URL = 'https://kenmare.port-log.net/live/GetDownload.php'

print("\n" + "="*80)
print("🔐 VERIFICAR STATUS DA SESSÃO E API")
print("="*80 + "\n")

# Teste os datasets que sabemos que funcionam
datasets_test = [
    (1, "Tides"),
    (3, "Water Quality"),
    (5, "Meteorological"),
    (6, "Waves"),
    (7, "Currents"),
]

print("Testando quais datasets estão funcionando agora:\n")

params = {
    'dataset': None,
    'site': SITE_ID,
    'start': START_DATE,
    'period': 7,
    'format': 'csv',
}

for ds_id, name in datasets_test:
    params['dataset'] = ds_id
    
    try:
        res = requests.get(URL, params=params, headers=headers, timeout=5)
        
        if res.status_code == 200 and len(res.text) > 200:
            lines = len([l for l in res.text.split('\n') if l.strip()])
            print(f"✓ Dataset {ds_id} ({name:20}): {res.status_code} | {lines:5} linhas | {len(res.text):7} bytes")
        elif res.status_code == 200:
            print(f"? Dataset {ds_id} ({name:20}): {res.status_code} | Vazio ({len(res.text)} bytes)")
        elif res.status_code == 204:
            print(f"⚠️  Dataset {ds_id} ({name:20}): 204 No Data")
        elif res.status_code == 400:
            print(f"❌ Dataset {ds_id} ({name:20}): 400 Invalid")
        else:
            print(f"? Dataset {ds_id} ({name:20}): {res.status_code}")
    except Exception as e:
        print(f"⚠️  Dataset {ds_id} ({name:20}): ERROR - {str(e)[:30]}")

print("\n" + "="*80)
print("📊 ANÁLISE DE RESPOSTA DATASET 7\n")

params['dataset'] = 7

try:
    res = requests.get(URL, params=params, headers=headers, timeout=5)
    print(f"Status Code: {res.status_code}")
    print(f"Content Length: {len(res.text)} bytes")
    print(f"Headers: {dict(res.headers)}")
    print(f"Text: '{res.text}'")
    print(f"\nPossível causa: {'Sensor não disponível neste site' if res.status_code == 400 else 'Outro erro'}")
except Exception as e: 
    print(f"ERROR: {e}")

print("\n" + "="*80)   
print("TENTAR COM OUTRO SITE PARA COMPARAR\n")

# Tentar com site 1 
params['site'] = 1 
params['dataset'] = 7 

try:
    res = requests.get(URL, params=params, headers=headers, timeout=5)
    print(f"Dataset 7, Site 1: Status {res.status_code} ({len(res.text)} bytes)")
except Exception as e:
    print(f"Dataset 7, Site 1: ERROR")

print("\n" + "="*80)
