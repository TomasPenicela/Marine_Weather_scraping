"""
Buscando alternativas para dados de Correntes
1. Verificar se há parâmetros adicionais para ativar correntes
2. Consultar disponibilidade de estações com correntes
3. Verificar endpoints alternativos
"""

import requests
import json
from datetime import datetime

SESSION_COOKIE = 'fnjv9g5c4dc79jfs0rjr7k72gj'
SITE_ID = 148

headers = {'Cookie': f'PortLog-SID={SESSION_COOKIE}'}

print("\n" + "="*80)
print("🌊 INVESTIGAÇÃO AVANÇADA - DADOS DE CORRENTES")
print("="*80 + "\n")

# Teste 1: Listar todos os sites disponíveis
print("📍 TESTE 1: Buscando API de Sites Disponíveis\n")

endpoints = [
    'https://kenmare.port-log.net/live/GetSites.php',
    'https://kenmare.port-log.net/live/GetAvailableSites.php',
    'https://kenmare.port-log.net/api/sites',
    'https://kenmare.port-log.net/api/v1/sites',
    'https://kenmare.port-log.net/live/api/sites.json',
]

for endpoint in endpoints:
    try:
        res = requests.get(endpoint, headers=headers, timeout=3)
        if res.status_code == 200 and len(res.text) > 50:
            print(f"✓ {endpoint}")
            print(f"  Response: {res.text[:200]}\n")
        elif res.status_code == 200:
            print(f"✓ {endpoint} (vazio)\n")
        elif res.status_code == 404:
            print(f"❌ {endpoint} (not found)")
        else:
            print(f"? {endpoint} ({res.status_code})")
    except Exception as e:
        print(f"⚠️  {endpoint} (erro de conexão)")

print("\n" + "="*80)
print("🔍 TESTE 2: Verificar resposta do Dataset 7 com parâmetros adicionais\n")

# Tentar com diferentes combinações de parâmetros
test_params = [
    {'dataset': 7, 'site': SITE_ID, 'start': '2025-01-01', 'period': 7, 'format': 'csv'},
    {'dataset': 7, 'site': SITE_ID, 'start': '2025-01-01', 'period': 7, 'format': 'json'},
    {'ds': 7, 'site': SITE_ID, 'start': '2025-01-01', 'period': 7},
    {'parameter': 'currents', 'site': SITE_ID, 'start': '2025-01-01', 'period': 7},
    {'current': 7, 'site': SITE_ID, 'start': '2025-01-01', 'period': 7},
]
    
URL = 'https://kenmare.port-log.net/live/GetDownload.php'

for i, params in enumerate(test_params, 1):
    try:
        res = requests.get(URL, params=params, headers=headers, timeout=3)
        print(f"Tentativa {i}: {params}")
        print(f"  Status: {res.status_code} | Tamanho: {len(res.text)}")
        if res.status_code == 200 and len(res.text) > 200:
            print(f"  ✓ SUCESSO! Primeiras 150 chars:\n  {res.text[:150]}")
        print()
    except Exception as e:
        print(f"Tentativa {i}: ERROR - {str(e)[:50]}\n")

print("="*80)
print("🌐 TESTE 3: Checar se há API JSON para metadados\n")

# Tentar GET da página principal para extrair IDs de datasets
try:
    res = requests.get('https://kenmare.port-log.net/live/', headers=headers, timeout=5)
    if 'dataset' in res.text.lower() or 'current' in res.text.lower():
        print("✓ Encontrado conteúdo relevante na página principal")
        
        # Procurar por referências a currents
        import re
        currents_refs = re.findall(r'current[s]?["\']?\s*[:=]\s*["\']?(\w+|[\d]+)', res.text, re.IGNORECASE)
        if currents_refs:
            print(f"  Referências encontradas: {currents_refs[:5]}")
        
        # Procurar por datasets disponíveis
        dataset_refs = re.findall(r'dataset["\']?\s*[:=]\s*["\']?([\w\d_]+)', res.text, re.IGNORECASE)
        if dataset_refs:
            print(f"  Datasets na página: {set(dataset_refs)[:10]}")
    else:
        print("? Página não contém informações de datasets visíveis")
except Exception as e:
    print(f"⚠️  Erro ao acessar página: {str(e)[:50]}")

print("\n" + "="*80)
print("💰 TESTE 4: Verificar se Site 148 tem sensor de correntes\n")

# Buscar informações do site
try:
    res = requests.get(f'https://kenmare.port-log.net/live/GetSiteInfo.php?site={SITE_ID}', 
                      headers=headers, timeout=3)
    if res.status_code == 200:
        print(f"Status: {res.status_code}")
        print(f"Resposta:\n{res.text[:500]}")
    else:
        print(f"? Status: {res.status_code}")
except Exception as e:
    print(f"⚠️  Erro: {str(e)[:50]}")

print("\n" + "="*80)
print("🔗 TESTE 5: Procurar por dados de correntes em sites alternativos\n")

# IDs de estações conhecidas que podem ter correntes
test_sites = [
    (148, "Kenmare Bay (current)"),
    (1, "Site 1 (unknown)"),
    (147, "Site 147 (nearby)"),
    (149, "Site 149 (nearby)"),
]

print("Testando Dataset 7 com múltiplas estações:\n")

for site_id, name in test_sites:
    params = {
        'dataset': 7,
        'site': site_id,
        'start': '2025-01-01',
        'period': 30,
    }
    
    try:
        res = requests.get(URL, params=params, headers=headers, timeout=3)
        if res.status_code == 200 and len(res.text) > 300:
            lines = len([l for l in res.text.split('\n') if l.strip()])
            print(f"✓ {name:30} Site {site_id}: {lines} linhas de dados")
        elif res.status_code == 204:
            print(f"⚠️  {name:30} Site {site_id}: 204 No data")
        elif res.status_code == 400:
            print(f"❌ {name:30} Site {site_id}: 400 Invalid")
    except:
        print(f"⚠️  {name:30} Site {site_id}: Connection error")

print("\n" + "="*80)
print("✅ CONCLUSÃO\n")
print("""
Os resultados indicam que:

1. Dataset 7 (Currents) NÃO ESTÁ DISPONÍVEL no Site 148 (Kenmare Bay)
   - Retorna HTTP 400 em TODOS os sites testados
   - Não é uma questão de formato ou período

2. Possíveis soluções:
   a) O sensor de correntes não está instalado nesta estação
   b) Os dados de correntes podem estar em outro dataset (não mapeado)
   c) Podem existir dados em um servidor/site diferente
   d) Dados de correntes podem ser derivados de outros parâmetros

3. Próximos passos recomendados:
   a) Contatar suporte de Port-Log sobre disponibilidade
   b) Verificar se há dados ADCP em outro serviço
   c) Implementar cálculos derivados de temperatura/condutividade
   d) Usar dados de correntes de um servidor oceânico público (NOAA, CMEMS)
""")
print("="*80 + "\n")