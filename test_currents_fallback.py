"""
Teste do novo downloader com fallback para Currentes
"""

from downloader import WeatherDownloader

SESSION_COOKIE = 'fnjv9g5c4dc79jfs0rjr7k72gj'
SITE_ID = 148

print("\n" + "="*80)
print("🌊 TESTE - DOWNLOADER COM FALLBACK PARA CORRENTES")
print("="*80 + "\n")

downloader = WeatherDownloader(SESSION_COOKIE)

# Teste 1: Tentar baixar Correntes (Dataset 7)
print("Testando Dataset 7 (Currents) com fallback:\n")

csv_chunks = downloader.fetch_period_range(
    site=SITE_ID,
    start_date="2025-01-01",
    end_date="2025-01-15",
    dataset_id=7,
    period_days=7
)

print(f"Total de chunks: {len(csv_chunks)}\n")

for i, (csv_data, status_code, period_start) in enumerate(csv_chunks, 1):
    if csv_data:
        lines = len(csv_data.split('\n'))
        print(f"Chunk {i} ({period_start}): Status {status_code} | {lines} linhas")
        
        # Mostrar amostra
        first_lines = csv_data.split('\n')[:4]
        for line in first_lines[:3]:
            print(f"  {line[:100]}")
        print()
    else:
        print(f"Chunk {i} ({period_start}): Status {status_code} | Vazio\n")

print("="*80)
print("✅ Teste concluído!")
print("\nOs dados de Correntes agora estão sendo GERADOS usando modelo tidal")
print("quando not estão disponíveis na API Port-Log (Dataset 7 retorna 400).\n")
print("="*80 + "\n")
