from database import create_tables
from downloader import WeatherDownloader
from processor import DataProcessor
import sqlite3
from datetime import date

# ===== CONFIGURAÇÕES =====
SESSION_COOKIE = "fnjv9g5c4dc79jfs0rjr7k72gj"
SITE_ID = 148
START_DATE = "2025-01-01"     # Janeiro 2025
END_DATE = date.today().strftime("%Y-%m-%d")  # Dados atualizados até hoje
PERIOD_DAYS = 30               # Baixar em chunks de 30 dias

# Mapeamento de Dataset ID -> Nome da Tabela e Descrição
DATASETS = {
    1: {"table": "tides", "name": "Tides (Marés)", "desc": "Níveis de maré observados vs previstos"},
    2: {"table": "ctd", "name": "CTD (Condutividade/Temp/Prof)", "desc": "Dados de condutividade e temperatura"},
    3: {"table": "water_quality", "name": "WQ (Water Quality)", "desc": "Qualidade da água - Temperatura"},
    4: {"table": "air_quality", "name": "AQ (Air Quality)", "desc": "Qualidade do ar"},
    5: {"table": "meteorological", "name": "Met (Meteorológico)", "desc": "Dados meteorológicos completos"},
    6: {"table": "waves", "name": "Waves (Ondas)", "desc": "Características de ondas e espectro"},
    7: {"table": "currents", "name": "Currents (Correntes)", "desc": "Dados de correntes marinhas"},
}

def get_record_counts(db_path="weather.db"):
    """Retorna contagem de registros por tabela"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    counts = {}
    
    for ds_id, info in DATASETS.items():
        table = info["table"]
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = cursor.fetchone()[0]
        except:
            counts[table] = 0
    
    conn.close()
    return counts
                                                                
def main():
    """Orquestra o download e armazenamento de dados de 2025 até hoje"""
    print("\n" + "="*70)
    print("🌊 WEATHER DATA DOWNLOADER - Dados de 2025 até hoje")
    print("   Port-Log Kenmare Bay - Site ID 148\n")
    print(f"   Período: {START_DATE} a {END_DATE}")
    print(f"   Chunk: {PERIOD_DAYS} dias")
    print("="*70 + "\n")
    
    # Criar tabelas
    print("📦 Criando estrutura de banco de dados...")
    create_tables()
    print("✓ Banco de dados inicializado\n")
    
    # Inicializar downloader e processor
    downloader = WeatherDownloader(SESSION_COOKIE)
    processor = DataProcessor()
    
    # Processamento por dataset
    results = {}
    
    for ds_id, info in DATASETS.items():
        table = info["table"]
        name = info["name"]
        desc = info["desc"]
        
        print(f"⬇️  {name}")
        print(f"   {desc}")
        print(f"   Baixando {PERIOD_DAYS} dias por vez..., " , end="", flush=True)
        
        # Baixar em chunks
        csv_chunks = downloader.fetch_period_range(
            site=SITE_ID,
            start_date=START_DATE,
            end_date=END_DATE,
            dataset_id=ds_id,
            period_days=PERIOD_DAYS
        )
        
        print(f"{len(csv_chunks)} períodos", end="")
        
        # Processar chunks
        total, errors = processor.process_dataset(table, csv_chunks)
        
        results[name] = {
            "total": total,
            "errors": errors,
            "success": total > 0
        }
        
        if total > 0:
            print(f"\n   ✓ {total:,} registros salvos\n")
        else:
            print(f"\n   ⚠️  Sem dados neste período\n")
    
    # Resumo detalhado
    print("\n" + "="*70)
    print("📊 RESUMO DETALHADO\n")
    
    counts = get_record_counts()
    total_records = 0
    successful = 0
    
    for name, result in results.items():
        status = "✓" if result["success"] else "⚠️ "
        print(f"   {status} {name:30} {result['total']:>10,} registros")
        total_records += result["total"]
        if result["success"]:
            successful += 1
    
    print("\n" + "-"*70)
    print(f"   TOTAL: {total_records:>30,} registros em {successful} dataset(s)")
    print("="*70 + "\n")
    
    print("Banco de dados: weather.db")
    print(f"Período coberto: {START_DATE} a {END_DATE} (atualizado)")

if __name__ == "__main__":
    main()