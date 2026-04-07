"""
Teste rápido do sistema de auto-updater
Executa uma atualização completa sem esperar pela agenda
"""

import sys
from datetime import datetime, timedelta
from database import create_tables
from downloader import WeatherDownloader
from processor import DataProcessor
import sqlite3

SESSION_COOKIE = "fnjv9g5c4dc79jfs0rjr7k72gj"
SITE_ID = 148
DB_PATH = "weather.db"

DATASETS = {
    1: {"table": "tides", "name": "Tides (Marés)"},
    3: {"table": "water_quality", "name": "WQ (Water Quality)"},
    5: {"table": "meteorological", "name": "Met (Meteorológico)"},
    6: {"table": "waves", "name": "Waves (Ondas)"},
    7: {"table": "currents", "name": "Currents (Correntes)"},
}

def get_record_count(table_name):
    """Obtém contagem de registros"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except:
        return 0

def test_update():
    """Testa uma atualização completa"""
    
    print("\n" + "="*70)
    print("🧪 TESTE - Auto-Updater")
    print("="*70 + "\n")
    
    # Inicializar
    print("📦 Inicializando banco de dados...")
    create_tables(DB_PATH)
    print("✓ Pronto\n")
    
    downloader = WeatherDownloader(SESSION_COOKIE)
    processor = DataProcessor()
    
    total_new = 0
    
    for ds_id, info in DATASETS.items():
        table = info["table"]
        name = info["name"]
        
        # Contar antes
        count_before = get_record_count(table)
        
        # Baixar dados das últimas 48 horas
        start_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
        end_date = datetime.now().strftime("%Y-%m-%d")
        
        print(f"📥 {name:30} ", end="", flush=True)
        
        csv_chunks = downloader.fetch_period_range(
            site=SITE_ID,
            start_date=start_date,
            end_date=end_date,
            dataset_id=ds_id,
            period_days=1
        )
        
        total, errors = processor.process_dataset(table, csv_chunks)
        
        # Contar depois
        count_after = get_record_count(table)
        new_records = count_after - count_before
        
        if new_records > 0:
            print(f"✓ +{new_records} registros (Total: {count_after:,})")
            total_new += new_records
        elif total > 0:
            print(f"✓ {total} processados (Total: {count_after:,})")
        else:
            print(f"⚠️ Sem novos dados (Total: {count_after:,})")
    
    print("\n" + "="*70)
    print(f"✅ Teste concluído: +{total_new} registros adicionados")
    print("="*70 + "\n")
    
    print("Para ativar atualizações automáticas, execute:")
    print("  python auto_updater.py\n")
    print("Ou configure agendador automático com:")
    print("  python setup_scheduler.py\n")

if __name__ == "__main__":
    try:
        test_update()
    except KeyboardInterrupt:
        print("\n\n⏹️ Teste cancelado")
    except Exception as e:
        print(f"\n❌ Erro: {str(e)}")
        import traceback
        traceback.print_exc()
