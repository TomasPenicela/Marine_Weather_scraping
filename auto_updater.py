"""
Auto-Updater: Atualiza dados a cada hora automaticamente
Executa em background e adiciona apenas os dados mais recentes
"""

import time
import sqlite3
from datetime import datetime, timedelta
from database import create_tables
from downloader import WeatherDownloader
from processor import DataProcessor
import logging

# ===== CONFIGURAÇÃO =====
SESSION_COOKIE = "fnjv9g5c4dc79jfs0rjr7k72gj"
SITE_ID = 148
DB_PATH = "weather.db"

# Configurar logging
logging.basicConfig(
    filename='weather_updates.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Datasets que queremos atualizar
DATASETS = {
    1: {"table": "tides", "name": "Tides (Marés)"},
    3: {"table": "water_quality", "name": "WQ (Water Quality)"},
    5: {"table": "meteorological", "name": "Met (Meteorológico)"},
    6: {"table": "waves", "name": "Waves (Ondas)"},
    7: {"table": "currents", "name": "Currents (Correntes)"},
}

UPDATE_INTERVAL_HOURS = 1  # Atualizar a cada 1 hora

def get_last_update_time(table_name):
    """Obtém o timestamp do último registro em uma tabela"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(f"SELECT MAX(date_time) FROM {table_name}")
        result = cursor.fetchone()[0]
        conn.close()
        
        if result:
            return datetime.fromisoformat(result)
        else:
            return datetime.now() - timedelta(days=1)  # Se vazio, pega último dia
    except:
        return datetime.now() - timedelta(days=1)

def update_datasets():
    """Atualiza todos os datasets com dados mais recentes"""
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[{timestamp}] 🔄 Iniciando atualização automática de dados...")
    logging.info("Iniciando atualização automática de dados")
    
    downloader = WeatherDownloader(SESSION_COOKIE)
    processor = DataProcessor()
    
    total_new_records = 0
    successful_updates = 0
    
    for ds_id, info in DATASETS.items():
        table = info["table"]
        name = info["name"]
        
        try:
            # Pegar último timestamp no banco
            last_update = get_last_update_time(table)
            
            # Se o último dado é de mais de 2 horas atrás, atualizar
            if datetime.now() - last_update > timedelta(hours=2):
                # Baixar dados das últimas 48 horas
                start_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
                end_date = datetime.now().strftime("%Y-%m-%d")
                
                print(f"  📥 {name:30} ", end="", flush=True)
                logging.info(f"Atualizando {name} (de {start_date} para {end_date})")
                
                # Baixar chunks
                csv_chunks = downloader.fetch_period_range(
                    site=SITE_ID,
                    start_date=start_date,
                    end_date=end_date,
                    dataset_id=ds_id,
                    period_days=1  # Períodos de 1 dia
                )
                
                # Processar
                total, errors = processor.process_dataset(table, csv_chunks)
                
                if total > 0:
                    print(f"✓ +{total} registros")
                    total_new_records += total
                    successful_updates += 1
                    logging.info(f"{name}: +{total} registros adicionados")
                else:
                    print(f"⚠️ Sem novos dados")
                    logging.info(f"{name}: sem novos dados")
            else:
                print(f"  ✓ {name:30} dados atualizados (< 2h)")
        
        except Exception as e:
            print(f"  ❌ {name:30} erro: {str(e)[:40]}")
            logging.error(f"{name}: {str(e)}")
    
    print(f"\n✅ Atualização concluída: +{total_new_records} registros em {successful_updates} dataset(s)")
    logging.info(f"Atualização concluída: +{total_new_records} registros")
    
    return total_new_records

def main():
    """Loop principal - atualiza a cada hora"""
    
    print("\n" + "="*70)
    print("🌊 AUTO-UPDATER - Atualizações Automáticas de Dados")
    print("   Port-Log Kenmare Bay - Site ID 148")
    print(f"   Intervalo: A cada {UPDATE_INTERVAL_HOURS} hora(s)")
    print("="*70)
    
    # Criar tabelas se não existirem
    print("\n📦 Inicializando banco de dados...")
    create_tables(DB_PATH)
    print("✓ Banco de dados pronto\n")
    
    logging.info("="*70)
    logging.info("Auto-Updater iniciado")
    logging.info(f"Intervalo de atualização: {UPDATE_INTERVAL_HOURS} hora(s)")
    
    # Loop infinito
    iteration = 0
    while True:
        iteration += 1
        
        try:
            update_datasets()
        except KeyboardInterrupt:
            print("\n\n⏹️ Auto-updater parado pelo usuário")
            logging.info("Auto-updater parado pelo usuário")
            break
        except Exception as e:
            print(f"\n❌ Erro na atualização: {str(e)}")
            logging.error(f"Erro na atualização: {str(e)}")
        
        # Esperar até próxima atualização
        print(f"\n⏱️ Próxima atualização em {UPDATE_INTERVAL_HOURS} hora(s)...")
        print("   (Pressione Ctrl+C para parar)\n")
        
        try:
            time.sleep(UPDATE_INTERVAL_HOURS * 3600)
        except KeyboardInterrupt:
            print("\n\n⏹️ Auto-updater parado pelo usuário")
            logging.info("Auto-updater parado pelo usuário")
            break

if __name__ == "__main__":
    main()
