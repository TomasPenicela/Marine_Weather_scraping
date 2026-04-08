"""
🌊 WEATHER DATA MANAGER - Sistema Unificado de Coleta e Atualização
Coleta dados meteorológicos da API Port-Log Kenmare Bay e armazena em SQLite

Modos de operação:
- initial: Download completo dos dados históricos (2025 até hoje)
- update: Atualização incremental dos dados mais recentes
- auto: Atualização automática contínua a cada hora
"""

import time
import sqlite3
import logging
import sys
import argparse
from datetime import datetime, timedelta
from scraping import WeatherDownloader, DataProcessor
from db_insert import create_tables

# ===== CONFIGURAÇÕES =====
SESSION_COOKIE = "fnjv9g5c4dc79jfs0rjr7k72gj"
SITE_ID = 148
DB_PATH = "weather.db"
UPDATE_INTERVAL_HOURS = 1

# Mapeamento de datasets
DATASETS = {
    1: {"table": "tides", "name": "Tides (Marés)", "desc": "Níveis de maré observados vs previstos"},
    2: {"table": "ctd", "name": "CTD (Condutividade/Temp/Prof)", "desc": "Dados de condutividade e temperatura"},
    3: {"table": "water_quality", "name": "WQ (Water Quality)", "desc": "Qualidade da água - Temperatura"},
    4: {"table": "air_quality", "name": "AQ (Air Quality)", "desc": "Qualidade do ar"},
    5: {"table": "meteorological", "name": "Met (Meteorológico)", "desc": "Dados meteorológicos completos"},
    6: {"table": "waves", "name": "Waves (Ondas)", "desc": "Características de ondas e espectro"},
    7: {"table": "currents", "name": "Currents (Correntes)", "desc": "Dados de correntes marinhas"},
}

# ===== FUNÇÕES PRINCIPAIS =====

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

def get_last_update_time(table_name):
    """Obtém timestamp do último registro"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(f"SELECT MAX(date_time) FROM {table_name}")
        result = cursor.fetchone()[0]
        conn.close()
        return datetime.fromisoformat(result) if result else datetime.now() - timedelta(days=1)
    except:
        return datetime.now() - timedelta(days=1)

def initial_download():
    """Download completo dos dados históricos"""
    print("\n" + "="*70)
    print("🌊 WEATHER DATA DOWNLOADER - Download Inicial Completo")
    print("   Port-Log Kenmare Bay - Site ID 148\n")
    print(f"   Período: 2025-07-01 até hoje")
    print("="*70 + "\n")

    # Criar tabelas
    print("📦 Criando estrutura de banco de dados...")
    create_tables()
    print("✓ Banco de dados inicializado\n")

    # Inicializar componentes
    downloader = WeatherDownloader(SESSION_COOKIE)
    processor = DataProcessor()

    # Processamento por dataset
    results = {}
    total_records = 0
    successful = 0

    for ds_id, info in DATASETS.items():
        table = info["table"]
        name = info["name"]
        desc = info["desc"]

        print(f"⬇️  {name}")
        print(f"   {desc}")
        print("   Baixando dados históricos..." , end="", flush=True)

        # Baixar dados históricos
        csv_chunks = downloader.fetch_period_range(
            site=SITE_ID,
            start_date="2025-07-01",
            end_date=datetime.now().strftime("%Y-%m-%d"),
            dataset_id=ds_id,
            period_days=30
        )

        print(f" {len(csv_chunks)} períodos", end="")

        # Processar
        total, errors = processor.process_dataset(table, csv_chunks)

        results[name] = {"total": total, "errors": errors, "success": total > 0}
        total_records += total
        if total > 0:
            successful += 1
            print(f"\n   ✓ {total:,} registros salvos\n")
        else:
            print(f"\n   ⚠️  Sem dados disponíveis\n")

    # Resumo
    print("\n" + "="*70)
    print("📊 RESUMO DO DOWNLOAD INICIAL\n")
    counts = get_record_counts()

    for name, result in results.items():
        status = "✓" if result["success"] else "⚠️ "
        table = next(info["table"] for info in DATASETS.values() if info["name"] == name)
        count = counts.get(table, 0)
        print(f"   {status} {name:30} {count:>10,} registros")

    print("\n" + "-"*70)
    print(f"   TOTAL: {total_records:>30,} registros em {successful} dataset(s)")
    print("="*70 + "\n")

def update_data():
    """Atualização incremental dos dados mais recentes"""
    print("\n" + "="*60)
    print("🔄 WEATHER DATA UPDATER - Atualização Incremental")
    print("="*60)

    downloader = WeatherDownloader(SESSION_COOKIE)
    processor = DataProcessor()

    total_new_records = 0
    successful_updates = 0

    for ds_id, info in DATASETS.items():
        table = info["table"]
        name = info["name"]

        try:
            # Verificar último update
            last_update = get_last_update_time(table)

            if datetime.now() - last_update > timedelta(hours=2):
                # Buscar dados das últimas 48 horas
                start_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
                end_date = datetime.now().strftime("%Y-%m-%d")

                print(f"  📥 {name:30} ", end="", flush=True)

                csv_chunks = downloader.fetch_period_range(
                    site=SITE_ID,
                    start_date=start_date,
                    end_date=end_date,
                    dataset_id=ds_id,
                    period_days=1
                )

                # Processar
                total, errors = processor.process_dataset(table, csv_chunks)

                if total > 0:
                    print(f"✓ +{total} registros")
                    total_new_records += total
                    successful_updates += 1
                else:
                    print("⚠️ Sem novos dados")
            else:
                print(f"  ✓ {name:30} dados atualizados (< 2h)")

        except Exception as e:
            print(f"  ❌ {name:30} erro: {str(e)[:40]}")

    print(f"\n✅ Atualização concluída: +{total_new_records} registros em {successful_updates} dataset(s)")

    # Verificação final
    print("\n📊 STATUS ATUAL DO BANCO:")
    counts = get_record_counts()
    for table, count in counts.items():
        name = next(info["name"] for info in DATASETS.values() if info["table"] == table)
        print(f"  {name:25} : {count:>8,} registros")

def auto_update():
    """Modo automático - atualização contínua"""
    print("\n" + "="*70)
    print("🌊 AUTO-UPDATER - Atualizações Automáticas Contínuas")
    print("   Port-Log Kenmare Bay - Site ID 148")
    print(f"   Intervalo: A cada {UPDATE_INTERVAL_HOURS} hora(s)")
    print("="*70)

    # Configurar logging
    logging.basicConfig(
        filename='weather_updates.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logging.info("Auto-updater iniciado")

    iteration = 0
    while True:
        iteration += 1
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n[{timestamp}] 🔄 Iniciando atualização #{iteration}...")

        try:
            update_data()
            logging.info(f"Atualização #{iteration} concluída")
        except Exception as e:
            print(f"❌ Erro na atualização: {str(e)}")
            logging.error(f"Erro na atualização #{iteration}: {str(e)}")

        print(f"\n⏱️ Próxima atualização em {UPDATE_INTERVAL_HOURS} hora(s)...")
        print("   (Pressione Ctrl+C para parar)\n")

        try:
            time.sleep(UPDATE_INTERVAL_HOURS * 3600)
        except KeyboardInterrupt:
            print("\n\n⏹️ Auto-updater parado pelo usuário")
            logging.info("Auto-updater parado pelo usuário")
            break

def main():
    """Função principal com argumentos de linha de comando"""
    global SESSION_COOKIE  # Declarar global antes de usar

    parser = argparse.ArgumentParser(description='Weather Data Manager - Sistema Unificado')
    parser.add_argument('mode', choices=['initial', 'update', 'auto'],
                       help='Modo de operação: initial (download completo), update (atualização), auto (automático)')
    parser.add_argument('--cookie', default=SESSION_COOKIE,
                       help='Session cookie para autenticação na API')

    args = parser.parse_args()

    # Atualizar cookie se fornecido
    SESSION_COOKIE = args.cookie

    if args.mode == 'initial':
        initial_download()
    elif args.mode == 'update':
        update_data()
    elif args.mode == 'auto':
        auto_update()

if __name__ == "__main__":
    main()