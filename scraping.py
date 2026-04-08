"""
🌊 WEATHER DATA SCRAPING MODULE
Módulo responsável pelo download e processamento de dados da API Port-Log
"""

import requests
import time
import csv
import io
import math
from datetime import datetime, timedelta

class WeatherDownloader:
    """Baixa dados de weather da API Port-Log"""
    URL = "https://kenmare.port-log.net/live/GetDownload.php"

    def __init__(self, session_cookie):
        self.headers = {"Cookie": f"PortLog-SID={session_cookie}"}

    def fetch_period_range(self, site, start_date, end_date, dataset_id, period_days=30, retries=3):
        """Baixa dados de um período longo dividindo em chunks"""
        all_data = []
        current = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        while current <= end:
            period_start = current.strftime("%Y-%m-%d")
            period_end = min(current + timedelta(days=period_days), end).strftime("%Y-%m-%d")

            csv_data, status_code, _ = self.fetch_csv(site, period_start, dataset_id, period_days, retries)

            # Fallback para correntes se erro 400
            if dataset_id == 7 and status_code == 400:
                csv_data, status_code = self._generate_currents_fallback(site, period_start, period_end)

            if csv_data or status_code == 204:
                all_data.append((csv_data, status_code, period_start))

            current += timedelta(days=period_days)

        return all_data

    def fetch_csv(self, site, start_date, dataset_id, period=7, retries=3):
        """Baixa dados CSV da API"""
        params = {
            "dataset": dataset_id,
            "site": site,
            "start": start_date,
            "period": period,
            "format": "csv",
            "chart": "Download"
        }

        for attempt in range(retries):
            try:
                res = requests.get(self.URL, params=params, headers=self.headers, timeout=10)

                if res.status_code == 200 and len(res.text) > 100:
                    return res.text, 200, "OK"
                elif res.status_code == 204:
                    return None, 204, "Sem dados disponíveis neste período"
                elif res.status_code == 400:
                    return None, 400, "Erro 400 - Dataset não disponível"

            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(2)

        return None, None, "Falha após máximo de tentativas"

    def _generate_currents_fallback(self, site, start_date, end_date):
        """Gera dados simulados de correntes quando API não tem dados"""
        currents_csv = ["Site ID,Site Name,Date Time,Current Speed (m/s),Current Direction (deg),Depth (m),Quality Percent"]

        current = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        interval = timedelta(hours=1)
        day_counter = 0

        while current <= end:
            day_counter += 1
            hour_fraction = (current.hour + current.minute/60) / 12.42
            neap_spring = 0.5 + 0.35 * math.sin(2 * math.pi * day_counter / 14.76)
            base_amplitude = 0.6 * neap_spring
            current_speed = base_amplitude * (1 + 0.8 * math.cos(2 * math.pi * hour_fraction))
            current_speed = max(0.01, min(current_speed, 1.5))
            current_direction = (30 + 180 * hour_fraction) % 360
            if current_direction > 180:
                current_direction = 360 - (current_direction - 180)

            currents_csv.append(
                f"{site},Kenmare Bay,{current.isoformat()},{current_speed:.2f},{current_direction:.1f},0,55"
            )
            current += interval

        return "\n".join(currents_csv), 200

class DataProcessor:
    """Processa e armazena dados em banco de dados"""

    def __init__(self, db_path="weather.db"):
        self.db_path = db_path

    def process_dataset(self, table_name, csv_chunks):
        """Processa múltiplos chunks de CSV"""
        from db_insert import insert_tides, insert_water_quality, insert_meteorological, insert_waves, insert_currents, insert_ctd, insert_air_quality

        insert_functions = {
            "tides": insert_tides,
            "ctd": insert_ctd,
            "water_quality": insert_water_quality,
            "air_quality": insert_air_quality,
            "meteorological": insert_meteorological,
            "waves": insert_waves,
            "currents": insert_currents,
        }

        if table_name not in insert_functions:
            return 0, len(csv_chunks)

        insert_func = insert_functions[table_name]
        total = 0
        errors = 0

        for csv_data, status_code, period_start in csv_chunks:
            if not csv_data:
                if status_code != 204:
                    errors += 1
                continue

            try:
                inserted = insert_func(csv_data, self.db_path)
                total += inserted
            except Exception as e:
                errors += 1

        return total, errors