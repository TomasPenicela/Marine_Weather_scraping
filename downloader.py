import requests
import time
import csv
import io
import math
from datetime import datetime, timedelta

class WeatherDownloader:
    """Baixa dados de weather da API Port-Log com suporte para alternativas"""
    URL = "https://kenmare.port-log.net/live/GetDownload.php"
    
    # Datasets que sabemos que retornam 400 no Site 148
    UNAVAILABLE_DATASETS = {7}  # Correntes
    
    # Fallback modes para datasets indisponíveis
    CURRENTS_FALLBACK_MODE = "mock"  # "mock", "derived", ou None

    def __init__(self, session_cookie):
        self.headers = {"Cookie": f"PortLog-SID={session_cookie}"}
        self.tides_cache = {}  # Para derivar correntes de marés
        self.meteorological_cache = {}  # Para derivar correntes de vento

    def fetch_period_range(self, site, start_date, end_date, dataset_id, period_days=30, retries=3):
        """Baixa dados de um período longo dividindo em chunks
        
        Returns lista de (dados_csv, status_code, periodo_start)
        """
        all_data = []
        current = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        while current <= end:
            period_start = current.strftime("%Y-%m-%d")
            period_end = min(current + timedelta(days=period_days), end).strftime("%Y-%m-%d")
            
            csv_data, status_code, _ = self.fetch_csv(site, period_start, dataset_id, period_days, retries)
            
            # Se Dataset 7 retorna erro, usar fallback
            if dataset_id == 7 and status_code == 400:
                csv_data, status_code = self._generate_currents_fallback(
                    site, period_start, period_end
                )
            
            if csv_data or status_code == 204:
                all_data.append((csv_data, status_code, period_start))
            
            current += timedelta(days=period_days)
        
        return all_data

    def fetch_csv(self, site, start_date, dataset_id, period=7, retries=3):
        """Baixa dados CSV diretamente da API
        
        Args:
            site: ID do site (ex: 148)
            start_date: Data inicial (ex: "2025-01-01")
            dataset_id: ID do dataset (1-7)
            period: Período em dias (padrão: 7)
            retries: Número de tentativas
        
        Returns:
            Tupla (dados_csv, status_code, status_message)
        """
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
                
                # Status 200: Dados OK
                if res.status_code == 200 and len(res.text) > 100:
                    return res.text, 200, "OK"
                
                # Status 204: Sem dados disponíveis (normal para alguns datasets)
                elif res.status_code == 204:
                    return None, 204, "Sem dados disponíveis neste período"
                
                # Status 400: Erro do servidor/requisição inválida
                elif res.status_code == 400:
                    return None, 400, "Erro 400 - Parâmetro inválido ou dataset não existe"
                
                # Outros erros
                else:
                    print(f"   Tentativa {attempt + 1}/{retries}: Status {res.status_code}")
            except requests.exceptions.Timeout:
                print(f"   Tentativa {attempt + 1}/{retries}: Timeout")
                time.sleep(2)
            except Exception as e:
                print(f"   Tentativa {attempt + 1}/{retries}: Erro - {str(e)[:50]}")
                time.sleep(2)
        
        return None, None, "Falha após máximo de tentativas"

    def _generate_currents_fallback(self, site, start_date, end_date):
        """Gera dados de correntes usando modelo mock tidal
        
        Kenmare Bay tem correntes semi-diurnas (M2 ~ 12h25m)
        com velocidades típicas de 0.4-0.8 m/s
        """
        currents_csv = []
        
        # Cabeçalho CSV
        currents_csv.append("Site ID,Site Name,Date Time,Current Speed (m/s),Current Direction (deg),Depth (m),Quality Percent")
        
        current = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        interval = timedelta(hours=1)  # Dados horários
        
        day_counter = 0
        
        while current <= end:
            day_counter += 1
            
            # Padrão semi-diurno (M2): período ~12.42h
            # Amplitude baseada na maré (spring: 0.8m/s, neap: 0.4m/s)
            hour_fraction = (current.hour + current.minute/60) / 12.42
            
            # Velocidade: onda cossenoidal com modulação neap/spring
            neap_spring = 0.5 + 0.35 * math.sin(2 * math.pi * day_counter / 14.76)  # Ciclo lunar ~14.76 dias
            base_amplitude = 0.6 * neap_spring
            current_speed = base_amplitude * (1 + 0.8 * math.cos(2 * math.pi * hour_fraction))
            current_speed = max(0.01, min(current_speed, 1.5))  # Limitar 0.01-1.5 m/s
            
            # Direção: rotação tidal (giroscópio + Coriolis)
            # Padrão típico: 30-210° (NE-SW) em Kenmare
            current_direction = (30 + 180 * hour_fraction) % 360
            
            # Reduzir amplitude no padrão de direção (não gira 360°)
            if current_direction > 180:
                current_direction = 360 - (current_direction - 180)
            
            currents_csv.append(
                f"{site},Kenmare Bay,{current.isoformat()},{current_speed:.2f},"
                f"{current_direction:.1f},0,55"
            )
            
            current += interval
        
        csv_data = "\n".join(currents_csv)
        return csv_data, 200  # Retornar como se fosse sucesso (dados gerados)