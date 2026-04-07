"""
Solução para obter dados de Correntes
Incluindo múltiplas alternativas e fallbacks
"""

import requests
import json
from datetime import datetime, timedelta

class CurrentsAlternativeSources:
    """Múltiplas fontes para dados de correntes marinhas"""
    
    # Opção 1: NOAA Tidal Predictions (público, gratuito)
    NOAA_API = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
    
    # Opção 2: CMEMS Copernicus (público, gratuito com registro)
    CMEMS_API = "https://tools.coastalmodeling.noaa.gov/stofs/natlantic"
    
    # Opção 3: UK Hydrographic Office
    UKHO_API = "https://www.bodc.ac.uk/sdn"
    
    # Opção 4: Dados teóricos baseados em Marés + Vento
    @staticmethod
    def calculate_currents_from_tides_and_wind(tides_data, meteorological_data):
        """
        Calcula correntes estimadas baseado em:
        - Componentes de maré (surge)
        - Velocidade e direção do vento
        Este é um método simplificado de Ekman spiral
        """
        deriv_currents = []
        
        for i, tide in enumerate(tides_data):
            try:
                # Extrair componentes de maré
                surge_m = tide.get('surge_m', 0) or 0
                
                # Buscar dados meteorológicos correspondentes
                met_data = meteorological_data[i] if i < len(meteorological_data) else {}
                
                wind_speed = met_data.get('wind_speed_ms', 0) or 0
                wind_direction = met_data.get('wind_direction_deg', 0) or 0
                
                # Cálculo simplificado de corrente
                # Corrente = Velocidade do vento * fator + Componente de maré
                wind_factor = 0.02  # ~2% da velocidade do vento gera corrente
                current_speed = (wind_speed * wind_factor) + abs(surge_m) * 0.05
                
                # Direção da corrente (aproximadamente na direção do vento)
                # Com rotação devida a Coriolis (~45°)
                current_direction = (wind_direction + 45) % 360
                
                deriv_currents.append({
                    'date_time': tide.get('date_time'),
                    'current_speed_ms': round(current_speed, 2),
                    'current_direction_deg': round(current_direction, 1),
                    'depth_m': 0,  # Superficial (Ekman layer)
                    'quality_percent': 50,  # Baixa confiabilidade - interpolação
                    'source': 'derived_from_tides_and_wind'
                })
            except Exception as e:
                continue
        
        return deriv_currents
    
    @staticmethod
    def noaa_harmonic_currents(latitude, longitude, station_id, start_date, end_date):
        """
        Obtém previsões harmônicas de correntes do NOAA
        Stations para Irlanda/UK:
        - Dunmore East (IE): Velocidade e direção
        """
        try:
            params = {
                'station': station_id,
                'begin_date': start_date.replace('-', ''),
                'end_date': end_date.replace('-', ''),
                'product': 'currents_predictions',
                'units': 'metric',
                'time_zone': 'gmt',
                'format': 'json',
                'application': 'web'
            }
            
            response = requests.get(
                "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter",
                params=params,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data:
                    return data['data']
        except:
            pass
        
        return None
    
    @staticmethod
    def mock_currents_generator(start_date, end_date, interval_minutes=10, depth_m=0):
        """
        Gera dados mock de correntes para testes
        Baseado em padrões realistas de Kenmare Bay
        """
        mock_currents = []
        current = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        interval = timedelta(minutes=interval_minutes)
        
        day_counter = 0
        
        while current <= end:
            day_counter += 1
            
            # Padrão tidal semi-diurno (M2) ~ 12.42 horas
            # Pico de corrente ~ 0.5-1.0 m/s em Kenmare
            hour_angle = (current.hour + current.minute/60) * 360 / 12.42
            
            # Velocidade: semi-diurna + variações neap/spring
            spring_neap_factor = 0.015 * day_counter  # Varia ao longo do mês
            current_speed = 0.6 + 0.3 * abs(__import__('math').sin(__import__('math').radians(hour_angle)))
            current_speed += 0.1 * __import__('math').sin(spring_neap_factor)
            current_speed = max(0, min(current_speed, 2.0))  # 0-2 m/s
            
            # Direção: rotação tidal (giroscopio)  
            # Kenmare típicamente entre 40-240° (NE-SW)
            current_direction = (40 + hour_angle) % 360
            if 180 < current_direction < 360:
                current_direction = (current_direction + 270) % 360
            
            mock_currents.append({
                'site_id': 148,
                'site_name': 'Kenmare Bay',
                'date_time': current.isoformat(),
                'current_speed_ms': round(current_speed, 2),
                'current_direction_deg': round(current_direction, 1),
                'depth_m': depth_m,
                'quality_percent': 60,  # Baixa confiabilidade - dados mock
                'source': 'mock_tidal_model'
            })
            
            current += interval
        
        return mock_currents

# Teste de derivação de correntes
if __name__ == "__main__":
    print("\n" + "="*80)
    print("🌊 TESTE - FONTES ALTERNATIVAS DE DADOS DE CORRENTES")
    print("="*80 + "\n")
    
    print("Opções disponíveis:")
    print("1. Derivar de dados de Maré + Vento (estimativa Ekman)")
    print("2. NOAA Harmonic Currents (se disponível)")
    print("3. Gerar dados Mock para testes\n")
    
    # Testar gerador de dados mock
    print("Testando gerador de dados mock:\n")
    currents = CurrentsAlternativeSources.mock_currents_generator(
        "2025-01-01", "2025-01-10", interval_minutes=60
    )
    
    print(f"Gerados {len(currents)} registros de correntes mock\n")
    print("Primeiros 3 registros:")
    for i, curr in enumerate(currents[:3]):
        print(f"{i+1}. {curr['date_time']}: {curr['current_speed_ms']:4.2f} m/s @ {curr['current_direction_deg']:6.1f}°")
    
    print(f"\n... ({len(currents)-3} mais)")
    
    print("\n" + "="*80)
