"""
Preditor Marítimo Simplificado - Apenas Tabelas
Atualiza a cada 10 minutos
"""
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import time
import threading

class MarinePredictorSimple:
    def __init__(self, db_path="weather.db", excel_path="Opsdata_MarineHistory.xlsx"):
        self.db_path = db_path
        self.excel_path = excel_path
        self.output_dir = Path("marine_reports")
        self.output_dir.mkdir(exist_ok=True)
    
    def load_weather_data(self):
        """Carrega apenas último registro meteorológico"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Obter último registro de cada tabela
            met = pd.read_sql_query("SELECT * FROM meteorological ORDER BY date_time DESC LIMIT 1", conn)
            waves = pd.read_sql_query("SELECT * FROM waves ORDER BY date_time DESC LIMIT 1", conn)
            currents = pd.read_sql_query("SELECT * FROM currents ORDER BY date_time DESC LIMIT 1", conn)
            tides = pd.read_sql_query("SELECT * FROM tides ORDER BY date_time DESC LIMIT 1", conn)
            wq = pd.read_sql_query("SELECT * FROM water_quality ORDER BY date_time DESC LIMIT 1", conn)
            
            conn.close()
            
            return {
                'met': met.iloc[0].to_dict() if not met.empty else {},
                'waves': waves.iloc[0].to_dict() if not waves.empty else {},
                'currents': currents.iloc[0].to_dict() if not currents.empty else {},
                'tides': tides.iloc[0].to_dict() if not tides.empty else {},
                'wq': wq.iloc[0].to_dict() if not wq.empty else {}
            }
        except Exception as e:
            print(f"❌ Erro ao carregar dados: {e}")
            return {}
    
    def safe_get(self, data, key, default="-"):
        """Obtém valor segurado do dicionário"""
        try:
            val = data.get(key)
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return default
            return val
        except:
            return default
    
    def format_direction(self, degrees):
        """Converte graus para cardinal direction"""
        try:
            if degrees is None or (isinstance(degrees, float) and pd.isna(degrees)):
                return "--"
            degrees = float(degrees) % 360
            dirs = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW']
            idx = int((degrees + 11.25) / 22.5) % 16
            return dirs[idx]
        except:
            return "--"
    
    def create_html(self, data):
        """Cria relatório HTML simplificado com apenas tabelas"""
        
        # Extrair dados
        met = data.get('met', {})
        waves = data.get('waves', {})
        currents = data.get('currents', {})
        tides = data.get('tides', {})
        wq = data.get('wq', {})
        
        # Helper para obter e formatar valores
        def fmt(key, d, decimals=2, mult=1):
            val = d.get(key)
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return "-"
            try:
                return f"{float(val) * mult:.{decimals}f}"
            except:
                return "-"
        
        # Cálculo simples de risk score para recomendação
        wind_speed = float(met.get('wind_speed_ms', 0)) if met.get('wind_speed_ms') else 0
        wave_height = float(waves.get('wave_height_sig_m', 0)) if waves.get('wave_height_sig_m') else 0
        current_speed = float(currents.get('current_speed_ms', 0)) if currents.get('current_speed_ms') else 0
        
        risk = 0
        if wind_speed < 5:
            risk += 0
        elif wind_speed < 10:
            risk += 10
        elif wind_speed < 15:
            risk += 25
        else:
            risk += 30
        
        if wave_height < 1:
            risk += 0
        elif wave_height < 2:
            risk += 10
        elif wave_height < 3:
            risk += 20
        else:
            risk += 30
        
        current = min(20, int(current_speed * 20))
        risk += current
        risk = min(100, risk)
        
        if risk <= 20:
            rec_icon = "✅"
            rec_text = "IDEAL - Perfeito para embarcar!"
            rec_color = "#84fab0"
        elif risk <= 40:
            rec_icon = "🟢"
            rec_text = "BOM - Pode embarcar"
            rec_color = "#8fd3f4"
        elif risk <= 60:
            rec_icon = "🟡"
            rec_text = "MODERADO - Com precaução"
            rec_color = "#fee140"
        else:
            rec_icon = "🔴"
            rec_text = "PERIGOSO - Não recomendado"
            rec_color = "#fa709a"
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <meta http-equiv="refresh" content="600">
            <meta http-equiv="cache-control" content="no-cache, no-store, must-revalidate">
            <meta http-equiv="expires" content="0">
            <meta http-equiv="pragma" content="no-cache">
            <title>Preditor Marítimo</title>
            <style>
                * {{ margin: 0; padding: 0; box-sizing: border-box; }}
                body {{
                    font-family: Arial, sans-serif;
                    background: #f5f5f5;
                    padding: 20px;
                }}
                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    background: white;
                    border-radius: 10px;
                    box-shadow: 0 0 20px rgba(0,0,0,0.1);
                    padding: 30px;
                }}
                header {{
                    text-align: center;
                    border-bottom: 3px solid #1a3a52;
                    padding-bottom: 20px;
                    margin-bottom: 30px;
                }}
                h1 {{ color: #1a3a52; font-size: 1.8em; margin-bottom: 5px; }}
                .subtitle {{ color: #666; font-size: 0.95em; }}
                
                .recommendation-box {{
                    padding: 25px;
                    border-radius: 10px;
                    text-align: center;
                    color: #333;
                    margin: 20px 0;
                    background: linear-gradient(135deg, {rec_color}, rgba(255,255,255,0.5));
                }}
                
                .risk-score {{ font-size: 2.5em; font-weight: bold; margin: 10px 0; }}
                .recommendation-text {{ font-size: 1.3em; font-weight: bold; }}
                
                .timestamp {{
                    text-align: center;
                    color: #999;
                    font-size: 0.9em;
                    margin: 15px 0;
                }}
                
                table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin: 20px 0;
                    background: white;
                    border: 2px solid #1a3a52;
                }}
                
                table thead {{
                    background: #1a3a52;
                    color: white;
                }}
                
                table th {{
                    padding: 12px;
                    text-align: center;
                    font-weight: bold;
                    border: 1px solid #0f2a3f;
                    font-size: 0.95em;
                }}
                
                table td {{
                    padding: 12px;
                    text-align: center;
                    border: 1px solid #ddd;
                    font-weight: bold;
                    color: #1a3a52;
                    font-size: 1.1em;
                }}
                
                .section-title {{
                    background: #1a3a52;
                    color: white;
                    padding: 10px;
                    margin-top: 30px;
                    margin-bottom: 15px;
                    border-radius: 5px;
                    font-size: 1.2em;
                    font-weight: bold;
                }}
                
                footer {{
                    text-align: center;
                    margin-top: 30px;
                    padding-top: 20px;
                    border-top: 1px solid #ddd;
                    color: #999;
                    font-size: 0.85em;
                }}
            </style>
            <script>
                setTimeout(function() {{
                    window.location.reload(true);
                }}, 600000);
            </script>
        </head>
        <body>
            <div class="container">
                <header>
                    <h1>⛵ PREDITOR DE OPERAÇÕES MARÍTIMAS</h1>
                    <p class="subtitle">Kenmare Bay - Condições Marítimas em Tempo Real</p>
                </header>
                
                <div class="recommendation-box">
                    <div class="risk-score">{risk}/100</div>
                    <div class="recommendation-text">{rec_icon} {rec_text}</div>
                </div>
                
                <div class="timestamp">
                    Atualizado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br>
                    Próxima atualização: +10 minutos
                </div>
                
                <!-- TIDES TABLE -->
                <div class="section-title">🌊 TIDES (MOMA JETTY - KENM-MOMA)</div>
                <table>
                    <thead>
                        <tr>
                            <th>Observed (m)</th>
                            <th>Predicted (m)</th>
                            <th>Surge (m)</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>{fmt('observed_m', tides, 2)}</td>
                            <td>{fmt('predicted_m', tides, 2)}</td>
                            <td>{fmt('surge_m', tides, 2)}</td>
                        </tr>
                    </tbody>
                </table>
                
                <!-- WIND TABLE -->
                <div class="section-title">💨 WIND</div>
                <table>
                    <thead>
                        <tr>
                            <th>Wind Direction (Deg)</th>
                            <th>Wind Speed (Knots)</th>
                            <th>Gust Speed (Knots)</th>
                            <th>Gust Direction (Deg)</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>{self.format_direction(met.get('wind_direction_deg', 0))}</td>
                            <td>{fmt('wind_speed_ms', met, 1, 1.94384)}</td>
                            <td>{fmt('gust_speed_ms', met, 1, 1.94384)}</td>
                            <td>{self.format_direction(met.get('gust_direction_deg', 0))}</td>
                        </tr>
                    </tbody>
                </table>
                
                <!-- MET TABLE -->
                <div class="section-title">🌡️ MET</div>
                <table>
                    <thead>
                        <tr>
                            <th>Atmos Pressure (mBar)</th>
                            <th>Air Temperature (°C)</th>
                            <th>Humidity (%)</th>
                            <th>DewPoint (°C)</th>
                            <th>Precipitation (mm/h)</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>{fmt('atmos_pressure_mbar', met, 0)}</td>
                            <td>{fmt('air_temp_c', met, 1)}</td>
                            <td>{fmt('relative_humidity_pct', met, 0)}</td>
                            <td>{fmt('dew_point_c', met, 1)}</td>
                            <td>{fmt('precipitation_mmh', met, 1)}</td>
                        </tr>
                    </tbody>
                </table>
                
                <!-- CURRENTS TABLE -->
                <div class="section-title">⚡ CURRENTS</div>
                <table>
                    <thead>
                        <tr>
                            <th>Direction (Deg)</th>
                            <th>Speed (Knots)</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>{fmt('current_direction_deg', currents, 0)}</td>
                            <td>{fmt('current_speed_ms', currents, 2, 1.94384)}</td>
                        </tr>
                    </tbody>
                </table>
                
                <!-- WAVES TABLE -->
                <div class="section-title">🌀 WAVES</div>
                <table>
                    <thead>
                        <tr>
                            <th>Wave Direction (Mean) (Deg)</th>
                            <th>Wave Height (Sig) (m)</th>
                            <th>Wave Height (Max) (m)</th>
                            <th>Wave Period (Sig) (s)</th>
                            <th>Wave Period (Avg) (s)</th>
                            <th>Wave Period (Peak) (s)</th>
                            <th>Zero Upcrossing (s)</th>
                            <th>Wave Direction (Peak) (Deg)</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>{fmt('wave_direction_mean_deg', waves, 0)}</td>
                            <td>{fmt('wave_height_sig_m', waves, 2)}</td>
                            <td>{fmt('wave_height_max_m', waves, 2)}</td>
                            <td>{fmt('wave_period_sig_s', waves, 2)}</td>
                            <td>{fmt('wave_period_avg_s', waves, 2)}</td>
                            <td>{fmt('wave_period_peak_s', waves, 2)}</td>
                            <td>{fmt('wave_period_zero_crossing_s', waves, 2)}</td>
                            <td>{fmt('wave_direction_peak_deg', waves, 0)}</td>
                        </tr>
                    </tbody>
                </table>
                
                <!-- WATER QUALITY TABLE -->
                <div class="section-title">💧 WQ</div>
                <table>
                    <thead>
                        <tr>
                            <th>Water Temperature (°C)</th>
                            <th>Salinity (PSU)</th>
                            <th>pH</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>{fmt('water_temp_c', wq, 1)}</td>
                            <td>{fmt('salinity_psu', wq, 2)}</td>
                            <td>{fmt('ph', wq, 2)}</td>
                        </tr>
                    </tbody>
                </table>
                
                <footer>
                    <p>Preditor de Operações Marítimas | Dados: weather.db</p>
                    <p>Atualiza automaticamente a cada 10 minutos</p>
                </footer>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def generate_report(self):
        """Gera relatório simples"""
        print(f"⏳ [{datetime.now().strftime('%H:%M:%S')}] Gerando relatório...")
        
        data = self.load_weather_data()
        
        if not data or len(data.get('met', {})) == 0:
            print("❌ Erro: Sem dados meteorológicos")
            return False
        
        html = self.create_html(data)
        
        output_file = self.output_dir / "RELATORIO_PREDICAO.html"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html)
        
        print(f"✅ [{datetime.now().strftime('%H:%M:%S')}] Relatório salvo")
        return True
    
    def auto_update_loop(self):
        """Loop de atualização a cada 10 minutos"""
        print("✅ Serviço iniciado - Atualiza a cada 10 minutos")
        print("Pressione Ctrl+C para parar\n")
        
        while True:
            try:
                self.generate_report()
                next_update = (datetime.now() + timedelta(minutes=10)).strftime('%H:%M:%S')
                print(f"⏰ Próxima atualização: {next_update}\n")
                time.sleep(600)  # 10 minutos
            except KeyboardInterrupt:
                print("\n❌ Serviço parado")
                break
            except Exception as e:
                print(f"❌ Erro: {e}")
                time.sleep(60)

def main():
    """Função principal"""
    predictor = MarinePredictorSimple()
    
    # Gerar relatório inicial
    predictor.generate_report()
    
    # Iniciar loop de atualização em thread daemon
    update_thread = threading.Thread(target=predictor.auto_update_loop, daemon=True)
    update_thread.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n✅ Aplicação finalizada")

if __name__ == "__main__":
    main()
