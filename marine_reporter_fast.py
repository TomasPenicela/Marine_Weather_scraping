"""
Preditor de Operações Marítimas - Versão Otimizada
Gera relatórios com atualização automática a cada 10 minutos
"""
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import warnings
warnings.filterwarnings('ignore')

class MarinePredictorOptimized:
    def __init__(self, db_path="weather.db", excel_path="Opsdata_MarineHistory.xlsx"):
        self.db_path = db_path
        self.excel_path = excel_path
        self.output_dir = Path("marine_reports")
        self.output_dir.mkdir(exist_ok=True)
    
    def load_weather_data_optimized(self, sample_rate=1):
        """Carrega dados com amostragem para velocidade"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Carregar últimos 30 dias com amostragem
            thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            
            # Meteorológicos
            met_query = f"""
            SELECT * FROM meteorological 
            WHERE date_time >= '{thirty_days_ago}'
            ORDER BY date_time DESC
            """
            met = pd.read_sql_query(met_query, conn)
            met['date_time'] = pd.to_datetime(met['date_time'])
            met = met.iloc[::sample_rate].reset_index(drop=True)
            
            # Ondas
            waves_query = f"""
            SELECT * FROM waves 
            WHERE date_time >= '{thirty_days_ago}'
            ORDER BY date_time DESC
            """
            waves = pd.read_sql_query(waves_query, conn)
            waves['date_time'] = pd.to_datetime(waves['date_time'])
            waves = waves.iloc[::sample_rate].reset_index(drop=True)
            
            # Correntes
            currents_query = f"""
            SELECT * FROM currents 
            WHERE date_time >= '{thirty_days_ago}'
            ORDER BY date_time DESC
            """
            currents = pd.read_sql_query(currents_query, conn)
            currents['date_time'] = pd.to_datetime(currents['date_time'])
            currents = currents.iloc[::sample_rate].reset_index(drop=True)
            
            # Marés
            tides_query = f"""
            SELECT * FROM tides 
            WHERE date_time >= '{thirty_days_ago}'
            ORDER BY date_time DESC
            """
            tides = pd.read_sql_query(tides_query, conn)
            tides['date_time'] = pd.to_datetime(tides['date_time'])
            tides = tides.iloc[::sample_rate].reset_index(drop=True)
            
            # Water Quality
            wq_query = f"""
            SELECT * FROM water_quality 
            WHERE date_time >= '{thirty_days_ago}'
            ORDER BY date_time DESC
            """
            wq = pd.read_sql_query(wq_query, conn)
            wq['date_time'] = pd.to_datetime(wq['date_time'])
            wq = wq.iloc[::sample_rate].reset_index(drop=True)
            
            conn.close()
            
            return {
                'meteorological': met,
                'waves': waves,
                'currents': currents,
                'tides': tides,
                'water_quality': wq
            }
        except Exception as e:
            print(f"❌ Erro ao carregar dados meteorológicos: {e}")
            return {
                'meteorological': pd.DataFrame(),
                'waves': pd.DataFrame(),
                'currents': pd.DataFrame(),
                'tides': pd.DataFrame(),
                'water_quality': pd.DataFrame()
            }
    
    def load_operations_history(self):
        """Carrega histórico operacional"""
        try:
            excel = pd.read_excel(self.excel_path)
            excel['date'] = pd.to_datetime(excel.get('date', ''))
            
            thirty_days_ago = datetime.now() - timedelta(days=30)
            excel = excel[excel['date'] >= thirty_days_ago]
            
            return excel
        except Exception as e:
            print(f"❌ Erro ao carregar histórico: {e}")
            return pd.DataFrame()
    
    def calculate_risk_current(self, met_row, waves_row, currents_row):
        """Calcula score de risco 0-100"""
        risk = 0
        
        # Fator 1: Vento (0-30 pts)
        wind = met_row.get('wind_speed_ms', 0)
        if wind < 5:
            risk += 0
        elif wind < 10:
            risk += 10
        elif wind < 15:
            risk += 25
        else:
            risk += 30
        
        # Fator 2: Ondas (0-30 pts)
        wave_height = waves_row.get('wave_height_sig_m', 0)
        if wave_height < 1:
            risk += 0
        elif wave_height < 2:
            risk += 10
        elif wave_height < 3:
            risk += 20
        else:
            risk += 30
        
        # Fator 3: Pressão (0-20 pts)
        pressure = met_row.get('atmos_pressure_mbar', 1013)
        if pressure > 1010:
            risk += 0
        elif pressure > 1000:
            risk += 5
        elif pressure > 990:
            risk += 15
        else:
            risk += 20
        
        # Fator 4: Correntes (0-20 pts)
        current = currents_row.get('current_speed_ms', 0)
        if current < 0.5:
            risk += 0
        elif current < 1.0:
            risk += 10
        else:
            risk += 20
        
        return min(100, risk)
    
    def get_recommendation(self, score):
        """Retorna recomendação baseada no score"""
        if score <= 20:
            return "✅ IDEAL - Perfeito para embarcar!"
        elif score <= 40:
            return "🟢 BOM - Pode embarcar com segurança"
        elif score <= 60:
            return "🟡 MODERADO - Embarque com precaução"
        elif score <= 80:
            return "🟠 PERIGOSO - Não recomendado"
        else:
            return "🔴 MUITO PERIGOSO - Embarque proibido"
    
    def get_color_class(self, score):
        """Retorna classe CSS baseada no score"""
        if score <= 20:
            return "green"
        elif score <= 40:
            return "yellow"
        elif score <= 60:
            return "orange"
        else:
            return "red"
    
    def create_currents_graph(self, currents):
        """Cria gráfico detalhado de correntes"""
        if currents.empty or 'current_speed_ms' not in currents.columns:
            return None
        
        currents = currents.sort_values('date_time')
        
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=("Velocidade de Correntes (m/s)", "Direção de Correntes (°)"),
            row_heights=[0.6, 0.4]
        )
        
        fig.add_trace(
            go.Scatter(
                x=currents['date_time'],
                y=currents['current_speed_ms'],
                mode='lines',
                name='Velocidade',
                line=dict(color='#e74c3c', width=2),
                fill='tozeroy'
            ),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=currents['date_time'],
                y=currents['current_direction_deg'],
                mode='markers',
                name='Direção',
                marker=dict(color='#3498db', size=5)
            ),
            row=2, col=1
        )
        
        fig.update_yaxes(title_text="Speed (m/s)", row=1, col=1)
        fig.update_yaxes(title_text="Direction (°)", row=2, col=1)
        fig.update_layout(height=500, hovermode='x unified', template='plotly_white')
        
        return fig
    
    def create_waves_graph(self, waves):
        """Cria gráfico detalhado de ondas"""
        if waves.empty or 'wave_height_sig_m' not in waves.columns:
            return None
        
        waves = waves.sort_values('date_time')
        
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=("Altura Significativa de Ondas (m)", "Período de Ondas (s)"),
            row_heights=[0.6, 0.4]
        )
        
        fig.add_trace(
            go.Scatter(
                x=waves['date_time'],
                y=waves['wave_height_sig_m'],
                mode='lines',
                name='Altura Sig',
                line=dict(color='#e74c3c', width=2),
                fill='tozeroy'
            ),
            row=1, col=1
        )
        
        if 'wave_height_max_m' in waves.columns:
            fig.add_trace(
                go.Scatter(
                    x=waves['date_time'],
                    y=waves['wave_height_max_m'],
                    mode='lines',
                    name='Altura Máx',
                    line=dict(color='#c0392b', width=1, dash='dash')
                ),
                row=1, col=1
            )
        
        if 'wave_period_peak_s' in waves.columns:
            fig.add_trace(
                go.Scatter(
                    x=waves['date_time'],
                    y=waves['wave_period_peak_s'],
                    mode='lines',
                    name='Período',
                    line=dict(color='#3498db', width=2)
                ),
                row=2, col=1
            )
        
        fig.update_yaxes(title_text="Height (m)", row=1, col=1)
        fig.update_yaxes(title_text="Period (s)", row=2, col=1)
        fig.update_layout(height=500, hovermode='x unified', template='plotly_white')
        
        return fig
    
    def create_wind_graph(self, met):
        """Cria gráfico detalhado de vento"""
        if met.empty or 'wind_speed_ms' not in met.columns:
            return None
        
        met = met.sort_values('date_time')
        
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=("Velocidade de Vento (m/s)", "Direção de Vento (°)"),
            row_heights=[0.6, 0.4]
        )
        
        fig.add_trace(
            go.Scatter(
                x=met['date_time'],
                y=met['wind_speed_ms'],
                mode='lines',
                name='Vento',
                line=dict(color='#3498db', width=2),
                fill='tozeroy'
            ),
            row=1, col=1
        )
        
        if 'gust_speed_ms' in met.columns:
            fig.add_trace(
                go.Scatter(
                    x=met['date_time'],
                    y=met['gust_speed_ms'],
                    mode='lines',
                    name='Rajada',
                    line=dict(color='#2980b9', width=1, dash='dash')
                ),
                row=1, col=1
            )
        
        fig.add_trace(
            go.Scatter(
                x=met['date_time'],
                y=met['wind_direction_deg'],
                mode='markers',
                name='Direção',
                marker=dict(color='#27ae60', size=4)
            ),
            row=2, col=1
        )
        
        fig.update_yaxes(title_text="Speed (m/s)", row=1, col=1)
        fig.update_yaxes(title_text="Direction (°)", row=2, col=1)
        fig.update_layout(height=500, hovermode='x unified', template='plotly_white')
        
        return fig
    
    def create_pressure_graph(self, met):
        """Cria gráfico detalhado de pressão"""
        if met.empty or 'atmos_pressure_mbar' not in met.columns:
            return None
        
        met = met.sort_values('date_time')
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=met['date_time'],
            y=met['atmos_pressure_mbar'],
            mode='lines',
            name='Pressão',
            line=dict(color='#2ecc71', width=2),
            fill='tozeroy'
        ))
        
        fig.add_hline(y=1013, line_dash="dash", line_color="green")
        fig.add_hline(y=1010, line_dash="dash", line_color="orange")
        fig.add_hline(y=990, line_dash="dash", line_color="red")
        
        fig.update_layout(
            title="Histórico de Pressão Atmosférica",
            xaxis_title="Data/Hora",
            yaxis_title="Pressão (mbar)",
            height=400,
            hovermode='x unified',
            template='plotly_white'
        )
        
        return fig
    
    def create_tides_graph(self, tides):
        """Cria gráfico detalhado de marés"""
        if tides.empty or 'observed_m' not in tides.columns:
            return None
        
        tides = tides.sort_values('date_time')
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=tides['date_time'],
            y=tides['observed_m'],
            mode='lines',
            name='Observado',
            line=dict(color='#e74c3c', width=2)
        ))
        
        if 'predicted_m' in tides.columns:
            fig.add_trace(go.Scatter(
                x=tides['date_time'],
                y=tides['predicted_m'],
                mode='lines',
                name='Previsto',
                line=dict(color='#3498db', width=2, dash='dash')
            ))
        
        fig.update_layout(
            title="Histórico de Marés",
            xaxis_title="Data/Hora",
            yaxis_title="Altura (m)",
            height=400,
            hovermode='x unified',
            template='plotly_white'
        )
        
        return fig
    
    def create_details_table_html(self, weather_data):
        """Cria tabelas detalhadas de dados"""
        html = ""
        
        met = weather_data.get('meteorological', pd.DataFrame())
        waves = weather_data.get('waves', pd.DataFrame())
        currents = weather_data.get('currents', pd.DataFrame())
        tides = weather_data.get('tides', pd.DataFrame())
        wq = weather_data.get('water_quality', pd.DataFrame())
        
        if not met.empty:
            latest = met.iloc[-1]
            html += f"""
            <div class="detail-table">
                <h4>💨 Dados Meteorológicos (Último)</h4>
                <table>
                    <tr class="header">
                        <td>Parâmetro</td>
                        <td>Valor</td>
                        <td>Unidade</td>
                    </tr>
                    <tr>
                        <td>Velocidade Vento</td>
                        <td><strong>{latest.get('wind_speed_ms', 0):.2f}</strong></td>
                        <td>m/s ({latest.get('wind_speed_ms', 0)*1.94384:.1f} knots)</td>
                    </tr>
                    <tr>
                        <td>Direção Vento</td>
                        <td><strong>{latest.get('wind_direction_deg', 0):.0f}</strong></td>
                        <td>°</td>
                    </tr>
                    <tr>
                        <td>Pressão</td>
                        <td><strong>{latest.get('atmos_pressure_mbar', 0):.1f}</strong></td>
                        <td>mbar</td>
                    </tr>
                    <tr>
                        <td>Temperatura</td>
                        <td><strong>{latest.get('air_temp_c', 0):.1f}</strong></td>
                        <td>°C</td>
                    </tr>
                    <tr>
                        <td>Umidade</td>
                        <td><strong>{latest.get('relative_humidity_pct', 0):.0f}</strong></td>
                        <td>%</td>
                    </tr>
                </table>
            </div>
            """
        
        if not waves.empty:
            latest = waves.iloc[-1]
            html += f"""
            <div class="detail-table">
                <h4>🌊 Dados de Ondas (Último)</h4>
                <table>
                    <tr class="header">
                        <td>Parâmetro</td>
                        <td>Valor</td>
                        <td>Unidade</td>
                    </tr>
                    <tr>
                        <td>Altura Significativa</td>
                        <td><strong>{latest.get('wave_height_sig_m', 0):.2f}</strong></td>
                        <td>m</td>
                    </tr>
                    <tr>
                        <td>Altura Máxima</td>
                        <td><strong>{latest.get('wave_height_max_m', 0):.2f}</strong></td>
                        <td>m</td>
                    </tr>
                    <tr>
                        <td>Período Peak</td>
                        <td><strong>{latest.get('wave_period_peak_s', 0):.2f}</strong></td>
                        <td>s</td>
                    </tr>
                    <tr>
                        <td>Direção Onda</td>
                        <td><strong>{latest.get('wave_direction_deg', 0):.0f}</strong></td>
                        <td>°</td>
                    </tr>
                </table>
            </div>
            """
        
        if not currents.empty:
            latest = currents.iloc[-1]
            html += f"""
            <div class="detail-table">
                <h4>⚡ Dados de Correntes (Último)</h4>
                <table>
                    <tr class="header">
                        <td>Parâmetro</td>
                        <td>Valor</td>
                        <td>Unidade</td>
                    </tr>
                    <tr>
                        <td>Velocidade</td>
                        <td><strong>{latest.get('current_speed_ms', 0):.2f}</strong></td>
                        <td>m/s ({latest.get('current_speed_ms', 0)*1.94384:.1f} knots)</td>
                    </tr>
                    <tr>
                        <td>Direção</td>
                        <td><strong>{latest.get('current_direction_deg', 0):.0f}</strong></td>
                        <td>°</td>
                    </tr>
                </table>
            </div>
            """
        
        if not tides.empty:
            latest = tides.iloc[-1]
            html += f"""
            <div class="detail-table">
                <h4>🌙 Dados de Marés (Último)</h4>
                <table>
                    <tr class="header">
                        <td>Parâmetro</td>
                        <td>Valor</td>
                        <td>Unidade</td>
                    </tr>
                    <tr>
                        <td>Altura Observada</td>
                        <td><strong>{latest.get('observed_m', 0):.2f}</strong></td>
                        <td>m</td>
                    </tr>
                    <tr>
                        <td>Altura Prevista</td>
                        <td><strong>{latest.get('predicted_m', 0):.2f}</strong></td>
                        <td>m</td>
                    </tr>
                </table>
            </div>
            """
        
        if not wq.empty:
            latest = wq.iloc[-1]
            html += f"""
            <div class="detail-table">
                <h4>💧 Qualidade da Água (Último)</h4>
                <table>
                    <tr class="header">
                        <td>Parâmetro</td>
                        <td>Valor</td>
                        <td>Unidade</td>
                    </tr>
                    <tr>
                        <td>Salinidade</td>
                        <td><strong>{latest.get('salinity_psu', 0):.2f}</strong></td>
                        <td>PSU</td>
                    </tr>
                    <tr>
                        <td>Temperatura Água</td>
                        <td><strong>{latest.get('water_temp_c', 0):.2f}</strong></td>
                        <td>°C</td>
                    </tr>
                    <tr>
                        <td>pH</td>
                        <td><strong>{latest.get('ph', 0):.2f}</strong></td>
                        <td>-</td>
                    </tr>
                </table>
            </div>
            """
        
        return html
    
    def create_dashboard_html(self, weather_data, risk_score, factors, recommendation, color):
        """Cria dashboard HTML com gráficos detalhados"""
        
        met = weather_data.get('meteorological', pd.DataFrame())
        waves = weather_data.get('waves', pd.DataFrame())
        currents = weather_data.get('currents', pd.DataFrame())
        tides = weather_data.get('tides', pd.DataFrame())
        
        fig_currents = self.create_currents_graph(currents)
        fig_waves = self.create_waves_graph(waves)
        fig_wind = self.create_wind_graph(met)
        fig_pressure = self.create_pressure_graph(met)
        fig_tides = self.create_tides_graph(tides)
        
        html_currents = fig_currents.to_html(div_id="fig_currents", include_plotlyjs=False) if fig_currents else ""
        html_waves = fig_waves.to_html(div_id="fig_waves", include_plotlyjs=False) if fig_waves else ""
        html_wind = fig_wind.to_html(div_id="fig_wind", include_plotlyjs=False) if fig_wind else ""
        html_pressure = fig_pressure.to_html(div_id="fig_pressure", include_plotlyjs=False) if fig_pressure else ""
        html_tides = fig_tides.to_html(div_id="fig_tides", include_plotlyjs=False) if fig_tides else ""
        
        factors_html = ""
        for name, data in factors.items():
            factors_html += f"""
            <div class="factor-card">
                <h4>{name}</h4>
                <p><strong>{data['valor']:.2f} {data['unidade']}</strong></p>
                <p style="color: white; font-size: 0.9em;">Score: {data['score']}/100</p>
            </div>
            """
        
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
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                * {{
                    margin: 0; padding: 0; box-sizing: border-box;
                }}
                body {{
                    font-family: Arial, sans-serif;
                    background: #f5f5f5;
                    padding: 20px;
                }}
                .container {{
                    max-width: 1400px;
                    margin: 0 auto;
                    background: white;
                    border-radius: 10px;
                    box-shadow: 0 0 20px rgba(0,0,0,0.1);
                    padding: 30px;
                }}
                header {{
                    text-align: center;
                    border-bottom: 3px solid #3498db;
                    padding-bottom: 20px;
                    margin-bottom: 30px;
                }}
                h1 {{ color: #333; font-size: 2em; margin-bottom: 5px; }}
                h2 {{ color: #333; font-size: 1.4em; margin: 40px 0 20px; border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
                .subtitle {{ color: #666; font-size: 1em; }}
                
                .recommendation-box {{
                    padding: 30px;
                    border-radius: 10px;
                    text-align: center;
                    color: white;
                    margin: 20px 0;
                }}
                .recommendation-box.green {{
                    background: linear-gradient(135deg, #84fab0, #8fd3f4); color: #333;
                }}
                .recommendation-box.yellow {{
                    background: linear-gradient(135deg, #fa709a, #fee140); color: #333;
                }}
                .recommendation-box.orange {{
                    background: linear-gradient(135deg, #fa709a, #fee140); color: #333;
                }}
                .recommendation-box.red {{
                    background: linear-gradient(135deg, #fa709a, #feca57); color: #333;
                }}
                
                .risk-score {{ font-size: 3em; font-weight: bold; margin: 15px 0; }}
                .recommendation-text {{ font-size: 1.5em; font-weight: bold; }}
                
                .factors-grid {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                    gap: 15px;
                    margin: 30px 0;
                }}
                
                .factor-card {{
                    background: #3498db;
                    color: white;
                    padding: 15px;
                    border-radius: 8px;
                    text-align: center;
                }}
                
                .factor-card h4 {{ margin-bottom: 10px; font-size: 0.95em; }}
                .factor-card p {{ margin: 5px 0; }}
                
                .plot-section {{
                    margin: 30px 0;
                    padding: 20px;
                    background: #f9f9f9;
                    border-radius: 8px;
                    border-left: 4px solid #3498db;
                }}
                
                .plot-section h3 {{
                    color: #333;
                    margin-bottom: 15px;
                    font-size: 1.1em;
                    font-weight: bold;
                }}
                
                footer {{
                    text-align: center;
                    margin-top: 30px;
                    padding-top: 20px;
                    border-top: 1px solid #ddd;
                    color: #999;
                    font-size: 0.9em;
                }}
                
                .info {{
                    background: #e3f2fd;
                    padding: 15px;
                    border-radius: 5px;
                    margin: 15px 0;
                    border-left: 4px solid #2196F3;
                    color: #1565c0;
                }}
                
                .detail-table {{
                    background: white;
                    border: 2px solid #1a3a52;
                    border-radius: 5px;
                    margin: 15px 0;
                    overflow: hidden;
                }}
                
                .detail-table table {{
                    width: 100%;
                    border-collapse: collapse;
                    font-family: 'Courier New', monospace;
                }}
                
                .detail-table h4 {{
                    background: #1a3a52;
                    color: white;
                    padding: 15px;
                    margin: 0;
                    font-size: 1.1em;
                    font-weight: bold;
                }}
                
                .detail-table td {{
                    padding: 10px 15px;
                    border-bottom: 1px solid #e0e0e0;
                    text-align: center;
                }}
                
                .detail-table tr.header td {{
                    background: #1a3a52;
                    color: white;
                    font-weight: bold;
                    text-align: left;
                    padding: 12px 15px;
                    border: none;
                }}
                
                .detail-table tr:last-child td {{
                    border-bottom: none;
                }}
                
                .detail-table tr:hover:not(.header) {{
                    background: #f5f5f5;
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
                    <h1>⛵ Preditor de Operações Marítimas</h1>
                    <p class="subtitle">Previsão para Embarque Seguro - Kenmare Bay</p>
                </header>
                
                <div class="info">
                    <strong>Atualizado:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 
                    <strong>Próxima atualização:</strong> +10 minutos
                </div>
                
                <div class="recommendation-box {color}">
                    <div class="risk-score">{risk_score}/100</div>
                    <div class="recommendation-text">{recommendation}</div>
                </div>
                
                <h2>📊 Fatores de Análise</h2>
                <div class="factors-grid">
                    {factors_html}
                </div>
                
                <h2>📈 Históricos Detalhados (Últimos 30 dias)</h2>
                
                <div class="plot-section">
                    <h3>🌊 Histórico de Correntes</h3>
                    {html_currents}
                </div>
                
                <div class="plot-section">
                    <h3>🌀 Histórico de Ondas</h3>
                    {html_waves}
                </div>
                
                <div class="plot-section">
                    <h3>💨 Histórico de Vento</h3>
                    {html_wind}
                </div>
                
                <div class="plot-section">
                    <h3>🔽 Histórico de Pressão</h3>
                    {html_pressure}
                </div>
                
                <div class="plot-section">
                    <h3>🌊 Histórico de Marés</h3>
                    {html_tides}
                </div>
                
                <h2>📋 Dados Detalhados das Estações</h2>
                {self.create_details_table_html(weather_data)}
                
                <footer>
                    <p>Preditor de Operações Marítimas | Dados: weather.db + Opsdata_MarineHistory.xlsx</p>
                    <p>Atualiza automaticamente a cada 10 minutos</p>
                </footer>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def generate_report(self):
        """Gera relatório completo"""
        print("⏳ Carregando dados...")
        
        weather_data = self.load_weather_data_optimized(sample_rate=1)
        operations = self.load_operations_history()
        
        met = weather_data.get('meteorological', pd.DataFrame())
        waves = weather_data.get('waves', pd.DataFrame())
        currents = weather_data.get('currents', pd.DataFrame())
        
        if met.empty:
            print("❌ Erro: Sem dados meteorológicos")
            return False
        
        print(f"✅ Carregados {len(met)} registros meteorológicos")
        
        # Calcular score
        latest_met = met.iloc[-1] if not met.empty else {}
        latest_waves = waves.iloc[-1] if not waves.empty else {}
        latest_currents = currents.iloc[-1] if not currents.empty else {}
        
        risk_score = self.calculate_risk_current(latest_met, latest_waves, latest_currents)
        recommendation = self.get_recommendation(risk_score)
        color = self.get_color_class(risk_score)
        
        # Fatores
        factors = {
            'Vento': {
                'valor': latest_met.get('wind_speed_ms', 0),
                'unidade': 'm/s',
                'score': min(30, int(latest_met.get('wind_speed_ms', 0) * 2.5))
            },
            'Ondas': {
                'valor': latest_waves.get('wave_height_sig_m', 0),
                'unidade': 'm',
                'score': min(30, int(latest_waves.get('wave_height_sig_m', 0) * 15))
            },
            'Pressão': {
                'valor': latest_met.get('atmos_pressure_mbar', 0),
                'unidade': 'mbar',
                'score': max(0, int((1013 - latest_met.get('atmos_pressure_mbar', 1013)) / 2))
            },
            'Correntes': {
                'valor': latest_currents.get('current_speed_ms', 0),
                'unidade': 'm/s',
                'score': min(20, int(latest_currents.get('current_speed_ms', 0) * 20))
            }
        }
        
        html = self.create_dashboard_html(weather_data, risk_score, factors, recommendation, color)
        
        output_file = self.output_dir / "RELATORIO_PREDICAO.html"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html)
        
        print(f"\n✅ Score: {risk_score}/100")
        print(f"📊 Recomendação: {recommendation}")
        print(f"📁 Relatório salvo: {output_file}")
        
        return True
    
    def auto_update_loop(self):
        """Loop de atualização automática"""
        try:
            while True:
                self.generate_report()
                next_update = (datetime.now() + timedelta(minutes=10)).strftime('%H:%M:%S')
                print(f"⏰ Próxima atualização em 10 minutos ({next_update})")
                import time
                time.sleep(600)
        except KeyboardInterrupt:
            print("\n❌ Serviço interrompido")
        except Exception as e:
            print(f"❌ Erro: {e}")

def main():
    """Função principal"""
    predictor = MarinePredictorOptimized()
    
    # Gerar relatório inicial
    predictor.generate_report()
    
    # Iniciar loop de atualização
    import threading
    update_thread = threading.Thread(target=predictor.auto_update_loop, daemon=True)
    update_thread.start()
    
    print("\n✅ Serviço iniciado com atualizações automáticas")
    print("Pressione Ctrl+C para parar")
    
    try:
        while True:
            import time
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n✅ Serviço parado")

if __name__ == "__main__":
    main()
