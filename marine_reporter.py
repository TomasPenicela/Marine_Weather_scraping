import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os
import time
import threading
from pathlib import Path

import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

import warnings
warnings.filterwarnings('ignore')

class MarinePredictorReporter:
    """Gera relatórios e atualiza previsões automaticamente"""
    
    def __init__(self, db_path="weather.db", excel_path="Opsdata_MarineHistory.xlsx"):
        self.db_path = db_path
        self.excel_path = excel_path
        self.output_dir = Path("marine_reports")
        self.output_dir.mkdir(exist_ok=True)
        
        # Status de atualização
        self.last_update = None
        self.update_interval = 3600  # 1 hora em segundos
        self.running = False
    
    def load_weather_data(self):
        """Carrega dados meteorológicos"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            met = pd.read_sql_query("""
                SELECT date_time, wind_speed_ms, wind_direction_deg, 
                       atmos_pressure_mbar, temperature_celsius, humidity_percent
                FROM meteorological
                WHERE date_time IS NOT NULL
                ORDER BY date_time ASC
            """, conn)
            
            waves = pd.read_sql_query("""
                SELECT date_time, wave_height_sig_m, wave_period_peak_s, 
                       wave_direction_mean_deg, total_energy
                FROM waves
                WHERE date_time IS NOT NULL
                ORDER BY date_time ASC
            """, conn)
            
            currents = pd.read_sql_query("""
                SELECT date_time, current_speed_ms, current_direction_deg, depth_m
                FROM currents
                WHERE date_time IS NOT NULL
                ORDER BY date_time ASC
            """, conn)
            
            tides = pd.read_sql_query("""
                SELECT date_time, observed_m, predicted_m
                FROM tides
                WHERE date_time IS NOT NULL
                ORDER BY date_time ASC
            """, conn)
            
            conn.close()
            
            # Converter strings para datetime
            for df in [met, waves, currents, tides]:
                if not df.empty and 'date_time' in df.columns:
                    df['date_time'] = pd.to_datetime(df['date_time'])
            
            return {
                'meteorological': met,
                'waves': waves,
                'currents': currents,
                'tides': tides
            }
        except Exception as e:
            print(f"❌ Erro ao carregar dados meteorológicos: {e}")
            return {}
    
    def load_operation_history(self):
        """Carrega histórico de operações"""
        try:
            df = pd.read_excel(self.excel_path, sheet_name='Sheet1')
            
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            df['Start time'] = pd.to_datetime(df['Start time'], errors='coerce')
            df['End time'] = pd.to_datetime(df['End time'], errors='coerce')
            df['Duration'] = pd.to_numeric(df['Duration'], errors='coerce')
            
            # Dados dos últimos 30 dias
            thirty_days_ago = datetime.now() - timedelta(days=30)
            df_recent = df[df['Date'] >= thirty_days_ago]
            
            daily_issues = df_recent.groupby(df_recent['Date'].dt.date).agg({
                'Duration': 'sum',
                'EventID': 'count',
                'Reason description': lambda x: ', '.join(x.dropna().unique()[:2])
            }).rename(columns={'EventID': 'num_events'})
            
            return df_recent, daily_issues
        except Exception as e:
            print(f"❌ Erro ao carregar histórico de operações: {e}")
            return None, None
    
    def calculate_risk(self, weather_data):
        """Calcula score de risco"""
        met = weather_data.get('meteorological', pd.DataFrame())
        waves = weather_data.get('waves', pd.DataFrame())
        currents = weather_data.get('currents', pd.DataFrame())
        
        if met.empty or waves.empty:
            return None, {}
        
        # Últimas observações
        latest_met = met.iloc[-1]
        latest_waves = waves.iloc[-1]
        latest_currents = currents.iloc[-1] if not currents.empty else None
        
        score = 0
        factors = {}
        
        # Vento (0-30)
        wind_speed = float(latest_met.get('wind_speed_ms') or 0)
        wind_score = min(30, int(wind_speed * 2.5))
        score += wind_score
        factors['Vento'] = {
            'valor': wind_speed,
            'unidade': 'm/s',
            'score': wind_score,
            'limite': '5 m/s (ideal)'
        }
        
        # Ondas (0-30)
        wave_height = float(latest_waves.get('wave_height_sig_m') or 0)
        wave_score = min(30, int(wave_height * 10))
        score += wave_score
        factors['Altura de Ondas'] = {
            'valor': wave_height,
            'unidade': 'm',
            'score': wave_score,
            'limite': '<1 m (ideal)'
        }
        
        # Pressão (0-20)
        pressure = float(latest_met.get('atmos_pressure_mbar') or 1013)
        pressure_score = 0 if pressure > 1010 else (20 if pressure < 990 else 10)
        score += pressure_score
        factors['Pressão Atmosférica'] = {
            'valor': pressure,
            'unidade': 'mbar',
            'score': pressure_score,
            'limite': '>1010 mbar (ideal)'
        }
        
        # Correntes (0-20)
        if latest_currents is not None:
            current_speed = float(latest_currents.get('current_speed_ms') or 0)
            current_score = min(20, int(current_speed * 20))
            score += current_score
            factors['Velocidade de Correntes'] = {
                'valor': current_speed,
                'unidade': 'm/s',
                'score': current_score,
                'limite': '<0.5 m/s (ideal)'
            }
        
        return score, factors
    
    def get_recommendation(self, risk_score):
        """Gera recomendação"""
        if risk_score is None:
            return "⚠️ Dados insuficientes", "gray"
        
        if risk_score <= 20:
            return "✅ CONDIÇÕES IDEAIS - Embarque recomendado", "green"
        elif risk_score <= 40:
            return "⚠️ BOM - Condições aceitáveis com precaução", "yellow"
        elif risk_score <= 60:
            return "⚠️ MODERADO - Risco significativo", "orange"
        else:
            return "🚫 PERIGOSO - Não recomendado", "red"
    
    def create_maritime_conditions_plot(self, weather_data):
        """Cria gráfico de condições marítimas"""
        met = weather_data.get('meteorological', pd.DataFrame())
        waves = weather_data.get('waves', pd.DataFrame())
        currents = weather_data.get('currents', pd.DataFrame())
        tides = weather_data.get('tides', pd.DataFrame())
        
        # Pegar últimos 30 dias
        thirty_days_ago = pd.Timestamp.now() - pd.Timedelta(days=30)
        
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=("Velocidade do Vento", "Altura de Ondas", 
                           "Pressão", "Velocidade de Correntes"),
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        # Vento
        if not met.empty and 'wind_speed_ms' in met.columns:
            met_recent = met[met['date_time'] > thirty_days_ago]
            fig.add_trace(
                go.Scatter(x=met_recent['date_time'], y=met_recent['wind_speed_ms'],
                          mode='lines', name='Vento', line=dict(color='#3498db', width=2)),
                row=1, col=1
            )
        
        # Ondas
        if not waves.empty and 'wave_height_sig_m' in waves.columns:
            waves_recent = waves[waves['date_time'] > thirty_days_ago]
            fig.add_trace(
                go.Scatter(x=waves_recent['date_time'], y=waves_recent['wave_height_sig_m'],
                          mode='lines', name='Altura de Ondas', line=dict(color='#e74c3c', width=2)),
                row=1, col=2
            )
        
        # Pressão
        if not met.empty and 'atmos_pressure_mbar' in met.columns:
            met_recent = met[met['date_time'] > thirty_days_ago]
            fig.add_trace(
                go.Scatter(x=met_recent['date_time'], y=met_recent['atmos_pressure_mbar'],
                          mode='lines', name='Pressão', line=dict(color='#2ecc71', width=2)),
                row=2, col=1
            )
        
        # Correntes
        if not currents.empty and 'current_speed_ms' in currents.columns:
            currents_recent = currents[currents['date_time'] > thirty_days_ago]
            fig.add_trace(
                go.Scatter(x=currents_recent['date_time'], y=currents_recent['current_speed_ms'],
                          mode='lines', name='Correntes', line=dict(color='#f39c12', width=2)),
                row=2, col=2
            )
        
        fig.update_layout(
            title="Condições Marítimas - Últimos 30 dias",
            height=800,
            hovermode='x unified',
            template='plotly_white'
        )
        
        return fig
    
    def create_risk_score_plot(self, weather_data):
        """Cria gráfico de score de risco histórico"""
        met = weather_data.get('meteorological', pd.DataFrame())
        
        if met.empty:
            return None
        
        # Calcular scores históricos
        scores = []
        for idx in range(len(met)):
            single_weather = {
                'meteorological': met.iloc[[idx]],
                'waves': weather_data['waves'].iloc[[idx]] if idx < len(weather_data['waves']) else pd.DataFrame(),
                'currents': weather_data['currents'].iloc[[idx]] if idx < len(weather_data['currents']) else pd.DataFrame()
            }
            score, _ = self.calculate_risk(single_weather)
            scores.append(score)
        
        met['risk_score'] = scores
        
        # Últimos 30 dias
        thirty_days_ago = pd.Timestamp.now() - pd.Timedelta(days=30)
        met_recent = met[met['date_time'] > thirty_days_ago]
        
        fig = go.Figure()
        
        # Linha de risco
        fig.add_trace(go.Scatter(
            x=met_recent['date_time'],
            y=met_recent['risk_score'],
            mode='lines+markers',
            name='Score de Risco',
            line=dict(color='#3498db', width=3),
            marker=dict(size=4)
        ))
        
        # Zonas de risco
        fig.add_hline(y=20, line_dash="dash", line_color="green", 
                     annotation_text="IDEAL (≤20)")
        fig.add_hline(y=40, line_dash="dash", line_color="orange", 
                     annotation_text="BOM (20-40)")
        fig.add_hline(y=60, line_dash="dash", line_color="red", 
                     annotation_text="PERIGOSO (>60)")
        
        # Preenchimento de áreas
        fig.add_hrect(y0=0, y1=20, line_width=0, fillcolor="green", opacity=0.1)
        fig.add_hrect(y0=20, y1=40, line_width=0, fillcolor="yellow", opacity=0.1)
        fig.add_hrect(y0=40, y1=60, line_width=0, fillcolor="orange", opacity=0.1)
        fig.add_hrect(y0=60, y1=100, line_width=0, fillcolor="red", opacity=0.1)
        
        fig.update_layout(
            title="Histórico de Score de Risco - Últimos 30 dias",
            xaxis_title="Data",
            yaxis_title="Score (0-100)",
            height=500,
            hovermode='x unified',
            template='plotly_white'
        )
        
        return fig
    
    def create_operations_plot(self, operations_df):
        """Cria gráfico de eventos operacionais"""
        if operations_df is None or operations_df.empty:
            return None
        
        daily_events = operations_df.groupby(operations_df['Date'].dt.date).size()
        daily_duration = operations_df.groupby(operations_df['Date'].dt.date)['Duration'].sum()
        
        fig = make_subplots(
            rows=1, cols=2,
            subplot_titles=("Número de Eventos por Dia", "Duração Total (horas)")
        )
        
        fig.add_trace(
            go.Bar(x=daily_events.index, y=daily_events.values,
                  name='Eventos', marker=dict(color='#e74c3c')),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Bar(x=daily_duration.index, y=daily_duration.values,
                  name='Duração', marker=dict(color='#f39c12')),
            row=1, col=2
        )
        
        fig.update_layout(
            title="Histórico de Operações",
            height=500,
            template='plotly_white'
        )
        
        return fig
    
    def generate_report(self):
        """Gera relatório completo"""
        print(f"\n📊 Gerando relatório... ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
        
        # Carregar dados
        weather_data = self.load_weather_data()
        operations_df, daily_issues = self.load_operation_history()
        
        if not weather_data:
            print("❌ Sem dados meteorológicos disponíveis")
            return
        
        # Calcular risco
        risk_score, factors = self.calculate_risk(weather_data)
        recommendation, color = self.get_recommendation(risk_score)
        
        # Criar gráficos
        print("  ✓ Criando gráficos...")
        fig_conditions = self.create_maritime_conditions_plot(weather_data)
        fig_risk = self.create_risk_score_plot(weather_data)
        fig_operations = self.create_operations_plot(operations_df)
        
        # Salvar gráficos como HTML
        if fig_conditions:
            fig_conditions.write_html(self.output_dir / "maritime_conditions.html")
        if fig_risk:
            fig_risk.write_html(self.output_dir / "risk_score_history.html")
        if fig_operations:
            fig_operations.write_html(self.output_dir / "operations_history.html")
        
        # Gerar relatório HTML consolidado
        print("  ✓ Gerando relatório HTML...")
        html_contents = {
            'conditions': fig_conditions.to_html(div_id="plot_conditions", include_plotlyjs='cdn') if fig_conditions else "",
            'risk': fig_risk.to_html(div_id="plot_risk", include_plotlyjs=False) if fig_risk else "",
            'operations': fig_operations.to_html(div_id="plot_operations", include_plotlyjs=False) if fig_operations else ""
        }
        
        # Formatar fatores
        factors_html = ""
        for name, data in factors.items():
            valor = f"{data['valor']:.2f}"
            factors_html += f"""
            <div class="factor-card">
                <h4>{name}</h4>
                <p><strong>Valor:</strong> {valor} {data['unidade']}</p>
                <p><strong>Score:</strong> {data['score']}/100</p>
                <p style="color: #666; font-size: 0.9em;">Ideal: {data['limite']}</p>
            </div>
            """
        
        # Salvar JSON com dados
        report_data = {
            'timestamp': datetime.now().isoformat(),
            'risk_score': risk_score,
            'recommendation': recommendation,
            'recommendation_color': color,
            'factors': {k: {**v} for k, v in factors.items()},
            'weather_summary': {
                'latest_wind': float(weather_data['meteorological']['wind_speed_ms'].iloc[-1]) if not weather_data['meteorological'].empty else None,
                'latest_wave_height': float(weather_data['waves']['wave_height_sig_m'].iloc[-1]) if not weather_data['waves'].empty else None,
                'total_records_meteorological': len(weather_data['meteorological']),
                'total_records_waves': len(weather_data['waves']),
                'total_records_currents': len(weather_data['currents']),
                'total_records_tides': len(weather_data['tides'])
            },
            'operations_summary': {
                'recent_events': len(operations_df) if operations_df is not None else 0,
                'days_with_issues': len(daily_issues) if daily_issues is not None else 0
            }
        }
        
        with open(self.output_dir / "latest_report.json", 'w') as f:
            json.dump(report_data, f, indent=2, default=str)
        
        # Criar HTML principal
        html_report = f"""
        <!DOCTYPE html>
        <html lang="pt-BR">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Relatório de Predição Marítima</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                * {{
                    margin: 0;
                    padding: 0;
                    box-sizing: border-box;
                }}
                
                body {{
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    min-height: 100vh;
                    padding: 20px;
                }}
                
                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    background: white;
                    border-radius: 15px;
                    box-shadow: 0 20px 60px rgba(0,0,0,0.3);
                    overflow: hidden;
                }}
                
                header {{
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 40px;
                    text-align: center;
                }}
                
                h1 {{
                    font-size: 2.5em;
                    margin-bottom: 10px;
                }}
                
                .subtitle {{
                    font-size: 1.1em;
                    opacity: 0.9;
                }}
                
                .content {{
                    padding: 40px;
                }}
                
                .recommendation-box {{
                    padding: 40px;
                    border-radius: 10px;
                    text-align: center;
                    margin: 30px 0;
                    color: white;
                }}
                
                .recommendation-box.green {{
                    background: linear-gradient(135deg, #84fab0 0%, #8fd3f4 100%);
                    color: #333;
                }}
                
                .recommendation-box.yellow {{
                    background: linear-gradient(135deg, #fa709a 0%, #fee140 100%);
                    color: #333;
                }}
                
                .recommendation-box.orange {{
                    background: linear-gradient(135deg, #fa709a 0%, #fee140 100%);
                    color: #333;
                }}
                
                .recommendation-box.red {{
                    background: linear-gradient(135deg, #fa709a 0%, #feca57 100%);
                    color: #333;
                }}
                
                .risk-score {{
                    font-size: 4em;
                    font-weight: bold;
                    margin: 20px 0;
                }}
                
                .recommendation-text {{
                    font-size: 1.8em;
                    font-weight: bold;
                }}
                
                .factors-grid {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                    gap: 20px;
                    margin: 30px 0;
                }}
                
                .factor-card {{
                    background: #f8f9fa;
                    padding: 20px;
                    border-radius: 10px;
                    border-left: 5px solid #667eea;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                }}
                
                .factor-card h4 {{
                    color: #667eea;
                    margin-bottom: 15px;
                    font-size: 1.1em;
                }}
                
                .factor-card p {{
                    margin: 8px 0;
                    color: #555;
                }}
                
                .plots {{
                    margin-top: 40px;
                }}
                
                .plot-section {{
                    margin: 30px 0;
                    border-radius: 10px;
                    overflow: hidden;
                    box-shadow: 0 5px 20px rgba(0,0,0,0.1);
                }}
                
                .plot-title {{
                    background: #667eea;
                    color: white;
                    padding: 15px 20px;
                    font-weight: bold;
                    font-size: 1.1em;
                }}
                
                footer {{
                    background: #f8f9fa;
                    padding: 20px;
                    text-align: center;
                    color: #666;
                    font-size: 0.9em;
                }}
                
                .info-box {{
                    background: #e3f2fd;
                    padding: 15px;
                    border-radius: 5px;
                    margin: 15px 0;
                    border-left: 4px solid #2196F3;
                }}
                
                .info-box strong {{
                    color: #1976D2;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <header>
                    <h1>⛵ Preditor de Operações Marítimas</h1>
                    <p class="subtitle">Previsão de Dias Ideais para Embarcar - Kenmare Bay</p>
                </header>
                
                <div class="content">
                    <div class="info-box">
                        <strong>Última Atualização:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 
                        <strong>Próxima Atualização:</strong> Em 1 hora
                    </div>
                    
                    <div class="recommendation-box {color}">
                        <div class="risk-score">{risk_score}/100</div>
                        <div class="recommendation-text">{recommendation}</div>
                    </div>
                    
                    <h2 style="color: #333; margin-top: 30px; margin-bottom: 20px;">📋 Análise de Fatores</h2>
                    <div class="factors-grid">
                        {factors_html}
                    </div>
                    
                    <h2 style="color: #333; margin-top: 40px; margin-bottom: 20px;">📊 Gráficos Detalhados</h2>
                    
                    <div class="plots">
                        <div class="plot-section">
                            <div class="plot-title">🌊 Condições Marítimas (Últimos 30 dias)</div>
                            {html_contents['conditions']}
                        </div>
                        
                        <div class="plot-section">
                            <div class="plot-title">📈 Histórico de Score de Risco</div>
                            {html_contents['risk']}
                        </div>
                        
                        <div class="plot-section">
                            <div class="plot-title">📋 Histórico de Operações</div>
                            {html_contents['operations']}
                        </div>
                    </div>
                </div>
                
                <footer>
                    <p>Dashboard de Predição Marítima | Dados: weather.db + Opsdata_MarineHistory.xlsx</p>
                    <p>Registros carregados | Meteorológicos: {report_data['weather_summary']['total_records_meteorological']} | Ondas: {report_data['weather_summary']['total_records_waves']} | Correntes: {report_data['weather_summary']['total_records_currents']} | Marés: {report_data['weather_summary']['total_records_tides']}</p>
                    <p>Eventos recentes carregados: {report_data['operations_summary']['recent_events']}</p>
                </footer>
            </div>
        </body>
        </html>
        """
        
        # Salvar relatório principal
        with open(self.output_dir / "RELATORIO_PREDICAO.html", 'w', encoding='utf-8') as f:
            f.write(html_report)
        
        print(f"  ✓ Relatório salvo em: {self.output_dir}/RELATORIO_PREDICAO.html")
        print(f"  ✓ Score de Risco: {risk_score}/100")
        print(f"  ✓ Recomendação: {recommendation}")
        print(f"  ✓ Registros carregados: MET={len(weather_data['meteorological'])}, "
              f"ONDAS={len(weather_data['waves'])}, CORRENTES={len(weather_data['currents'])}")
        
        self.last_update = datetime.now()
    
    def auto_update_loop(self):
        """Loop de atualização automática"""
        while self.running:
            try:
                self.generate_report()
                print(f"⏰ Próxima atualização em {self.update_interval/3600:.0f} hora(s)")
                time.sleep(self.update_interval)
            except Exception as e:
                print(f"❌ Erro durante atualização: {e}")
                time.sleep(60)  # Tentar novamente em 1 minuto
    
    def start(self):
        """Inicia o serviço de atualização automática"""
        if self.running:
            print("⚠️ Serviço já está rodando")
            return
        
        print("\n" + "="*70)
        print("🌊 MARINE OPERATION PREDICTOR - REPORTER")
        print("="*70)
        
        self.running = True
        
        # Primeira geração
        self.generate_report()
        
        # Iniciar thread de atualização automática
        update_thread = threading.Thread(target=self.auto_update_loop, daemon=True)
        update_thread.start()
        
        print("\n✅ Serviço iniciado!")
        print(f"📁 Relatórios salvos em: {self.output_dir.absolute()}")
        print("⏰ Atualizando automaticamente a cada 1 hora")
        print("="*70 + "\n")
        
        # Manter a aplicação rodando
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n\n⚠️ Serviço interrompido pelo usuário")
            self.running = False

if __name__ == '__main__':
    reporter = MarinePredictorReporter()
    reporter.start()
