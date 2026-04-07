import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os
from pathlib import Path

# Configuração Flask para dashboard
from flask import Flask, render_template_string
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

import warnings
warnings.filterwarnings('ignore')

app = Flask(__name__)

class MarineOperationPredictor:
    def __init__(self, db_path="weather.db", excel_path="Opsdata_MarineHistory.xlsx"):
        self.db_path = db_path
        self.excel_path = excel_path
        self.model_data = {}
        self.last_update = None
        
    def load_weather_data(self):
        """Carrega dados meteorológicos e oceanográficos do banco de dados"""
        conn = sqlite3.connect(self.db_path)
        
        # Dados meteorológicos
        met = pd.read_sql_query("""
            SELECT date_time, wind_speed_ms, wind_direction_deg, 
                   atmos_pressure_mbar, temperature_celsius, humidity_percent
            FROM meteorological
            WHERE date_time IS NOT NULL
            ORDER BY date_time DESC
            LIMIT 720
        """, conn)
        
        # Dados de ondas
        waves = pd.read_sql_query("""
            SELECT date_time, wave_height_sig_m, wave_period_peak_s, 
                   wave_direction_mean_deg
            FROM waves
            WHERE date_time IS NOT NULL
            ORDER BY date_time DESC
            LIMIT 720
        """, conn)
        
        # Dados de correntes
        currents = pd.read_sql_query("""
            SELECT date_time, current_speed_ms, current_direction_deg
            FROM currents
            WHERE date_time IS NOT NULL
            ORDER BY date_time DESC
            LIMIT 720
        """, conn)
        
        # Dados de marés
        tides = pd.read_sql_query("""
            SELECT date_time, observed_m, predicted_m
            FROM tides
            WHERE date_time IS NOT NULL
            ORDER BY date_time DESC
            LIMIT 720
        """, conn)
        
        conn.close()
        
        return {
            'meteorological': met,
            'waves': waves,
            'currents': currents,
            'tides': tides
        }
    
    def load_operation_history(self):
        """Carrega histórico de operações do Excel"""
        try:
            df = pd.read_excel(self.excel_path, sheet_name='Sheet1')
            
            # Limpar dados
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            df['Start time'] = pd.to_datetime(df['Start time'], errors='coerce')
            df['End time'] = pd.to_datetime(df['End time'], errors='coerce')
            df['Duration'] = pd.to_numeric(df['Duration'], errors='coerce')
            
            # Agrupar por data para ver padrões de problema
            daily_issues = df.groupby(df['Date'].dt.date).agg({
                'Duration': 'sum',
                'EventID': 'count',
                'Reason description': lambda x: ', '.join(x.dropna().unique()[:3])
            }).rename(columns={'EventID': 'num_events'})
            
            return df, daily_issues
        except Exception as e:
            print(f"Erro ao carregar Excel: {e}")
            return None, None
    
    def calculate_maritime_risk_score(self, weather_data):
        """
        Calcula um score de risco marítimo baseado em condições climáticas
        Score: 0 (ideal) a 100 (muito perigoso)
        """
        met = weather_data.get('meteorological', pd.DataFrame())
        waves = weather_data.get('waves', pd.DataFrame())
        currents = weather_data.get('currents', pd.DataFrame())
        
        if met.empty or waves.empty:
            return None, {}
        
        # Pegar últimas observações
        latest_met = met.iloc[0] if not met.empty else {}
        latest_waves = waves.iloc[0] if not waves.empty else {}
        latest_currents = currents.iloc[0] if not currents.empty else {}
        
        score = 0
        factors = {}
        
        # Fator 1: Velocidade do vento (0-30)
        wind_speed = latest_met.get('wind_speed_ms', 0) or 0
        if wind_speed < 5:
            wind_score = 0
        elif wind_speed < 10:
            wind_score = 10
        elif wind_speed < 15:
            wind_score = 25
        else:
            wind_score = 30
        score += wind_score
        factors['vento'] = {'valor': wind_speed, 'score': wind_score, 'unidade': 'm/s'}
        
        # Fator 2: Altura significativa de ondas (0-30)
        wave_height = latest_waves.get('wave_height_sig_m', 0) or 0
        if wave_height < 1:
            wave_score = 0
        elif wave_height < 2:
            wave_score = 10
        elif wave_height < 3:
            wave_score = 20
        else:
            wave_score = 30
        score += wave_score
        factors['ondas'] = {'valor': wave_height, 'score': wave_score, 'unidade': 'm'}
        
        # Fator 3: Pressão atmosférica (0-20)
        pressure = latest_met.get('atmos_pressure_mbar', 1013) or 1013
        if pressure > 1010:
            pressure_score = 0
        elif pressure > 1000:
            pressure_score = 5
        elif pressure > 990:
            pressure_score = 15
        else:
            pressure_score = 20
        score += pressure_score
        factors['pressao'] = {'valor': pressure, 'score': pressure_score, 'unidade': 'mbar'}
        
        # Fator 4: Velocidade da corrente (0-20)
        current_speed = latest_currents.get('current_speed_ms', 0) or 0
        if current_speed < 0.5:
            current_score = 0
        elif current_speed < 1:
            current_score = 10
        else:
            current_score = 20
        score += current_score
        factors['correntes'] = {'valor': current_speed, 'score': current_score, 'unidade': 'm/s'}
        
        return score, factors
    
    def generate_recommendation(self, risk_score):
        """Gera recomendação baseada no score de risco"""
        if risk_score is None:
            return "⚠️ Dados insuficientes", "gray"
        
        if risk_score <= 20:
            return "✅ CONDIÇÕES IDEAIS - Embarque recomendado", "green"
        elif risk_score <= 40:
            return "⚠️ BOM - Condições aceitáveis com precaução", "yellow"
        elif risk_score <= 60:
            return "⚠️ MODERADO - Risco significativo, avaliar operação", "orange"
        else:
            return "🚫 PERIGOSO - Não recomendado, aguarde melhoria", "red"
    
    def update(self):
        """Atualiza todos os dados"""
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Atualizando modelo preditivo...")
        
        weather_data = self.load_weather_data()
        operation_df, daily_issues = self.load_operation_history()
        
        risk_score, factors = self.calculate_maritime_risk_score(weather_data)
        recommendation, color = self.generate_recommendation(risk_score)
        
        self.model_data = {
            'timestamp': datetime.now().isoformat(),
            'weather': weather_data,
            'operations': operation_df,
            'daily_issues': daily_issues,
            'risk_score': risk_score,
            'factors': factors,
            'recommendation': recommendation,
            'recommendation_color': color
        }
        
        self.last_update = datetime.now()
        return self.model_data
    
    def plot_maritime_conditions(self):
        """Cria gráficos das condições marítimas"""
        if not self.model_data.get('weather'):
            return None
        
        weather = self.model_data['weather']
        met = weather['meteorological']
        waves = weather['waves']
        currents = weather['currents']
        tides = weather['tides']
        
        # Converter string para datetime se necessário
        if not met.empty and 'date_time' in met.columns:
            met['date_time'] = pd.to_datetime(met['date_time'])
        if not waves.empty and 'date_time' in waves.columns:
            waves['date_time'] = pd.to_datetime(waves['date_time'])
        
        # Criar subplot
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=("Vento (m/s)", "Altura de Ondas (m)", 
                          "Pressão Atmosférica (mbar)", "Correntes (m/s)"),
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        # Gráfico 1: Vento
        if not met.empty and 'wind_speed_ms' in met.columns:
            fig.add_trace(
                go.Scatter(x=met['date_time'], y=met['wind_speed_ms'],
                          mode='lines', name='Vento',
                          line=dict(color='#1f77b4')),
                row=1, col=1
            )
        
        # Gráfico 2: Ondas
        if not waves.empty and 'wave_height_sig_m' in waves.columns:
            fig.add_trace(
                go.Scatter(x=waves['date_time'], y=waves['wave_height_sig_m'],
                          mode='lines', name='Altura Ondas',
                          line=dict(color='#ff7f0e')),
                row=1, col=2
            )
        
        # Gráfico 3: Pressão
        if not met.empty and 'atmos_pressure_mbar' in met.columns:
            fig.add_trace(
                go.Scatter(x=met['date_time'], y=met['atmos_pressure_mbar'],
                          mode='lines', name='Pressão',
                          line=dict(color='#2ca02c')),
                row=2, col=1
            )
        
        # Gráfico 4: Correntes
        if not currents.empty and 'current_speed_ms' in currents.columns:
            fig.add_trace(
                go.Scatter(x=currents['date_time'], y=currents['current_speed_ms'],
                          mode='lines', name='Correntes',
                          line=dict(color='#d62728')),
                row=2, col=2
            )
        
        fig.update_layout(
            title_text="Condições Marítimas - Últimas 30 dias",
            height=800,
            showlegend=True,
            hovermode='x unified'
        )
        
        return fig
    
    def plot_risk_score_history(self):
        """Plota histórico de score de risco"""
        if not self.model_data.get('weather'):
            return None
        
        weather = self.model_data['weather']
        met = weather['meteorological']
        
        if met.empty:
            return None
        
        # Calcular scores históricos
        scores = []
        dates = []
        
        for idx, row in met.iterrows():
            single_weather = {
                'meteorological': pd.DataFrame([row]),
                'waves': weather['waves'].iloc[[idx]] if idx < len(weather['waves']) else pd.DataFrame(),
                'currents': weather['currents'].iloc[[idx]] if idx < len(weather['currents']) else pd.DataFrame()
            }
            score, _ = self.calculate_maritime_risk_score(single_weather)
            if score is not None:
                scores.append(score)
                dates.append(row.get('date_time'))
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=dates, y=scores,
            mode='lines+markers',
            name='Score de Risco',
            fill='tozeroy',
            line=dict(color='#1f77b4')
        ))
        
        # Adicionar zonas de risco
        fig.add_hline(y=20, line_dash="dash", line_color="green", annotation_text="Ideal")
        fig.add_hline(y=40, line_dash="dash", line_color="orange", annotation_text="Bom")
        fig.add_hline(y=60, line_dash="dash", line_color="red", annotation_text="Perigoso")
        
        fig.update_layout(
            title="Histórico de Score de Risco - Últimas 30 dias",
            xaxis_title="Data",
            yaxis_title="Score (0-100)",
            height=500,
            hovermode='x unified'
        )
        
        return fig
    
    def plot_operation_events(self):
        """Plota histórico de eventos operacionais"""
        daily_issues = self.model_data.get('daily_issues')
        
        if daily_issues is None or daily_issues.empty:
            return None
        
        # Reset index para ter a data como coluna
        daily_issues_reset = daily_issues.reset_index()
        daily_issues_reset.columns = ['date', 'duration', 'num_events', 'reasons']
        
        fig = make_subplots(
            rows=1, cols=2,
            subplot_titles=("Número de Eventos por Dia", "Duração Total de Downtime (horas)")
        )
        
        fig.add_trace(
            go.Bar(x=daily_issues_reset['date'], y=daily_issues_reset['num_events'],
                  name='Eventos', marker=dict(color='#d62728')),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Bar(x=daily_issues_reset['date'], y=daily_issues_reset['duration'],
                  name='Duração', marker=dict(color='#ff7f0e')),
            row=1, col=2
        )
        
        fig.update_layout(
            title="Histórico de Operações - Últimos 30 dias",
            height=500,
            showlegend=True
        )
        
        return fig

# Instanciar o preditor global
predictor = MarineOperationPredictor()

@app.route('/')
def dashboard():
    """Renderiza o dashboard com todos os gráficos"""
    
    # Atualizar dados
    predictor.update()
    
    # Gerar gráficos
    fig_conditions = predictor.plot_maritime_conditions()
    fig_risk = predictor.plot_risk_score_history()
    fig_operations = predictor.plot_operation_events()
    
    # Converter para JSON
    plot_conditions = fig_conditions.to_html(div_id="plot_conditions") if fig_conditions else ""
    plot_risk = fig_risk.to_html(div_id="plot_risk") if fig_risk else ""
    plot_operations = fig_operations.to_html(div_id="plot_operations") if fig_operations else ""
    
    # Dados atuais
    risk_score = predictor.model_data.get('risk_score')
    factors = predictor.model_data.get('factors', {})
    recommendation = predictor.model_data.get('recommendation', 'N/A')
    color = predictor.model_data.get('recommendation_color', 'gray')
    last_update = predictor.last_update.strftime('%Y-%m-%d %H:%M:%S') if predictor.last_update else 'N/A'
    
    # Formatar fatores
    factors_html = ""
    for factor_name, factor_data in factors.items():
        valor = f"{factor_data['valor']:.2f}" if isinstance(factor_data['valor'], float) else factor_data['valor']
        factors_html += f"""
        <div class="factor-card">
            <h4>{factor_name.upper()}</h4>
            <p>Valor: <strong>{valor} {factor_data['unidade']}</strong></p>
            <p>Score: <span class="score-badge">{factor_data['score']}</span></p>
        </div>
        """
    
    html_template = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Preditor de Operações Marítimas</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
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
                max-width: 1400px;
                margin: 0 auto;
                background: white;
                border-radius: 15px;
                box-shadow: 0 20px 60px rgba(0,0,0,0.3);
                padding: 30px;
            }}
            
            header {{
                text-align: center;
                margin-bottom: 40px;
                border-bottom: 3px solid #667eea;
                padding-bottom: 20px;
            }}
            
            h1 {{
                color: #333;
                font-size: 2.5em;
                margin-bottom: 10px;
            }}
            
            .subtitle {{
                color: #666;
                font-size: 1.1em;
            }}
            
            .update-info {{
                text-align: center;
                color: #999;
                font-size: 0.9em;
                margin-top: 10px;
            }}
            
            .recommendation-box {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                padding: 30px;
                border-radius: 10px;
                margin: 30px 0;
                text-align: center;
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
            
            .recommendation-text {{
                font-size: 1.8em;
                font-weight: bold;
                margin-bottom: 10px;
            }}
            
            .risk-score {{
                font-size: 3em;
                font-weight: bold;
                margin: 20px 0;
            }}
            
            .factors-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
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
                font-size: 0.95em;
            }}
            
            .score-badge {{
                background: #667eea;
                color: white;
                padding: 5px 10px;
                border-radius: 5px;
                font-weight: bold;
            }}
            
            .plots-container {{
                margin-top: 40px;
            }}
            
            .plot {{
                margin: 30px 0;
                border-radius: 10px;
                overflow: hidden;
                box-shadow: 0 5px 20px rgba(0,0,0,0.1);
            }}
            
            .plot-title {{
                background: #667eea;
                color: white;
                padding: 15px;
                font-weight: bold;
                font-size: 1.1em;
            }}
            
            footer {{
                text-align: center;
                margin-top: 40px;
                padding-top: 20px;
                border-top: 1px solid #eee;
                color: #999;
                font-size: 0.9em;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h1>⛵ Preditor de Operações Marítimas</h1>
                <p class="subtitle">Previsão de Dias Ideais para Embarcar - Kenmare Bay</p>
                <div class="update-info">
                    Última atualização: {last_update} | Atualiza automaticamente a cada 1 hora
                </div>
            </header>
            
            <div class="recommendation-box {color}">
                <div class="risk-score">{risk_score if risk_score is not None else 'N/A'}/100</div>
                <div class="recommendation-text">{recommendation}</div>
            </div>
            
            <div class="factors-grid">
                {factors_html}
            </div>
            
            <div class="plots-container">
                <div class="plot">
                    <div class="plot-title">📊 Condições Marítimas Atuais (Últimos 30 dias)</div>
                    {plot_conditions}
                </div>
                
                <div class="plot">
                    <div class="plot-title">📈 Histórico de Score de Risco</div>
                    {plot_risk}
                </div>
                
                <div class="plot">
                    <div class="plot-title">📋 Eventos Operacionais Históricos</div>
                    {plot_operations}
                </div>
            </div>
            
            <footer>
                <p>Dashboard de Predição Marítima | Baseado em dados de weather.db e Opsdata_MarineHistory.xlsx</p>
                <p>Próxima atualização automática em 1 hora</p>
            </footer>
        </div>
        
        <script>
            // Auto-refresh a cada 1 hora (3600000ms)
            setTimeout(function() {{
                location.reload();
            }}, 3600000);
        </script>
    </body>
    </html>
    """
    
    return render_template_string(html_template)

if __name__ == '__main__':
    print("\n" + "="*70)
    print("🌊 MARINE OPERATION PREDICTOR - Iniciando...")
    print("="*70)
    print("\n📊 Carregando dados...")
    predictor.update()
    
    print(f"✓ Score de Risco: {predictor.model_data.get('risk_score')}/100")
    print(f"✓ Recomendação: {predictor.model_data.get('recommendation')}")
    print("\n🚀 Iniciando servidor Flask em http://localhost:5000")
    print("⏰ Dashboard se atualizará automaticamente a cada 1 hora")
    print("="*70 + "\n")
    
    # Iniciar aplicação
    app.run(debug=False, host='0.0.0.0', port=5000, use_reloader=False)
