"""
Quick Launch - Abre o Relatório de Predição Marítima no Navegador
Executa: python open_report.py
"""
import os
import sys
import subprocess
import webbrowser
from pathlib import Path
import time

report_path = Path("marine_reports") / "RELATORIO_PREDICAO.html"

print("\n" + "="*70)
print("⛵ PREDITOR DE OPERAÇÕES MARÍTIMAS - ABRIR RELATÓRIO")
print("="*70)

if not report_path.exists():
    print(f"\n❌ Relatório não encontrado: {report_path}")
    print("\n📊 Gerando relatório...")
    
    # Executar reporter se não existe
    subprocess.Popen([sys.executable, "marine_reporter_fast.py"], creationflags=subprocess.CREATE_NEW_CONSOLE)
    for attempt in range(3):
        print(f"⏳ Aguardando geração do relatório... ({attempt + 1}/3)")
        time.sleep(5)
        if report_path.exists():
            break

if report_path.exists():
    print("\n✅ Relatório encontrado.")
    response = input("Deseja iniciar o serviço de atualização automática em background? (s/n): ").strip().lower()
    if response == 's':
        print("\n🚀 Iniciando serviço de atualização em nova janela...")
        subprocess.Popen([sys.executable, "marine_reporter_fast.py"], creationflags=subprocess.CREATE_NEW_CONSOLE)
        time.sleep(10)

    abs_path = report_path.absolute()
    print(f"\n✅ Abrindo: {abs_path}")
    webbrowser.open(f"file:///{abs_path}")
    print("🌐 Relatório aberto no navegador padrão")
    print("\n⏰ O relatório atualiza automaticamente a cada 10 minutos")
    print("📄 Recarregue a página (F5) para ver dados mais recentes")
    print("="*70 + "\n")
else:
    print(f"\n❌ Erro ao gerar relatório")
    print("Execute: python marine_reporter_fast.py")
