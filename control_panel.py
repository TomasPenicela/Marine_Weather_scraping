"""
Control Panel - Centro de Controle do Preditor Marítimo
Execute: python control_panel.py
"""
import subprocess
import os
import sys
import time
from pathlib import Path

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def show_menu():
    clear_screen()
    print("\n" + "="*70)
    print("⛵ PREDITOR DE OPERAÇÕES MARÍTIMAS - CENTRO DE CONTROLE")
    print("="*70)
    print("\n Selecione uma opção:\n")
    print(" 1) 📊 Dashboard Web (Servidor Flask)")
    print("    └─ Acesso em http://localhost:5000")
    print("    └─ Interface interativa com atualização automática")
    print()
    print(" 2) 📄 Relatório HTML (Modo Offline)")
    print("    └─ Gera relatório em marine_reports/RELATORIO_PREDICAO.html")
    print("    └─ Atualiza automaticamente a cada 10 minutos")
    print()
    print(" 3) 🌐 Abrir Relatório no Navegador")
    print("    └─ Abre o última relatório gerado")
    print()
    print(" 4) 📋 Ver Dados JSON mais Recentes")
    print("    └─ Exibe últimas previsões em formato JSON")
    print()
    print(" 5) 📈 Verificar Status dos Serviços")
    print("    └─ Lista processos Python em execução")
    print()
    print(" 6) ❌ Parar Todos os Serviços")
    print("    └─ Encerra todos os processos")
    print()
    print(" 0) Sair")
    print("\n" + "="*70)
    
    choice = input("\nDigite sua escolha (0-6): ").strip()
    return choice

def start_dashboard():
    print("\n🚀 Iniciando Dashboard Web...")
    print("📍 Acesse: http://localhost:5000")
    print("⏹️  Pressione Ctrl+C para parar\n")
    os.system("python marine_predictor.py")

def start_reporter():
    print("\n🚀 Iniciando Reporter com Atualização Automática...")
    print("📁 Relatório em: marine_reports/RELATORIO_PREDICAO.html")
    print("⏹️  Pressione Ctrl+C para parar\n")
    os.system("python marine_reporter_fast.py")

def view_report():
    report_path = Path("marine_reports") / "RELATORIO_PREDICAO.html"
    
    if not report_path.exists():
        print("\n❌ Relatório não encontrado")
        print("Por favor, gere o relatório primeira (opção 2)")
        input("\nPressione Enter para voltar...")
        return
    
    print("\n🌐 Abrindo relatório no navegador...")
    import webbrowser
    webbrowser.open(f"file:///{report_path.absolute()}")
    print("✅ Relatório aberto!")
    input("\nPressione Enter para voltar...")

def view_json():
    json_path = Path("marine_reports") / "latest_report.json"
    
    if not json_path.exists():
        print("\n❌ Arquivo JSON não encontrado")
        print("Por favor, gere os dados primeiro (opção 2)")
        input("\nPressione Enter para voltar...")
        return
    
    print("\n📋 Últimos dados JSON:")
    print("-"*70)
    with open(json_path, 'r', encoding='utf-8') as f:
        print(f.read())
    print("-"*70)
    input("\nPressione Enter para voltar...")

def check_services():
    print("\n📊 Status dos Serviços:")
    print("-"*70)
    
    # Verificar se Flask está rodando
    result = os.popen("netstat -ano 2>nul | findstr :5000").read()
    if result:
        print("✅ Dashboard Web (porta 5000)")
    else:
        print("❌ Dashboard Web (porta 5000)")
    
    # Verificar processos Python
    result = os.popen("tasklist 2>nul | findstr python").read()
    if result:
        print("\n🐍 Processos Python em execução:")
        print(result)
    else:
        print("❌ Nenhum processo Python em execução")
    
    # Verificar relatórios
    reports = Path("marine_reports")
    if reports.exists():
        files = list(reports.glob("*"))
        if files:
            print(f"\n📁 Arquivos em marine_reports/:")
            for f in files:
                size = f.stat().st_size / 1024
                print(f"  ✓ {f.name} ({size:.1f} KB)")
    
    print("-"*70)
    input("\nPressione Enter para voltar...")

def stop_services():
    print("\n⏹️  Parando serviços...")
    
    # Matar processos Python
    os.system("taskkill /F /IM python.exe /T >nul 2>&1")
    
    print("✅ Todos os serviços foram interrompidos")
    input("\nPressione Enter para sair...")

def main():
    while True:
        choice = show_menu()
        
        if choice == '1':
            start_dashboard()
        elif choice == '2':
            start_reporter()
        elif choice == '3':
            view_report()
        elif choice == '4':
            view_json()
        elif choice == '5':
            check_services()
        elif choice == '6':
            confirm = input("\n⚠️  Tem certeza que deseja parar TODOS os serviços? (s/n): ")
            if confirm.lower() == 's':
                stop_services()
                break
        elif choice == '0':
            print("\n👋 Até logo!")
            sys.exit(0)
        else:
            print("\n❌ Opção inválida. Tente novamente.")
            input("\nPressione Enter para voltar...")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 Até logo!")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Erro: {e}")
        input("\nPressione Enter para sair...")
