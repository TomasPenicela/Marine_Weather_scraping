"""
Setup do Preditor de Operações Marítimas
Execute: python setup.py
"""
import subprocess
import sys
import os
from pathlib import Path

def run_cmd(cmd, description):
    """Executa comando e mostra output"""
    print(f"\n📌 {description}...")
    result = os.system(cmd)
    if result == 0:
        print(f"✅ {description} - OK")
        return True
    else:
        print(f"❌ {description} - FALHOU")
        return False

def main():
    print("\n" + "="*70)
    print("⛵ SETUP - PREDITOR DE OPERAÇÕES MARÍTIMAS")
    print("="*70)
    
    # Verificar Python
    print("\n🔍 Verificando Python...")
    py_version = sys.version.split()[0]
    print(f"   Python {py_version}")
    
    # Upgrade pip
    print("\n📦 Atualizando gerenciador de pacotes...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pip", "-q"])
    print("✅ pip atualizado")
    
    # Dependências
    packages = [
        'pandas',
        'numpy',
        'plotly',
        'flask',
        'openpyxl',
        'sqlalchemy',
        'pyodbc',
        'scikit-learn'
    ]
    
    print("\n📚 Instalando dependências:")
    for pkg in packages:
        subprocess.check_call([sys.executable, "-m", "pip", "install", pkg, "-q"])
        print(f"   ✅ {pkg}")
    
    print(f"\n✅ Todas as {len(packages)} dependências instaladas!")
    
    # Criar pastas
    print("\n📁 Criando estrutura de diretórios...")
    Path("marine_reports").mkdir(exist_ok=True)
    print("   ✅ Pasta 'marine_reports' criada")
    
    # Gerar primeiro relatório
    print("\n🚀 Gerando primeiro relatório...")
    subprocess.run([sys.executable, "marine_reporter_fast.py"], 
                   timeout=60, capture_output=True)
    
    if Path("marine_reports/RELATORIO_PREDICAO.html").exists():
        print("   ✅ Primeiro relatório gerado!")
    else:
        print("   ⚠️  Relatório ainda não disponível (pode tentar manualmente)")
    
    # Summary
    print("\n" + "="*70)
    print("✅ SETUP COMPLETO!")
    print("="*70)
    print("\n📖 Próximos passos:\n")
    print("1. Iniciar relatório automático:")
    print("   >>> python marine_reporter_fast.py\n")
    print("2. Iniciar dashboard web:")
    print("   >>> python marine_predictor.py\n")
    print("3. Centro de controle:")
    print("   >>> python control_panel.py\n")
    print("4. Leia o guia completo:")
    print("   >>> MARINE_PREDICTOR_README.md\n")
    print("="*70 + "\n")

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"\n❌ Erro durante setup: {e}")
        print("\nTente executar manualmente:")
        print("pip install pandas numpy plotly flask openpyxl sqlalchemy pyodbc scikit-learn")
        sys.exit(1)
