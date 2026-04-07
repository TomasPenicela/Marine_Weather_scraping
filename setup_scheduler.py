"""
Setup para agendador automático - Windows Task Scheduler
Cria uma tarefa que executa auto_updater.py a cada hora
Execute este script UMA VEZ como administrador para configurar
"""

import subprocess
import sys
import os
from pathlib import Path

def setup_task_scheduler():
    """Cria tarefa agendada no Windows para atualizar a cada hora"""
    
    # Localização do script
    script_path = Path(__file__).parent / "auto_updater.py"
    python_exe = sys.executable
    work_dir = Path(__file__).parent
    
    task_name = "WeatherDataAutoUpdater"
    
    print("="*70)
    print("⚙️ SETUP - Agendador de Atualização Automática")
    print("="*70 + "\n")
    
    print(f"Script: {script_path}")
    print(f"Python: {python_exe}")
    print(f"Diretório: {work_dir}")
    print(f"Tarefa: {task_name}\n")
    
    # Comando XML da tarefa
    task_xml = f'''<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Date>2026-04-01T00:00:00</Date>
    <Author>Weather Data System</Author>
    <Description>Atualiza dados de weather a cada hora automaticamente</Description>
  </RegistrationInfo>
  <Triggers>
    <CalendarTrigger>
      <StartBoundary>2026-04-01T00:00:00</StartBoundary>
      <Enabled>true</Enabled>
      <ScheduleByDay>
        <DaysInterval>1</DaysInterval>
      </ScheduleByDay>
    </CalendarTrigger>
    <BootTrigger>
      <Enabled>true</Enabled>
    </BootTrigger>
  </Triggers>
  <Principals>
    <Principal id="LocalSystem">
      <UserId>S-1-5-18</UserId>
      <RunLevel>HighestAvailable</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>Queue</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
    <IdleSettings>
      <Duration>PT10M</Duration>
      <WaitTimeout>PT1H</WaitTimeout>
      <StopOnIdleEnd>true</StopOnIdleEnd>
      <RestartOnIdle>false</RestartOnIdle>
    </IdleSettings>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>false</Hidden>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <WakeToRun>true</WakeToRun>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <Priority>5</Priority>
  </Settings>
  <Actions Context="LocalSystem">
    <Exec>
      <Command>"{python_exe}"</Command>
      <Arguments>"{script_path}"</Arguments>
      <WorkingDirectory>{work_dir}</WorkingDirectory>
    </Exec>
  </Actions>
</Task>
'''
    
    # Salvar XML temporário
    xml_temp = work_dir / f"{task_name}.xml"
    
    try:
        print("📝 Criando arquivo de tarefa temporário...")
        xml_temp.write_text(task_xml, encoding='utf-16-le')
        
        print("✓ Arquivo criado\n")
        
        print("⚙️ Configurando tarefa no Windows Task Scheduler...")
        print("   (pode solicitar permissão de administrador)\n")
        
        # Comando para criar tarefa
        cmd = [
            "schtasks",
            "/create",
            "/f",  # Force (sobrescrever se existir)
            "/tn",
            task_name,
            "/xml",
            str(xml_temp)
        ]
        
        # Executar comando
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ SUCESSO! Tarefa agendada criada.\n")
            print("A atualização automática vai:")
            print("  • Executar a cada hora")
            print("  • Iniciar automaticamente ao ligar o computador")
            print("  • Executar em background\n")
            
            print("Para gerenciar a tarefa:")
            print("  1. Abra 'Agendador de Tarefas' (Task Scheduler)")
            print("  2. Procure por 'WeatherDataAutoUpdater'")
            print("  3. Clique com direita para: Executar, Desabilitar, Excluir, etc.\n")
            
            print("Para executar manualmente a qualquer momento:")
            print(f"  schtasks /run /tn \"{task_name}\"\n")
            
            print("Para desabilitar:")
            print(f"  schtasks /change /tn \"{task_name}\" /disable\n")
            
            print("Para excluir:")
            print(f"  schtasks /delete /tn \"{task_name}\" /f\n")
            
        else:
            print("❌ ERRO ao criar tarefa:")
            print(result.stderr)
            print("\nTente executar este script como administrador.\n")
        
        # Limpar arquivo temporário
        print("🧹 Limpando arquivo temporário...")
        xml_temp.unlink()
        print("✓ Pronto\n")
    
    except PermissionError:
        print("❌ ERRO: Permissão negada. Execute como administrador.\n")
        print("Passos:")
        print("  1. Clique com direita no menu Iniciar")
        print("  2. Selecione 'Terminal do Windows (Admin)'")
        print("  3. Execute este script novamente\n")
    
    except Exception as e:
        print(f"❌ ERRO: {e}\n")

def list_scheduled_tasks():
    """Lista tarefas agendadas relacionadas"""
    print("\n" + "="*70)
    print("📋 TAREFAS AGENDADAS (Weather Data)")
    print("="*70 + "\n")
    
    try:
        result = subprocess.run(
            ["schtasks", "/query", "/fo", "list", "/v"],
            capture_output=True,
            text=True
        )
        
        for line in result.stdout.split('\n'):
            if 'WeatherData' in line or 'weather' in line.lower():
                print(line)
    except:
        print("Não foi possível listar tarefas.\n")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--list":
        list_scheduled_tasks()
    else:
        setup_task_scheduler()
