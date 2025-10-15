"""
ORQUESTADOR MAESTRO DEL PIPELINE
Ejecuta todas las fases en orden
"""
import sys
import subprocess
import logging
from datetime import datetime
from pathlib import Path

# Configurar logging
LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

log_file = LOGS_DIR / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# Scripts del pipeline en orden
SCRIPTS = [
    "01_ingest_data.py",
    "02_transform_data.py",
    "03_fraud_detection.py",
    "04_create_hive_tables.py",
    "05_analytical_queries.py",
    "06_generate_reports.py"
]

def print_banner():
    """Imprime banner inicial"""
    banner = """
    ╔═══════════════════════════════════════════════════════════════╗
    ║                                                               ║
    ║       CLOUDERA DATA ENGINEERING PIPELINE                      ║
    ║       Análisis de Transacciones Bancarias                     ║
    ║                                                               ║
    ╚═══════════════════════════════════════════════════════════════╝
    """
    print(banner)
    logging.info(banner)

def execute_script(script_name):
    """Ejecuta un script del pipeline"""
    script_path = Path(__file__).parent / script_name
    
    logging.info("=" * 80)
    logging.info(f"EJECUTANDO: {script_name}")
    logging.info("=" * 80)
    
    start_time = datetime.now()
    
    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            check=True
        )
        
        # Loguear output
        if result.stdout:
            logging.info(result.stdout)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logging.info(f"✅ {script_name} completado en {duration:.2f} segundos")
        
        return True, duration
        
    except subprocess.CalledProcessError as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logging.error(f"❌ Error en {script_name}")
        logging.error(f"Código de salida: {e.returncode}")
        
        if e.stdout:
            logging.error(f"STDOUT:\n{e.stdout}")
        if e.stderr:
            logging.error(f"STDERR:\n{e.stderr}")
        
        return False, duration

def main():
    """Función principal del orquestador"""
    print_banner()
    
    logging.info(f"Inicio del pipeline: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"Scripts a ejecutar: {len(SCRIPTS)}")
    logging.info("")
    
    pipeline_start = datetime.now()
    results = {}
    
    # Ejecutar cada script
    for i, script in enumerate(SCRIPTS, 1):
        logging.info(f"\n{'#'*80}")
        logging.info(f"FASE {i}/{len(SCRIPTS)}: {script}")
        logging.info(f"{'#'*80}\n")
        
        success, duration = execute_script(script)
        results[script] = {
            'success': success,
            'duration': duration
        }
        
        if not success:
            logging.error(f"\n❌ Pipeline abortado en {script}")
            break
        
        logging.info(f"\n{'─'*80}\n")
    
    pipeline_end = datetime.now()
    total_duration = (pipeline_end - pipeline_start).total_seconds()
    
    # Resumen final
    logging.info("\n" + "=" * 80)
    logging.info("RESUMEN DEL PIPELINE")
    logging.info("=" * 80)
    
    successful = sum(1 for r in results.values() if r['success'])
    failed = len(results) - successful
    
    logging.info(f"\nEjecutados: {len(results)}/{len(SCRIPTS)}")
    logging.info(f"Exitosos: {successful}")
    logging.info(f"Fallidos: {failed}")
    logging.info(f"\nTiempo total: {total_duration:.2f} segundos ({total_duration/60:.2f} minutos)")
    
    logging.info("\nDetalle por script:")
    for script, result in results.items():
        status = "✅ OK" if result['success'] else "❌ ERROR"
        logging.info(f"  {status} | {script:<30} | {result['duration']:.2f}s")
    
    logging.info("\n" + "=" * 80)
    
    if failed == 0:
        logging.info("🎉 PIPELINE COMPLETADO EXITOSAMENTE")
        logging.info("=" * 80)
        logging.info(f"\n📊 Revisa los reportes en: data/results/reports/")
        logging.info(f"📝 Log completo: {log_file}")
        return 0
    else:
        logging.error("💥 PIPELINE COMPLETADO CON ERRORES")
        logging.error("=" * 80)
        logging.error(f"\n📝 Revisa el log para más detalles: {log_file}")
        return 1

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        logging.warning("\n⚠️ Pipeline interrumpido por el usuario")
        sys.exit(130)
    except Exception as e:
        logging.error(f"\n❌ Error inesperado: {str(e)}", exc_info=True)
        sys.exit(1)