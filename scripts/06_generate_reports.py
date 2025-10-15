"""
FASE 6: GENERACIÓN DE REPORTES
Consolida resultados y genera reportes ejecutivos
"""
import logging
import sys
import json
from datetime import datetime
from pathlib import Path

sys.path.append('..')
from utils import create_spark_session, stop_spark_session
from utils.constants import (
    PROCESSED_DATA_PATH, FRAUD_ALERTS_PATH, 
    ANALYTICS_PATH, REPORTS_PATH, LOGS_DIR
)

# Configurar logging
log_file = LOGS_DIR / f"06_reports_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

def load_analytics_results():
    """Carga resultados de las queries analíticas"""
    json_file = ANALYTICS_PATH / "analytics_results.json"
    
    if not json_file.exists():
        logging.warning("⚠️ No se encontraron resultados analíticos")
        return None
    
    with open(json_file, 'r', encoding='utf-8') as f:
        return json.load(f)

def generate_executive_summary(spark):
    """Genera resumen ejecutivo"""
    logging.info("Generando resumen ejecutivo...")
    
    # Leer datos
    df = spark.read.parquet(str(PROCESSED_DATA_PATH))
    
    # Calcular métricas clave
    total_transactions = df.count()
    approved = df.filter(df.status == 'APPROVED').count()
    declined = df.filter(df.status == 'DECLINED').count()
    fraud = df.filter(df.status == 'FRAUD').count()
    pending = df.filter(df.status == 'PENDING').count()
    
    total_amount = df.agg({'amount': 'sum'}).collect()[0][0]
    avg_amount = df.agg({'amount': 'avg'}).collect()[0][0]
    unique_customers = df.select('customer_id').distinct().count()
    
    # Alertas de fraude
    fraud_alerts_file = FRAUD_ALERTS_PATH / "fraud_alerts.parquet"
    fraud_alerts_count = 0
    if fraud_alerts_file.exists():
        df_fraud = spark.read.parquet(str(fraud_alerts_file))
        fraud_alerts_count = df_fraud.count()
    
    summary = {
        'fecha_generacion': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'periodo_analizado': '2025-10-01 a 2025-10-14',
        'metricas_generales': {
            'total_transacciones': total_transactions,
            'clientes_unicos': unique_customers,
            'monto_total': round(total_amount, 2),
            'monto_promedio': round(avg_amount, 2)
        },
        'distribucion_estados': {
            'aprobadas': approved,
            'rechazadas': declined,
            'fraude': fraud,
            'pendientes': pending,
            'tasa_aprobacion': round(approved * 100.0 / total_transactions, 2)
        },
        'alertas_fraude': {
            'total_alertas': fraud_alerts_count,
            'tasa_deteccion': round(fraud_alerts_count * 100.0 / total_transactions, 2)
        }
    }
    
    return summary

def generate_markdown_report(summary, analytics_results):
    """Genera reporte en formato Markdown"""
    logging.info("Generando reporte Markdown...")
    
    report = f"""# 📊 REPORTE DE ANÁLISIS BANCARIO
## Pipeline Cloudera Data Engineering

**Fecha de generación:** {summary['fecha_generacion']}  
**Período analizado:** {summary['periodo_analizado']}

---

## 📈 RESUMEN EJECUTIVO

### Métricas Generales
- **Total de transacciones procesadas:** {summary['metricas_generales']['total_transacciones']:,}
- **Clientes únicos:** {summary['metricas_generales']['clientes_unicos']:,}
- **Monto total procesado:** €{summary['metricas_generales']['monto_total']:,.2f}
- **Monto promedio por transacción:** €{summary['metricas_generales']['monto_promedio']:.2f}

### Distribución de Estados
| Estado | Cantidad | Porcentaje |
|--------|----------|------------|
| ✅ Aprobadas | {summary['distribucion_estados']['aprobadas']:,} | {summary['distribucion_estados']['tasa_aprobacion']}% |
| ❌ Rechazadas | {summary['distribucion_estados']['rechazadas']:,} | {round(summary['distribucion_estados']['rechazadas'] * 100.0 / summary['metricas_generales']['total_transacciones'], 2)}% |
| ⚠️ Fraude | {summary['distribucion_estados']['fraude']:,} | {round(summary['distribucion_estados']['fraude'] * 100.0 / summary['metricas_generales']['total_transacciones'], 2)}% |
| 🕐 Pendientes | {summary['distribucion_estados']['pendientes']:,} | {round(summary['distribucion_estados']['pendientes'] * 100.0 / summary['metricas_generales']['total_transacciones'], 2)}% |

### 🚨 Alertas de Fraude
- **Total de alertas detectadas:** {summary['alertas_fraude']['total_alertas']}
- **Tasa de detección:** {summary['alertas_fraude']['tasa_deteccion']}%

---

## 🌍 ANÁLISIS POR PAÍS

"""
    
    # Agregar análisis por país si existe
    if analytics_results and 'by_country' in analytics_results:
        report += "| País | Transacciones | Monto Total | Monto Promedio | Tasa Aprobación |\n"
        report += "|------|---------------|-------------|----------------|------------------|\n"
        for row in analytics_results['by_country'][:10]:
            report += f"| {row['country']} | {row['total_transactions']:,} | €{row['total_amount']:,.2f} | €{row['avg_amount']:.2f} | {row['approval_rate']}% |\n"
    
    report += "\n---\n\n## 💳 ANÁLISIS POR CATEGORÍA\n\n"
    
    if analytics_results and 'by_category' in analytics_results:
        report += "| Categoría | Transacciones | Monto Total | Porcentaje |\n"
        report += "|-----------|---------------|-------------|------------|\n"
        for row in analytics_results['by_category']:
            report += f"| {row['category']} | {row['total_transactions']:,} | €{row['total_amount']:,.2f} | {row['percentage']}% |\n"
    
    report += "\n---\n\n## 🕐 ANÁLISIS TEMPORAL\n\n"
    
    if analytics_results and 'by_time_slot' in analytics_results:
        report += "| Franja Horaria | Transacciones | Monto Total | Rechazadas |\n"
        report += "|----------------|---------------|-------------|------------|\n"
        for row in analytics_results['by_time_slot']:
            report += f"| {row['time_slot']} | {row['total_transactions']:,} | €{row['total_amount']:,.2f} | {row['declined_count']} |\n"
    
    report += f"""

---

## 🔧 ARQUITECTURA TÉCNICA

### Pipeline Implementado
1. **Ingesta y Validación** - Lectura desde "HDFS" (data/raw)
2. **Transformación** - Feature engineering con PySpark
3. **Detección de Fraude** - Aplicación de reglas de negocio
4. **Capa Hive/Impala** - Tablas externas sobre Parquet
5. **Queries Analíticas** - Spark SQL para análisis
6. **Reportes** - Generación automatizada

### Tecnologías Utilizadas
- **Apache Spark** - Procesamiento distribuido
- **Parquet** - Almacenamiento columnar comprimido
- **Spark SQL** - Queries analíticas (simula Hive/Impala)
- **Python/PySpark** - Lenguaje de desarrollo

### Formatos de Datos
- **Input:** CSV (raw data)
- **Processing:** Parquet particionado por fecha
- **Output:** Parquet, JSON, CSV (reportes)

---

## 📝 CONCLUSIONES

1. Se procesaron exitosamente {summary['metricas_generales']['total_transacciones']:,} transacciones
2. Tasa de aprobación: **{summary['distribucion_estados']['tasa_aprobacion']}%**
3. Se detectaron **{summary['alertas_fraude']['total_alertas']} alertas** de posible fraude
4. El pipeline es escalable y replicable en entorno Cloudera real

---

**Generado por:** Cloudera Data Engineering Pipeline  
**Fecha:** {summary['fecha_generacion']}
"""
    
    return report

def generate_text_report(summary):
    """Genera reporte en texto plano"""
    logging.info("Generando reporte de texto...")
    
    report = f"""
{'='*80}
REPORTE DE ANÁLISIS BANCARIO - PIPELINE CLOUDERA DATA ENGINEERING
{'='*80}

Fecha de generación: {summary['fecha_generacion']}
Período analizado: {summary['periodo_analizado']}

{'='*80}
RESUMEN EJECUTIVO
{'='*80}

MÉTRICAS GENERALES:
  - Total transacciones procesadas: {summary['metricas_generales']['total_transacciones']:,}
  - Clientes únicos: {summary['metricas_generales']['clientes_unicos']:,}
  - Monto total procesado: €{summary['metricas_generales']['monto_total']:,.2f}
  - Monto promedio: €{summary['metricas_generales']['monto_promedio']:.2f}

DISTRIBUCIÓN DE ESTADOS:
  - Aprobadas: {summary['distribucion_estados']['aprobadas']:,} ({summary['distribucion_estados']['tasa_aprobacion']}%)
  - Rechazadas: {summary['distribucion_estados']['rechazadas']:,}
  - Fraude: {summary['distribucion_estados']['fraude']:,}
  - Pendientes: {summary['distribucion_estados']['pendientes']:,}

ALERTAS DE FRAUDE:
  - Total alertas detectadas: {summary['alertas_fraude']['total_alertas']}
  - Tasa de detección: {summary['alertas_fraude']['tasa_deteccion']}%

{'='*80}
PIPELINE EJECUTADO EXITOSAMENTE
{'='*80}

Fases completadas:
  ✅ Fase 1: Ingesta y validación
  ✅ Fase 2: Transformación y enriquecimiento
  ✅ Fase 3: Detección de anomalías
  ✅ Fase 4: Creación de tablas Hive/Impala
  ✅ Fase 5: Queries analíticas
  ✅ Fase 6: Generación de reportes

{'='*80}
"""
    return report

def main():
    """Función principal de generación de reportes"""
    logging.info("=" * 70)
    logging.info("INICIANDO FASE 6: GENERACIÓN DE REPORTES")
    logging.info("=" * 70)
    
    spark = None
    
    try:
        spark = create_spark_session("06_GenerateReports")
        
        # Generar resumen ejecutivo
        summary = generate_executive_summary(spark)
        
        # Cargar resultados analíticos
        analytics_results = load_analytics_results()
        
        # Generar reporte Markdown
        markdown_report = generate_markdown_report(summary, analytics_results)
        markdown_file = REPORTS_PATH / f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        
        with open(markdown_file, 'w', encoding='utf-8') as f:
            f.write(markdown_report)
        
        logging.info(f"✅ Reporte Markdown guardado: {markdown_file}")
        
        # Generar reporte de texto
        text_report = generate_text_report(summary)
        text_file = REPORTS_PATH / f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        with open(text_file, 'w', encoding='utf-8') as f:
            f.write(text_report)
        
        logging.info(f"✅ Reporte de texto guardado: {text_file}")
        
        # Guardar resumen en JSON
        json_file = REPORTS_PATH / f"summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        logging.info(f"✅ Resumen JSON guardado: {json_file}")
        
        # Mostrar resumen en consola
        logging.info("\n" + text_report)
        
        logging.info("\n" + "=" * 70)
        logging.info("REPORTES GENERADOS")
        logging.info("=" * 70)
        logging.info(f"📄 Markdown: {markdown_file}")
        logging.info(f"📄 Texto: {text_file}")
        logging.info(f"📄 JSON: {json_file}")
        logging.info("=" * 70)
        
        logging.info("✅ FASE 6 COMPLETADA EXITOSAMENTE")
        
        return 0
        
    except Exception as e:
        logging.error(f"❌ Error generando reportes: {str(e)}", exc_info=True)
        return 1
        
    finally:
        if spark:
            stop_spark_session(spark)

if __name__ == "__main__":
    sys.exit(main())