"""
FASE 5: QUERIES ANALÍTICAS CON SPARK SQL
Simula consultas que se harían con Hive/Impala
"""
import logging
import sys
import json
from datetime import datetime

sys.path.append('..')
from utils import create_spark_session, stop_spark_session
from utils.constants import PROCESSED_DATA_PATH, ANALYTICS_PATH, LOGS_DIR

# Configurar logging
log_file = LOGS_DIR / f"05_queries_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

def execute_query(spark, query_name, sql_query):
    """Ejecuta una query y retorna el resultado"""
    logging.info(f"\n{'='*70}")
    logging.info(f"EJECUTANDO: {query_name}")
    logging.info(f"{'='*70}")
    logging.info(f"SQL:\n{sql_query}\n")
    
    result = spark.sql(sql_query)
    result.show(20, truncate=False)
    
    return result

def query_summary_stats(spark):
    """Query 1: Estadísticas generales"""
    query = """
    SELECT 
        COUNT(*) as total_transactions,
        COUNT(DISTINCT customer_id) as unique_customers,
        ROUND(SUM(amount), 2) as total_amount,
        ROUND(AVG(amount), 2) as avg_amount,
        ROUND(MIN(amount), 2) as min_amount,
        ROUND(MAX(amount), 2) as max_amount,
        COUNT(CASE WHEN status = 'APPROVED' THEN 1 END) as approved,
        COUNT(CASE WHEN status = 'DECLINED' THEN 1 END) as declined,
        ROUND(COUNT(CASE WHEN status = 'APPROVED' THEN 1 END) * 100.0 / COUNT(*), 2) as approval_rate
    FROM transactions_processed
    """
    return execute_query(spark, "Estadísticas Generales", query)

def query_by_country(spark):
    """Query 2: Análisis por país"""
    query = """
    SELECT 
        country,
        COUNT(*) as total_transactions,
        ROUND(SUM(amount), 2) as total_amount,
        ROUND(AVG(amount), 2) as avg_amount,
        COUNT(DISTINCT customer_id) as unique_customers,
        ROUND(COUNT(CASE WHEN status = 'APPROVED' THEN 1 END) * 100.0 / COUNT(*), 2) as approval_rate
    FROM transactions_processed
    GROUP BY country
    ORDER BY total_amount DESC
    """
    return execute_query(spark, "Análisis por País", query)

def query_by_currency(spark):
    """Query 3: Distribución por moneda"""
    query = """
    SELECT 
        currency,
        COUNT(*) as total_transactions,
        ROUND(SUM(amount), 2) as total_amount,
        ROUND(AVG(amount), 2) as avg_amount
    FROM transactions_processed
    GROUP BY currency
    ORDER BY total_transactions DESC
    """
    return execute_query(spark, "Distribución por Moneda", query)

def query_by_category(spark):
    """Query 4: Transacciones por categoría"""
    query = """
    SELECT 
        category,
        COUNT(*) as total_transactions,
        ROUND(SUM(amount), 2) as total_amount,
        ROUND(AVG(amount), 2) as avg_amount,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM transactions_processed), 2) as percentage
    FROM transactions_processed
    GROUP BY category
    ORDER BY total_transactions DESC
    """
    return execute_query(spark, "Transacciones por Categoría", query)

def query_by_time_slot(spark):
    """Query 5: Análisis por franja horaria"""
    query = """
    SELECT 
        time_slot,
        COUNT(*) as total_transactions,
        ROUND(SUM(amount), 2) as total_amount,
        ROUND(AVG(amount), 2) as avg_amount,
        COUNT(CASE WHEN status = 'DECLINED' THEN 1 END) as declined_count
    FROM transactions_processed
    GROUP BY time_slot
    ORDER BY 
        CASE time_slot
            WHEN 'Madrugada' THEN 1
            WHEN 'Mañana' THEN 2
            WHEN 'Tarde' THEN 3
            WHEN 'Noche' THEN 4
        END
    """
    return execute_query(spark, "Análisis por Franja Horaria", query)

def query_top_customers(spark):
    """Query 6: Top clientes por volumen"""
    query = """
    SELECT 
        customer_id,
        COUNT(*) as total_transactions,
        ROUND(SUM(amount), 2) as total_spent,
        ROUND(AVG(amount), 2) as avg_transaction,
        MAX(account_type) as account_type,
        COUNT(CASE WHEN status = 'DECLINED' THEN 1 END) as declined_count
    FROM transactions_processed
    GROUP BY customer_id
    ORDER BY total_spent DESC
    LIMIT 20
    """
    return execute_query(spark, "Top 20 Clientes por Volumen", query)

def query_fraud_rate_by_hour(spark):
    """Query 7: Tasa de fraude por hora"""
    query = """
    SELECT 
        hour,
        COUNT(*) as total_transactions,
        SUM(CASE WHEN risk_score >= 7 THEN 1 ELSE 0 END) as high_risk_count,
        ROUND(SUM(CASE WHEN risk_score >= 7 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as high_risk_rate
    FROM transactions_processed
    GROUP BY hour
    ORDER BY hour
    """
    return execute_query(spark, "Tasa de Alto Riesgo por Hora", query)

def query_weekend_vs_weekday(spark):
    """Query 8: Fin de semana vs días laborables"""
    query = """
    SELECT 
        CASE WHEN is_weekend THEN 'Fin de Semana' ELSE 'Entre Semana' END as period_type,
        COUNT(*) as total_transactions,
        ROUND(SUM(amount), 2) as total_amount,
        ROUND(AVG(amount), 2) as avg_amount,
        COUNT(DISTINCT customer_id) as unique_customers
    FROM transactions_processed
    GROUP BY is_weekend
    ORDER BY is_weekend
    """
    return execute_query(spark, "Fin de Semana vs Entre Semana", query)

def query_by_channel(spark):
    """Query 9: Análisis por canal de transacción"""
    query = """
    SELECT 
        channel,
        COUNT(*) as total_transactions,
        ROUND(SUM(amount), 2) as total_amount,
        ROUND(AVG(amount), 2) as avg_amount,
        ROUND(COUNT(CASE WHEN status = 'APPROVED' THEN 1 END) * 100.0 / COUNT(*), 2) as approval_rate
    FROM transactions_processed
    GROUP BY channel
    ORDER BY total_transactions DESC
    """
    return execute_query(spark, "Análisis por Canal", query)

def query_high_value_declined(spark):
    """Query 10: Transacciones de alto valor rechazadas"""
    query = """
    SELECT 
        transaction_id,
        customer_id,
        country,
        amount,
        currency,
        category,
        timestamp,
        risk_score
    FROM transactions_processed
    WHERE status = 'DECLINED' AND amount > 1000
    ORDER BY amount DESC
    LIMIT 20
    """
    return execute_query(spark, "Top Transacciones Alto Valor Rechazadas", query)

def save_results_to_json(results_dict, output_path):
    """Guarda resultados en formato JSON"""
    json_data = {}
    
    for query_name, df in results_dict.items():
        # Convertir DataFrame a lista de diccionarios
        json_data[query_name] = [row.asDict() for row in df.collect()]
    
    output_file = output_path / "analytics_results.json"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, indent=2, ensure_ascii=False, default=str)
    
    logging.info(f"\n💾 Resultados guardados en JSON: {output_file}")

def main():
    """Función principal de queries analíticas"""
    logging.info("=" * 70)
    logging.info("INICIANDO FASE 5: QUERIES ANALÍTICAS")
    logging.info("=" * 70)
    
    spark = None
    
    try:
        spark = create_spark_session("05_AnalyticalQueries")
        
        # Cargar tabla
        logging.info(f"📂 Cargando datos desde: {PROCESSED_DATA_PATH}")
        df = spark.read.parquet(str(PROCESSED_DATA_PATH))
        df.createOrReplaceTempView("transactions_processed")
        
        logging.info(f"✅ Tabla cargada: {df.count()} registros\n")
        
        # Ejecutar queries y almacenar resultados
        results = {}
        
        results['summary_stats'] = query_summary_stats(spark)
        results['by_country'] = query_by_country(spark)
        results['by_currency'] = query_by_currency(spark)
        results['by_category'] = query_by_category(spark)
        results['by_time_slot'] = query_by_time_slot(spark)
        results['top_customers'] = query_top_customers(spark)
        results['fraud_rate_by_hour'] = query_fraud_rate_by_hour(spark)
        results['weekend_vs_weekday'] = query_weekend_vs_weekday(spark)
        results['by_channel'] = query_by_channel(spark)
        results['high_value_declined'] = query_high_value_declined(spark)
        
        # Guardar resultados
        save_results_to_json(results, ANALYTICS_PATH)
        
        logging.info("\n" + "=" * 70)
        logging.info("RESUMEN")
        logging.info("=" * 70)
        logging.info(f"Queries ejecutadas: {len(results)}")
        logging.info(f"Resultados guardados en: {ANALYTICS_PATH}")
        logging.info("=" * 70)
        
        logging.info("✅ FASE 5 COMPLETADA EXITOSAMENTE")
        
        return 0
        
    except Exception as e:
        logging.error(f"❌ Error ejecutando queries: {str(e)}", exc_info=True)
        return 1
        
    finally:
        if spark:
            stop_spark_session(spark)

if __name__ == "__main__":
    sys.exit(main())