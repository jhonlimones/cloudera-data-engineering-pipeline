"""
FASE 3: DETECCIÓN DE ANOMALÍAS Y FRAUDE
Aplica reglas de negocio para identificar transacciones sospechosas
"""
import logging
import sys
from datetime import datetime
from pyspark.sql.functions import col, when, lit

sys.path.append('..')
from utils import create_spark_session, stop_spark_session
from utils.constants import (
    PROCESSED_DATA_PATH, FRAUD_ALERTS_PATH, LOGS_DIR,
    FRAUD_THRESHOLDS
)

# Configurar logging
log_file = LOGS_DIR / f"03_fraud_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

def detect_fraud_patterns(df):
    """Detecta patrones de fraude basados en reglas de negocio"""
    logging.info("Aplicando reglas de detección de fraude...")
    
    # Regla 1: Transacciones marcadas como FRAUD
    df = df.withColumn("fraud_rule_status",
        when(col("status") == "FRAUD", True).otherwise(False)
    )
    
    # Regla 2: Montos muy altos rechazados
    df = df.withColumn("fraud_rule_high_declined",
        when(
            (col("status") == "DECLINED") & 
            (col("amount") > FRAUD_THRESHOLDS['high_amount']),
            True
        ).otherwise(False)
    )
    
    # Regla 3: Transacciones nocturnas sospechosas
    df = df.withColumn("fraud_rule_night",
        col("is_suspicious_night")
    )
    
    # Regla 4: Velocidad transaccional sospechosa
    df = df.withColumn("fraud_rule_velocity",
        col("is_suspicious_velocity")
    )
    
    # Regla 5: Múltiples transacciones rechazadas del mismo cliente
    # (este análisis ya está implícito en el risk_score)
    
    # Consolidar detección: si cumple cualquier regla
    df = df.withColumn("is_fraud_detected",
        when(
            col("fraud_rule_status") |
            col("fraud_rule_high_declined") |
            col("fraud_rule_night") |
            col("fraud_rule_velocity") |
            (col("risk_score") >= 7),
            True
        ).otherwise(False)
    )
    
    # Clasificar prioridad de alerta
    df = df.withColumn("alert_priority",
        when(col("fraud_rule_status"), "CRÍTICA")
        .when(col("risk_score") >= 8, "ALTA")
        .when(col("risk_score") >= 5, "MEDIA")
        .otherwise("BAJA")
    )
    
    # Razones de alerta (concatenar reglas que se activaron)
    df = df.withColumn("fraud_reasons",
        when(col("fraud_rule_status"), "Estado FRAUD | ")
        .otherwise("") +
        when(col("fraud_rule_high_declined"), "Monto alto rechazado | ")
        .otherwise("") +
        when(col("fraud_rule_night"), "Transacción nocturna sospechosa | ")
        .otherwise("") +
        when(col("fraud_rule_velocity"), "Velocidad sospechosa | ")
        .otherwise("") +
        when(col("risk_score") >= 7, f"Risk score alto ({col('risk_score')}) | ")
        .otherwise("")
    )
    
    logging.info("✅ Reglas de fraude aplicadas")
    return df

def generate_fraud_summary(df):
    """Genera resumen de detecciones"""
    logging.info("\n" + "=" * 70)
    logging.info("RESUMEN DE DETECCIÓN DE FRAUDE")
    logging.info("=" * 70)
    
    total = df.count()
    fraud_detected = df.filter(col("is_fraud_detected")).count()
    fraud_percentage = (fraud_detected / total * 100) if total > 0 else 0
    
    logging.info(f"Total transacciones: {total}")
    logging.info(f"Fraudes detectados: {fraud_detected} ({fraud_percentage:.2f}%)")
    
    # Por prioridad
    logging.info("\nPor prioridad de alerta:")
    df.filter(col("is_fraud_detected")) \
      .groupBy("alert_priority") \
      .count() \
      .orderBy(col("count").desc()) \
      .show()
    
    # Por país
    logging.info("Fraudes por país (Top 5):")
    df.filter(col("is_fraud_detected")) \
      .groupBy("country") \
      .count() \
      .orderBy(col("count").desc()) \
      .limit(5) \
      .show()
    
    # Por regla activada
    logging.info("Detecciones por regla:")
    logging.info(f"  - Status FRAUD: {df.filter(col('fraud_rule_status')).count()}")
    logging.info(f"  - Monto alto rechazado: {df.filter(col('fraud_rule_high_declined')).count()}")
    logging.info(f"  - Nocturna sospechosa: {df.filter(col('fraud_rule_night')).count()}")
    logging.info(f"  - Velocidad sospechosa: {df.filter(col('fraud_rule_velocity')).count()}")
    
    logging.info("=" * 70)

def main():
    """Función principal de detección de fraude"""
    logging.info("=" * 70)
    logging.info("INICIANDO FASE 3: DETECCIÓN DE ANOMALÍAS Y FRAUDE")
    logging.info("=" * 70)
    
    spark = None
    
    try:
        spark = create_spark_session("03_FraudDetection")
        
        # Leer datos procesados
        logging.info(f"📂 Leyendo datos desde: {PROCESSED_DATA_PATH}")
        df = spark.read.parquet(str(PROCESSED_DATA_PATH))
        
        total_count = df.count()
        logging.info(f"✅ Datos cargados: {total_count} registros")
        
        # Aplicar detección de fraude
        df_fraud = detect_fraud_patterns(df)
        
        # Generar resumen
        generate_fraud_summary(df_fraud)
        
        # Filtrar solo alertas detectadas
        fraud_alerts = df_fraud.filter(col("is_fraud_detected"))
        alerts_count = fraud_alerts.count()
        
        if alerts_count > 0:
            logging.info(f"\n📊 Muestra de alertas de fraude (Top 10 por risk score):")
            fraud_alerts.select(
                "transaction_id", "customer_id", "amount", "country",
                "timestamp", "status", "alert_priority", "risk_score"
            ).orderBy(col("risk_score").desc()).show(10, truncate=False)
            
            # Guardar alertas
            output_path = FRAUD_ALERTS_PATH / "fraud_alerts.parquet"
            logging.info(f"\n💾 Guardando alertas en: {output_path}")
            
            fraud_alerts.write.mode("overwrite").parquet(str(output_path))
            
            # También guardar en CSV para revisión manual
            csv_path = FRAUD_ALERTS_PATH / "fraud_alerts.csv"
            fraud_alerts.select(
                "transaction_id", "customer_id", "account_type", "country",
                "amount", "currency", "category", "timestamp", "status",
                "alert_priority", "risk_score", "fraud_reasons"
            ).coalesce(1).write.mode("overwrite") \
             .option("header", "true") \
             .csv(str(csv_path))
            
            logging.info(f"💾 Alertas también guardadas en CSV: {csv_path}")
        else:
            logging.info("\n✅ No se detectaron fraudes en el dataset")
        
        logging.info("\n" + "=" * 70)
        logging.info("ESTADÍSTICAS FINALES")
        logging.info("=" * 70)
        logging.info(f"Total transacciones analizadas: {total_count}")
        logging.info(f"Alertas generadas: {alerts_count}")
        logging.info(f"Tasa de detección: {(alerts_count/total_count*100):.2f}%")
        logging.info("=" * 70)
        
        logging.info("✅ FASE 3 COMPLETADA EXITOSAMENTE")
        
        return 0
        
    except Exception as e:
        logging.error(f"❌ Error en detección de fraude: {str(e)}", exc_info=True)
        return 1
        
    finally:
        if spark:
            stop_spark_session(spark)

if __name__ == "__main__":
    sys.exit(main())